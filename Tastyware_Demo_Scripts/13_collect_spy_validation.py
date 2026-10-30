"""Collect today's SPY validation pilot into durable DuckDB transactions.

Run with the standard-Python collector environment, not the free-threaded demo
environment. See README.md for permissions, quality flags, exports and recovery.
No orders are placed. --plan and --export do not require credentials.
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
import json
import logging
import math
from pathlib import Path
import re
import subprocess
from time import monotonic
from typing import TypeVar

from anyio import current_time, fail_after
from httpx2.websockets import HTTPXWSException
from tastytrade.dxfeed import Greeks, Quote, Trade
from tastytrade.instruments import get_option_chain
from tastytrade.session import Session
from tastytrade.utils import TastytradeError

try:
    from Tastyware_Demo_Scripts.collector_core import (
        CaptureState, CollectorConfig, Instrument, RawMarketStreamer, SessionWindow,
        current_session, json_text, quote_quality, select_universe, utc_now,
    )
    from Tastyware_Demo_Scripts.collector_store import CollectorStore, StorageLimitReached, export_database
    from Tastyware_Demo_Scripts.demo_shared import (
        DemoHelperError, classify_failure, load_runtime_config,
    )
except ModuleNotFoundError:
    from collector_core import (
        CaptureState, CollectorConfig, Instrument, RawMarketStreamer, SessionWindow,
        current_session, json_text, quote_quality, select_universe, utc_now,
    )
    from collector_store import CollectorStore, StorageLimitReached, export_database
    from demo_shared import DemoHelperError, classify_failure, load_runtime_config

T = TypeVar("T")


def log(message: str) -> None:
    print(f"{utc_now().isoformat()} {message}", flush=True)


def leaves(error: BaseException) -> list[BaseException]:
    if isinstance(error, BaseExceptionGroup):
        return [leaf for child in error.exceptions for leaf in leaves(child)]
    return [error]


class SessionCollector:
    def __init__(self, config: CollectorConfig, window: SessionWindow, store: CollectorStore):
        self.config, self.window, self.store = config, window, store
        self.state = CaptureState(config)
        self.instruments: dict[str, Instrument] = {}
        self.next_sample = window.opens + timedelta(seconds=config.sample_seconds)
        self.next_universe = window.opens
        self.next_candles = window.opens
        self.closing_requested = False

    async def request(
        self, operation: str, symbols: list[str], action: Callable[[], Awaitable[T]],
    ) -> T:
        start = utc_now()
        try:
            remaining = (
                self.window.closes + timedelta(
                    seconds=self.config.candle_grace + self.config.reconciliation_seconds,
                ) - start
            ).total_seconds()
            if remaining <= 0:
                raise TimeoutError("Session reconciliation deadline has elapsed.")
            async with asyncio.timeout(min(self.config.request_timeout, remaining)):
                result = await action()
        except Exception as exc:
            classified = classify_failure(exc, operation)
            self.store.request(operation, start, symbols, "failed", {
                "category": type(classified).__name__, "error_type": type(exc).__name__,
                "rate_limited": bool(re.search(r"\b429\b", str(exc))),
            })
            raise classified from None
        self.store.request(operation, start, symbols, "sent_or_returned", {
            "note": "Subscription send is not proof every instrument received data.",
        })
        return result

    async def subscribe(self, streamer: RawMarketStreamer, symbols: list[str]) -> None:
        for offset in range(0, len(symbols), 25):
            batch = symbols[offset:offset + 25]
            for event_class in (Quote, Trade, Greeks):
                selected = [symbol for symbol in batch if symbol != "SPY" or event_class is not Greeks]
                if selected:
                    await self.request(
                        f"subscribe_{event_class.__name__}", selected,
                        lambda: streamer.subscribe(event_class, selected),
                    )
            await self.request(
                "subscribe_candles_from_open", batch,
                lambda: streamer.subscribe_candle(batch, "1m", self.window.opens, refresh_interval=1),
            )
            await asyncio.sleep(0.1)

    async def refresh_universe(self, session: Session, streamer: RawMarketStreamer) -> None:
        deadline = monotonic() + self.config.request_timeout
        while True:
            now = utc_now()
            spot = self.state.latest.get(("Quote", "SPY"))
            mid, _, flags = quote_quality(spot, now, self.config.quote_age)
            if mid is not None and mid > 0 and not flags:
                break
            if monotonic() >= deadline:
                raise TimeoutError("No fresh two-sided SPY quote for universe selection.")
            await asyncio.sleep(0.1)
        chain = await self.request("option_chain", ["SPY"], lambda: get_option_chain(session, "SPY"))
        selected, details = select_universe(chain, mid, utc_now(), self.config)
        new = [instrument for instrument in selected if instrument.streamer_symbol not in self.instruments]
        room = self.config.max_contracts - len(self.instruments)
        if len(new) > room:
            self.state.diagnostic("universe_cumulative_cap", "SPY", {
                "retained": len(self.instruments), "proposed_new": len(new),
                "not_added": [instrument.streamer_symbol for instrument in new],
                "rule": "retain all previously selected contracts; do not silently truncate new selection",
            })
            new = []
            details["expansion_status"] = "rejected_by_cumulative_cap"
        self.instruments.update((instrument.streamer_symbol, instrument) for instrument in new)
        details["retained_symbols"] = sorted(self.instruments)
        details["refresh_policy"] = "retain prior selections for the session, including quotes and candles"
        self.store.universe(list(self.instruments.values()), details, utc_now())
        if new:
            await self.subscribe(streamer, [instrument.streamer_symbol for instrument in new])
        self.next_universe = utc_now() + timedelta(seconds=self.config.refresh_seconds)
        log(f"Universe: {len(self.instruments)} options retained.")

    async def candle_overlap(self, streamer: RawMarketStreamer, closing: bool = False) -> None:
        symbols = ["SPY", *self.instruments]
        start = max(
            self.window.opens,
            min(utc_now(), self.window.closes) - timedelta(minutes=self.config.overlap_minutes),
        )
        for offset in range(0, len(symbols), 25):
            batch = symbols[offset:offset + 25]

            async def resubscribe() -> None:
                for symbol in batch:
                    await streamer.unsubscribe_candle(symbol, "1m")
                await streamer.subscribe_candle(batch, "1m", start, refresh_interval=1)

            await self.request("closing_candle_overlap" if closing else "candle_overlap", batch, resubscribe)
            await asyncio.sleep(0.1)
        self.next_candles = utc_now() + timedelta(seconds=self.config.candle_refresh_seconds)

    def record_due_samples(self, now: datetime) -> None:
        while self.next_sample <= min(now, self.window.closes):
            missed = (now - self.next_sample).total_seconds() >= self.config.sample_seconds
            self.store.sample(
                self.state, list(self.instruments.values()), self.next_sample, now, missed=missed,
            )
            self.next_sample += timedelta(seconds=self.config.sample_seconds)
            self.state.diagnostic("feed_counts", "", {"cumulative_counts": dict(self.state.counts)})
            log(f"Minute {'missed' if missed else 'committed'}; options={len(self.instruments)}; "
                f"received={dict(self.state.counts)}")

    def restore_universe(self) -> None:
        rows = self.store.connection.execute("""
            SELECT i.symbol, i.streamer_symbol, i.option_type, i.strike, i.expiration,
                   i.expires_at, i.stops_trading_at, i.exercise_style, i.settlement_type,
                   i.shares_per_contract, raw.payload
            FROM instruments i JOIN runs r USING (run_id)
            JOIN raw_records raw ON raw.record_id=i.raw_record_id
            WHERE r.session_date=? AND i.option_type IS NOT NULL
            QUALIFY row_number() OVER (
                PARTITION BY i.streamer_symbol ORDER BY i.received_at_utc DESC, i.record_id DESC
            )=1
        """, [self.window.day]).fetchall()
        if len(rows) > self.config.max_contracts:
            raise ValueError("Today's retained universe exceeds max_contracts; use the previous cap or a new database.")
        for row in rows:
            instrument = Instrument(*row[:-1], reference=json.loads(row[-1]))
            self.instruments[instrument.streamer_symbol] = instrument
        if rows:
            self.store.universe(list(self.instruments.values()), {
                "rule": "restored earlier selections from this session", "count": len(rows),
            }, utc_now())

    async def connected(self, session: Session, streamer: RawMarketStreamer) -> None:
        self.state.reset_connection()
        await self.subscribe(streamer, ["SPY", *self.instruments])
        if utc_now() < self.window.closes:
            await self.refresh_universe(session, streamer)
        self.next_candles = utc_now() + timedelta(seconds=self.config.candle_refresh_seconds)
        flush_at = monotonic() + self.config.flush_seconds
        checkpoint_at = monotonic() + 60
        previous_wall, previous_mono = utc_now(), monotonic()
        deadline = self.window.closes + timedelta(
            seconds=self.config.candle_grace + self.config.reconciliation_seconds,
        )
        self.closing_requested = False
        while utc_now() < deadline:
            now, tick = utc_now(), monotonic()
            if not streamer.channels_active(include_greeks=bool(self.instruments)):
                raise TimeoutError("A required DXLink channel closed; resubscription is required.")
            jump = (now - previous_wall).total_seconds() - (tick - previous_mono)
            if abs(jump) > self.config.max_skew:
                self.state.diagnostic("local_clock_jump", "", {"seconds": jump})
            previous_wall, previous_mono = now, tick
            self.record_due_samples(now)
            if tick >= flush_at:
                self.store.flush(self.state)
                flush_at = tick + self.config.flush_seconds
            if tick >= checkpoint_at:
                self.store.checkpoint()
                checkpoint_at = tick + 60
            if now < self.window.closes:
                if self.state.last_received is None or (
                    now - self.state.last_received
                ).total_seconds() > self.config.transport_idle:
                    raise TimeoutError("No market-data events within the idle budget.")
                if now >= self.next_universe:
                    await self.refresh_universe(session, streamer)
                if now >= self.next_candles:
                    await self.candle_overlap(streamer)
            elif now >= self.window.closes + timedelta(seconds=self.config.candle_grace):
                if not self.closing_requested:
                    await self.candle_overlap(streamer, closing=True)
                    self.closing_requested = True
            await asyncio.sleep(0.25)
        self.store.flush(self.state)

    async def collect(self) -> None:
        self.store.universe([Instrument("SPY", "SPY", reference={
            "symbol": "SPY", "source": "configured underlying",
            "currency_adjustments": "unverified",
        })], {"rule": "configured underlying"}, utc_now())
        self.restore_universe()
        runtime = load_runtime_config()
        failures = 0
        deadline = self.window.closes + timedelta(
            seconds=self.config.candle_grace + self.config.reconciliation_seconds,
        )
        while utc_now() < deadline:
            try:
                async with Session(
                    provider_secret=runtime.client_secret, refresh_token=runtime.refresh_token,
                ) as session:
                    # Keep entry/exit in the same task as the SDK's AnyIO scopes.
                    with fail_after(min(
                        self.config.request_timeout, max(0, (deadline - utc_now()).total_seconds()),
                    )) as scope:
                        async with RawMarketStreamer(session, self.state.receive) as streamer:
                            scope.deadline = math.inf
                            try:
                                await self.connected(session, streamer)
                            finally:
                                scope.deadline = current_time() + self.config.request_timeout
                return
            except Exception as exc:
                errors = leaves(exc)
                if any(isinstance(error, StorageLimitReached) for error in errors):
                    raise StorageLimitReached("Storage threshold reached.") from None
                retryable = (DemoHelperError, TimeoutError, OSError, HTTPXWSException, TastytradeError)
                if not all(isinstance(error, retryable) for error in errors):
                    raise
                failures += 1
                self.state.diagnostic("connection_gap", "", {
                    "attempt": failures, "error_types": [type(error).__name__ for error in errors],
                    "received_counts": dict(self.state.counts),
                    "last_received_at": self.state.last_received,
                })
                self.store.flush(self.state)
                self.state.reset_connection()
                if failures > self.config.reconnect_attempts:
                    raise DemoHelperError("Reconnect budget exhausted; partial data retained.") from None
                log(f"Connection failed; retry {failures}/{self.config.reconnect_attempts}.")
                await asyncio.sleep(min(2 ** failures, 30, max(0, (deadline - utc_now()).total_seconds())))
        raise DemoHelperError("Session/reconciliation deadline reached during recovery; partial data retained.")


async def run_collection(config: CollectorConfig, window: SessionWindow) -> int:
    store = CollectorStore(config)
    started = False
    try:
        revision = subprocess.run(
            ["git", "--no-pager", "rev-parse", "HEAD"], capture_output=True, text=True, timeout=5,
            cwd=Path(__file__).resolve().parents[1], check=False,
        )
        store.start(window, revision.stdout.strip() if revision.returncode == 0 else "unknown")
        started = True
        log(f"Run {store.run_id}; database={config.database}; session={window.opens} to {window.closes}.")
        if utc_now() < window.opens:
            log("Waiting for today's regular session open.")
        while utc_now() < window.opens:
            store.check_size()
            await asyncio.sleep(min(30, max(0, (window.opens - utc_now()).total_seconds())))
        if utc_now() >= window.closes:
            store.finish("partial", "session_missed_while_waiting")
            return 2
        store.connection.execute("UPDATE runs SET status='running' WHERE run_id=?", [store.run_id])
        await SessionCollector(config, window, store).collect()
        summary = store.summary()
        partial = (
            summary["missed_cycles"] > 0 or summary["unrecorded_cycles"] > 0
            or summary["distinct_contracts"] == 0
            or summary["missing_quote_samples"] > 0 or summary["missing_analytics_samples"] > 0
            or summary["unobserved_candle_intervals"] > 0
        )
        store.finish("partial" if partial else "completed", "session_closed")
        print(json_text(store.summary()), flush=True)
        return 2 if partial else 0
    except StorageLimitReached:
        if started:
            store.finish("partial", "storage_limit")
        log("Storage threshold reached; committed data retained. No further collection.")
        return 2
    except asyncio.CancelledError:
        if started:
            store.finish("interrupted", "cancelled")
        raise
    except Exception as exc:
        if started:
            store.finish("failed", type(exc).__name__)
        raise
    finally:
        store.close()


def parse_arguments(argv: list[str] | None = None) -> tuple[argparse.Namespace, CollectorConfig]:
    parser = argparse.ArgumentParser(description=__doc__)
    defaults = CollectorConfig()
    parser.add_argument("--database", type=Path, default=defaults.database)
    parser.add_argument("--max-gb", type=float, default=10, help="Decimal GB; database + WAL stop trigger, not a filesystem quota.")
    parser.add_argument("--plan", action="store_true", help="Print today's schedule/configuration without credentials or writes.")
    parser.add_argument("--export", type=Path, metavar="NEW_DIRECTORY", help="Offline Parquet export; writer must be stopped.")
    parser.add_argument("--acknowledge-data-permissions", action="store_true", help="Confirm permission to retain and transfer this market data.")
    parser.add_argument("--delay-status", choices=("unknown", "realtime", "delayed"), default="unknown")
    parser.add_argument("--clock-offset-ms", type=float, help="Measured local clock offset; omitted means unknown, not zero.")
    parser.add_argument("--expiry-days", default="0,7,14,17,28,56", help="Nearest listed calendar-day expiry targets.")
    parser.add_argument("--put-moneyness", default="0.95,0.975,1", help="Put strike/spot anchors; select adjacent strikes below each.")
    numeric = (
        "sample_seconds", "quote_age", "analytics_age", "max_skew", "max_lateness",
        "candle_grace", "max_contracts", "puts_per_expiry", "calls_per_expiry",
        "refresh_seconds", "flush_seconds", "overlap_minutes", "candle_refresh_seconds",
        "reconciliation_seconds", "request_timeout", "reconnect_attempts", "transport_idle",
    )
    for name in numeric:
        default = getattr(defaults, name)
        integer_fields = {
            "sample_seconds", "max_contracts", "puts_per_expiry", "calls_per_expiry",
            "refresh_seconds", "overlap_minutes", "candle_refresh_seconds", "reconnect_attempts",
        }
        parser.add_argument(
            "--" + name.replace("_", "-"), type=int if name in integer_fields else float, default=default,
        )
    args = parser.parse_args(argv)
    if not math.isfinite(args.max_gb) or args.max_gb <= 0:
        parser.error("--max-gb must be finite and positive.")
    try:
        config = CollectorConfig(
            database=args.database.resolve(), max_bytes=int(args.max_gb * 1_000_000_000),
            expiry_days=tuple(int(value) for value in args.expiry_days.split(",")),
            put_moneyness=tuple(float(value) for value in args.put_moneyness.split(",")),
            delay_status=args.delay_status, clock_offset_ms=args.clock_offset_ms,
            **{name: getattr(args, name) for name in numeric},
        )
        config.validate()
    except ValueError as exc:
        parser.error(str(exc))
    return args, config


def main(argv: list[str] | None = None) -> int:
    args, config = parse_arguments(argv)
    # SDK debug logging includes authentication frames; never enable it here.
    logging.getLogger("tastytrade").setLevel(logging.CRITICAL)
    try:
        if args.export:
            manifest = export_database(config.database, args.export)
            log(f"Exported {len(manifest['files'])} tables/views to {args.export}.")
            return 0
        now = utc_now()
        window = current_session(now)
        if args.plan:
            print(json_text({
                "session": None if window is None else {
                    "date": window.day, "open": window.opens, "close": window.closes,
                    "already_closed": now >= window.closes,
                },
                "database": config.database, "max_bytes_database_plus_wal": config.max_bytes,
                "rule": "today only; holidays/after-close exit; no automatic rollover",
            }))
            return 0
        if window is None or now >= window.closes:
            log("No remaining regular session today; exiting without authentication.")
            return 0
        if not args.acknowledge_data_permissions:
            log("Collection requires --acknowledge-data-permissions; verify your feed's retention/export rights.")
            return 1
        load_runtime_config()
        return asyncio.run(run_collection(config, window))
    except KeyboardInterrupt:
        log("Interrupted; committed data remains in DuckDB.")
        return 130
    except StorageLimitReached:
        log("Existing database/WAL is already at the configured storage threshold.")
        return 2
    except Exception as exc:
        # Do not print API exception bodies, tokens, or exception-group payloads.
        log(f"Collector failed ({type(exc).__name__}); committed data retained. "
            "Check configuration, database diagnostics, permissions and connectivity.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
