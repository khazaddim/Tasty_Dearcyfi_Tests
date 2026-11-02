"""Collect today's SPY validation pilot into durable DuckDB transactions.

Run with the standard-Python collector environment, not the free-threaded demo
environment. See README.md for permissions, quality flags, exports and recovery.
No orders are placed. --plan and --export do not require credentials.

Reading guide:
    main() chooses offline export, a read-only plan, or today's live collection.
    run_collection() owns the database and records the run's final status.
    SessionCollector coordinates subscriptions, sampling, retries and flushing.
    collector_core supplies configuration, the exchange-calendar window, market
    data models, the raw streamer adapter and in-memory CaptureState.
    collector_store supplies transactional persistence and offline exports.

Two kinds of data are retained: every decoded market-data update received by
the adapter, and scheduled snapshots used for later quality checks. Receiving
an event does not imply it is fresh, complete or suitable for model validation.
Missing periods remain explicit; reconnecting cannot recreate a live feed gap.

The database path is reused by default. Each collection adds a distinct run_id
and retains previous runs rather than replacing their data. Same-day option
selections can be restored from that database when the script is restarted.
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
    from Tastyware_Demo_Scripts.collector_store import CollectorStore, StorageLimitReached, database_bytes, export_database
    from Tastyware_Demo_Scripts.demo_shared import (
        DemoHelperError, classify_failure, load_runtime_config,
    )
except ModuleNotFoundError:
    from collector_core import (
        CaptureState, CollectorConfig, Instrument, RawMarketStreamer, SessionWindow,
        current_session, json_text, quote_quality, select_universe, utc_now,
    )
    from collector_store import CollectorStore, StorageLimitReached, database_bytes, export_database
    from demo_shared import DemoHelperError, classify_failure, load_runtime_config

T = TypeVar("T")


def log(message: str) -> None:
    """Print a UTC-stamped progress message immediately, without buffering.

    Callers must supply safe summaries, not credentials or API response bodies.
    """
    print(f"{utc_now().isoformat()} {message}", flush=True)


def leaves(error: BaseException) -> list[BaseException]:
    """Flatten nested task exception groups into their original leaf errors.

    Async task groups can wrap one failure or several concurrent failures.
    Inspecting the leaves lets recovery distinguish retryable network failures
    from storage-limit failures and unexpected errors that must propagate.
    """
    if isinstance(error, BaseExceptionGroup):
        return [leaf for child in error.exceptions for leaf in leaves(child)]
    return [error]


class SessionCollector:
    """Coordinate one run's market-data capture within a fixed session window.

    The caller supplies config, window and an already-started CollectorStore;
    this object uses the store but does not close it. It creates CaptureState
    for incoming observations and keeps a cumulative option universe keyed by
    streamer symbol. SPY is subscribed separately from that option dictionary.

    collect() runs connection management and periodic persistence concurrently
    on one asyncio event loop. The streamer's state.receive callback buffers
    observations; flush() persists those buffers, while record_due_samples()
    asks the store to build scheduled, quality-flagged snapshots.

    Reconnecting replaces the authenticated session and streamer, not the
    collector or store. The retained universe and sample schedule survive;
    connection-local quote caches are cleared to avoid using stale feed state.
    """

    def __init__(self, config: CollectorConfig, window: SessionWindow, store: CollectorStore):
        """Retain collaborators and initialize timers without opening a feed.

        The first sample is one sample interval after the session open, even
        when starting late. Earlier due cycles can therefore be recorded as
        missed rather than silently moving the schedule to the current time.
        """
        self.config, self.window, self.store = config, window, store
        self.state = CaptureState(config)
        self.instruments: dict[str, Instrument] = {}
        self.next_sample = window.opens + timedelta(seconds=config.sample_seconds)
        self.next_universe = window.opens
        self.next_candles = window.opens
        self.closing_requested = False
        self.stage = "initializing"
        self.reported_counts: dict[str, int] = {}
        self.persisted_updates = 0

    def verbose_log(self, message: str) -> None:
        """Emit an optional safe progress message when verbose mode is enabled."""
        if self.config.verbose:
            log(f"[verbose] {message}")

    def report_feed(self, now: datetime) -> None:
        """Describe feed activity without equating receipt with usable data.

        Report cumulative counts, increments since the previous report, receipt
        age, cached quote timestamp availability and persistence progress.
        ``now`` is the UTC wall time used to calculate the last-event age.
        Verbose mode is required; this method does not write database records.
        """
        if not self.config.verbose:
            return
        kinds = ("Quote", "Trade", "Greeks", "Candle")
        counts = {kind: self.state.counts[kind] for kind in kinds}
        added = {kind: counts[kind] - self.reported_counts.get(kind, 0) for kind in kinds}
        age = (
            f"{(now - self.state.last_received).total_seconds():.1f}s"
            if self.state.last_received is not None else "never"
        )
        quotes = [observation for (kind, _), observation in self.state.latest.items() if kind == "Quote"]
        timestamped = sum(quote.timestamp is not None for quote in quotes)
        self.verbose_log(
            f"Feed received_total={counts}; new_since_report={added}; last_event_age={age}; "
            f"cached_quotes={len(quotes)}; source_timestamp_known={timestamped}/{len(quotes)}. "
            f"raw_updates_committed={self.persisted_updates}; buffered_updates={len(self.state.updates)}. "
            "Receipt counts are not persisted-row counts or validation eligibility."
        )
        self.reported_counts = counts

    def flush(self) -> None:
        """Commit pending observations and diagnostics through CollectorStore.

        The store clears buffers after its transaction succeeds. Only then is
        the collector's persisted-update counter advanced. Storage errors
        propagate to the run lifecycle handler rather than being hidden.
        """
        updates, diagnostics = len(self.state.updates), len(self.state.diagnostics)
        candles = sum(update.observation.kind == "Candle" for update in self.state.updates)
        self.store.flush(self.state)
        self.persisted_updates += updates
        if self.config.verbose and (updates or diagnostics):
            self.verbose_log(
                f"Database buffers committed: candle_revisions={candles}; diagnostics={diagnostics}; "
                f"raw_updates={updates}; raw_updates_committed={self.persisted_updates}; "
                f"database_plus_wal_bytes={database_bytes(self.config.database)}."
            )

    async def request(
        self, operation: str, symbols: list[str], action: Callable[[], Awaitable[T]],
    ) -> T:
        """Run and audit one bounded REST call or subscription operation.

        ``action`` is a zero-argument async callable; its result is returned
        unchanged. The timeout is the smaller of the configured request budget
        and time remaining until the session's reconciliation deadline.

        Record operation/symbol metadata and a safe outcome in the database.
        Ordinary exceptions are classified and re-raised for recovery.
        A successful subscription send does not prove that data was delivered
        for every requested instrument.
        """
        self.stage = operation
        self.verbose_log(f"Request starting: {operation}; symbols={len(symbols)}.")
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
        self.verbose_log(
            f"Request returned: {operation}; elapsed_seconds={(utc_now() - start).total_seconds():.3f}. "
            "Subscription send does not confirm data receipt."
        )
        return result

    async def subscribe(self, streamer: RawMarketStreamer, symbols: list[str]) -> None:
        """Subscribe symbol batches to live events and one-minute candles.

        Request Quote and Trade for all symbols, and Greeks for options only.
        Candles are requested from the session open, so a later subscription
        may deliver earlier bars; receipt time still reflects actual arrival.
        Batches contain at most 25 symbols and are briefly paced. Each send
        passes through request() for timeout handling and audit records.
        """
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
        """Select additional options from a fresh SPY quote and the REST chain.

        Wait up to the request budget for a positive, usable midpoint. Unknown
        source timestamps may be tolerated here only when receipt age is fresh;
        that exception is diagnosed and does not relax stored sample quality.
        select_universe() applies configured expiry and moneyness rules.

        Previously retained options are never removed. If all proposed new
        contracts would exceed the cumulative cap, reject that expansion as a
        whole rather than silently truncating it. Persist selection details,
        subscribe additions, and schedule the next refresh after completion.
        """
        self.stage = "waiting_for_spy_quote"
        self.verbose_log("Waiting for a usable SPY quote to select options.")
        deadline = monotonic() + self.config.request_timeout
        while True:
            now = utc_now()
            spot = self.state.latest.get(("Quote", "SPY"))
            mid, _, flags = quote_quality(spot, now, self.config.quote_age)
            unknown_times = {"bid_time.unknown", "ask_time.unknown"}
            receipt_age = (now - spot.received_at).total_seconds() if spot else math.inf
            if (
                spot is not None and mid is not None and mid > 0 and not (set(flags) - unknown_times)
                and 0 <= receipt_age <= self.config.quote_age
            ):
                if flags:
                    self.state.diagnostic("universe_source_time_unknown", "SPY", {
                        "source_time_flags": flags, "received_at": spot.received_at,
                        "receipt_age_seconds": receipt_age,
                        "use": "universe selection only; strict sample quality is unchanged",
                    })
                    log("SPY source timestamps unavailable; using receipt-fresh quote for universe selection only.")
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
        """Re-request recent candles to capture revisions and late publication.

        Unsubscribe and resubscribe SPY plus retained options over a rolling
        overlap, bounded below by the session open and anchored no later than
        its close. ``closing`` changes the audit label for final reconciliation.
        Returned bars are observations, not proof of provider finality.
        """
        symbols = ["SPY", *self.instruments]
        start = max(
            self.window.opens,
            min(utc_now(), self.window.closes) - timedelta(minutes=self.config.overlap_minutes),
        )
        for offset in range(0, len(symbols), 25):
            batch = symbols[offset:offset + 25]

            async def resubscribe() -> None:
                """Replace this batch's candle subscriptions at the overlap start."""
                for symbol in batch:
                    await streamer.unsubscribe_candle(symbol, "1m")
                await streamer.subscribe_candle(batch, "1m", start, refresh_interval=1)

            await self.request("closing_candle_overlap" if closing else "candle_overlap", batch, resubscribe)
            await asyncio.sleep(0.1)
        self.next_candles = utc_now() + timedelta(seconds=self.config.candle_refresh_seconds)

    def record_due_samples(self, now: datetime) -> None:
        """Record every due cycle up to ``now``, never beyond the session close.

        A cycle at least one full sample interval late is recorded as missed;
        the store does not manufacture historical option snapshots for it.
        Other cycles use current cached observations, with lateness and
        freshness evaluated by the store. Advance the fixed schedule for each
        cycle and buffer a cumulative feed-count diagnostic.
        """
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
        """Restore today's previously retained options from this same database.

        Choose the newest persisted metadata for each streamer symbol across
        runs for the session date, rebuild Instrument objects, and record the
        restored selection under the current run. An oversized prior universe
        raises ValueError instead of dropping contracts. This restores metadata,
        not live quote caches or an earlier process's in-memory state.
        """
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
        """Manage one established connection until bounded reconciliation ends.

        Clear connection-local caches, subscribe SPY and retained options, and
        refresh the universe while regular trading remains. The monitor loop
        records due samples, detects inactive channels or an idle feed,
        diagnoses clock jumps, refreshes candles/universe, and checkpoints.
        Monotonic time drives elapsed-time checks; UTC anchors market schedules.

        Sampling stops at the close. After the publication grace, request one
        final candle overlap and continue receiving until the reconciliation
        deadline. Connection failures propagate to collect_connections() for
        retry; a normal exit flushes pending updates.
        """
        self.state.reset_connection()
        await self.subscribe(streamer, ["SPY", *self.instruments])
        if utc_now() < self.window.closes:
            await self.refresh_universe(session, streamer)
        self.next_candles = utc_now() + timedelta(seconds=self.config.candle_refresh_seconds)
        checkpoint_at = monotonic() + 60
        report_at = monotonic()
        previous_wall, previous_mono = utc_now(), monotonic()
        deadline = self.window.closes + timedelta(
            seconds=self.config.candle_grace + self.config.reconciliation_seconds,
        )
        self.closing_requested = False
        while utc_now() < deadline:
            self.stage = "monitoring_market_data"
            now, tick = utc_now(), monotonic()
            if not streamer.channels_active(include_greeks=bool(self.instruments)):
                raise TimeoutError("A required DXLink channel closed; resubscription is required.")
            jump = (now - previous_wall).total_seconds() - (tick - previous_mono)
            if abs(jump) > self.config.max_skew:
                self.state.diagnostic("local_clock_jump", "", {"seconds": jump})
            previous_wall, previous_mono = now, tick
            self.record_due_samples(now)
            if self.config.verbose and tick >= report_at:
                self.report_feed(now)
                report_at = tick + 5
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
        self.flush()

    async def collect(self) -> None:
        """Run collection and periodic flushing together, then flush on exit.

        TaskGroup ties their lifetimes together: a writer failure cancels
        connection work, and completion of connection work cancels the writer.
        A single grouped failure is unwrapped for the caller; multiple failures
        stay grouped. The final flush is attempted on success, error or
        cancellation, and persistence failures are allowed to propagate.
        """
        try:
            async with asyncio.TaskGroup() as group:
                writer = group.create_task(self.persist_updates())
                try:
                    await self.collect_connections()
                finally:
                    writer.cancel()
        except BaseExceptionGroup as exc:
            errors = leaves(exc)
            if len(errors) == 1:
                raise errors[0] from None
            raise
        finally:
            self.flush()

    async def persist_updates(self) -> None:
        """Flush buffers every configured interval until cancelled or failed.

        This coroutine shares the event loop with reception and collection;
        it is not a separate database thread. collect() owns its lifetime.
        """
        while True:
            await asyncio.sleep(self.config.flush_seconds)
            self.flush()

    async def collect_connections(self) -> None:
        """Open authenticated feed connections and recover within fixed limits.

        Persist SPY metadata, restore today's option universe and load runtime
        credentials. Each attempt creates a Session and RawMarketStreamer,
        wiring observations directly into state.receive. Context entry/exit
        stays in this task to respect the SDK's AnyIO cancellation scopes.

        Connection setup and teardown are time-bounded; connected() controls
        the active session duration. Retry only when all leaf errors belong to
        the supported retryable categories. Before retrying, diagnose the gap,
        flush received data, clear connection-local caches and back off.
        Storage limits and unexpected errors propagate without retry.

        The retry budget is cumulative for this call, and no retry may roll
        collection into a new trading day. Exhaustion or deadline expiry raises
        DemoHelperError; already committed observations remain in the database.
        """
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
                self.stage = "session_authentication"
                self.verbose_log("Opening authenticated session.")
                async with Session(
                    provider_secret=runtime.client_secret, refresh_token=runtime.refresh_token,
                ) as session:
                    self.stage = "streamer_connection"
                    self.verbose_log("Connecting DXLink market-data streamer.")
                    # Keep entry/exit in the same task as the SDK's AnyIO scopes.
                    with fail_after(min(
                        self.config.request_timeout, max(0, (deadline - utc_now()).total_seconds()),
                    )) as scope:
                        async with RawMarketStreamer(session, self.state.receive) as streamer:
                            self.verbose_log("DXLink connected; starting subscriptions.")
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
                    "stage": self.stage,
                    "received_counts": dict(self.state.counts),
                    "last_received_at": self.state.last_received,
                })
                self.flush()
                self.state.reset_connection()
                if failures > self.config.reconnect_attempts:
                    raise DemoHelperError("Reconnect budget exhausted; partial data retained.") from None
                error_types = ",".join(type(error).__name__ for error in errors)
                log(f"Collection failed at {self.stage} ({error_types}); "
                    f"retry {failures}/{self.config.reconnect_attempts}.")
                await asyncio.sleep(min(2 ** failures, 30, max(0, (deadline - utc_now()).total_seconds())))
        raise DemoHelperError("Session/reconciliation deadline reached during recovery; partial data retained.")


async def run_collection(config: CollectorConfig, window: SessionWindow) -> int:
    """Own persistence and final run status for one scheduled collection.

    Open CollectorStore at config.database and start a uniquely identified run
    without discarding earlier runs. Record the Git revision when available.
    Before the open, wait in bounded sleeps while checking storage usage; if
    the session is missed while waiting, finish as partial without connecting.

    After collection, derive completed/partial status from recorded coverage.
    Return 0 for completed or 2 for partial coverage/storage limits. Cancellation
    marks a started run interrupted; other exceptions mark it failed and
    propagate to main(). The store is closed in all paths after it is opened.
    """
    store = CollectorStore(config)
    started = False
    try:
        revision = subprocess.run(
            ["git", "--no-pager", "rev-parse", "HEAD"], capture_output=True, text=True, timeout=5,
            cwd=Path(__file__).resolve().parents[1], check=False,
        )
        store.start(window, revision.stdout.strip() if revision.returncode == 0 else "unknown")
        started = True
        log(f"Run {store.run_id}; database={config.database}; session={window.opens} to {window.closes}; "
            f"recording=all received updates + minute samples; limit_bytes={config.max_bytes}.")
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
    """Parse CLI options and return both mode flags and validated configuration.

    ``argv=None`` reads process arguments; a supplied list supports tests and
    programmatic invocation. Convert decimal gigabytes to bytes, comma-separated
    selections to tuples, and the database path to an absolute path.

    Mode flags such as --plan and --export remain in the Namespace; collection
    settings populate CollectorConfig. Invalid input is reported through
    argparse with SystemExit. Parsing does not authenticate or open a database.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    defaults = CollectorConfig()
    parser.add_argument("--database", type=Path, default=defaults.database)
    parser.add_argument("--max-gb", type=float, default=defaults.max_bytes / 1_000_000_000, help="Decimal GB; database + WAL stop trigger, not a filesystem quota.")
    parser.add_argument("--plan", action="store_true", help="Print today's schedule/configuration without credentials or writes.")
    parser.add_argument("--verbose", action="store_true", help="Print safe request progress, five-second feed heartbeats and buffer commit confirmations; no SDK debug payloads.")
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
            verbose=args.verbose,
            **{name: getattr(args, name) for name in numeric},
        )
        config.validate()
    except ValueError as exc:
        parser.error(str(exc))
    return args, config


def main(argv: list[str] | None = None) -> int:
    """Dispatch offline modes or today's collection and return a CLI exit code.

    Export takes precedence over planning and does not authenticate. Planning
    prints today's exchange-calendar window and configuration without writes.
    Normal collection exits without authentication on non-session days or
    after the close; there is no automatic rollover to tomorrow.

    Suppress SDK debug logging because authentication frames can contain
    secrets. Report runtime failures using safe exception type names, not API
    bodies. Return 0 for success/no remaining session, 1 for a runtime failure
    or the permission-check branch, 2 for partial/storage-limited collection,
    and 130 for Ctrl+C. Argument parsing may raise SystemExit separately.
    """
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
                "database": config.database, "max_bytes_database_pluqs_wal": config.max_bytes,
                "rule": "today only; holidays/after-close exit; no automatic rollover",
            }))
            return 0
        if window is None or now >= window.closes:
            log("No remaining regular session today; exiting without authentication.")
            return 0
        if  args.acknowledge_data_permissions:  # i changed this so it was not needed... grrr
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
