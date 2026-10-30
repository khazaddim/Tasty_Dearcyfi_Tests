"""Credential-free collector tests, including actual DuckDB WAL crash recovery."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
import importlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from time import perf_counter
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from tastytrade.dxfeed import Quote
from tastytrade.instruments import Option, OptionType

from Tastyware_Demo_Scripts.collector_core import (
    CaptureState, CollectorConfig, Instrument, Observation, RawMarketStreamer,
    SessionWindow, analytics_quality, current_session, decode_events,
    quote_quality, select_universe, source_time,
)
from Tastyware_Demo_Scripts.collector_store import (
    CollectorStore, StorageLimitReached, database_bytes, export_database, report,
)

RUNNER = importlib.import_module("Tastyware_Demo_Scripts.13_collect_spy_validation")
NOW = datetime(2026, 10, 29, 14, 0, tzinfo=UTC)
WINDOW = SessionWindow(NOW.date(), NOW - timedelta(minutes=30), NOW + timedelta(hours=6))


def event(kind: str = "Quote", symbol: str = "SPY", at: datetime = NOW, **changes: object) -> Observation:
    timestamp = int(at.timestamp() * 1000)
    if kind == "Quote":
        fields = {
            "event_symbol": symbol, "event_time": 0, "bid_time": timestamp,
            "ask_time": timestamp, "bid_price": "100", "ask_price": "102",
            "bid_size": 10, "ask_size": 20, "sequence": 1, "time_nano_part": 0,
            "bid_exchange_code": "Q", "ask_exchange_code": "Q",
        }
    elif kind == "Greeks":
        fields = {
            "event_symbol": symbol, "time": timestamp, "event_time": 0, "event_flags": 0,
            "price": "2", "volatility": "0.2", "delta": "-0.2", "gamma": "0.01",
            "theta": "-0.02", "vega": "0.1", "rho": "-0.1",
        }
    elif kind == "Candle":
        fields = {
            "event_symbol": symbol, "time": timestamp, "event_time": 0, "event_flags": 0,
            "open": "100", "high": "104", "low": "99", "close": "102",
            "volume": "10", "count": 2, "vwap": "101",
        }
    else:
        fields = {
            "event_symbol": symbol, "time": timestamp, "price": "101", "size": 1,
            "day_volume": "10", "exchange_code": "Q", "extended_trading_hours": False,
        }
    fields.update(changes)
    return Observation(kind, symbol, at, fields, dict(fields))


def option(strike: int, expiry: date, kind: OptionType = OptionType.PUT, **changes: object) -> Option:
    fields = {
        "symbol": f"SPY {expiry:%y%m%d}{kind.value}{strike}", "streamer_symbol": f".SPY{expiry:%y%m%d}{kind.value}{strike}",
        "active": True, "strike_price": Decimal(strike), "root_symbol": "SPY",
        "underlying_symbol": "SPY", "expiration_date": expiry, "exercise_style": "American",
        "shares_per_contract": 100, "option_type": kind, "option_chain_type": "Standard",
        "expiration_type": "Weekly", "settlement_type": "PM",
        "stops_trading_at": datetime.combine(expiry, datetime.min.time(), UTC) + timedelta(hours=20),
        "expires_at": None, "market_time_instrument_collection": "Equity Option",
        "days_to_expiration": (expiry - NOW.date()).days, "is_closing_only": False,
    }
    fields.update(changes)
    return Option.model_construct(**fields)


class CalendarAndSelectionTests(unittest.TestCase):
    def test_today_only_holiday_weekend_and_after_close(self) -> None:
        self.assertIsNone(current_session(datetime(2026, 12, 25, 12, tzinfo=UTC)))
        self.assertIsNone(current_session(datetime(2026, 10, 31, 12, tzinfo=UTC)))
        window = current_session(datetime(2026, 10, 29, 23, tzinfo=UTC))
        self.assertEqual(window.day, date(2026, 10, 29))
        self.assertEqual(window.closes.hour, 20)

    def test_dst_and_early_close(self) -> None:
        summer = current_session(datetime(2026, 10, 29, 12, tzinfo=UTC))
        winter = current_session(datetime(2026, 11, 27, 12, tzinfo=UTC))
        self.assertEqual(summer.opens.hour, 13)
        self.assertEqual(winter.opens.hour, 14)
        self.assertEqual(winter.closes.hour, 18)
        self.assertEqual((winter.closes - winter.opens).total_seconds(), 3.5 * 3600)

    def test_universe_is_moneyness_based_and_bounded(self) -> None:
        expiry = NOW.date() + timedelta(days=17)
        contracts = [option(strike, expiry, kind) for kind in OptionType for strike in range(90, 112)]
        contracts += [option(100, expiry, shares_per_contract=10), option(100, expiry, active=False)]
        selected, details = select_universe({expiry: contracts}, 100.5, NOW, CollectorConfig())
        puts = [instrument.strike for instrument in selected if instrument.option_type == "P"]
        self.assertEqual(puts, list(range(100, 91, -1)))
        self.assertEqual(len(selected), 12)
        self.assertEqual(details["rejected_counts"], {"nonstandard_terms": 1, "inactive_or_expired": 1})
        with self.assertRaisesRegex(ValueError, "above max_contracts"):
            select_universe({expiry: contracts}, 100.5, NOW, replace(CollectorConfig(), max_contracts=2))

    def test_config_rejects_nonfinite_and_nonpositive(self) -> None:
        for config in (
            replace(CollectorConfig(), quote_age=float("nan")),
            replace(CollectorConfig(), max_bytes=0),
            replace(CollectorConfig(), expiry_days=(-1,)),
        ):
            with self.assertRaises(ValueError):
                config.validate()


class CaptureAndQualityTests(unittest.TestCase):
    def test_full_and_compact_raw_preserve_invalid_values(self) -> None:
        source = event(bid_price="NaN", bid_time=0)
        aliases = [field.alias or name for name, field in Quote.model_fields.items()]
        payload = dict(zip(aliases, [source.fields.get(name) for name in Quote.model_fields]))
        full = decode_events("Quote", [payload], NOW)[0]
        compact = decode_events("Quote", ["Quote", list(payload.values())], NOW)[0]
        self.assertEqual(full.payload, compact.payload)
        self.assertEqual(compact.fields["bid_price"], "NaN")
        _, age, flags = quote_quality(compact, NOW, 5)
        self.assertIsNone(age)
        self.assertIn("quote.one_sided_or_nonfinite", flags)
        self.assertIn("bid_time.unknown", flags)
        with self.assertRaises(ValueError):
            decode_events("Quote", ["Quote", ["too_short"]], NOW)

    def test_raw_adapter_does_not_use_sdk_lossy_parser(self) -> None:
        receive = MagicMock()
        streamer = RawMarketStreamer(MagicMock(), receive)
        raw = event(bid_price="NaN").payload
        with patch.object(Quote, "model_validate", side_effect=AssertionError("Must not validate")):
            streamer._map_message([raw], streamer._channels["Quote"])
        self.assertEqual(receive.call_args.args[0].fields["bid_price"], "NaN")
        self.assertFalse(streamer.channels_active(include_greeks=False))
        for kind in ("Quote", "Trade", "Candle", "Greeks"):
            ready = MagicMock()
            ready.is_set.return_value = True
            streamer._subscription_state[kind] = ready
        self.assertTrue(streamer.channels_active(include_greeks=True))
        del streamer._subscription_state["Quote"]
        self.assertFalse(streamer.channels_active(include_greeks=True))

    def test_backward_pair_does_not_choose_future_spot(self) -> None:
        state = CaptureState(CollectorConfig())
        earlier = event(at=NOW - timedelta(milliseconds=200))
        future = event(at=NOW + timedelta(milliseconds=100))
        state.receive(earlier)
        state.receive(future)
        self.assertEqual(state.pair_spot(event(symbol="OPT"), NOW + timedelta(seconds=1)), earlier)
        self.assertIsNone(state.pair_spot(event(symbol="OPT", bid_time=0), NOW))

    def test_age_is_source_age_not_receipt_age(self) -> None:
        old = int((NOW - timedelta(seconds=30)).timestamp() * 1000)
        _, age, flags = quote_quality(event(bid_time=old, ask_time=old), NOW, 5)
        self.assertEqual(age, 30)
        self.assertIn("bid_time.stale", flags)
        self.assertIsNone(source_time(0))

    def test_numeric_age_threshold_boundaries(self) -> None:
        for seconds, stale in ((5, False), (5.001, True)):
            _, _, flags = quote_quality(event(at=NOW - timedelta(seconds=seconds)), NOW, 5)
            self.assertEqual("bid_time.stale" in flags, stale)
        for seconds, stale in ((15, False), (15.001, True)):
            _, flags = analytics_quality(event("Greeks", at=NOW - timedelta(seconds=seconds)), NOW, 15)
            self.assertEqual("analytics.future_or_stale" in flags, stale)

    def test_invalid_zero_crossed_and_locked_quotes_remain(self) -> None:
        for bid, ask, reason in ((0, 1, "bid.zero"), (2, 1, "quote.crossed"), (1, 1, "quote.locked")):
            with self.subTest(reason=reason):
                _, _, flags = quote_quality(event(bid_price=bid, ask_price=ask), NOW, 5)
                self.assertIn(reason, flags)

    def test_stale_analytics_and_pending_are_separate_from_quotes(self) -> None:
        old = int((NOW - timedelta(seconds=20)).timestamp() * 1000)
        age, flags = analytics_quality(event("Greeks", time=old, event_flags=1), NOW, 15)
        self.assertEqual(age, 20)
        self.assertIn("analytics.future_or_stale", flags)
        self.assertIn("analytics.pending_or_removed", flags)
        self.assertEqual(quote_quality(event(), NOW, 5)[2], [])

    def test_out_of_order_and_buffer_overflow_are_visible(self) -> None:
        state = CaptureState(replace(CollectorConfig(), max_pending=3))
        current = event()
        state.receive(current)
        state.receive(event(at=NOW - timedelta(seconds=1)))
        self.assertEqual(state.latest[("Quote", "SPY")], current)
        self.assertEqual(state.diagnostics[0][0], "out_of_order")
        state.receive(event("Candle"))
        with self.assertRaises(BufferError):
            state.receive(event("Candle"))
        self.assertEqual(len(state.updates), 3)
        state.reset_connection()
        self.assertFalse(state.latest)
        self.assertFalse(state.spot_history)


class StoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "capture.duckdb"
        self.config = replace(
            CollectorConfig(), database=self.path, delay_status="realtime", clock_offset_ms=0,
        )
        self.store = CollectorStore(self.config)
        self.addCleanup(self.store.close)
        self.store.start(WINDOW, "test-revision")
        self.instrument = Instrument("OPTION", "OPT", "P", 100, NOW.date() + timedelta(days=17))
        self.store.universe([Instrument("SPY", "SPY"), self.instrument], {"rule": "test"}, NOW)

    def populated_state(self) -> CaptureState:
        state = CaptureState(self.config)
        state.receive(event(at=NOW - timedelta(milliseconds=100)))
        state.receive(event(symbol="OPT", bid_price="1", ask_price="2"))
        state.receive(event("Greeks", "OPT", time=int((NOW - timedelta(seconds=30)).timestamp() * 1000)))
        state.receive(event("Trade"))
        return state

    def test_all_updates_retain_duplicates_invalid_and_out_of_order_receipts(self) -> None:
        state = CaptureState(self.config)
        observations = [
            event(bid_time=0, ask_time=0),
            event(bid_time=0, ask_time=0),
            event(symbol="OPT", bid_price="NaN"),
            event(symbol="OPT", at=NOW - timedelta(seconds=1)),
            event("Trade"), event("Greeks", "OPT"), event("Candle", "SPY{=1m}"),
        ]
        for observation in observations:
            state.receive(observation)
        first_connection = state.connection_id
        state.reset_connection()
        state.receive(event())
        second_connection = state.connection_id
        self.store.flush(state)
        rows = self.store.connection.execute("""
            SELECT receive_sequence, received_monotonic_ns, connection_id, payload,
                   source_time, observation_id
            FROM raw_update_payloads ORDER BY receive_sequence
        """).fetchall()
        self.assertEqual([row[0] for row in rows], list(range(1, 9)))
        self.assertEqual(len(set(row[1] for row in rows)), 8)
        self.assertEqual(rows[0][2], first_connection)
        self.assertEqual(rows[-1][2], second_connection)
        self.assertNotEqual(first_connection, second_connection)
        self.assertEqual(rows[0][3], rows[1][3])
        self.assertIsNone(rows[0][4])
        self.assertEqual(json.loads(rows[2][3])["bid_price"], "NaN")
        self.assertEqual(self.store.connection.execute("SELECT count(*) FROM quotes").fetchone()[0], 5)
        self.assertEqual(self.store.connection.execute("SELECT count(*) FROM samples").fetchone()[0], 0)
        self.assertFalse(state.updates)
        self.store.flush(state)
        self.assertEqual(self.store.connection.execute("SELECT count(*) FROM raw_updates").fetchone()[0], 8)
        summary = self.store.summary()
        self.assertEqual(summary["received_updates"], 8)
        self.assertTrue(summary["all_received_updates_recording_enabled"])

    def test_flush_and_sampling_share_observation_ids_without_losing_updates(self) -> None:
        state = self.populated_state()
        self.store.sample(state, [self.instrument], NOW, NOW)
        self.store.flush(state)
        self.store.sample(state, [self.instrument], NOW + timedelta(minutes=1), NOW + timedelta(minutes=1))
        self.assertEqual(self.store.connection.execute("SELECT count(*) FROM raw_updates").fetchone()[0], 4)
        self.assertEqual(self.store.connection.execute("SELECT count(*) FROM quotes").fetchone()[0], 2)
        self.assertEqual(self.store.connection.execute("SELECT count(*) FROM samples").fetchone()[0], 2)
        self.assertEqual(self.store.connection.execute("""
            SELECT count(*) FROM samples s JOIN raw_updates u ON u.observation_id=s.quote_id
        """).fetchone()[0], 2)

    def test_failed_flush_keeps_pending_updates_for_retry(self) -> None:
        state = self.populated_state()
        with patch.object(self.store, "write_rows", side_effect=RuntimeError("injected")):
            with self.assertRaises(RuntimeError):
                self.store.flush(state)
        self.assertEqual(len(state.updates), 4)
        self.assertEqual(self.store.connection.execute("SELECT count(*) FROM raw_updates").fetchone()[0], 0)
        self.store.flush(state)
        self.assertEqual(self.store.connection.execute("SELECT count(*) FROM raw_updates").fetchone()[0], 4)

    def test_schema_one_upgrade_preserves_prior_samples(self) -> None:
        state = self.populated_state()
        self.store.sample(state, [self.instrument], NOW, NOW)
        old_run = self.store.run_id
        self.store.connection.execute("DROP VIEW raw_update_payloads")
        self.store.connection.execute("DROP VIEW run_coverage")
        self.store.connection.execute("DROP TABLE raw_updates")
        self.store.connection.execute("UPDATE collector_schema SET version=1")
        self.store.connection.execute("UPDATE runs SET schema_version=1, capabilities='{}'")
        self.store.connection.execute("UPDATE quotes SET schema_version=1")
        self.store.close()
        self.store = CollectorStore(self.config)
        self.addCleanup(self.store.close)
        self.store.start(WINDOW, "schema-two")
        self.assertEqual(self.store.connection.execute("SELECT version FROM collector_schema").fetchone()[0], 2)
        self.assertEqual(self.store.connection.execute("SELECT count(*) FROM samples WHERE run_id=?", [old_run]).fetchone()[0], 1)
        self.assertEqual(self.store.connection.execute("SELECT DISTINCT schema_version FROM quotes").fetchall(), [(1,)])
        self.assertFalse(self.store.summary()["received_updates"])
        self.assertFalse(report(self.store.connection, old_run)["all_received_updates_recording_enabled"])
        self.store.flush(state)
        self.assertEqual(self.store.connection.execute("SELECT count(*) FROM raw_updates").fetchone()[0], 4)

    def test_bulk_update_flush_within_five_second_budget(self) -> None:
        state = CaptureState(self.config)
        for index in range(5000):
            kind = ("Quote", "Trade", "Greeks", "Candle")[index % 4]
            state.receive(event(kind, "SPY" if kind != "Greeks" else "OPT", sequence=index))
        start = perf_counter()
        self.store.flush(state)
        elapsed = perf_counter() - start
        self.assertLess(elapsed, 5, f"5000-update commit took {elapsed:.3f}s")
        self.assertEqual(self.store.connection.execute("SELECT count(*) FROM raw_updates").fetchone()[0], 5000)

    def test_sample_retains_raw_links_and_independent_eligibility(self) -> None:
        state = self.populated_state()
        self.store.sample(state, [self.instrument], NOW, NOW)
        row = self.store.connection.execute("""
            SELECT s.quote_eligible, s.analytics_fresh, s.source_skew_seconds, raw.payload
            FROM samples s JOIN quotes q ON q.record_id=s.quote_id
            JOIN raw_records raw ON raw.record_id=q.raw_record_id
        """).fetchone()
        self.assertTrue(row[0])
        self.assertFalse(row[1])
        self.assertAlmostEqual(row[2], 0.1)
        self.assertEqual(json.loads(row[3])["bid_price"], "1")
        self.store.sample(state, [self.instrument], NOW + timedelta(minutes=1), NOW + timedelta(minutes=1))
        self.assertEqual(self.store.connection.execute("SELECT count(*) FROM quotes").fetchone()[0], 2)
        self.assertEqual(self.store.connection.execute("SELECT count(*) FROM samples").fetchone()[0], 2)

    def test_skew_boundary_and_missing_fields_are_retained(self) -> None:
        for skew in (2, 2.001):
            state = CaptureState(self.config)
            state.receive(event(at=NOW - timedelta(seconds=skew)))
            state.receive(event(symbol="OPT", bid_price="1", ask_price="2"))
            self.store.sample(state, [self.instrument], NOW, NOW)
        self.store.sample(CaptureState(self.config), [self.instrument], NOW, NOW)
        rows = self.store.connection.execute(
            "SELECT source_skew_seconds, quote_eligible FROM samples WHERE source_skew_seconds IS NOT NULL ORDER BY source_skew_seconds",
        ).fetchall()
        self.assertEqual(rows, [(2.0, True), (2.001, False)])
        self.assertEqual(self.store.connection.execute(
            "SELECT count(*) FROM samples WHERE quote_id IS NULL AND NOT quote_eligible",
        ).fetchone()[0], 1)

    def test_unknown_clock_and_delay_do_not_claim_readiness(self) -> None:
        self.store.config = replace(self.config, delay_status="unknown", clock_offset_ms=None)
        self.store.sample(self.populated_state(), [self.instrument], NOW, NOW)
        row = self.store.connection.execute("SELECT quote_eligible, quality FROM samples").fetchone()
        self.assertFalse(row[0])
        self.assertIn("clock.offset_unknown", row[1])
        self.assertIn("feed.delay_unverified_or_delayed", row[1])

    def test_runner_restores_same_day_universe(self) -> None:
        collector = RUNNER.SessionCollector(self.config, WINDOW, self.store)
        collector.restore_universe()
        self.assertEqual(collector.instruments["OPT"].symbol, "OPTION")

    def test_transaction_rolls_back_failure(self) -> None:
        before = self.store.connection.execute("SELECT count(*) FROM diagnostics").fetchone()[0]
        with self.assertRaisesRegex(RuntimeError, "injected"):
            with self.store.transaction():
                self.store.diagnostic("should_rollback", {})
                raise RuntimeError("injected")
        self.assertEqual(self.store.connection.execute("SELECT count(*) FROM diagnostics").fetchone()[0], before)

    def test_size_checks_before_and_after_commit(self) -> None:
        with patch("Tastyware_Demo_Scripts.collector_store.database_bytes", return_value=self.config.max_bytes):
            with self.assertRaises(StorageLimitReached):
                with self.store.transaction():
                    self.fail("Must stop before batch")
        with patch("Tastyware_Demo_Scripts.collector_store.database_bytes", side_effect=[0, self.config.max_bytes]):
            with self.assertRaises(StorageLimitReached):
                with self.store.transaction():
                    self.store.diagnostic("committed_before_limit_stop", {})
        self.assertEqual(self.store.connection.execute(
            "SELECT count(*) FROM diagnostics WHERE code='committed_before_limit_stop'",
        ).fetchone()[0], 1)

    def test_byte_counter_includes_wal(self) -> None:
        other = Path(self.directory.name) / "size.duckdb"
        other.write_bytes(b"x" * 50)
        Path(str(other) + ".wal").write_bytes(b"x" * 30)
        self.assertEqual(database_bytes(other), 80)

    def test_candle_revisions_invalid_prices_and_latest_view(self) -> None:
        state = CaptureState(self.config)
        bar = event("Candle", "OPT{=1m,tho=true}", at=NOW - timedelta(minutes=1))
        first = replace(bar, received_at=NOW + timedelta(seconds=20))
        second = replace(
            event("Candle", bar.symbol, at=bar.received_at, close="NaN"),
            received_at=NOW + timedelta(seconds=30),
        )
        state.receive(first)
        state.receive(second)
        self.store.flush(state)
        self.assertEqual(self.store.connection.execute("SELECT count(*) FROM candles").fetchone()[0], 2)
        row = self.store.connection.execute("SELECT close, quality, elapsed_with_grace FROM latest_candles").fetchone()
        self.assertIsNone(row[0])
        self.assertIn("candle.invalid_ohlc", row[1])
        self.assertTrue(row[2])
        self.assertEqual(self.store.summary()["candle_coverage"][0]["absence_reason"], "unknown; not proof of no trades")

    def test_recovery_marks_prior_run_interrupted(self) -> None:
        previous = self.store.run_id
        self.store.close()
        self.store = CollectorStore(self.config)
        self.addCleanup(self.store.close)
        self.store.start(WINDOW, "new-revision")
        self.assertEqual(self.store.connection.execute(
            "SELECT status, ended_at FROM runs WHERE run_id=?", [previous],
        ).fetchone(), ("interrupted", None))
        self.assertEqual(self.store.connection.execute("SELECT count(*) FROM instruments").fetchone()[0], 2)

    def test_export_contains_parquet_schemas_checksums_and_coverage(self) -> None:
        self.store.sample(self.populated_state(), [self.instrument], NOW, NOW)
        self.store.finish("partial", "test")
        self.store.close()
        destination = Path(self.directory.name) / "export"
        manifest = export_database(self.path, destination)
        self.assertTrue((destination / "manifest.json").is_file())
        self.assertTrue((destination / "samples.parquet").is_file())
        self.assertTrue((destination / "raw_updates.parquet").is_file())
        self.assertTrue((destination / "raw_update_payloads.parquet").is_file())
        self.assertEqual(manifest["runs"][0]["option_samples"], 1)
        self.assertTrue(all(len(item["sha256"]) == 64 for item in manifest["files"]))
        with self.assertRaises(FileExistsError):
            export_database(self.path, destination)


class CrashRecoveryTests(unittest.TestCase):
    def test_committed_wal_survives_abrupt_process_exit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "crash.duckdb"
            script = """
import os, sys
from pathlib import Path
from dataclasses import replace
from Tastyware_Demo_Scripts.collector_core import CaptureState, CollectorConfig
from Tastyware_Demo_Scripts.collector_store import CollectorStore
from Tastyware_Demo_Scripts.test_spy_collector import WINDOW, event
store = CollectorStore(replace(CollectorConfig(), database=Path(sys.argv[1])))
store.start(WINDOW, "crash-test")
with store.transaction():
    store.diagnostic("durable_before_crash", {})
state = CaptureState(store.config)
state.receive(event(bid_time=0, ask_time=0))
state.receive(event(bid_time=0, ask_time=0))
store.flush(state)
os._exit(0)
"""
            result = subprocess.run(
                [sys.executable, "-c", script, str(path)], capture_output=True, timeout=60, check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            config = replace(CollectorConfig(), database=path)
            store = CollectorStore(config)
            try:
                store.start(WINDOW, "restart")
                self.assertEqual(store.connection.execute(
                    "SELECT count(*) FROM diagnostics WHERE code='durable_before_crash'",
                ).fetchone()[0], 1)
                self.assertEqual(store.connection.execute(
                    "SELECT count(*) FROM runs WHERE status='interrupted'",
                ).fetchone()[0], 1)
                self.assertEqual(store.connection.execute("SELECT count(*) FROM raw_updates").fetchone()[0], 2)
            finally:
                store.close()


class RunnerTests(unittest.IsolatedAsyncioTestCase):
    async def test_updates_flush_during_slow_connection_setup(self) -> None:
        store = MagicMock()
        collector = RUNNER.SessionCollector(replace(CollectorConfig(), flush_seconds=0.01), WINDOW, store)
        store.flush.side_effect = lambda state: state.updates.clear()

        async def setup() -> None:
            collector.state.receive(event())
            await asyncio.sleep(0.04)
            self.assertFalse(collector.state.updates)
            self.assertGreaterEqual(collector.persisted_updates, 1)

        with patch.object(collector, "collect_connections", side_effect=setup):
            await collector.collect()
        self.assertGreaterEqual(store.flush.call_count, 2)

    async def test_writer_failure_cancels_collection_and_is_not_silent(self) -> None:
        store = MagicMock()
        store.flush.side_effect = OSError("injected disk failure")
        collector = RUNNER.SessionCollector(replace(CollectorConfig(), flush_seconds=0.01), WINDOW, store)
        cancelled = asyncio.Event()

        async def connection() -> None:
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        with patch.object(collector, "collect_connections", side_effect=connection):
            with self.assertRaises(OSError):
                await collector.collect()
        self.assertTrue(cancelled.is_set())

    async def test_graceful_cancellation_flushes_last_raw_updates(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = replace(CollectorConfig(), database=Path(directory) / "cancel.duckdb")
            store = CollectorStore(config)
            store.start(WINDOW, "cancel")
            collector = RUNNER.SessionCollector(config, WINDOW, store)
            received = asyncio.Event()

            async def connection() -> None:
                collector.state.receive(event())
                received.set()
                await asyncio.Event().wait()

            try:
                with patch.object(collector, "collect_connections", side_effect=connection):
                    task = asyncio.create_task(collector.collect())
                    await received.wait()
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await task
                self.assertEqual(store.connection.execute("SELECT count(*) FROM raw_updates").fetchone()[0], 1)
            finally:
                store.close()

    def test_verbose_flag_is_opt_in(self) -> None:
        self.assertFalse(RUNNER.parse_arguments([])[1].verbose)
        self.assertTrue(RUNNER.parse_arguments(["--verbose"])[1].verbose)
        self.assertEqual(RUNNER.parse_arguments([])[1].max_bytes, 20_000_000_000)
        collector = RUNNER.SessionCollector(CollectorConfig(), WINDOW, MagicMock())
        with patch.object(RUNNER, "log") as output:
            collector.report_feed(NOW)
            collector.verbose_log("hidden")
        output.assert_not_called()

    def test_verbose_feed_reports_receipts_idle_and_unknown_source_times(self) -> None:
        collector = RUNNER.SessionCollector(replace(CollectorConfig(), verbose=True), WINDOW, MagicMock())
        with patch.object(RUNNER, "log") as output:
            collector.report_feed(NOW)
            self.assertIn("last_event_age=never", output.call_args.args[0])
            collector.state.receive(event(bid_time=0, ask_time=0, private_payload="do-not-print"))
            collector.state.receive(event("Greeks", "OPT"))
            collector.report_feed(NOW)
            text = output.call_args.args[0]
            self.assertIn("'Quote': 1", text)
            self.assertIn("'Greeks': 1", text)
            self.assertIn("source_timestamp_known=0/1", text)
            self.assertIn("last_event_age=0.0s", text)
            collector.report_feed(NOW + timedelta(seconds=5))
            self.assertIn("new_since_report={'Quote': 0, 'Trade': 0, 'Greeks': 0, 'Candle': 0}", output.call_args.args[0])
            self.assertIn("last_event_age=5.0s", output.call_args.args[0])
            self.assertNotIn("do-not-print", str(output.call_args_list))

    def test_verbose_never_claims_commit_when_flush_fails(self) -> None:
        store = MagicMock()
        store.flush.side_effect = OSError("private-error")
        collector = RUNNER.SessionCollector(replace(CollectorConfig(), verbose=True), WINDOW, store)
        collector.state.receive(event("Candle"))
        with patch.object(RUNNER, "log") as output:
            with self.assertRaises(OSError):
                collector.flush()
        output.assert_not_called()

    async def test_universe_accepts_recent_receipt_with_unknown_source_times(self) -> None:
        collector = RUNNER.SessionCollector(CollectorConfig(), WINDOW, MagicMock())
        quote = event(bid_time=0, ask_time=0)
        collector.state.receive(quote)
        with (
            patch.object(RUNNER, "utc_now", return_value=NOW),
            patch.object(RUNNER, "get_option_chain", AsyncMock(return_value={})),
            patch.object(RUNNER, "select_universe", return_value=([], {})) as select,
            patch.object(RUNNER, "log"),
        ):
            await collector.refresh_universe(MagicMock(), MagicMock())
        self.assertEqual(select.call_args.args[1], 101)
        self.assertIn("universe_source_time_unknown", [row[0] for row in collector.state.diagnostics])
        self.assertEqual(quote_quality(quote, NOW, 5)[2], ["bid_time.unknown", "ask_time.unknown"])
        self.assertIsNone(quote.timestamp)

    async def test_universe_rejects_bad_or_stale_quotes(self) -> None:
        for changes in (
            {"bid_time": 0, "ask_time": 0, "bid_price": "NaN"},
            {"bid_time": 0, "ask_time": 0, "bid_price": "103"},
            {"bid_time": 0, "ask_time": 0, "at": NOW - timedelta(seconds=6)},
            {"bid_time": 0, "ask_time": 0, "at": NOW + timedelta(seconds=1)},
            {"bid_time": int((NOW - timedelta(seconds=6)).timestamp() * 1000)},
        ):
            with self.subTest(changes=changes):
                collector = RUNNER.SessionCollector(CollectorConfig(), WINDOW, MagicMock())
                collector.state.receive(event(**changes))
                with (
                    patch.object(RUNNER, "utc_now", return_value=NOW),
                    patch.object(RUNNER, "monotonic", side_effect=[0, 31]),
                    patch.object(RUNNER, "get_option_chain", AsyncMock()) as chain,
                ):
                    with self.assertRaises(TimeoutError):
                        await collector.refresh_universe(MagicMock(), MagicMock())
                chain.assert_not_awaited()

    async def test_preopen_wait_then_collect_only_today(self) -> None:
        config = CollectorConfig()
        store = MagicMock()
        store.summary.return_value = {
            "missed_cycles": 0, "unrecorded_cycles": 0, "distinct_contracts": 1,
            "missing_quote_samples": 0, "missing_analytics_samples": 0, "unobserved_candle_intervals": 0,
        }
        collector = MagicMock()
        collector.collect = AsyncMock()
        before = WINDOW.opens - timedelta(seconds=10)
        with (
            patch.object(RUNNER, "CollectorStore", return_value=store),
            patch.object(RUNNER, "SessionCollector", return_value=collector),
            patch.object(RUNNER, "utc_now", side_effect=[before, before, before, WINDOW.opens, WINDOW.opens]),
            patch.object(RUNNER, "log"),
            patch.object(RUNNER.subprocess, "run", return_value=MagicMock(returncode=0, stdout="revision")),
            patch.object(RUNNER.asyncio, "sleep", new_callable=AsyncMock) as sleep,
            patch("builtins.print"),
        ):
            self.assertEqual(await RUNNER.run_collection(config, WINDOW), 0)
        sleep.assert_awaited_once_with(10)
        collector.collect.assert_awaited_once()
        store.finish.assert_called_once_with("completed", "session_closed")
        store.close.assert_called_once()

    async def test_failed_requests_are_sanitized_and_recorded(self) -> None:
        store = MagicMock()
        collector = RUNNER.SessionCollector(CollectorConfig(), WINDOW, store)
        request = AsyncMock(side_effect=OSError("secret-token-example"))
        with patch.object(RUNNER, "utc_now", return_value=NOW):
            with self.assertRaises(RUNNER.DemoHelperError):
                await collector.request("test", ["SPY"], request)
        self.assertNotIn("secret-token-example", str(store.request.call_args))
        self.assertEqual(store.request.call_args.args[3], "failed")

    async def test_rate_limit_has_explicit_diagnostic(self) -> None:
        store = MagicMock()
        collector = RUNNER.SessionCollector(CollectorConfig(), WINDOW, store)
        with patch.object(RUNNER, "utc_now", return_value=NOW):
            with self.assertRaises(RUNNER.DemoHelperError):
                await collector.request("test", ["SPY"], AsyncMock(side_effect=RuntimeError("HTTP 429 private")))
        self.assertTrue(store.request.call_args.args[4]["rate_limited"])
        self.assertNotIn("private", str(store.request.call_args))

    async def test_storage_limit_finishes_partial_and_closes(self) -> None:
        store = MagicMock()
        collector = MagicMock()
        collector.collect = AsyncMock(side_effect=StorageLimitReached())
        with (
            patch.object(RUNNER, "CollectorStore", return_value=store),
            patch.object(RUNNER, "SessionCollector", return_value=collector),
            patch.object(RUNNER, "utc_now", return_value=NOW),
            patch.object(RUNNER, "log"),
            patch.object(RUNNER.subprocess, "run", return_value=MagicMock(returncode=0, stdout="revision")),
        ):
            self.assertEqual(await RUNNER.run_collection(CollectorConfig(), WINDOW), 2)
        store.finish.assert_called_once_with("partial", "storage_limit")
        store.close.assert_called_once()

    async def test_exhausted_reconnects_are_not_success(self) -> None:
        config = replace(CollectorConfig(), reconnect_attempts=1)
        collector = RUNNER.SessionCollector(config, WINDOW, MagicMock())

        @asynccontextmanager
        async def resource(*_args, **_kwargs):
            yield MagicMock()

        with (
            patch.object(collector, "restore_universe"),
            patch.object(collector, "connected", AsyncMock(side_effect=OSError())),
            patch.object(RUNNER, "Session", resource),
            patch.object(RUNNER, "RawMarketStreamer", resource),
            patch.object(RUNNER, "load_runtime_config", return_value=MagicMock()),
            patch.object(RUNNER, "utc_now", return_value=NOW),
            patch.object(RUNNER.asyncio, "sleep", new_callable=AsyncMock),
            patch.object(RUNNER, "log"),
        ):
            with self.assertRaisesRegex(RUNNER.DemoHelperError, "budget exhausted"):
                await collector.collect()

    async def test_reconnect_budget_and_resubscription(self) -> None:
        config = replace(CollectorConfig(), reconnect_attempts=1)
        store = MagicMock()
        collector = RUNNER.SessionCollector(config, WINDOW, store)

        @asynccontextmanager
        async def resource(*_args, **_kwargs):
            yield MagicMock()

        runtime = MagicMock(client_secret="not-real", refresh_token="not-real")
        with (
            patch.object(collector, "restore_universe"),
            patch.object(collector, "connected", AsyncMock(side_effect=[OSError(), None])) as connected,
            patch.object(RUNNER, "Session", resource),
            patch.object(RUNNER, "RawMarketStreamer", resource),
            patch.object(RUNNER, "load_runtime_config", return_value=runtime),
            patch.object(RUNNER, "utc_now", return_value=NOW),
            patch.object(RUNNER.asyncio, "sleep", new_callable=AsyncMock),
            patch.object(RUNNER, "log"),
        ):
            await collector.collect()
        self.assertEqual(connected.await_count, 2)
        self.assertGreaterEqual(store.flush.call_count, 1)

    def test_closed_day_does_not_authenticate(self) -> None:
        with (
            patch.object(RUNNER, "utc_now", return_value=WINDOW.closes + timedelta(seconds=1)),
            patch.object(RUNNER, "load_runtime_config") as credentials,
            patch.object(RUNNER, "log"),
        ):
            self.assertEqual(RUNNER.main([]), 0)
        credentials.assert_not_called()

    def test_late_cycles_are_not_fabricated_snapshots(self) -> None:
        store = MagicMock()
        collector = RUNNER.SessionCollector(CollectorConfig(), WINDOW, store)
        collector.next_sample = NOW
        collector.instruments = {"OPT": Instrument("OPTION", "OPT")}
        with patch.object(RUNNER, "log"):
            collector.record_due_samples(NOW + timedelta(seconds=61))
        self.assertEqual(store.sample.call_count, 2)
        self.assertTrue(store.sample.call_args_list[0].kwargs["missed"])
        self.assertFalse(store.sample.call_args_list[1].kwargs["missed"])

    async def test_complete_session_loop_samples_flushes_and_reconciles(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = replace(
                CollectorConfig(), database=Path(directory) / "loop.duckdb",
                delay_status="realtime", clock_offset_ms=0, candle_grace=1,
                reconciliation_seconds=2, candle_refresh_seconds=60,
                verbose=True,
            )
            window = SessionWindow(NOW.date(), NOW, NOW + timedelta(minutes=2))
            store = CollectorStore(config)
            try:
                store.start(window, "test")
                collector = RUNNER.SessionCollector(config, window, store)
                instrument = Instrument("OPTION", "OPT", "P", 100, NOW.date() + timedelta(days=17))
                wall = [NOW]
                overlaps = []
                messages = []

                def feed() -> None:
                    collector.state.receive(event(at=wall[0]))
                    collector.state.receive(event(symbol="OPT", at=wall[0], bid_price="1", ask_price="2"))
                    collector.state.receive(event("Greeks", "OPT", at=wall[0]))

                async def sleep(seconds: float) -> None:
                    wall[0] += timedelta(seconds=seconds)
                    feed()

                async def subscribe(*_args) -> None:
                    feed()

                async def universe(*_args) -> None:
                    collector.instruments = {"OPT": instrument}
                    store.universe([Instrument("SPY", "SPY"), instrument], {"rule": "test"}, wall[0])
                    collector.next_universe = window.closes + timedelta(days=1)

                async def overlap(*_args, closing=False) -> None:
                    overlaps.append(closing)
                    collector.next_candles = wall[0] + timedelta(seconds=60)
                    bar = event("Candle", "OPT{=1m,tho=true}", at=wall[0].replace(second=0, microsecond=0) - timedelta(minutes=1))
                    collector.state.receive(replace(bar, received_at=wall[0]))

                with (
                    patch.object(RUNNER, "utc_now", side_effect=lambda: wall[0]),
                    patch.object(RUNNER, "monotonic", side_effect=lambda: (wall[0] - NOW).total_seconds()),
                    patch.object(RUNNER.asyncio, "sleep", side_effect=sleep),
                    patch.object(RUNNER, "log", side_effect=lambda text: messages.append((wall[0], text))),
                    patch.object(collector, "subscribe", side_effect=subscribe),
                    patch.object(collector, "refresh_universe", side_effect=universe),
                    patch.object(collector, "candle_overlap", side_effect=overlap),
                ):
                    await collector.connected(MagicMock(), MagicMock())
                self.assertEqual(store.connection.execute("SELECT count(*) FROM samples").fetchone()[0], 2)
                self.assertEqual(store.connection.execute("SELECT count(*) FROM cycles WHERE status='missed'").fetchone()[0], 0)
                self.assertEqual(store.connection.execute("SELECT count(*) FROM candles").fetchone()[0], 2)
                self.assertEqual(overlaps, [False, True])
                self.assertTrue(collector.closing_requested)
                self.assertGreaterEqual(wall[0], window.closes)
                heartbeats = [at for at, text in messages if "Feed received_total=" in text]
                self.assertGreaterEqual(len(heartbeats), 24)
                self.assertTrue(all(
                    (later - earlier).total_seconds() >= 5
                    for earlier, later in zip(heartbeats, heartbeats[1:])
                ))
                self.assertTrue(any("Database buffers committed: candle_revisions=2" in text for _, text in messages))
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
