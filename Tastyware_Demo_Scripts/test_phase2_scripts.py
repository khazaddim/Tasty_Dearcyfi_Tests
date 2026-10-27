"""Credential-free tests for Phase 2 market and history scripts."""

from __future__ import annotations

import asyncio
import importlib
import os
import unittest
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from io import StringIO
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, patch

from Tastyware_Demo_Scripts.demo_shared import (
    DemoConfigurationError,
    DemoNetworkError,
    DemoTimeoutError,
    load_runtime_config,
)


SCRIPT_05 = importlib.import_module("Tastyware_Demo_Scripts.05_market_data_snapshot")
SCRIPT_06 = importlib.import_module("Tastyware_Demo_Scripts.06_stream_quotes")
SCRIPT_07 = importlib.import_module("Tastyware_Demo_Scripts.07_historical_candles")
SCRIPT_08 = importlib.import_module("Tastyware_Demo_Scripts.08_account_transaction_history")
SCRIPT_09 = importlib.import_module("Tastyware_Demo_Scripts.09_account_value_history")


class Phase2SnapshotTest(unittest.TestCase):
    def test_field_status_unavailable_and_stale(self) -> None:
        stale_time = datetime.now(UTC) - timedelta(minutes=10)
        self.assertEqual(SCRIPT_05.field_status(None, stale_time, 120), "unavailable")
        self.assertEqual(SCRIPT_05.field_status(100, stale_time, 120), "stale")

    def test_field_status_current(self) -> None:
        fresh_time = datetime.now(UTC) - timedelta(seconds=15)
        self.assertEqual(SCRIPT_05.field_status(100, fresh_time, 120), "current")


class Phase2SnapshotLookupTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.config = load_runtime_config(
            {
                "Tasty_SECRET": "test-secret",
                "Tasty_Refresh": "test-refresh",
                "TASTYTRADE_ENV": "production",
            }
        )
        self.session = SimpleNamespace(aclose=AsyncMock())
        self.snapshot = SimpleNamespace(
            updated_at=datetime(2020, 1, 1, tzinfo=UTC),
            bid="100.00",
            ask="101.00",
            last="100.50",
            mark="100.50",
            volume=None,
        )
        self.lookup = self.enterContext(
            patch.object(SCRIPT_05, "get_market_data", AsyncMock(return_value=self.snapshot))
        )
        self.output = StringIO()
        self.enterContext(patch.object(SCRIPT_05, "load_runtime_config", return_value=self.config))
        self.enterContext(patch.object(SCRIPT_05, "Session", return_value=self.session))
        self.enterContext(
            patch.dict(
                os.environ,
                {
                    "TASTYTRADE_SYMBOL": " spy ",
                    "TASTYTRADE_INSTRUMENT_TYPE": "Equity",
                    "TASTYTRADE_STALE_SECONDS": "120",
                },
            )
        )
        self.enterContext(patch("sys.stdout", self.output))

    def assert_snapshot_output(self) -> None:
        self.assertEqual(
            self.output.getvalue().splitlines(),
            [
                "symbol=SPY",
                "instrument_type=Equity",
                "updated_at=2020-01-01 00:00:00+00:00",
                "bid=100.00 status=stale",
                "ask=101.00 status=stale",
                "last=100.50 status=stale",
                "mark=100.50 status=stale",
                "volume=unavailable status=unavailable",
            ],
        )
        self.session.aclose.assert_awaited_once_with()

    async def test_awaits_async_snapshot_lookup(self) -> None:
        self.assertEqual(await SCRIPT_05.main(), 0)
        self.lookup.assert_awaited_once_with(self.session, "SPY", SCRIPT_05.InstrumentType.EQUITY)
        self.assert_snapshot_output()

    async def test_supports_sync_snapshot_lookup(self) -> None:
        with patch.object(SCRIPT_05, "get_market_data", Mock(return_value=self.snapshot)) as lookup:
            self.assertEqual(await SCRIPT_05.main(), 0)
        lookup.assert_called_once_with(self.session, "SPY", SCRIPT_05.InstrumentType.EQUITY)
        self.assert_snapshot_output()

    async def test_reports_async_snapshot_failure_and_closes_session(self) -> None:
        self.lookup.side_effect = OSError("connection failed")
        self.assertEqual(await SCRIPT_05.main(), 1)
        self.assertEqual(
            self.output.getvalue(),
            "Snapshot request failed: market-data snapshot failed due to a network transport error.\n",
        )
        self.lookup.assert_awaited_once_with(self.session, "SPY", SCRIPT_05.InstrumentType.EQUITY)
        self.session.aclose.assert_awaited_once_with()

    async def test_times_out_async_snapshot_lookup_and_closes_session(self) -> None:
        async def slow_snapshot(session: object, symbol: str, instrument_type: object) -> None:
            await asyncio.sleep(60)

        self.lookup.side_effect = slow_snapshot
        with patch.object(
            SCRIPT_05,
            "load_runtime_config",
            return_value=replace(self.config, timeout_seconds=0.01),
        ):
            self.assertEqual(await SCRIPT_05.main(), 1)
        self.assertEqual(
            self.output.getvalue(),
            "Snapshot request failed: market-data snapshot timed out before completion.\n",
        )
        self.lookup.assert_awaited_once_with(self.session, "SPY", SCRIPT_05.InstrumentType.EQUITY)
        self.session.aclose.assert_awaited_once_with()


class Phase2QuoteStreamerTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.session = Mock()
        self.streamer = MagicMock()
        self.streamer.__aenter__.return_value = self.streamer
        self.streamer.subscribe = AsyncMock()
        self.streamer.unsubscribe = AsyncMock()
        self.events = [
            SimpleNamespace(
                event_symbol="SPY",
                bid_price=100 + index,
                ask_price=101 + index,
                bid_size=10,
                ask_size=20,
                event_time=0,
                bid_time=1793066400000 + index * 100,
                ask_time=1793066400000 + index * 125,
                time_nano_part=index * 123456,
            )
            for index in range(2)
        ]

        async def quotes():
            for event in self.events:
                yield event

        self.streamer.listen.side_effect = lambda _event_class: quotes()
        self.factory = self.enterContext(
            patch.object(SCRIPT_06, "DXLinkStreamer", return_value=self.streamer)
        )

    def assert_cleanup(self) -> None:
        self.streamer.unsubscribe.assert_awaited_once_with(SCRIPT_06.Quote, ["SPY"])
        self.streamer.__aexit__.assert_awaited_once()

    def test_timing_output_is_elapsed_and_flushed(self) -> None:
        with (
            patch.object(SCRIPT_06, "_START_TIME", 100.0),
            patch.object(SCRIPT_06, "perf_counter", return_value=101.25),
            patch("builtins.print") as output,
        ):
            SCRIPT_06._log_timing("Test checkpoint")
        output.assert_called_once_with(
            "[   1.250s] Test checkpoint", flush=True
        )

    async def test_reports_stream_timing_checkpoints_in_order(self) -> None:
        with patch.object(SCRIPT_06, "_log_timing") as timing:
            await SCRIPT_06.run_streamer(self.session, ["SPY"], 1, 1.0)
        self.assertEqual(
            [call.args[0] for call in timing.call_args_list],
            [
                "Creating DXLinkStreamer...",
                "DXLinkStreamer created; connecting streamer context...",
                "Streamer connected; subscribing to quotes...",
                "Quote subscription complete.",
                "Collecting up to 1 quote events (collection timeout=1s)...",
                "Quote collection ended; unsubscribing...",
                "Quote unsubscription complete.",
                "Collection finished: 1 events, state=event_limit; closing streamer context...",
                "Streamer context closed.",
            ],
        )
        self.assert_cleanup()

    async def test_prints_each_quote_before_requesting_the_next(self) -> None:
        with (
            patch("builtins.print") as output,
            patch.object(SCRIPT_06, "_log_timing"),
            patch.object(SCRIPT_06, "perf_counter", side_effect=[100.0, 100.125]),
        ):
            async def quotes():
                for index, event in enumerate(self.events):
                    yield event
                    delta_ms = "N/A" if index == 0 else "125.000"
                    output.assert_any_call(
                        f"- symbol=SPY bid={100 + index} ask={101 + index} "
                        "bid_size=10 ask_size=20 event_time=0 "
                        f"bid_time={1793066400000 + index * 100} "
                        f"ask_time={1793066400000 + index * 125} "
                        f"time_nano_part={index * 123456} delta_ms={delta_ms}",
                        flush=True,
                    )
                    self.streamer.unsubscribe.assert_not_awaited()

            self.streamer.listen.side_effect = lambda _event_class: quotes()
            result = await SCRIPT_06.run_streamer(self.session, ["SPY"], 10, 1.0)
        self.assertEqual(result.events, self.events)
        self.assertEqual(result.completion_state, "stream_complete")
        self.assert_cleanup()

    async def test_quote_deltas_track_consecutive_arrivals_across_symbols(self) -> None:
        self.events.extend([
            SimpleNamespace(
                event_symbol="QQQ", bid_price=200, ask_price=201,
                bid_size=10, ask_size=20,
                event_time=0, bid_time=1793066402500,
                ask_time=1793066402600, time_nano_part=654321,
            ),
            self.events[0],
        ])
        with (
            patch("builtins.print") as output,
            patch.object(SCRIPT_06, "_log_timing"),
            patch.object(
                SCRIPT_06, "perf_counter",
                side_effect=[100.0, 100.125, 102.625, 102.625],
            ),
        ):
            result = await SCRIPT_06.run_streamer(self.session, ["SPY"], 10, 1.0)
        self.assertEqual(
            [call.args[0].split("delta_ms=")[1] for call in output.call_args_list],
            ["N/A", "125.000", "2500.000", "0.000"],
        )
        self.assertEqual(result.events, self.events)
        self.assert_cleanup()

    async def test_final_summary_does_not_repeat_live_quotes(self) -> None:
        self.session.aclose = AsyncMock()
        config = SimpleNamespace(
            client_secret="test-secret", refresh_token="test-token", timeout_seconds=1.0
        )
        with (
            patch.object(SCRIPT_06, "load_runtime_config", return_value=config),
            patch.object(SCRIPT_06, "Session", return_value=self.session),
            patch.dict(os.environ, {
                "TASTYTRADE_SYMBOLS": "SPY",
                "TASTYTRADE_EVENT_LIMIT": "2",
                "TASTYTRADE_STREAM_TIMEOUT_SECONDS": "1",
            }),
            patch("builtins.print") as output,
        ):
            exit_code = await SCRIPT_06.stream_quotes()
        lines = [call.args[0] for call in output.call_args_list]
        quote_lines = [line for line in lines if line.startswith("- symbol=")]
        self.assertEqual(len(quote_lines), 2)
        self.assertLess(lines.index(quote_lines[-1]), lines.index("event_count=2"))
        self.assertIn("completion_state=event_limit", lines)
        self.assertEqual(exit_code, 0)
        self.session.aclose.assert_awaited_once_with()
        self.assert_cleanup()

    async def test_awaits_subscription_and_cleanup_at_event_limit(self) -> None:
        result = await SCRIPT_06.run_streamer(self.session, ["SPY"], 1, 1.0)
        self.factory.assert_called_once_with(self.session)
        self.streamer.subscribe.assert_awaited_once_with(SCRIPT_06.Quote, ["SPY"])
        self.streamer.listen.assert_called_once_with(SCRIPT_06.Quote)
        self.assertEqual(result.events, self.events[:1])
        self.assertEqual(result.completion_state, "event_limit")
        self.assert_cleanup()

    async def test_cleans_up_when_stream_completes(self) -> None:
        result = await SCRIPT_06.run_streamer(self.session, ["SPY"], 10, 1.0)
        self.assertEqual(result.events, self.events)
        self.assertEqual(result.completion_state, "stream_complete")
        self.assert_cleanup()

    async def test_cleans_up_when_collection_times_out(self) -> None:
        async def slow_quotes():
            await asyncio.sleep(60)
            yield object()

        self.streamer.listen.side_effect = lambda _event_class: slow_quotes()
        result = await SCRIPT_06.run_streamer(self.session, ["SPY"], 1, 0.01)
        self.assertEqual(result.completion_state, "timeout")
        self.assertIsInstance(result.error, DemoTimeoutError)
        self.assert_cleanup()

    async def test_cleans_up_when_collection_fails(self) -> None:
        async def failed_quotes():
            raise OSError("connection failed")
            yield object()

        self.streamer.listen.side_effect = lambda _event_class: failed_quotes()
        result = await SCRIPT_06.run_streamer(self.session, ["SPY"], 1, 1.0)
        self.assertEqual(result.completion_state, "failed")
        self.assertIsInstance(result.error, DemoNetworkError)
        self.assert_cleanup()

    async def test_classifies_subscription_failure_and_closes_streamer(self) -> None:
        self.streamer.subscribe.side_effect = OSError("connection failed")
        with self.assertRaisesRegex(DemoNetworkError, "quote subscription"):
            await SCRIPT_06.run_streamer(self.session, ["SPY"], 1, 1.0)
        self.streamer.listen.assert_not_called()
        self.streamer.unsubscribe.assert_not_called()
        self.streamer.__aexit__.assert_awaited_once()

    async def test_bounds_subscription_and_cleanup_waits(self) -> None:
        async def slow_operation(*args: object) -> None:
            await asyncio.sleep(60)

        for operation in ("subscribe", "unsubscribe"):
            with self.subTest(operation=operation):
                self.streamer.reset_mock()
                self.streamer.subscribe.side_effect = None
                self.streamer.unsubscribe.side_effect = None
                getattr(self.streamer, operation).side_effect = slow_operation
                with self.assertRaises(DemoTimeoutError):
                    await SCRIPT_06.run_streamer(self.session, ["SPY"], 1, 0.01)
                self.streamer.__aexit__.assert_awaited_once()

    async def test_classifies_cleanup_failure_and_closes_streamer(self) -> None:
        self.streamer.unsubscribe.side_effect = OSError("connection failed")
        with self.assertRaisesRegex(DemoNetworkError, "quote unsubscription"):
            await SCRIPT_06.run_streamer(self.session, ["SPY"], 1, 1.0)
        self.assert_cleanup()

    async def test_supports_sync_subscription_and_cleanup(self) -> None:
        self.streamer.subscribe = Mock()
        self.streamer.unsubscribe = Mock()
        result = await SCRIPT_06.run_streamer(self.session, ["SPY"], 1, 1.0)
        self.assertEqual(result.completion_state, "event_limit")
        self.streamer.subscribe.assert_called_once_with(SCRIPT_06.Quote, ["SPY"])
        self.streamer.unsubscribe.assert_called_once_with(SCRIPT_06.Quote, ["SPY"])
        self.streamer.__aexit__.assert_awaited_once()


class Phase2CandlePlanningTest(unittest.TestCase):
    def test_resolve_endpoint_plan_weekend_adjustment(self) -> None:
        start = datetime.fromisoformat("2026-10-03T10:00:00-04:00")  # Saturday
        end = datetime.fromisoformat("2026-10-05T16:00:00-04:00")
        now = datetime.fromisoformat("2026-10-05T20:00:00+00:00")
        plan = SCRIPT_07.resolve_endpoint_plan(calendar_name="XNYS", start=start, end=end, now_utc=now)
        self.assertEqual(plan.start_session, date(2026, 10, 5))
        self.assertEqual(plan.end_session, date(2026, 10, 5))

    def test_resolve_endpoint_plan_unknown_calendar(self) -> None:
        start = datetime.fromisoformat("2026-10-05T10:00:00-04:00")
        end = datetime.fromisoformat("2026-10-05T16:00:00-04:00")
        with self.assertRaises(DemoConfigurationError):
            SCRIPT_07.resolve_endpoint_plan(calendar_name="NOT_A_CALENDAR", start=start, end=end)

    def test_resolve_endpoint_plan_pre_open_end_uses_previous_session(self) -> None:
        start = datetime.fromisoformat("2026-10-01T10:00:00-04:00")
        end = datetime.fromisoformat("2026-10-02T08:00:00-04:00")
        now = datetime.fromisoformat("2026-10-02T08:00:00-04:00")
        plan = SCRIPT_07.resolve_endpoint_plan(
            calendar_name="XNYS",
            start=start,
            end=end,
            now_utc=now,
        )
        self.assertEqual(plan.end_session, date(2026, 10, 1))

    def test_endpoint_observation_uses_calendar_timezone_labels(self) -> None:
        plan = SCRIPT_07.EndpointPlan(
            calendar_name="XNYS",
            calendar_timezone="America/New_York",
            start_session=date(2026, 10, 1),
            end_session=date(2026, 10, 2),
            effective_end_utc=datetime.fromisoformat("2026-10-02T20:00:00+00:00"),
        )
        rows = [
            {"symbol": "SPY", "timestamp": "2026-10-01T14:00:00+00:00"},
            {"symbol": "SPY", "timestamp": "2026-10-02T14:00:00+00:00"},
        ]
        self.assertEqual(SCRIPT_07.endpoint_observation(rows, plan), (True, True))

    def test_merge_rows_dedupes_revisions_and_sorts(self) -> None:
        older = [{"symbol": "SPY", "timestamp": "2026-10-01T14:00:00+00:00", "close": "100"}]
        newer = [
            {"symbol": "SPY", "timestamp": "2026-10-01T14:01:00+00:00", "close": "101"},
            {"symbol": "SPY", "timestamp": "2026-10-01T14:00:00+00:00", "close": "100.5"},
        ]
        merged = SCRIPT_07.merge_rows(older, newer)
        self.assertEqual(len(merged), 2)
        self.assertEqual(merged[0]["close"], "100.5")


class Phase2RetryBehaviorTest(unittest.IsolatedAsyncioTestCase):
    async def test_collect_candle_block_stops_on_endpoint_coverage(self) -> None:
        start = datetime.fromisoformat("2026-10-01T10:00:00-04:00")
        end = datetime.fromisoformat("2026-10-02T16:00:00-04:00")
        attempt_1 = SCRIPT_07.CandleAttemptResult(
            rows=[{"symbol": "SPY", "timestamp": "2026-10-01T14:00:00+00:00", "open": "1", "high": "1", "low": "1", "close": "1", "volume": 1}],
            completion_state="timeout",
            attempt_seconds=1.0,
            error=DemoTimeoutError("timed out"),
        )
        attempt_2 = SCRIPT_07.CandleAttemptResult(
            rows=[{"symbol": "SPY", "timestamp": "2026-10-02T14:00:00+00:00", "open": "1", "high": "1", "low": "1", "close": "1", "volume": 1}],
            completion_state="event_limit",
            attempt_seconds=1.0,
            error=None,
        )

        @asynccontextmanager
        async def fake_manager(resource):
            yield resource

        with (
            patch.object(SCRIPT_07, "DXLinkStreamer", side_effect=lambda _session: object()),
            patch.object(SCRIPT_07, "managed_async_resource", side_effect=fake_manager),
            patch.object(SCRIPT_07, "collect_candle_attempt", new=AsyncMock(side_effect=[attempt_1, attempt_2])),
        ):
            report = await SCRIPT_07.collect_candle_block(
                session=object(),
                symbol="SPY",
                interval="5m",
                start=start,
                end=end,
                calendar_name="XNYS",
                per_attempt_timeout_seconds=10.0,
                overall_timeout_seconds=60.0,
                max_attempts=3,
                event_limit=10,
                extended_hours=False,
            )
        self.assertTrue(report.endpoint_coverage)
        self.assertEqual(report.stop_reason, "endpoint_coverage")
        self.assertTrue(report.partial)

    async def test_collect_candle_block_no_progress_stop(self) -> None:
        start = datetime.fromisoformat("2026-10-01T10:00:00-04:00")
        end = datetime.fromisoformat("2026-10-02T16:00:00-04:00")
        empty_attempt = SCRIPT_07.CandleAttemptResult(rows=[], completion_state="timeout", attempt_seconds=1.0, error=DemoTimeoutError("timed out"))

        @asynccontextmanager
        async def fake_manager(resource):
            yield resource

        with (
            patch.object(SCRIPT_07, "DXLinkStreamer", side_effect=lambda _session: object()),
            patch.object(SCRIPT_07, "managed_async_resource", side_effect=fake_manager),
            patch.object(SCRIPT_07, "collect_candle_attempt", new=AsyncMock(side_effect=[empty_attempt, empty_attempt])),
        ):
            report = await SCRIPT_07.collect_candle_block(
                session=object(),
                symbol="SPY",
                interval="5m",
                start=start,
                end=end,
                calendar_name="XNYS",
                per_attempt_timeout_seconds=10.0,
                overall_timeout_seconds=60.0,
                max_attempts=3,
                event_limit=10,
                extended_hours=False,
            )
        self.assertEqual(report.stop_reason, "no_progress")


class Phase2PaginationTest(unittest.IsolatedAsyncioTestCase):
    async def test_awaits_async_history_pages_with_date_filters(self) -> None:
        lookup = AsyncMock(side_effect=[[1, 2], [3]])
        session = Mock()
        start, end = date(2026, 10, 1), date(2026, 10, 2)
        rows, pages = await SCRIPT_08.fetch_history_pages(
            selected_account=SimpleNamespace(get_history=lookup),
            session=session,
            start_date=start,
            end_date=end,
            per_page=2,
            timeout_seconds=1.0,
        )
        self.assertEqual((rows, pages), ([1, 2, 3], 2))
        self.assertEqual(
            [call.args for call in lookup.await_args_list],
            [
                (session, 2, offset, "Desc", None, None, None, start, end)
                for offset in (0, 2)
            ],
        )

    async def test_classifies_async_history_failure(self) -> None:
        with self.assertRaises(DemoNetworkError):
            await SCRIPT_08.fetch_history_pages(
                selected_account=SimpleNamespace(get_history=AsyncMock(side_effect=OSError())),
                session=Mock(),
                start_date=date(2026, 10, 1),
                end_date=date(2026, 10, 2),
                per_page=2,
                timeout_seconds=1.0,
            )

    async def test_fetch_history_pages_handles_pagination(self) -> None:
        calls: list[int] = []

        class FakeAccount:
            def get_history(self, _session, per_page, page_offset, *_args):
                calls.append(page_offset)
                if page_offset == 0:
                    return [1, 2]
                return [3]

        rows, pages = await SCRIPT_08.fetch_history_pages(
            selected_account=FakeAccount(),
            session=object(),
            start_date=date(2026, 10, 1),
            end_date=date(2026, 10, 2),
            per_page=2,
            timeout_seconds=5.0,
        )
        self.assertEqual(rows, [1, 2, 3])
        self.assertEqual(pages, 2)
        self.assertEqual(calls, [0, 2])


class Phase2CandleSubscriptionTest(unittest.IsolatedAsyncioTestCase):
    async def test_awaits_candle_subscription_and_matching_cleanup(self) -> None:
        for extended_hours in (False, True):
            with self.subTest(extended_hours=extended_hours):
                streamer = Mock(subscribe_candle=AsyncMock(), unsubscribe_candle=AsyncMock())
                candle = SimpleNamespace(
                    time=0, open=1, high=2, low=1, close=2, volume=10,
                )

                async def candles():
                    yield candle

                streamer.listen.return_value = candles()
                start = datetime(2026, 10, 1, tzinfo=UTC)
                result = await SCRIPT_07.collect_candle_attempt(
                    streamer=streamer, symbol="SPY", interval="5m",
                    start_time=start, timeout_seconds=1.0,
                    event_limit=1, extended_hours=extended_hours,
                )
                streamer.subscribe_candle.assert_awaited_once_with(
                    ["SPY"], "5m", start, extended_hours,
                )
                streamer.unsubscribe_candle.assert_awaited_once_with("SPY", "5m", extended_hours)
                self.assertEqual(result.completion_state, "event_limit")
                self.assertEqual(result.rows[0]["close"], 2)

    async def test_candle_cleanup_on_collection_timeout_or_failure(self) -> None:
        for failure in (False, True):
            with self.subTest(failure=failure):
                streamer = Mock(subscribe_candle=AsyncMock(), unsubscribe_candle=AsyncMock())

                async def candles():
                    if failure:
                        raise OSError("connection failed")
                    await asyncio.sleep(60)
                    yield object()

                streamer.listen.return_value = candles()
                result = await SCRIPT_07.collect_candle_attempt(
                    streamer=streamer, symbol="SPY", interval="5m",
                    start_time=datetime(2026, 10, 1, tzinfo=UTC),
                    timeout_seconds=0.01, event_limit=1, extended_hours=False,
                )
                self.assertEqual(result.completion_state, "failed" if failure else "timeout")
                streamer.unsubscribe_candle.assert_awaited_once_with("SPY", "5m", False)


class Phase2ValueHistoryTest(unittest.IsolatedAsyncioTestCase):
    async def test_awaits_value_history_and_closes_session(self) -> None:
        for failure in (False, True):
            with self.subTest(failure=failure):
                session = SimpleNamespace(aclose=AsyncMock())
                lookup = AsyncMock(return_value=[SimpleNamespace(time="today", close="100")])
                if failure:
                    lookup.side_effect = OSError("connection failed")
                account = SimpleNamespace(
                    account_number="ABC12345", get_net_liquidating_value_history=lookup,
                )
                config = load_runtime_config({
                    "Tasty_SECRET": "test", "Tasty_Refresh": "test",
                    "TASTYTRADE_ENV": "production",
                })
                output = StringIO()
                with (
                    patch.object(SCRIPT_09, "load_runtime_config", return_value=config),
                    patch.object(SCRIPT_09, "Session", return_value=session),
                    patch.object(SCRIPT_09.Account, "get", AsyncMock(return_value=[account])),
                    patch.dict(os.environ, {
                        "TASTYTRADE_TIME_BACK": "1m", "TASTYTRADE_EXPORT_CSV": "false",
                    }),
                    patch("sys.stdout", output),
                ):
                    self.assertEqual(await SCRIPT_09.main(), 1 if failure else 0)
                lookup.assert_awaited_once_with(session, "1m")
                session.aclose.assert_awaited_once_with()
                self.assertNotIn("ABC12345", output.getvalue())
                self.assertIn(
                    "network transport error" if failure else "- timestamp=today value=100",
                    output.getvalue(),
                )


if __name__ == "__main__":
    unittest.main()
