"""Credential-free tests for Phase 2 market and history scripts."""

from __future__ import annotations

import importlib
import unittest
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from unittest.mock import AsyncMock, patch

from Tastyware_Demo_Scripts.demo_shared import DemoConfigurationError, DemoTimeoutError


SCRIPT_05 = importlib.import_module("Tastyware_Demo_Scripts.05_market_data_snapshot")
SCRIPT_07 = importlib.import_module("Tastyware_Demo_Scripts.07_historical_candles")
SCRIPT_08 = importlib.import_module("Tastyware_Demo_Scripts.08_account_transaction_history")


class Phase2SnapshotTest(unittest.TestCase):
    def test_field_status_unavailable_and_stale(self) -> None:
        stale_time = datetime.now(UTC) - timedelta(minutes=10)
        self.assertEqual(SCRIPT_05.field_status(None, stale_time, 120), "unavailable")
        self.assertEqual(SCRIPT_05.field_status(100, stale_time, 120), "stale")

    def test_field_status_current(self) -> None:
        fresh_time = datetime.now(UTC) - timedelta(seconds=15)
        self.assertEqual(SCRIPT_05.field_status(100, fresh_time, 120), "current")


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


if __name__ == "__main__":
    unittest.main()
