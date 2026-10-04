"""Credential-free tests for shared Tastyware demo helper behavior."""

from __future__ import annotations

import asyncio
import csv
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from Tastyware_Demo_Scripts.demo_shared import (
    DemoAuthenticationError,
    DemoConfigurationError,
    DemoNetworkError,
    DemoPermissionError,
    DemoTimeoutError,
    EXPORT_COLUMNS,
    MODULE_DOCSTRING_TEMPLATE,
    classify_failure,
    collect_bounded_stream,
    export_candles_to_csv,
    load_runtime_config,
    mask_account_number,
    normalize_candles,
    redact_sensitive_mapping,
    redact_sensitive_value,
)


class DemoSharedConfigTest(unittest.TestCase):
    def test_load_runtime_config_requires_secret(self) -> None:
        with self.assertRaises(DemoConfigurationError):
            load_runtime_config(
                {
                    "Tasty_Refresh": "refresh-token",
                    "TASTYTRADE_ENV": "production",
                }
            )

    def test_load_runtime_config_rejects_non_production(self) -> None:
        with self.assertRaises(DemoConfigurationError):
            load_runtime_config(
                {
                    "Tasty_SECRET": "top-secret",
                    "Tasty_Refresh": "refresh-token",
                    "TASTYTRADE_ENV": "sandbox",
                }
            )

    def test_load_runtime_config_parses_expected_values(self) -> None:
        config = load_runtime_config(
            {
                "Tasty_SECRET": "top-secret",
                "Tasty_Refresh": "refresh-token",
                "TASTYTRADE_ENV": "production",
                "TASTYTRADE_TIMEOUT_SECONDS": "45.5",
                "TASTYTRADE_ACCOUNT_NUMBER": " 12345678 ",
            }
        )
        self.assertEqual(config.environment, "production")
        self.assertEqual(config.timeout_seconds, 45.5)
        self.assertEqual(config.account_number, "12345678")


class DemoSharedRedactionTest(unittest.TestCase):
    def test_mask_account_number(self) -> None:
        self.assertEqual(mask_account_number("AB-12345678"), "******5678")

    def test_redact_sensitive_value(self) -> None:
        self.assertEqual(redact_sensitive_value("secret-value"), "<redacted>")
        self.assertEqual(redact_sensitive_value(""), "<redacted-empty>")

    def test_redact_sensitive_mapping(self) -> None:
        redacted = redact_sensitive_mapping(
            {
                "client_secret": "secret",
                "refreshToken": "refresh",
                "account": "1234",
            }
        )
        self.assertEqual(redacted["client_secret"], "<redacted>")
        self.assertEqual(redacted["refreshToken"], "<redacted>")
        self.assertEqual(redacted["account"], "1234")


class DemoSharedFailureClassificationTest(unittest.TestCase):
    def test_classifies_failures(self) -> None:
        self.assertIsInstance(
            classify_failure(asyncio.TimeoutError(), "demo operation"),
            DemoTimeoutError,
        )
        self.assertIsInstance(
            classify_failure(RuntimeError("401 unauthorized"), "demo operation"),
            DemoAuthenticationError,
        )
        self.assertIsInstance(
            classify_failure(RuntimeError("403 forbidden"), "demo operation"),
            DemoPermissionError,
        )
        self.assertIsInstance(
            classify_failure(OSError("socket disconnected"), "demo operation"),
            DemoNetworkError,
        )


class DemoSharedStreamingTest(unittest.IsolatedAsyncioTestCase):
    async def test_collect_bounded_stream_stops_on_event_limit(self) -> None:
        async def stream():
            for value in range(5):
                await asyncio.sleep(0)
                yield value

        cleaned_up = False

        async def cleanup() -> None:
            nonlocal cleaned_up
            cleaned_up = True

        result = await collect_bounded_stream(
            stream(),
            event_limit=3,
            timeout_seconds=1.0,
            cleanup=cleanup,
        )
        self.assertEqual(result.events, [0, 1, 2])
        self.assertEqual(result.completion_state, "event_limit")
        self.assertTrue(cleaned_up)

    async def test_collect_bounded_stream_reports_timeout(self) -> None:
        async def stream():
            while True:
                await asyncio.sleep(0.2)
                yield 1

        result = await collect_bounded_stream(
            stream(),
            event_limit=10,
            timeout_seconds=0.05,
        )
        self.assertEqual(result.completion_state, "timeout")
        self.assertIsNotNone(result.error)


class DemoSharedCandleTest(unittest.TestCase):
    def test_normalize_candles_sorts_dedupes_and_filters(self) -> None:
        start = datetime(2026, 10, 3, 13, 0, tzinfo=timezone.utc)
        end = start + timedelta(minutes=5)
        candles = [
            {
                "symbol": "SPY",
                "timestamp": (start + timedelta(minutes=2)).isoformat(),
                "open": "100.1",
                "high": "100.3",
                "low": "99.9",
                "close": "100.0",
                "volume": 100,
            },
            {
                "symbol": "SPY",
                "timestamp": (start + timedelta(minutes=2)).isoformat(),
                "open": "100.2",
                "high": "100.4",
                "low": "99.8",
                "close": "100.1",
                "volume": 101,
            },
            {
                "symbol": "SPY",
                "timestamp": start.isoformat(),
                "open": "99.0",
                "high": "100.0",
                "low": "98.9",
                "close": "99.8",
                "volume": 250,
            },
            {
                "symbol": "SPY",
                "timestamp": (end + timedelta(minutes=1)).isoformat(),
                "open": "101.0",
                "high": "101.2",
                "low": "100.8",
                "close": "101.1",
                "volume": 90,
            },
        ]
        normalized = normalize_candles(candles, start=start, end=end)
        self.assertEqual(len(normalized), 2)
        self.assertEqual(normalized[0]["timestamp"], start)
        self.assertEqual(normalized[1]["close"], Decimal("100.1"))
        self.assertEqual(normalized[1]["volume"], 101)

    def test_normalize_candles_requires_timezone(self) -> None:
        with self.assertRaises(DemoConfigurationError):
            normalize_candles(
                [
                    {
                        "symbol": "SPY",
                        "timestamp": "2026-10-03T13:00:00",
                        "open": "100",
                        "high": "101",
                        "low": "99",
                        "close": "100",
                        "volume": 10,
                    }
                ]
            )

    def test_export_candles_to_csv(self) -> None:
        rows = [
            {
                "symbol": "SPY",
                "timestamp": "2026-10-03T13:00:00+00:00",
                "open": "100",
                "high": "101",
                "low": "99",
                "close": "100.5",
                "volume": 10,
            }
        ]
        with tempfile.TemporaryDirectory() as directory:
            output = export_candles_to_csv(rows, Path(directory) / "candles.csv")
            with output.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.reader(handle)
                all_rows = list(reader)
        self.assertEqual(tuple(all_rows[0]), EXPORT_COLUMNS)
        self.assertEqual(all_rows[1][0], "SPY")
        self.assertEqual(all_rows[1][1], "2026-10-03T13:00:00+00:00")


class DemoSharedTemplateTest(unittest.TestCase):
    def test_module_docstring_template_contains_required_sections(self) -> None:
        for marker in (
            "Upstream link",
            "Tested versions",
            "PowerShell command",
            "Inputs",
            "Safety mode",
            "Expected redacted output",
            "Common failures",
        ):
            self.assertIn(marker, MODULE_DOCSTRING_TEMPLATE)


if __name__ == "__main__":
    unittest.main()
