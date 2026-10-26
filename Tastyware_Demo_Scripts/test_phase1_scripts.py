"""Credential-free tests for Phase 1 connection and account scripts."""

from __future__ import annotations

import asyncio
import importlib
import unittest
from dataclasses import dataclass, replace
from io import StringIO
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from Tastyware_Demo_Scripts.demo_shared import (
    DemoConfigurationError,
    load_runtime_config,
    mask_account_number,
    require_selected_account,
)


SCRIPT_02 = importlib.import_module("Tastyware_Demo_Scripts.02_list_accounts")
SCRIPT_03 = importlib.import_module("Tastyware_Demo_Scripts.03_account_balances")
SCRIPT_04 = importlib.import_module("Tastyware_Demo_Scripts.04_current_positions")


@dataclass
class FakeAccount:
    account_number: str
    nickname: str = ""


@dataclass
class FakePosition:
    quantity: int
    quantity_direction: str | None = None


class Phase1SelectionAndFormattingTest(unittest.TestCase):
    def test_require_selected_account_for_multiple_accounts(self) -> None:
        accounts = [FakeAccount("ABC12345"), FakeAccount("XYZ99999")]
        with self.assertRaises(DemoConfigurationError):
            require_selected_account(accounts, None)

    def test_require_selected_account_accepts_explicit_value(self) -> None:
        accounts = [FakeAccount("ABC12345"), FakeAccount("XYZ99999")]
        selected = require_selected_account(accounts, "XYZ99999")
        self.assertEqual(selected.account_number, "XYZ99999")

    def test_account_summary_lines_are_masked(self) -> None:
        lines = SCRIPT_02.format_account_summary_lines(
            [FakeAccount("ABC12345", "Main"), FakeAccount("ZZ001122", "")]
        )
        self.assertEqual(lines[0], f"- account={mask_account_number('ABC12345')} nickname=Main")
        self.assertEqual(lines[1], f"- account={mask_account_number('ZZ001122')}")

    def test_balance_formatter_labels_missing_values(self) -> None:
        self.assertEqual(SCRIPT_03._format_value(None), "unavailable")
        self.assertEqual(SCRIPT_03._format_value("100"), "100")

    def test_position_direction_falls_back_to_quantity_sign(self) -> None:
        self.assertEqual(SCRIPT_04._direction(FakePosition(quantity=5)), "long")
        self.assertEqual(SCRIPT_04._direction(FakePosition(quantity=-2)), "short")


class Phase1BalanceLookupTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.config = load_runtime_config(
            {
                "Tasty_SECRET": "test-secret",
                "Tasty_Refresh": "test-refresh",
                "TASTYTRADE_ENV": "production",
            }
        )
        self.session = SimpleNamespace(aclose=AsyncMock())
        self.balances = SimpleNamespace(
            cash_balance="100.00",
            equity_buying_power="200.00",
            net_liquidating_value="300.00",
            updated_at="2026-10-25T12:00:00Z",
            snapshot_date="2026-10-25",
        )
        self.account = SimpleNamespace(
            account_number="ABC12345",
            get_balances=AsyncMock(return_value=self.balances),
        )
        self.output = StringIO()
        self.enterContext(patch.object(SCRIPT_03, "load_runtime_config", return_value=self.config))
        self.enterContext(patch.object(SCRIPT_03, "Session", return_value=self.session))
        self.enterContext(
            patch.object(SCRIPT_03.Account, "get", AsyncMock(return_value=[self.account]))
        )
        self.enterContext(patch("sys.stdout", self.output))

    def assert_balance_output(self) -> None:
        self.assertEqual(
            self.output.getvalue().splitlines(),
            [
                f"account={mask_account_number('ABC12345')}",
                "cash_balance=100.00",
                "equity_buying_power=200.00",
                "net_liquidating_value=300.00",
                "updated_at=2026-10-25T12:00:00Z",
                "snapshot_date=2026-10-25",
            ],
        )
        self.assertNotIn("ABC12345", self.output.getvalue())
        self.session.aclose.assert_awaited_once_with()

    async def test_awaits_async_balance_lookup(self) -> None:
        self.assertEqual(await SCRIPT_03.main(), 0)
        self.account.get_balances.assert_awaited_once_with(self.session)
        self.assert_balance_output()

    async def test_supports_sync_balance_lookup(self) -> None:
        self.account.get_balances = Mock(return_value=self.balances)
        self.assertEqual(await SCRIPT_03.main(), 0)
        self.account.get_balances.assert_called_once_with(self.session)
        self.assert_balance_output()

    async def test_reports_async_balance_lookup_failure_and_closes_session(self) -> None:
        self.account.get_balances.side_effect = OSError("connection failed")
        self.assertEqual(await SCRIPT_03.main(), 1)
        self.assertEqual(
            self.output.getvalue(),
            "Balance lookup failed: balance lookup failed due to a network transport error.\n",
        )
        self.account.get_balances.assert_awaited_once_with(self.session)
        self.session.aclose.assert_awaited_once_with()

    async def test_times_out_async_balance_lookup_and_closes_session(self) -> None:
        async def slow_balances(session: object) -> None:
            await asyncio.sleep(60)

        self.account.get_balances.side_effect = slow_balances
        with patch.object(
            SCRIPT_03,
            "load_runtime_config",
            return_value=replace(self.config, timeout_seconds=0.01),
        ):
            self.assertEqual(await SCRIPT_03.main(), 1)
        self.assertEqual(
            self.output.getvalue(),
            "Balance lookup failed: balance lookup timed out before completion.\n",
        )
        self.account.get_balances.assert_awaited_once_with(self.session)
        self.session.aclose.assert_awaited_once_with()


class Phase1PositionLookupTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.config = load_runtime_config(
            {
                "Tasty_SECRET": "test-secret",
                "Tasty_Refresh": "test-refresh",
                "TASTYTRADE_ENV": "production",
            }
        )
        self.session = SimpleNamespace(aclose=AsyncMock())
        self.positions = [
            SimpleNamespace(
                symbol="SPY",
                instrument_type=SimpleNamespace(value="Equity"),
                quantity=2,
                average_open_price="100.00",
            )
        ]
        self.account = SimpleNamespace(
            account_number="ABC12345",
            get_positions=AsyncMock(return_value=self.positions),
        )
        self.output = StringIO()
        self.enterContext(patch.object(SCRIPT_04, "load_runtime_config", return_value=self.config))
        self.enterContext(patch.object(SCRIPT_04, "Session", return_value=self.session))
        self.enterContext(
            patch.object(SCRIPT_04.Account, "get", AsyncMock(return_value=[self.account]))
        )
        self.enterContext(patch("sys.stdout", self.output))

    def assert_position_output(self) -> None:
        self.assertEqual(
            self.output.getvalue().splitlines(),
            [
                f"account={mask_account_number('ABC12345')}",
                "position_count=1",
                "- symbol=SPY instrument_type=Equity quantity=2 direction=long average_open_price=100.00",
            ],
        )
        self.assertNotIn("ABC12345", self.output.getvalue())
        self.session.aclose.assert_awaited_once_with()

    async def test_awaits_async_position_lookup(self) -> None:
        self.assertEqual(await SCRIPT_04.main(), 0)
        self.account.get_positions.assert_awaited_once_with(self.session)
        self.assert_position_output()

    async def test_supports_sync_position_lookup(self) -> None:
        self.account.get_positions = Mock(return_value=self.positions)
        self.assertEqual(await SCRIPT_04.main(), 0)
        self.account.get_positions.assert_called_once_with(self.session)
        self.assert_position_output()

    async def test_handles_empty_async_positions(self) -> None:
        self.account.get_positions.return_value = []
        self.assertEqual(await SCRIPT_04.main(), 0)
        self.assertEqual(
            self.output.getvalue().splitlines(),
            [
                f"account={mask_account_number('ABC12345')}",
                "No open positions were returned for this account.",
            ],
        )
        self.account.get_positions.assert_awaited_once_with(self.session)
        self.session.aclose.assert_awaited_once_with()

    async def test_reports_async_position_lookup_failure_and_closes_session(self) -> None:
        self.account.get_positions.side_effect = OSError("connection failed")
        self.assertEqual(await SCRIPT_04.main(), 1)
        self.assertEqual(
            self.output.getvalue(),
            "Position lookup failed: position lookup failed due to a network transport error.\n",
        )
        self.account.get_positions.assert_awaited_once_with(self.session)
        self.session.aclose.assert_awaited_once_with()

    async def test_times_out_async_position_lookup_and_closes_session(self) -> None:
        async def slow_positions(session: object) -> None:
            await asyncio.sleep(60)

        self.account.get_positions.side_effect = slow_positions
        with patch.object(
            SCRIPT_04,
            "load_runtime_config",
            return_value=replace(self.config, timeout_seconds=0.01),
        ):
            self.assertEqual(await SCRIPT_04.main(), 1)
        self.assertEqual(
            self.output.getvalue(),
            "Position lookup failed: position lookup timed out before completion.\n",
        )
        self.account.get_positions.assert_awaited_once_with(self.session)
        self.session.aclose.assert_awaited_once_with()


if __name__ == "__main__":
    unittest.main()
