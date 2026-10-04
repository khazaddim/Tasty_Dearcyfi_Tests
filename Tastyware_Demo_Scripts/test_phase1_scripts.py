"""Credential-free tests for Phase 1 connection and account scripts."""

from __future__ import annotations

import importlib
import unittest
from dataclasses import dataclass

from Tastyware_Demo_Scripts.demo_shared import (
    DemoConfigurationError,
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


if __name__ == "__main__":
    unittest.main()

