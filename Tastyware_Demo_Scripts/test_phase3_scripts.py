"""Credential-free tests for Phase 3 option and order-preview scripts."""

from __future__ import annotations

import asyncio
import importlib
import unittest
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from Tastyware_Demo_Scripts.demo_shared import DemoConfigurationError


SCRIPT_10 = importlib.import_module("Tastyware_Demo_Scripts.10_option_chain")
SCRIPT_11 = importlib.import_module("Tastyware_Demo_Scripts.11_stream_option_greeks")
SCRIPT_12 = importlib.import_module("Tastyware_Demo_Scripts.12_order_dry_run")


@dataclass
class FakeOption:
    streamer_symbol: str


class Phase3OptionValidationTest(unittest.TestCase):
    def test_option_expiration_validation(self) -> None:
        with self.assertRaises(DemoConfigurationError):
            SCRIPT_10.parse_expiration("2026/10/10")

    def test_pick_streamer_symbols_from_chain(self) -> None:
        chain = {
            date(2026, 10, 16): [FakeOption("SYM1"), FakeOption("SYM2")],
            date(2026, 11, 20): [FakeOption("SYM3")],
        }
        symbols = SCRIPT_11.pick_streamer_symbols(chain, None, 2)
        self.assertEqual(symbols, ["SYM1", "SYM2"])

    def test_pick_streamer_symbols_requires_available_symbols(self) -> None:
        chain = {date(2026, 10, 16): [FakeOption("")]}
        with self.assertRaises(DemoConfigurationError):
            SCRIPT_11.pick_streamer_symbols(chain, None, 1)


class Phase3DryRunTest(unittest.TestCase):
    def test_load_order_input_parses_decimal_quantity_and_symbol(self) -> None:
        parsed = SCRIPT_12.load_order_input(
            {
                "TASTYTRADE_ORDER_SYMBOL": "spy",
                "TASTYTRADE_ORDER_QUANTITY": "2",
                "TASTYTRADE_ORDER_LIMIT_PRICE": "502.25",
                "TASTYTRADE_ORDER_ACTION": "BUY",
            }
        )
        self.assertEqual(parsed.symbol, "SPY")
        self.assertEqual(parsed.quantity, 2)
        self.assertEqual(parsed.limit_price, Decimal("502.25"))

    def test_no_live_order_flag_in_input_schema(self) -> None:
        field_names = SCRIPT_12.DryRunOrderInput.__dataclass_fields__.keys()
        self.assertNotIn("live", field_names)
        self.assertNotIn("dry_run", field_names)

    def test_preview_dry_run_order_always_passes_true(self) -> None:
        calls: list[bool] = []

        class FakeAccount:
            def place_order(self, _session, _order, dry_run):
                calls.append(dry_run)
                return {"ok": True}

        async def run_test() -> None:
            order_input = SCRIPT_12.DryRunOrderInput(
                symbol="SPY",
                quantity=1,
                limit_price=Decimal("500.00"),
                action=SCRIPT_12.OrderAction.BUY,
            )
            await SCRIPT_12.preview_dry_run_order(
                FakeAccount(),
                object(),
                order_input,
                timeout_seconds=2.0,
            )

        asyncio.run(run_test())
        self.assertEqual(calls, [True])

    def test_preview_formatting_exposes_validation_and_fee_fields(self) -> None:
        class FakeResponse:
            buying_power_effect = "effect"
            fee_calculation = "fees"
            warnings = ["warn"]

        lines = SCRIPT_12.format_preview_response(FakeResponse())
        rendered = "\n".join(lines)
        self.assertIn("buying_power_effect=effect", rendered)
        self.assertIn("fee_calculation=fees", rendered)
        self.assertIn("validation=['warn']", rendered)


if __name__ == "__main__":
    unittest.main()

