"""Credential-free tests for Phase 3 option and order-preview scripts."""

from __future__ import annotations

import asyncio
import importlib
import os
import unittest
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from io import StringIO
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from Tastyware_Demo_Scripts.demo_shared import (
    DemoConfigurationError,
    DemoNetworkError,
    DemoTimeoutError,
    load_runtime_config,
)


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


class Phase3AsyncSdkTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.config = load_runtime_config({
            "Tasty_SECRET": "test", "Tasty_Refresh": "test",
            "TASTYTRADE_ENV": "production",
        })
        self.session = SimpleNamespace(aclose=AsyncMock())
        self.chain = {
            date(2026, 11, 20): [
                SimpleNamespace(
                    symbol="OPTION", streamer_symbol="STREAMER",
                    option_type="Call", strike_price=Decimal("500"),
                )
            ]
        }
        self.output = StringIO()
        self.enterContext(patch("sys.stdout", self.output))
        self.enterContext(patch.dict(os.environ, {
            "TASTYTRADE_UNDERLYING": "SPY",
            "TASTYTRADE_OPTION_EXPIRATION": "",
            "TASTYTRADE_OPTION_LIMIT": "1",
            "TASTYTRADE_OPTION_CONTRACT_LIMIT": "1",
            "TASTYTRADE_EVENT_LIMIT": "1",
            "TASTYTRADE_STREAM_TIMEOUT_SECONDS": "0.02",
        }))
        for script in (SCRIPT_10, SCRIPT_11):
            self.enterContext(patch.object(script, "load_runtime_config", return_value=self.config))
            self.enterContext(patch.object(script, "Session", return_value=self.session))

    async def test_awaits_option_chain_and_preserves_output_limit(self) -> None:
        with patch.object(SCRIPT_10, "get_option_chain", AsyncMock(return_value=self.chain)) as lookup:
            self.assertEqual(await SCRIPT_10.main(), 0)
        lookup.assert_awaited_once_with(self.session, "SPY")
        self.assertIn("- symbol=OPTION", self.output.getvalue())
        self.assertIn("output_limit_reached=1", self.output.getvalue())
        self.session.aclose.assert_awaited_once_with()

    async def test_reports_async_option_chain_errors_and_closes_session(self) -> None:
        for script in (SCRIPT_10, SCRIPT_11):
            with self.subTest(script=script.__name__):
                self.session.aclose.reset_mock()
                with patch.object(script, "get_option_chain", AsyncMock(side_effect=OSError())):
                    self.assertEqual(await script.main(), 1)
                self.session.aclose.assert_awaited_once_with()
        self.assertIn("network transport error", self.output.getvalue())

    async def test_greeks_awaits_subscription_and_cleanup_for_all_collection_outcomes(self) -> None:
        for outcome in ("event_limit", "stream_complete", "timeout", "failed"):
            with self.subTest(outcome=outcome):
                self.session.aclose.reset_mock()
                streamer = MagicMock()
                streamer.__aenter__.return_value = streamer
                streamer.subscribe = AsyncMock()
                streamer.unsubscribe = AsyncMock()

                async def events():
                    if outcome == "timeout":
                        await asyncio.sleep(60)
                    if outcome == "failed":
                        raise OSError()
                    if outcome == "stream_complete":
                        return
                    yield SimpleNamespace(
                        event_symbol="STREAMER", delta=1, gamma=2, theta=3,
                        vega=4, rho=5, volatility=6,
                    )

                streamer.listen.return_value = events()
                with (
                    patch.object(SCRIPT_11, "get_option_chain", AsyncMock(return_value=self.chain)) as lookup,
                    patch.object(SCRIPT_11, "DXLinkStreamer", return_value=streamer),
                ):
                    self.assertEqual(await SCRIPT_11.main(), 0)
                lookup.assert_awaited_once_with(self.session, "SPY")
                streamer.subscribe.assert_awaited_once_with(SCRIPT_11.Greeks, ["STREAMER"])
                streamer.unsubscribe.assert_awaited_once_with(SCRIPT_11.Greeks, ["STREAMER"])
                streamer.__aexit__.assert_awaited_once()
                self.session.aclose.assert_awaited_once_with()
                self.assertIn(f"completion_state={outcome}", self.output.getvalue())

    async def test_greeks_subscription_and_cleanup_errors_close_resources(self) -> None:
        for operation in ("subscribe", "unsubscribe"):
            with self.subTest(operation=operation):
                self.session.aclose.reset_mock()
                streamer = MagicMock()
                streamer.__aenter__.return_value = streamer
                streamer.subscribe = AsyncMock()
                streamer.unsubscribe = AsyncMock()
                getattr(streamer, operation).side_effect = OSError()

                async def events():
                    return
                    yield object()

                streamer.listen.return_value = events()
                with (
                    patch.object(SCRIPT_11, "get_option_chain", AsyncMock(return_value=self.chain)),
                    patch.object(SCRIPT_11, "DXLinkStreamer", return_value=streamer),
                ):
                    self.assertEqual(await SCRIPT_11.main(), 1)
                streamer.__aexit__.assert_awaited_once()
                self.session.aclose.assert_awaited_once_with()
        self.assertIn("network transport error", self.output.getvalue())

    async def test_async_order_preview_awaits_response_and_forces_dry_run(self) -> None:
        session = object()
        response = SimpleNamespace(buying_power_effect="effect")
        lookup = AsyncMock(return_value=response)
        order_input = SCRIPT_12.DryRunOrderInput(
            symbol="SPY", quantity=1, limit_price=Decimal("500"),
            action=SCRIPT_12.OrderAction.BUY,
        )
        result = await SCRIPT_12.preview_dry_run_order(
            SimpleNamespace(place_order=lookup), session, order_input, 1.0,
        )
        self.assertIs(result, response)
        lookup.assert_awaited_once()
        args, kwargs = lookup.await_args
        self.assertIs(args[0], session)
        self.assertEqual(kwargs, {"dry_run": True})
        self.assertEqual(args[1].price, Decimal("500"))
        self.assertEqual(args[1].legs[0].symbol, "SPY")
        self.assertEqual(args[1].legs[0].quantity, 1)

    async def test_async_order_preview_classifies_failure_and_timeout(self) -> None:
        async def slow_preview(*args: object, **kwargs: object) -> None:
            await asyncio.sleep(60)

        order_input = SCRIPT_12.DryRunOrderInput(
            symbol="SPY", quantity=1, limit_price=Decimal("500"),
            action=SCRIPT_12.OrderAction.BUY,
        )
        for side_effect, error in ((OSError(), DemoNetworkError), (slow_preview, DemoTimeoutError)):
            with self.subTest(error=error):
                lookup = AsyncMock(side_effect=side_effect)
                with self.assertRaises(error):
                    await SCRIPT_12.preview_dry_run_order(
                        SimpleNamespace(place_order=lookup), object(), order_input, 0.01,
                    )
                lookup.assert_awaited_once()
                self.assertEqual(lookup.await_args.kwargs, {"dry_run": True})


if __name__ == "__main__":
    unittest.main()
