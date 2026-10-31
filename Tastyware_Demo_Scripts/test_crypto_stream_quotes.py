"""Credential-free tests for the crypto timestamp investigation demo."""

import importlib
import io
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


demo = importlib.import_module("Tastyware_Demo_Scripts.06_1_Crypto_Stream_Quotes")


class CryptoStreamQuotesTests(unittest.IsolatedAsyncioTestCase):
    async def test_resolves_metadata_in_requested_order(self) -> None:
        instruments = [
            SimpleNamespace(symbol="ETH/USD", active=True, streamer_symbol="ETH/USD:CXTALP"),
            SimpleNamespace(symbol="BTC/USD", active=True, streamer_symbol="BTC/USD:CXTALP"),
        ]
        session = object()
        with (
            patch.object(
                demo.quote_demo, "run_callable_with_timeout",
                new=AsyncMock(return_value=instruments),
            ) as lookup,
            redirect_stdout(io.StringIO()) as output,
        ):
            result = await demo.resolve_streamer_symbols(
                session, ["BTC/USD", "ETH/USD"], 12
            )
        self.assertEqual(result, ["BTC/USD:CXTALP", "ETH/USD:CXTALP"])
        self.assertIn("crypto_symbol=BTC/USD streamer_symbol=BTC/USD:CXTALP", output.getvalue())
        lookup.assert_awaited_once_with(
            "cryptocurrency instrument lookup",
            demo.Cryptocurrency.get, 12, session, ["BTC/USD", "ETH/USD"],
        )

    async def test_rejects_unavailable_instruments(self) -> None:
        for instruments in (
            [],
            [SimpleNamespace(symbol="BTC/USD", active=False, streamer_symbol="BTC/USD:CXTALP")],
            [SimpleNamespace(symbol="BTC/USD", active=True, streamer_symbol="")],
        ):
            with (
                self.subTest(instruments=instruments),
                patch.object(
                    demo.quote_demo, "run_callable_with_timeout",
                    new=AsyncMock(return_value=instruments),
                ),
                self.assertRaisesRegex(demo.quote_demo.DemoConfigurationError, "BTC/USD"),
            ):
                await demo.resolve_streamer_symbols(object(), ["BTC/USD"], 10)

    async def test_lookup_failure_is_not_swallowed(self) -> None:
        error = demo.quote_demo.DemoHelperError("lookup failed")
        with (
            patch.object(
                demo.quote_demo, "run_callable_with_timeout",
                new=AsyncMock(side_effect=error),
            ),
            self.assertRaises(demo.quote_demo.DemoHelperError),
        ):
            await demo.resolve_streamer_symbols(object(), ["BTC/USD"], 10)

    async def test_main_reports_configuration_failure(self) -> None:
        with (
            patch.object(
                demo, "stream_crypto_quotes",
                new=AsyncMock(side_effect=demo.quote_demo.DemoConfigurationError("missing config")),
            ),
            redirect_stdout(io.StringIO()) as output,
        ):
            self.assertEqual(await demo.main(), 1)
        self.assertIn("Crypto quote stream failed: missing config", output.getvalue())


if __name__ == "__main__":
    unittest.main()
