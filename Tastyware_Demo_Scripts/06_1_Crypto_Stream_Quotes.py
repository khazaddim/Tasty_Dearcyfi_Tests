"""Investigate spot-crypto Quote timestamps using the same streamer as demo 06.

Run from repository root:
    .\\.venv\\Scripts\\python.exe .\\Tastyware_Demo_Scripts\\06_1_Crypto_Stream_Quotes.py

Required: Tasty_SECRET, Tasty_Refresh, TASTYTRADE_ENV=production.
Optional: TASTYTRADE_CRYPTO_SYMBOLS (default BTC/USD,ETH/USD),
TASTYTRADE_EVENT_LIMIT (default 10), TASTYTRADE_STREAM_TIMEOUT_SECONDS.
TASTYTRADE_SYMBOLS is intentionally ignored. No orders or files are created.
"""

from __future__ import annotations

import asyncio
import importlib
import os

from tastytrade.instruments import Cryptocurrency
from tastytrade.session import Session


# Numeric demo filenames require importlib rather than an import statement.
quote_demo = importlib.import_module(
    ".06_stream_quotes" if __package__ else "06_stream_quotes",
    package=__package__ or None,
)


async def resolve_streamer_symbols(
    session: Session, symbols: list[str], timeout_seconds: float
) -> list[str]:
    instruments = await quote_demo.run_callable_with_timeout(
        "cryptocurrency instrument lookup",
        Cryptocurrency.get,
        timeout_seconds,
        session,
        symbols,
    )
    by_symbol = {instrument.symbol: instrument for instrument in instruments}
    streamer_symbols: list[str] = []
    for symbol in symbols:
        instrument = by_symbol.get(symbol)
        if instrument is None or not instrument.active or not instrument.streamer_symbol:
            raise quote_demo.DemoConfigurationError(
                f"No active cryptocurrency with a streaming symbol for {symbol}. "
                "Use instrument symbols such as BTC/USD, not futures or dxFeed symbols."
            )
        print(
            f"crypto_symbol={symbol} streamer_symbol={instrument.streamer_symbol}",
            flush=True,
        )
        if instrument.streamer_symbol not in streamer_symbols:
            streamer_symbols.append(instrument.streamer_symbol)
    return streamer_symbols


async def stream_crypto_quotes() -> int:
    quote_demo._log_timing("Loading crypto runtime configuration...")
    config = quote_demo.load_runtime_config()
    symbols = quote_demo.parse_csv_symbols(
        os.environ.get("TASTYTRADE_CRYPTO_SYMBOLS"), default="BTC/USD,ETH/USD"
    )
    event_limit = quote_demo.parse_positive_int(
        os.environ.get("TASTYTRADE_EVENT_LIMIT"),
        name="TASTYTRADE_EVENT_LIMIT",
        default=10,
    )
    stream_timeout = quote_demo.parse_timeout_seconds(
        os.environ.get("TASTYTRADE_STREAM_TIMEOUT_SECONDS"),
        default=config.timeout_seconds,
    )
    session = Session(provider_secret=config.client_secret, refresh_token=config.refresh_token)
    async with quote_demo.managed_async_resource(session):
        quote_demo._log_timing("Resolving spot-crypto streaming symbols...")
        streamer_symbols = await resolve_streamer_symbols(
            session, symbols, config.timeout_seconds
        )
        result = await quote_demo.run_streamer(
            session, streamer_symbols, event_limit, stream_timeout
        )

    print(f"symbols={','.join(streamer_symbols)}")
    print(f"event_count={len(result.events)}")
    print(f"completion_state={result.completion_state}")
    for symbol in streamer_symbols:
        events = [event for event in result.events if event.event_symbol == symbol]
        print(
            f"symbol={symbol} event_count={len(events)} "
            f"event_time_zero_count={sum(event.event_time == 0 for event in events)} "
            f"bid_time_zero_count={sum(event.bid_time == 0 for event in events)} "
            f"ask_time_zero_count={sum(event.ask_time == 0 for event in events)}"
        )
    print(
        "Zero feed timestamps are unknown, not local receipt times. "
        "delta_ms is local inter-arrival timing across all symbols. "
        "SDK Quote output alone does not establish whether timestamps were "
        "absent upstream or changed during decoding."
    )
    if result.error is not None:
        print(f"error={result.error}")
    return 0 if result.completion_state == "event_limit" else 1


async def main() -> int:
    try:
        return await stream_crypto_quotes()
    except (quote_demo.DemoConfigurationError, quote_demo.DemoHelperError) as exc:
        print(f"Crypto quote stream failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
