"""Collect bounded quote-stream updates with explicit completion states.

Upstream link:
- <https://tastyworks-api.readthedocs.io/en/latest/data-streamer.html>

Tested versions:
- CPython 3.14.8 free-threaded (3.14t)
- tastytrade 13.2.3

PowerShell command (run from repository root):
- .\\.venv\\Scripts\\python.exe .\\Tastyware_Demo_Scripts\\06_stream_quotes.py

Inputs:
- Tasty_SECRET (required, env var)
- Tasty_Refresh (required, env var)
- TASTYTRADE_ENV=production (required)
- TASTYTRADE_SYMBOLS (optional comma-separated symbols, default SPY)
- TASTYTRADE_EVENT_LIMIT (optional, default 10)
- TASTYTRADE_STREAM_TIMEOUT_SECONDS (optional, default request timeout)
"""

from __future__ import annotations

import asyncio
import os

from tastytrade.dxfeed import Quote
from tastytrade.session import Session
from tastytrade.streamer import DXLinkStreamer

from Tastyware_Demo_Scripts.demo_shared import (
    DemoConfigurationError,
    DemoHelperError,
    collect_bounded_stream,
    load_runtime_config,
    managed_async_resource,
    parse_csv_symbols,
    parse_positive_int,
    parse_timeout_seconds,
)


async def stream_quotes() -> int:
    config = load_runtime_config()
    symbols = parse_csv_symbols(os.environ.get("TASTYTRADE_SYMBOLS"), default="SPY")
    event_limit = parse_positive_int(
        os.environ.get("TASTYTRADE_EVENT_LIMIT"),
        name="TASTYTRADE_EVENT_LIMIT",
        default=10,
    )
    stream_timeout = parse_timeout_seconds(
        os.environ.get("TASTYTRADE_STREAM_TIMEOUT_SECONDS"),
        default=config.timeout_seconds,
    )

    session = Session(provider_secret=config.client_secret, refresh_token=config.refresh_token)
    async with managed_async_resource(session):
        streamer = await run_streamer(session, symbols, event_limit, stream_timeout)

    print(f"symbols={','.join(symbols)}")
    print(f"event_count={len(streamer.events)}")
    print(f"completion_state={streamer.completion_state}")
    if streamer.error is not None:
        print(f"error={streamer.error}")
    for event in streamer.events:
        print(
            f"- symbol={event.event_symbol} bid={event.bid_price} ask={event.ask_price} "
            f"bid_size={event.bid_size} ask_size={event.ask_size}"
        )
    return 0


async def run_streamer(
    session: Session,
    symbols: list[str],
    event_limit: int,
    timeout_seconds: float,
):
    streamer = DXLinkStreamer(session)
    async with managed_async_resource(streamer):
        await asyncio.to_thread(streamer.subscribe, Quote, symbols)

        async def cleanup() -> None:
            await asyncio.to_thread(streamer.unsubscribe, Quote, symbols)

        result = await collect_bounded_stream(
            streamer.listen(Quote),
            event_limit=event_limit,
            timeout_seconds=timeout_seconds,
            cleanup=cleanup,
        )
    return result


async def main() -> int:
    try:
        return await stream_quotes()
    except (DemoConfigurationError, DemoHelperError) as exc:
        print(f"Quote stream failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
