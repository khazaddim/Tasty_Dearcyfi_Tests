"""Print quote updates as received with bounded collection and completion states.

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

from time import perf_counter

_START_TIME = perf_counter()


def _log_timing(message: str) -> None:
    print(f"[{perf_counter() - _START_TIME:8.3f}s] {message}", flush=True)


_log_timing("Importing asyncio...")
import asyncio
from collections.abc import AsyncIterator

_log_timing("Imported asyncio; importing os...")
import os

_log_timing("Imported os; importing tastytrade.dxfeed.Quote...")
from tastytrade.dxfeed import Quote

_log_timing("Imported Quote; importing tastytrade.session.Session...")
from tastytrade.session import Session

_log_timing("Imported Session; importing tastytrade.streamer.DXLinkStreamer...")
from tastytrade.streamer import DXLinkStreamer

_log_timing("Imported DXLinkStreamer; importing demo_shared helpers...")

try:
    from Tastyware_Demo_Scripts.demo_shared import (
        DemoConfigurationError,
        DemoHelperError,
        collect_bounded_stream,
        load_runtime_config,
        managed_async_resource,
        parse_csv_symbols,
        parse_positive_int,
        parse_timeout_seconds,
        run_callable_with_timeout,
    )
except ModuleNotFoundError:
    from demo_shared import (
        DemoConfigurationError,
        DemoHelperError,
        collect_bounded_stream,
        load_runtime_config,
        managed_async_resource,
        parse_csv_symbols,
        parse_positive_int,
        parse_timeout_seconds,
        run_callable_with_timeout,
    )

_log_timing("All imports complete.")


async def stream_quotes() -> int:
    _log_timing("Loading runtime configuration...")
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

    _log_timing("Configuration loaded; creating Session...")
    session = Session(provider_secret=config.client_secret, refresh_token=config.refresh_token)
    _log_timing("Session created; entering session context...")
    async with managed_async_resource(session):
        _log_timing("Session context ready.")
        streamer = await run_streamer(session, symbols, event_limit, stream_timeout)
        _log_timing("Closing session context...")
    _log_timing("Session closed; printing summary...")

    print(f"symbols={','.join(symbols)}")
    print(f"event_count={len(streamer.events)}")
    print(f"completion_state={streamer.completion_state}")
    if streamer.error is not None:
        print(f"error={streamer.error}")
    return 0


async def run_streamer(
    session: Session,
    symbols: list[str],
    event_limit: int,
    timeout_seconds: float,
):
    _log_timing("Creating DXLinkStreamer...")
    streamer = DXLinkStreamer(session)
    _log_timing("DXLinkStreamer created; connecting streamer context...")
    async with managed_async_resource(streamer):
        _log_timing("Streamer connected; subscribing to quotes...")
        await run_callable_with_timeout(
            "quote subscription",
            streamer.subscribe,
            timeout_seconds,
            Quote,
            symbols,
        )
        _log_timing("Quote subscription complete.")

        async def cleanup() -> None:
            _log_timing("Quote collection ended; unsubscribing...")
            await run_callable_with_timeout(
                "quote unsubscription",
                streamer.unsubscribe,
                timeout_seconds,
                Quote,
                symbols,
            )
            _log_timing("Quote unsubscription complete.")

        async def live_quotes() -> AsyncIterator[Quote]:
            previous_received_at: float | None = None
            async for event in streamer.listen(Quote):
                received_at = perf_counter()
                delta_ms = (
                    "N/A" if previous_received_at is None
                    else f"{(received_at - previous_received_at) * 1000:.3f}"
                )
                previous_received_at = received_at
                print(
                    f"- symbol={event.event_symbol} bid={event.bid_price} ask={event.ask_price} "
                    f"bid_size={event.bid_size} ask_size={event.ask_size} "
                    f"event_time={event.event_time} bid_time={event.bid_time} "
                    f"ask_time={event.ask_time} time_nano_part={event.time_nano_part} "
                    f"delta_ms={delta_ms}",
                    flush=True,
                )
                yield event

        _log_timing(
            f"Collecting up to {event_limit} quote events "
            f"(collection timeout={timeout_seconds:g}s)..."
        )
        result = await collect_bounded_stream(
            live_quotes(),
            event_limit=event_limit,
            timeout_seconds=timeout_seconds,
            cleanup=cleanup,
        )
        _log_timing(
            f"Collection finished: {len(result.events)} events, "
            f"state={result.completion_state}; closing streamer context..."
        )
    _log_timing("Streamer context closed.")
    return result


async def main() -> int:
    try:
        return await stream_quotes()
    except (DemoConfigurationError, DemoHelperError) as exc:
        print(f"Quote stream failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
