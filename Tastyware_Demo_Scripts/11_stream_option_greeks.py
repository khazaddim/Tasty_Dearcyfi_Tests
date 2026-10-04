"""Stream bounded option Greeks using actual contract streamer symbols.

Upstream link:
- <https://github.com/tastyware/tastytrade#readme>

Tested versions:
- CPython 3.14.8 free-threaded (3.14t)
- tastytrade 13.2.3

PowerShell command (run from repository root):
- .\\.venv\\Scripts\\python.exe .\\Tastyware_Demo_Scripts\\11_stream_option_greeks.py

Inputs:
- Tasty_SECRET (required, env var)
- Tasty_Refresh (required, env var)
- TASTYTRADE_ENV=production (required)
- TASTYTRADE_UNDERLYING (optional, default SPY)
- TASTYTRADE_OPTION_EXPIRATION (optional, YYYY-MM-DD)
- TASTYTRADE_OPTION_CONTRACT_LIMIT (optional, default 2)
- TASTYTRADE_EVENT_LIMIT (optional, default 10)
- TASTYTRADE_STREAM_TIMEOUT_SECONDS (optional, default request timeout)
"""

from __future__ import annotations

import asyncio
import os
from datetime import date

from tastytrade.dxfeed import Greeks
from tastytrade.instruments import get_option_chain
from tastytrade.session import Session
from tastytrade.streamer import DXLinkStreamer

from Tastyware_Demo_Scripts.demo_shared import (
    DemoConfigurationError,
    DemoHelperError,
    collect_bounded_stream,
    load_runtime_config,
    managed_async_resource,
    parse_positive_int,
    parse_timeout_seconds,
    run_with_timeout,
)


def parse_expiration(value: str | None) -> date | None:
    if value is None or not value.strip():
        return None
    try:
        return date.fromisoformat(value.strip())
    except ValueError as exc:
        raise DemoConfigurationError("TASTYTRADE_OPTION_EXPIRATION must be YYYY-MM-DD.") from exc


def pick_streamer_symbols(chain: dict[date, list[object]], expiration: date | None, limit: int) -> list[str]:
    expirations = [expiration] if expiration is not None else sorted(chain.keys())
    symbols: list[str] = []
    for exp in expirations:
        if exp not in chain:
            continue
        for option in chain[exp]:
            streamer_symbol = str(getattr(option, "streamer_symbol", "") or "").strip()
            if not streamer_symbol:
                continue
            symbols.append(streamer_symbol)
            if len(symbols) >= limit:
                return symbols
    if not symbols:
        raise DemoConfigurationError("No streamer symbols were available for the requested option selection.")
    return symbols


async def stream_option_greeks() -> int:
    config = load_runtime_config()
    underlying = (os.environ.get("TASTYTRADE_UNDERLYING", "SPY").strip() or "SPY").upper()
    expiration = parse_expiration(os.environ.get("TASTYTRADE_OPTION_EXPIRATION"))
    contract_limit = parse_positive_int(
        os.environ.get("TASTYTRADE_OPTION_CONTRACT_LIMIT"),
        name="TASTYTRADE_OPTION_CONTRACT_LIMIT",
        default=2,
    )
    event_limit = parse_positive_int(
        os.environ.get("TASTYTRADE_EVENT_LIMIT"),
        name="TASTYTRADE_EVENT_LIMIT",
        default=10,
    )
    timeout_seconds = parse_timeout_seconds(
        os.environ.get("TASTYTRADE_STREAM_TIMEOUT_SECONDS"),
        default=config.timeout_seconds,
    )

    session = Session(provider_secret=config.client_secret, refresh_token=config.refresh_token)
    async with managed_async_resource(session):
        chain = await run_with_timeout(
            "option chain request",
            asyncio.to_thread(get_option_chain, session, underlying),
            config.timeout_seconds,
        )
        streamer_symbols = pick_streamer_symbols(chain, expiration, contract_limit)
        streamer = DXLinkStreamer(session)
        async with managed_async_resource(streamer):
            await asyncio.to_thread(streamer.subscribe, Greeks, streamer_symbols)

            async def cleanup() -> None:
                await asyncio.to_thread(streamer.unsubscribe, Greeks, streamer_symbols)

            result = await collect_bounded_stream(
                streamer.listen(Greeks),
                event_limit=event_limit,
                timeout_seconds=timeout_seconds,
                cleanup=cleanup,
            )

    print(f"underlying={underlying}")
    print(f"streamer_symbols={','.join(streamer_symbols)}")
    print(f"event_count={len(result.events)}")
    print(f"completion_state={result.completion_state}")
    if result.error is not None:
        print(f"error={result.error}")
    for event in result.events:
        print(
            f"- symbol={event.event_symbol} delta={event.delta} gamma={event.gamma} "
            f"theta={event.theta} vega={event.vega} rho={event.rho} volatility={event.volatility}"
        )
    return 0


async def main() -> int:
    try:
        return await stream_option_greeks()
    except (DemoConfigurationError, DemoHelperError) as exc:
        print(f"Option Greeks stream failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
