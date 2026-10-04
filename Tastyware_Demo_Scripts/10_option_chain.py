"""Inspect option-chain contracts with bounded output and expiration filtering.

Upstream link:
- <https://tastyworks-api.readthedocs.io/en/latest/instruments.html>

Tested versions:
- CPython 3.14.8 free-threaded (3.14t)
- tastytrade 13.2.3

PowerShell command (run from repository root):
- .\\.venv\\Scripts\\python.exe .\\Tastyware_Demo_Scripts\\10_option_chain.py

Inputs:
- Tasty_SECRET (required, env var)
- Tasty_Refresh (required, env var)
- TASTYTRADE_ENV=production (required)
- TASTYTRADE_UNDERLYING (optional, default SPY)
- TASTYTRADE_OPTION_EXPIRATION (optional, YYYY-MM-DD)
- TASTYTRADE_OPTION_LIMIT (optional, default 20)
- TASTYTRADE_TIMEOUT_SECONDS (optional, default 30)
"""

from __future__ import annotations

import asyncio
import os
from datetime import date

from tastytrade.instruments import get_option_chain
from tastytrade.session import Session

from Tastyware_Demo_Scripts.demo_shared import (
    DemoConfigurationError,
    DemoHelperError,
    load_runtime_config,
    managed_async_resource,
    parse_positive_int,
    run_with_timeout,
)


def parse_expiration(value: str | None) -> date | None:
    if value is None or not value.strip():
        return None
    try:
        return date.fromisoformat(value.strip())
    except ValueError as exc:
        raise DemoConfigurationError("TASTYTRADE_OPTION_EXPIRATION must be YYYY-MM-DD.") from exc


async def show_option_chain() -> int:
    config = load_runtime_config()
    underlying = (os.environ.get("TASTYTRADE_UNDERLYING", "SPY").strip() or "SPY").upper()
    expiration = parse_expiration(os.environ.get("TASTYTRADE_OPTION_EXPIRATION"))
    option_limit = parse_positive_int(
        os.environ.get("TASTYTRADE_OPTION_LIMIT"),
        name="TASTYTRADE_OPTION_LIMIT",
        default=20,
    )

    session = Session(provider_secret=config.client_secret, refresh_token=config.refresh_token)
    async with managed_async_resource(session):
        chain = await run_with_timeout(
            "option chain request",
            asyncio.to_thread(get_option_chain, session, underlying),
            config.timeout_seconds,
        )

    if expiration is not None:
        if expiration not in chain:
            available = ", ".join(sorted(exp.isoformat() for exp in chain.keys()))
            raise DemoConfigurationError(
                f"Requested expiration {expiration.isoformat()} is unavailable. Available expirations: {available}"
            )
        selected_expirations = [expiration]
    else:
        selected_expirations = sorted(chain.keys())

    output_count = 0
    print(f"underlying={underlying}")
    for exp in selected_expirations:
        options = chain.get(exp, [])
        print(f"expiration={exp.isoformat()} contracts={len(options)}")
        for option in options:
            print(
                f"- symbol={option.symbol} type={option.option_type} strike={option.strike_price} "
                f"streamer_symbol={option.streamer_symbol}"
            )
            output_count += 1
            if output_count >= option_limit:
                print(f"output_limit_reached={option_limit}")
                return 0
    return 0


async def main() -> int:
    try:
        return await show_option_chain()
    except (DemoConfigurationError, DemoHelperError) as exc:
        print(f"Option-chain request failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
