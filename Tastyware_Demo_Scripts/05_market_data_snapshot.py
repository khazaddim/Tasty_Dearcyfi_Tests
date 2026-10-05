"""Fetch a one-time market-data snapshot with stale/missing field labeling.

Upstream link:
- <https://tastyworks-api.readthedocs.io/en/latest/market-data.html>

Tested versions:
- CPython 3.14.8 free-threaded (3.14t)
- tastytrade 13.2.3

PowerShell command (run from repository root):
- .\\.venv\\Scripts\\python.exe .\\Tastyware_Demo_Scripts\\05_market_data_snapshot.py

Inputs:
- Tasty_SECRET (required, env var)
- Tasty_Refresh (required, env var)
- TASTYTRADE_ENV=production (required)
- TASTYTRADE_SYMBOL (optional, default SPY)
- TASTYTRADE_INSTRUMENT_TYPE (optional, default Equity)
- TASTYTRADE_STALE_SECONDS (optional, default 120)
- TASTYTRADE_TIMEOUT_SECONDS (optional, default 30)
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from tastytrade.market_data import get_market_data
from tastytrade.order import InstrumentType
from tastytrade.session import Session

try:
    from Tastyware_Demo_Scripts.demo_shared import (
        DemoConfigurationError,
        DemoHelperError,
        load_runtime_config,
        managed_async_resource,
        parse_positive_int,
        run_with_timeout,
    )
except ModuleNotFoundError:
    from demo_shared import (
        DemoConfigurationError,
        DemoHelperError,
        load_runtime_config,
        managed_async_resource,
        parse_positive_int,
        run_with_timeout,
    )


def parse_instrument_type(value: str | None) -> InstrumentType:
    if value is None or not value.strip():
        return InstrumentType.EQUITY
    requested = value.strip().lower()
    for member in InstrumentType:
        aliases = {member.name.lower(), str(member.value).lower()}
        if requested in aliases:
            return member
    raise DemoConfigurationError(
        "TASTYTRADE_INSTRUMENT_TYPE is invalid. Example values: Equity, Future, Equity Option."
    )


def _as_aware_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed
    return None


def field_status(value: object, updated_at: object, stale_seconds: int) -> str:
    if value is None:
        return "unavailable"
    updated = _as_aware_datetime(updated_at)
    if updated is None:
        return "available"
    age = datetime.now(timezone.utc) - updated.astimezone(timezone.utc)
    if age > timedelta(seconds=stale_seconds):
        return "stale"
    return "current"


async def show_snapshot() -> int:
    config = load_runtime_config()
    symbol = (str(__import__("os").environ.get("TASTYTRADE_SYMBOL", "SPY")).strip() or "SPY").upper()
    instrument_type = parse_instrument_type(__import__("os").environ.get("TASTYTRADE_INSTRUMENT_TYPE"))
    stale_seconds = parse_positive_int(
        __import__("os").environ.get("TASTYTRADE_STALE_SECONDS"),
        name="TASTYTRADE_STALE_SECONDS",
        default=120,
    )

    session = Session(provider_secret=config.client_secret, refresh_token=config.refresh_token)
    async with managed_async_resource(session):
        snapshot = await run_with_timeout(
            "market-data snapshot",
            asyncio.to_thread(get_market_data, session, symbol, instrument_type),
            config.timeout_seconds,
        )

    updated_at = getattr(snapshot, "updated_at", None)
    print(f"symbol={symbol}")
    print(f"instrument_type={instrument_type.value}")
    print(f"updated_at={updated_at if updated_at is not None else 'unavailable'}")
    for field_name in ("bid", "ask", "last", "mark", "volume"):
        value = getattr(snapshot, field_name, None)
        print(f"{field_name}={value if value is not None else 'unavailable'} status={field_status(value, updated_at, stale_seconds)}")
    return 0


async def main() -> int:
    try:
        return await show_snapshot()
    except (DemoConfigurationError, DemoHelperError) as exc:
        print(f"Snapshot request failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
