"""List open positions for a selected account with empty-position handling.

Upstream link:
- <https://tastyworks-api.readthedocs.io/en/latest/accounts.html>

Tested versions:
- CPython 3.14.8 free-threaded (3.14t)
- tastytrade 13.2.3

PowerShell command (run from repository root):
- .\\.venv\\Scripts\\python.exe .\\Tastyware_Demo_Scripts\\04_current_positions.py

Inputs:
- Tasty_SECRET (required, env var)
- Tasty_Refresh (required, env var)
- TASTYTRADE_ENV=production (required)
- TASTYTRADE_ACCOUNT_NUMBER (required when multiple accounts are accessible)
- TASTYTRADE_TIMEOUT_SECONDS (optional, default 30)
"""

from __future__ import annotations

import asyncio

from tastytrade.account import Account
from tastytrade.session import Session

try:
    from Tastyware_Demo_Scripts.demo_shared import (
        DemoConfigurationError,
        DemoHelperError,
        account_number_from_object,
        load_runtime_config,
        managed_async_resource,
        mask_account_number,
        normalize_account_collection,
        require_selected_account,
        run_with_timeout,
    )
except ModuleNotFoundError:
    from demo_shared import (
        DemoConfigurationError,
        DemoHelperError,
        account_number_from_object,
        load_runtime_config,
        managed_async_resource,
        mask_account_number,
        normalize_account_collection,
        require_selected_account,
        run_with_timeout,
    )


def _direction(position: object) -> str:
    direction = getattr(position, "quantity_direction", None)
    if direction is not None and getattr(direction, "value", None):
        return str(direction.value)
    quantity = getattr(position, "quantity", None)
    if quantity is None:
        return "unknown"
    return "long" if quantity >= 0 else "short"


async def show_positions() -> int:
    config = load_runtime_config()
    session = Session(provider_secret=config.client_secret, refresh_token=config.refresh_token)
    async with managed_async_resource(session):
        accounts_raw = await run_with_timeout(
            "account discovery",
            asyncio.to_thread(Account.get, session),
            config.timeout_seconds,
        )
        accounts = normalize_account_collection(accounts_raw)
        selected_account = require_selected_account(accounts, config.account_number)
        positions = await run_with_timeout(
            "position lookup",
            asyncio.to_thread(selected_account.get_positions, session),
            config.timeout_seconds,
        )

    masked = mask_account_number(account_number_from_object(selected_account))
    print(f"account={masked}")
    if not positions:
        print("No open positions were returned for this account.")
        return 0

    print(f"position_count={len(positions)}")
    for position in positions:
        symbol = str(getattr(position, "symbol", "") or "unknown")
        instrument_type = getattr(position, "instrument_type", None)
        instrument_label = (
            str(instrument_type.value) if instrument_type is not None and getattr(instrument_type, "value", None) else "unknown"
        )
        quantity = getattr(position, "quantity", "unavailable")
        average_open_price = getattr(position, "average_open_price", "unavailable")
        print(
            f"- symbol={symbol} instrument_type={instrument_label} "
            f"quantity={quantity} direction={_direction(position)} average_open_price={average_open_price}"
        )
    return 0


async def main() -> int:
    try:
        return await show_positions()
    except (DemoConfigurationError, DemoHelperError) as exc:
        print(f"Position lookup failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
