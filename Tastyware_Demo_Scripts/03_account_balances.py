"""Read balance and buying-power fields for a selected account.

Upstream link:
- <https://tastyworks-api.readthedocs.io/en/latest/accounts.html>

Tested versions:
- CPython 3.14.8 free-threaded (3.14t)
- tastytrade 13.2.3

PowerShell command (run from repository root):
- .\\.venv\\Scripts\\python.exe .\\Tastyware_Demo_Scripts\\03_account_balances.py

Inputs:
- Tasty_SECRET (required, env var)
- Tasty_Refresh (required, env var)
- TASTYTRADE_ENV=production (required)
- TASTYTRADE_ACCOUNT_NUMBER (required when multiple accounts are accessible)
- TASTYTRADE_TIMEOUT_SECONDS (optional, default 30)

Safety mode:
- Production-only read path, no secret or token output.
- Prints only masked account identifiers.
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
        run_callable_with_timeout,
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
        run_callable_with_timeout,
        run_with_timeout,
    )


def _format_value(value: object) -> str:
    return str(value) if value is not None else "unavailable"


async def show_balances() -> int:
    config = load_runtime_config()
    session = Session(provider_secret=config.client_secret, refresh_token=config.refresh_token)
    async with managed_async_resource(session):
        accounts_raw = await run_callable_with_timeout(
            "account discovery",
            Account.get,
            config.timeout_seconds,
            session,
        )
        accounts = normalize_account_collection(accounts_raw)
        selected_account = require_selected_account(accounts, config.account_number)
        balances = await run_with_timeout(
            "balance lookup",
            asyncio.to_thread(selected_account.get_balances, session),
            config.timeout_seconds,
        )

    masked = mask_account_number(account_number_from_object(selected_account))
    print(f"account={masked}")
    print(f"cash_balance={_format_value(getattr(balances, 'cash_balance', None))}")
    print(f"equity_buying_power={_format_value(getattr(balances, 'equity_buying_power', None))}")
    print(f"net_liquidating_value={_format_value(getattr(balances, 'net_liquidating_value', None))}")
    print(f"updated_at={_format_value(getattr(balances, 'updated_at', None))}")
    print(f"snapshot_date={_format_value(getattr(balances, 'snapshot_date', None))}")
    return 0


async def main() -> int:
    try:
        return await show_balances()
    except (DemoConfigurationError, DemoHelperError) as exc:
        print(f"Balance lookup failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
