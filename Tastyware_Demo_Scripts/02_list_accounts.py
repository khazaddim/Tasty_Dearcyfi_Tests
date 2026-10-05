"""List accessible accounts with masked identifiers and selection guidance.

Upstream link:
- <https://tastyworks-api.readthedocs.io/en/latest/accounts.html>

Tested versions:
- CPython 3.14.8 free-threaded (3.14t)
- tastytrade 13.2.3

PowerShell command (run from repository root):
- .\\.venv\\Scripts\\python.exe .\\Tastyware_Demo_Scripts\\02_list_accounts.py

Inputs:
- Tasty_SECRET (required, env var)
- Tasty_Refresh (required, env var)
- TASTYTRADE_ENV=production (required)
- TASTYTRADE_ACCOUNT_NUMBER (optional explicit selection)
- TASTYTRADE_TIMEOUT_SECONDS (optional, default 30)

Safety mode:
- Production-only read path, no secret or token output.
- Prints only masked account identifiers.

Expected redacted output:
- Count of accessible accounts
- One masked account line per account
- Explicit account-selection instructions
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


async def list_accounts() -> int:
    config = load_runtime_config()
    session = Session(provider_secret=config.client_secret, refresh_token=config.refresh_token)
    async with managed_async_resource(session):
        accounts_raw = await run_with_timeout(
            "account list request",
            asyncio.to_thread(Account.get, session),
            config.timeout_seconds,
        )

    accounts = normalize_account_collection(accounts_raw)
    if not accounts:
        print("No accessible accounts were returned for this authenticated session.")
        return 0

    print(f"accessible_account_count={len(accounts)}")
    for line in format_account_summary_lines(accounts):
        print(line)

    if config.account_number:
        selected = require_selected_account(accounts, config.account_number)
        selected_number = account_number_from_object(selected)
        print(f"selected_account={mask_account_number(selected_number)}")
    elif len(accounts) > 1:
        print("Set TASTYTRADE_ACCOUNT_NUMBER to choose one account for account-specific demos.")

    return 0


def format_account_summary_lines(accounts: list[object]) -> list[str]:
    lines: list[str] = []
    for account in accounts:
        account_number = account_number_from_object(account)
        account_nickname = str(getattr(account, "nickname", "") or "").strip()
        masked = mask_account_number(account_number)
        if account_nickname:
            lines.append(f"- account={masked} nickname={account_nickname}")
        else:
            lines.append(f"- account={masked}")
    return lines


async def main() -> int:
    try:
        return await list_accounts()
    except (DemoConfigurationError, DemoHelperError) as exc:
        print(f"Account listing failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
