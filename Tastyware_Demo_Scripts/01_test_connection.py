"""Test authenticated production connectivity with an account lookup.

Upstream link:
- <https://github.com/tastyware/tastytrade#readme>

Tested versions:
- CPython 3.14.8 free-threaded (3.14t)
- tastytrade 13.2.3

PowerShell command (run from repository root):
- .\\.venv\\Scripts\\python.exe .\\Tastyware_Demo_Scripts\\01_test_connection.py

Inputs:
- Tasty_SECRET (required, env var)
- Tasty_Refresh (required, env var)
- TASTYTRADE_ENV=production (required)
- TASTYTRADE_TIMEOUT_SECONDS (optional, default 30)

Safety mode:
- Production-only read path, no secret or token output.
- Finite timeout for the authenticated account request.

Expected redacted output:
- SDK/version/environment lines
- "Authenticated account lookup succeeded with N accessible account(s)."
- Optional masked example account number

Common failures:
- Missing env var -> actionable setup guidance
- Authentication/permission/network/timeout -> distinct error messages
- Zero accessible accounts -> explicit non-authentication result
"""

from __future__ import annotations

import asyncio
import platform

import tastytrade
from tastytrade.account import Account
from tastytrade.session import Session

from Tastyware_Demo_Scripts.demo_shared import (
    DemoAuthenticationError,
    DemoConfigurationError,
    DemoHelperError,
    account_number_from_object,
    load_runtime_config,
    managed_async_resource,
    mask_account_number,
    normalize_account_collection,
    run_with_timeout,
)


async def run_connection_check() -> int:
    config = load_runtime_config()
    print(f"tastytrade_version={tastytrade.__version__}")
    print(f"python_version={platform.python_version()}")
    print(f"environment={config.environment}")
    session = Session(provider_secret=config.client_secret, refresh_token=config.refresh_token)

    async with managed_async_resource(session):
        try:
            accounts_raw = await run_with_timeout(
                "authenticated account lookup",
                asyncio.to_thread(Account.get, session),
                config.timeout_seconds,
            )
        except DemoAuthenticationError:
            print("Authentication failed. Verify Tasty_SECRET/Tasty_Refresh for production.")
            return 2

    accounts = normalize_account_collection(accounts_raw)
    if not accounts:
        print("Authenticated successfully, but no accessible accounts were returned.")
        return 0

    print(f"Authenticated account lookup succeeded with {len(accounts)} accessible account(s).")
    masked = mask_account_number(account_number_from_object(accounts[0]))
    print(f"example_account={masked}")
    return 0


async def main() -> int:
    try:
        return await run_connection_check()
    except DemoConfigurationError as exc:
        print(f"Configuration error: {exc}")
        return 1
    except DemoHelperError as exc:
        print(f"Connection check failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
