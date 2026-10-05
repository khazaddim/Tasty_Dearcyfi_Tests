"""Read account value history with optional CSV export.

Upstream link:
- <https://tastyworks-api.readthedocs.io/en/latest/accounts.html>

Tested versions:
- CPython 3.14.8 free-threaded (3.14t)
- tastytrade 13.2.3

PowerShell command (run from repository root):
- .\\.venv\\Scripts\\python.exe .\\Tastyware_Demo_Scripts\\09_account_value_history.py

Inputs:
- Tasty_SECRET (required, env var)
- Tasty_Refresh (required, env var)
- TASTYTRADE_ENV=production (required)
- TASTYTRADE_ACCOUNT_NUMBER (required when multiple accounts are accessible)
- TASTYTRADE_TIME_BACK (required lookback, example: 1m)
- TASTYTRADE_EXPORT_CSV (optional true/false, default false)
- TASTYTRADE_TIMEOUT_SECONDS (optional, default 30)

Notes:
- Net liquidating value changes can include deposits/withdrawals in addition to trading P/L.
"""

from __future__ import annotations

import asyncio
import csv
import os

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
        parse_bool,
        require_env_var,
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
        parse_bool,
        require_env_var,
        require_selected_account,
        run_with_timeout,
    )


def export_value_history_csv(rows: list[object], output_path: str) -> str:
    with open(output_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=("timestamp", "value"))
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "timestamp": getattr(row, "time", None),
                    "value": getattr(row, "close", None),
                }
            )
    return output_path


async def fetch_value_history() -> int:
    config = load_runtime_config()
    time_back = require_env_var("TASTYTRADE_TIME_BACK", os.environ.get("TASTYTRADE_TIME_BACK"))
    export_csv = parse_bool(os.environ.get("TASTYTRADE_EXPORT_CSV"), default=False)

    session = Session(provider_secret=config.client_secret, refresh_token=config.refresh_token)
    async with managed_async_resource(session):
        accounts_raw = await run_with_timeout(
            "account discovery",
            asyncio.to_thread(Account.get, session),
            config.timeout_seconds,
        )
        accounts = normalize_account_collection(accounts_raw)
        selected_account = require_selected_account(accounts, config.account_number)
        history = await run_with_timeout(
            "net-liquidating-value history",
            asyncio.to_thread(
                selected_account.get_net_liquidating_value_history,
                session,
                time_back,
            ),
            config.timeout_seconds,
        )

    print(f"account={mask_account_number(account_number_from_object(selected_account))}")
    print(f"time_back={time_back}")
    print("note=values can include deposits and withdrawals, not only trading profit/loss")
    print(f"row_count={len(history)}")
    for row in history:
        print(f"- timestamp={getattr(row, 'time', None)} value={getattr(row, 'close', None)}")

    if export_csv:
        config.export_dir.mkdir(parents=True, exist_ok=True)
        output_path = config.export_dir / f"value_history_{time_back}.csv"
        export_value_history_csv(history, str(output_path))
        print(f"csv_export={output_path}")

    return 0


async def main() -> int:
    try:
        return await fetch_value_history()
    except (DemoConfigurationError, DemoHelperError) as exc:
        print(f"Account value history request failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
