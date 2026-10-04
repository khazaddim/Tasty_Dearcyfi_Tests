"""Read account transaction history with explicit pagination behavior.

Upstream link:
- <https://tastyworks-api.readthedocs.io/en/latest/accounts.html>

Tested versions:
- CPython 3.14.8 free-threaded (3.14t)
- tastytrade 13.2.3

PowerShell command (run from repository root):
- .\\.venv\\Scripts\\python.exe .\\Tastyware_Demo_Scripts\\08_account_transaction_history.py

Inputs:
- Tasty_SECRET (required, env var)
- Tasty_Refresh (required, env var)
- TASTYTRADE_ENV=production (required)
- TASTYTRADE_ACCOUNT_NUMBER (required when multiple accounts are accessible)
- TASTYTRADE_HISTORY_START_DATE (required YYYY-MM-DD)
- TASTYTRADE_HISTORY_END_DATE (required YYYY-MM-DD)
- TASTYTRADE_HISTORY_PER_PAGE (optional, default 250)
- TASTYTRADE_TIMEOUT_SECONDS (optional, default 30)
"""

from __future__ import annotations

import asyncio
import os
from datetime import date

from tastytrade.account import Account
from tastytrade.session import Session

from Tastyware_Demo_Scripts.demo_shared import (
    DemoConfigurationError,
    DemoHelperError,
    account_number_from_object,
    load_runtime_config,
    managed_async_resource,
    mask_account_number,
    normalize_account_collection,
    parse_positive_int,
    require_selected_account,
    run_with_timeout,
)


def parse_date(name: str, value: str | None) -> date:
    if value is None or not value.strip():
        raise DemoConfigurationError(f"{name} is required in YYYY-MM-DD format.")
    try:
        return date.fromisoformat(value.strip())
    except ValueError as exc:
        raise DemoConfigurationError(f"{name} must be YYYY-MM-DD.") from exc


async def fetch_history() -> int:
    config = load_runtime_config()
    start_date = parse_date("TASTYTRADE_HISTORY_START_DATE", os.environ.get("TASTYTRADE_HISTORY_START_DATE"))
    end_date = parse_date("TASTYTRADE_HISTORY_END_DATE", os.environ.get("TASTYTRADE_HISTORY_END_DATE"))
    if end_date < start_date:
        raise DemoConfigurationError("TASTYTRADE_HISTORY_END_DATE must be >= TASTYTRADE_HISTORY_START_DATE.")
    per_page = parse_positive_int(
        os.environ.get("TASTYTRADE_HISTORY_PER_PAGE"),
        name="TASTYTRADE_HISTORY_PER_PAGE",
        default=250,
    )

    session = Session(provider_secret=config.client_secret, refresh_token=config.refresh_token)
    async with managed_async_resource(session):
        accounts_raw = await run_with_timeout(
            "account discovery",
            asyncio.to_thread(Account.get, session),
            config.timeout_seconds,
        )
        accounts = normalize_account_collection(accounts_raw)
        selected_account = require_selected_account(accounts, config.account_number)

        transactions, page_count = await fetch_history_pages(
            selected_account=selected_account,
            session=session,
            start_date=start_date,
            end_date=end_date,
            per_page=per_page,
            timeout_seconds=config.timeout_seconds,
        )

    print(f"account={mask_account_number(account_number_from_object(selected_account))}")
    print(f"start_date={start_date.isoformat()} end_date={end_date.isoformat()}")
    print(f"pagination=per_page:{per_page} pages_fetched:{page_count}")
    print(f"transaction_count={len(transactions)}")
    for transaction in transactions:
        tx_date = getattr(transaction, "transaction_date", None)
        tx_type = getattr(transaction, "transaction_type", None)
        tx_sub_type = getattr(transaction, "transaction_sub_type", None)
        symbol = getattr(transaction, "symbol", None)
        value = getattr(transaction, "value", None)
        print(
            f"- date={tx_date} type={tx_type}/{tx_sub_type} symbol={symbol or 'n/a'} value={value}"
        )
    return 0


async def fetch_history_pages(
    *,
    selected_account: object,
    session: Session,
    start_date: date,
    end_date: date,
    per_page: int,
    timeout_seconds: float,
) -> tuple[list[object], int]:
    page_offset = 0
    page_count = 0
    transactions: list[object] = []
    while True:
        page_count += 1
        page = await run_with_timeout(
            f"transaction history page {page_count}",
            asyncio.to_thread(
                selected_account.get_history,
                session,
                per_page,
                page_offset,
                "Desc",
                None,
                None,
                None,
                start_date,
                end_date,
            ),
            timeout_seconds,
        )
        transactions.extend(page)
        if len(page) < per_page:
            break
        page_offset += per_page
    return transactions, page_count


async def main() -> int:
    try:
        return await fetch_history()
    except (DemoConfigurationError, DemoHelperError) as exc:
        print(f"Transaction history request failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
