"""Preview a limit order with dry-run enforcement and no live-order path.

Upstream link:
- <https://tastyworks-api.readthedocs.io/en/latest/orders.html>

Tested versions:
- CPython 3.14.8 free-threaded (3.14t)
- tastytrade 13.2.3

PowerShell command (run from repository root):
- .\\.venv\\Scripts\\python.exe .\\Tastyware_Demo_Scripts\\12_order_dry_run.py

Inputs:
- Tasty_SECRET (required, env var)
- Tasty_Refresh (required, env var)
- TASTYTRADE_ENV=production (required)
- TASTYTRADE_ACCOUNT_NUMBER (required when multiple accounts are accessible)
- TASTYTRADE_ORDER_SYMBOL (required)
- TASTYTRADE_ORDER_QUANTITY (required positive integer)
- TASTYTRADE_ORDER_LIMIT_PRICE (required decimal)
- TASTYTRADE_ORDER_ACTION (optional, BUY or SELL; default BUY)
- TASTYTRADE_TIMEOUT_SECONDS (optional, default 30)

Safety mode:
- Always invokes account.place_order(..., dry_run=True).
- No live-order switch or execution path exists in this script.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from decimal import Decimal

from tastytrade.account import Account
from tastytrade.order import InstrumentType, Leg, LimitOrder, OrderAction
from tastytrade.session import Session

from Tastyware_Demo_Scripts.demo_shared import (
    DemoConfigurationError,
    DemoHelperError,
    account_number_from_object,
    load_runtime_config,
    managed_async_resource,
    mask_account_number,
    normalize_account_collection,
    parse_decimal,
    parse_positive_int,
    require_env_var,
    require_selected_account,
    run_with_timeout,
)


@dataclass(frozen=True)
class DryRunOrderInput:
    symbol: str
    quantity: int
    limit_price: Decimal
    action: OrderAction


def parse_order_action(value: str | None) -> OrderAction:
    if value is None or not value.strip():
        return OrderAction.BUY
    requested = value.strip().upper()
    if requested == "BUY":
        return OrderAction.BUY
    if requested == "SELL":
        return OrderAction.SELL
    raise DemoConfigurationError("TASTYTRADE_ORDER_ACTION must be BUY or SELL.")


def load_order_input(environ: dict[str, str] | None = None) -> DryRunOrderInput:
    values = dict(os.environ if environ is None else environ)
    symbol = require_env_var("TASTYTRADE_ORDER_SYMBOL", values.get("TASTYTRADE_ORDER_SYMBOL")).upper()
    quantity = parse_positive_int(
        values.get("TASTYTRADE_ORDER_QUANTITY"),
        name="TASTYTRADE_ORDER_QUANTITY",
        default=0,
    )
    limit_price = parse_decimal(values.get("TASTYTRADE_ORDER_LIMIT_PRICE"), name="TASTYTRADE_ORDER_LIMIT_PRICE")
    return DryRunOrderInput(
        symbol=symbol,
        quantity=quantity,
        limit_price=limit_price,
        action=parse_order_action(values.get("TASTYTRADE_ORDER_ACTION")),
    )


async def preview_dry_run_order(
    selected_account: object,
    session: Session,
    order_input: DryRunOrderInput,
    timeout_seconds: float,
):
    order = LimitOrder(
        price=order_input.limit_price,
        legs=[
            Leg(
                instrument_type=InstrumentType.EQUITY,
                symbol=order_input.symbol,
                quantity=order_input.quantity,
                action=order_input.action,
            )
        ],
    )
    return await run_with_timeout(
        "dry-run order preview",
        asyncio.to_thread(selected_account.place_order, session, order, True),
        timeout_seconds,
    )


def format_preview_response(response: object) -> list[str]:
    buying_power_effect = getattr(response, "buying_power_effect", None)
    fee_calculation = getattr(response, "fee_calculation", None)
    validation_errors = getattr(response, "warnings", None) or getattr(response, "errors", None)
    lines = [f"response_type={response.__class__.__name__}"]
    lines.append(f"buying_power_effect={buying_power_effect if buying_power_effect is not None else 'unavailable'}")
    lines.append(f"fee_calculation={fee_calculation if fee_calculation is not None else 'unavailable'}")
    lines.append(f"validation={validation_errors if validation_errors is not None else 'none'}")
    return lines


async def run_order_preview() -> int:
    config = load_runtime_config()
    order_input = load_order_input()
    session = Session(provider_secret=config.client_secret, refresh_token=config.refresh_token)

    async with managed_async_resource(session):
        accounts_raw = await run_with_timeout(
            "account discovery",
            asyncio.to_thread(Account.get, session),
            config.timeout_seconds,
        )
        accounts = normalize_account_collection(accounts_raw)
        selected_account = require_selected_account(accounts, config.account_number)
        response = await preview_dry_run_order(
            selected_account,
            session,
            order_input,
            config.timeout_seconds,
        )

    print(f"account={mask_account_number(account_number_from_object(selected_account))}")
    print(
        f"proposed_order=symbol:{order_input.symbol} quantity:{order_input.quantity} "
        f"action:{order_input.action.value} limit_price:{order_input.limit_price}"
    )
    print("dry_run=True")
    print("signed_price_note=The SDK uses signed Decimal price conventions for some complex order scenarios.")
    for line in format_preview_response(response):
        print(line)
    return 0


async def main() -> int:
    try:
        return await run_order_preview()
    except (DemoConfigurationError, DemoHelperError) as exc:
        print(f"Dry-run order preview failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
