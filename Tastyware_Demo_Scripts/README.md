# Tastytrade SDK Demo Scripts

This folder is a production-only, read-focused knowledge base for the unofficial
[`tastyware/tastytrade`](https://github.com/tastyware/tastytrade) Python SDK.
Every demo is independently runnable from repository root with explicit inputs,
bounded timeouts, redacted output expectations, and cleanup behavior.

## Verified baseline

- CPython **3.14.8 free-threaded** (`3.14t`, `sys._is_gil_enabled() == False`)
- `tastytrade==13.2.3`
- Lock file: [requirements-3.14t.lock](./requirements-3.14t.lock)

Install/reconcile dependencies from repository root:

```powershell
uv pip install --python .\.venv\Scripts\python.exe -r .\Tastyware_Demo_Scripts\requirements-3.14t.lock
```

Run credential-free import compatibility check:

```powershell
.\.venv\Scripts\python.exe -m unittest -v Tastyware_Demo_Scripts.test_compatibility_imports
```

## Required safety setup

- Authenticated demos require local env vars:
  - `Tasty_SECRET`
  - `Tasty_Refresh`
  - `TASTYTRADE_ENV=production` (required, sandbox rejected)
- Account-specific demos use `TASTYTRADE_ACCOUNT_NUMBER` when multiple accounts
  are accessible.
- Default request timeout is 30 seconds (`TASTYTRADE_TIMEOUT_SECONDS`).
- Optional exports write only under `Tastyware_Demo_Scripts\exports\` (Git ignored).
- No script prints credentials or tokens; account numbers are masked in output.

PowerShell pre-check:

```powershell
$env:TASTYTRADE_ENV = 'production'
if ([string]::IsNullOrWhiteSpace($env:Tasty_SECRET) -or
    [string]::IsNullOrWhiteSpace($env:Tasty_Refresh)) {
    throw 'Set Tasty_SECRET and Tasty_Refresh in your local shell before authenticated demos.'
}
```

Windows note: Python may expose user environment variable names in uppercase
(`TASTY_SECRET` / `TASTY_REFRESH`) even when you set `Tasty_SECRET` /
`Tasty_Refresh` in PowerShell. The shared loader accepts either form.

## Shared infrastructure

- [demo_shared.py](./demo_shared.py): config validation, production-only
  enforcement, redaction, timeout/failure classification, bounded stream
  collection, candle normalization, CSV export helpers.
- [test_demo_shared.py](./test_demo_shared.py): credential-free tests for shared behavior.

## Script index and run commands

### Phase 1 — Connection and account references (implemented)

| Script | Status | Command | Inputs | Expected output / failures |
|---|---|---|---|---|
| `01_test_connection.py` | ✅ implemented | `.\.venv\Scripts\python.exe .\Tastyware_Demo_Scripts\01_test_connection.py` | credentials + production env | Prints SDK/Python/environment and authenticated account lookup result. Distinguishes zero accounts from auth failures. |
| `02_list_accounts.py` | ✅ implemented | `.\.venv\Scripts\python.exe .\Tastyware_Demo_Scripts\02_list_accounts.py` | credentials + production env | Prints masked account list and account-selection guidance. |
| `03_account_balances.py` | ✅ implemented | `.\.venv\Scripts\python.exe .\Tastyware_Demo_Scripts\03_account_balances.py` | credentials + production env (+ account when needed) | Prints labeled balance, buying power, net-liq value, and timestamps. |
| `04_current_positions.py` | ✅ implemented | `.\.venv\Scripts\python.exe .\Tastyware_Demo_Scripts\04_current_positions.py` | credentials + production env (+ account when needed) | Prints empty-position result or labeled position rows. |

Known failures:
- Missing/invalid config: actionable error naming required env vars.
- Auth/permission/network/timeout: explicit categorized failure messages.

### Phase 2 — Market and historical-data references (implemented)

| Script | Status | Command | Inputs | Expected output / failures |
|---|---|---|---|---|
| `05_market_data_snapshot.py` | ✅ implemented | `.\.venv\Scripts\python.exe .\Tastyware_Demo_Scripts\05_market_data_snapshot.py` | symbol/instrument type optional | Prints bid/ask/last/mark/volume with current/stale/unavailable status. |
| `06_stream_quotes.py` | ✅ implemented | `.\.venv\Scripts\python.exe .\Tastyware_Demo_Scripts\06_stream_quotes.py` | symbols/event limit/timeout optional | Bounded quote events with explicit completion state and cleanup. |
| `07_historical_candles.py` | ✅ implemented | `.\.venv\Scripts\python.exe .\Tastyware_Demo_Scripts\07_historical_candles.py` | explicit timezone-aware start/end + interval + calendar | Bounded retry collection; timezone-preserving normalized OHLCV; endpoint coverage report; always labels result partial unless stronger SDK signal is verified. Optional CSV export. |
| `08_account_transaction_history.py` | ✅ implemented | `.\.venv\Scripts\python.exe .\Tastyware_Demo_Scripts\08_account_transaction_history.py` | explicit account/date range | Labeled transaction rows with documented pagination behavior (`per_page`/`page_offset`). |
| `09_account_value_history.py` | ✅ implemented | `.\.venv\Scripts\python.exe .\Tastyware_Demo_Scripts\09_account_value_history.py` | explicit account/lookback | timestamp/value rows; optional CSV export; notes deposits/withdrawals can affect values. |

Phase 2 production restrictions:
- `TASTYTRADE_ENV` must be `production`.
- Historical collection uses finite attempt/time budgets and explicit stop reason.
- Endpoint coverage does **not** claim complete backfill; results remain partial-labeled.

### Phase 3 — Options and safe order preview (implemented)

| Script | Status | Command | Inputs | Expected output / failures |
|---|---|---|---|---|
| `10_option_chain.py` | ✅ implemented | `.\.venv\Scripts\python.exe .\Tastyware_Demo_Scripts\10_option_chain.py` | underlying/expiration/limit optional | Bounded option chain output; explicit unavailable-expiration handling. |
| `11_stream_option_greeks.py` | ✅ implemented | `.\.venv\Scripts\python.exe .\Tastyware_Demo_Scripts\11_stream_option_greeks.py` | underlying/expiration/contract-limit/event-limit/timeout | Streams bounded Greeks updates using contracts’ actual `streamer_symbol` values; explicit completion state and cleanup. |
| `12_order_dry_run.py` | ✅ implemented | `.\.venv\Scripts\python.exe .\Tastyware_Demo_Scripts\12_order_dry_run.py` | explicit account/symbol/quantity/Decimal limit price | Displays proposed order and dry-run preview effects. Always calls SDK preview with `dry_run=True`; no live-order option/path exists. |

## Credential-free validation suite

Run the focused credential-free suite (no credentials required):

```powershell
.\.venv\Scripts\python.exe -m unittest -v `
  Tastyware_Demo_Scripts.test_compatibility_imports `
  Tastyware_Demo_Scripts.test_demo_shared `
  Tastyware_Demo_Scripts.test_phase1_scripts `
  Tastyware_Demo_Scripts.test_phase2_scripts `
  Tastyware_Demo_Scripts.test_phase3_scripts
```

Additional script syntax check used in this repository:

```powershell
.\.venv\Scripts\python.exe -m compileall Tastyware_Demo_Scripts
```

## Production smoke-check workflow (opt-in, local secrets only)

Run these in order after setting local credentials, recording only redacted
results:

1. Phase 1 first-auth check: `01_test_connection.py`, then `02`–`04`
2. Phase 2 smoke checks: `05`–`09`
3. Phase 3 smoke checks: `10`–`12` (`12` remains dry-run only)

Do not commit credential material, full account numbers, tokens, or private exports.
