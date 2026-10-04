# Tastytrade SDK Demo Script Roadmap

This folder will be a hands-on knowledge base for the unofficial
[`tastyware/tastytrade`](https://github.com/tastyware/tastytrade) Python SDK.
Each demo should teach one task, run independently from PowerShell, and explain
its inputs, expected output, and common failure cases.

This is a plan only: the script names below are proposed, not implemented yet.
The initial goal is command-line examples; DearCyGui/DearCyFi integration can
build on these later without being required to run them.

## Verified baseline

The baseline was verified on 2026-10-04 with CPython **3.14.8 free-threading**
(`3.14t`, `sys._is_gil_enabled() == False`) and `tastytrade==13.2.3`.
Version 13.2.3 exposes the asynchronous OAuth SDK surface used by this roadmap:
`Session`, `Account`, market-data functions, `DXLinkStreamer`, DXFeed events,
instruments, and orders.

The exact resolved dependency set is committed in
[`requirements-3.14t.lock`](./requirements-3.14t.lock), generated from
[`requirements.in`](./requirements.in). From the repository root, install or
reconcile the baseline with:

```powershell
uv pip install --python .\.venv\Scripts\python.exe -r .\Tastyware_Demo_Scripts\requirements-3.14t.lock
```

Verify the credential-free compatibility smoke test without opening a session
or making a network request:

```powershell
.\.venv\Scripts\python.exe -m unittest -v Tastyware_Demo_Scripts.test_compatibility_imports
```

The smoke test imports only SDK modules; it does not construct `Session`,
provide credentials, or call any account, market-data, streamer, instrument,
or order API.

### Baseline verification

On the tested 3.14t interpreter, the lock installed successfully and the
credential-free import smoke test passed for the complete demo SDK surface.
`compileall` also passed for this directory. No unsupported dependency,
import failure, or free-threading-specific behavior was observed. This
repository has no existing Python lint, type-check, or test-runner
configuration; the standard-library `unittest` command above is the baseline
credential-free test command.

## Sources and version expectations

The roadmap is based on the upstream README and documentation reviewed on
2026-10-03. The documentation reported version 13.2.3 at that time.
Before implementing demos, choose and record a tested SDK version and compatible
Python version. Do not mix older username/password or synchronous examples with
the current asynchronous OAuth API.

- [Upstream README](https://github.com/tastyware/tastytrade#readme):
  sessions, streaming quotes, positions, dry-run orders, and option-chain greeks.
- [Sessions and OAuth setup](https://tastyworks-api.readthedocs.io/en/latest/sessions.html)
- [Accounts, balances, positions, and history](https://tastyworks-api.readthedocs.io/en/latest/accounts.html)
- [One-time market data](https://tastyworks-api.readthedocs.io/en/latest/market-data.html)
- [Streaming market data](https://tastyworks-api.readthedocs.io/en/latest/data-streamer.html)
- [Streamer API, including candle subscriptions](https://tastyworks-api.readthedocs.io/en/latest/api/streamer.html#tastytrade.streamer.DXLinkStreamer.subscribe_candle)
- [Orders](https://tastyworks-api.readthedocs.io/en/latest/orders.html)

## Setup and safety conventions

- All authenticated demos use the existing machine environment variables
  `Tasty_SECRET` (client secret) and `Tasty_Refresh` (refresh token). These are
  read locally at runtime; never paste their values into commands, source,
  documentation, logs, or test fixtures. Do not put credentials in a `.env`
  file.
- This knowledge base is production-only. Every authenticated demo requires
  `TASTYTRADE_ENV=production`; sandbox is not supported or used for validation
  because its behavior and feature availability differ from production. A
  script must reject a sandbox selection rather than silently switching
  environments.
- From PowerShell at the repository root, select production and check that the
  existing credentials are available without displaying their values:

  ```powershell
  $env:TASTYTRADE_ENV = 'production'
  if ([string]::IsNullOrWhiteSpace($env:Tasty_SECRET) -or
      [string]::IsNullOrWhiteSpace($env:Tasty_Refresh)) {
      throw 'Set the Tasty_SECRET and Tasty_Refresh environment variables before running an authenticated demo.'
  }
  ```

- Account-specific demos use `TASTYTRADE_ACCOUNT_NUMBER` when an account must
  be selected. If exactly one account is available, a demo may use it; if
  multiple accounts are available and no account is selected, it must stop and
  explain how to choose one. Display account numbers in masked form only.
- Network operations have a finite timeout. Use
  `TASTYTRADE_TIMEOUT_SECONDS` with a documented default of 30 seconds for
  requests; streaming demos also take an explicit finite event limit and
  timeout so collection always ends and reports whether it was bounded or
  timed out.
- Optional exports go only under
  `Tastyware_Demo_Scripts\exports\`. This local directory is ignored by Git;
  do not commit account data or generated exports.
- Keep account and market-data demos read-only. The order demo must always use
  `dry_run=True`; live submission, replacement, and cancellation are out of scope.
- Use the SDK's asynchronous patterns with a runnable async entry point, and
  close sessions/streamers using their supported lifecycle APIs.
- Put time limits on network calls and streaming demos. Report authentication,
  permission, network, and timeout errors clearly rather than claiming success.
- Keep dependencies minimal. Use standard-library CSV/JSON export initially;
  plotting and GUI dependencies belong in later, optional demos.
- Redact personal information in screenshots or saved examples.

## Phase 1: Connection and account basics

These are the recommended first scripts, extending the README's session and
position examples with the account documentation.

| Done | Proposed script | What it teaches | Inputs and expected result |
|---|---|---|---|
| [ ] | `01_test_connection.py` | Create an OAuth `Session` and make an authenticated request using `Account.get`. Constructing a session alone is not a connection test. | Credentials and explicit environment; print SDK version, environment, and confirmation that the request succeeded without exposing secrets. Report zero accessible accounts separately from authentication failure. |
| [ ] | `02_list_accounts.py` | List accessible accounts and explain how to select one by account number using `Account.get`. | Session settings; print a masked account summary and instructions for selecting an account in subsequent demos. Handle an empty account list explicitly. |
| [ ] | `03_account_balances.py` | Fetch cash balance, buying power, and net liquidating value using `account.get_balances`. | Selected account; show labeled monetary values and available snapshot/update timestamps. |
| [ ] | `04_current_positions.py` | Read holdings using `account.get_positions`, as in the README. | Selected account; show symbol, instrument type, quantity/direction, and average open price. Treat an account with no positions as a valid result rather than indexing the first item. |

## Phase 2: Market data and historical data

Start with a single equity such as SPY, then allow configurable symbols.
REST snapshots, streaming quotes, and historical candles are different tools:
one is not a substitute for the others.

| Done | Proposed script | What it teaches | Inputs and expected result |
|---|---|---|---|
| [ ] | `05_market_data_snapshot.py` | Fetch a one-time snapshot with `get_market_data`; optionally expand to `get_market_data_by_type`. | Symbol and instrument type; show bid, ask, last/mark, volume, and available timestamps. Identify missing or stale values instead of assuming every field is populated. |
| [ ] | `06_stream_quotes.py` | Use `DXLinkStreamer` and `Quote`, following the README. | Production session, symbols, event limit, and timeout; print a bounded sample of bid/ask updates, then unsubscribe/close cleanly. |
| [ ] | `07_historical_candles.py` | Request price history through `DXLinkStreamer.subscribe_candle` and consume `Candle` events. | Production session, symbol, interval, timezone-aware start/end, extended-hours choice, and timeout; collect OHLCV rows, sort/deduplicate them, filter to the requested range, and optionally export CSV. |
| [ ] | `08_account_transaction_history.py` | Fetch past account transactions with `account.get_history`. This is account activity, not price history. | Selected account and date range; show transaction date/type, symbol when present, and value. Document pagination for the tested SDK so the demo does not silently return an incomplete history. |
| [ ] | `09_account_value_history.py` | Fetch historical net liquidating value with `account.get_net_liquidating_value_history`. | Selected account and supported lookback such as `1m`; show timestamp/value rows and optionally export them. Explain that account value changes can include deposits and withdrawals, not just trading P/L. |

### Historical candle details to document

- Candle subscriptions accept an interval such as `5m`, `1h`, or `1d`, a
  `start_time`, and an extended-hours setting. Always specify the start time;
  the documented default reaches back to 2001.
- A subscription can deliver both backfill and continuing updates. It is not a
  finite REST download: define a bounded collection policy and filter locally
  to the requested end time.
- Verify the tested SDK's backfill/completion semantics before claiming that a
  date range is complete. An event limit or timeout alone does not prove this;
  label partial results explicitly.
- Preserve timestamps with explicit timezone information and explain conversion
  for display. Note whether the latest candle is still forming.
- Explain that available history depends on the instrument, interval, and data
  access. Do not promise unlimited history or silently fabricate missing bars.
- Keep export columns predictable: `symbol`, `timestamp`, `open`, `high`, `low`,
  `close`, and `volume`. This can become the input contract for a future chart demo.

## Phase 3: Options and safe order previews

These round out the major examples in the upstream README.

| Done | Proposed script | What it teaches | Inputs and expected result |
|---|---|---|---|
| [ ] | `10_option_chain.py` | Use `get_option_chain` to explore expirations, strikes, calls/puts, and streamer symbols. | Underlying symbol and optional expiration; show a small filtered chain. Handle unavailable expirations explicitly rather than assuming a monthly helper's date exists in the chain. |
| [ ] | `11_stream_option_greeks.py` | Subscribe to `Greeks` using actual contracts' `streamer_symbol` values, as in the README. | Production session and selected contracts; show bounded updates for delta, gamma, theta, vega, rho, and volatility with timeout/cleanup behavior. |
| [ ] | `12_order_dry_run.py` | Resolve an instrument, build a leg and `LimitOrder`, and call `account.place_order(..., dry_run=True)`. | Explicit account, symbol, quantity, and limit price; show the proposed order, buying-power effect, fees, and validation errors. Explain the SDK's signed-price convention using `Decimal`. Never submit a live order. |

## Later extensions

- **Account alerts:** use `AlertStreamer` for balance, position, and order updates,
  with a fixed runtime and a clear explanation that an idle account may emit none.
- **Connection resilience:** demonstrate bounded retries and backoff for transient
  failures, without retrying invalid credentials indefinitely.
- **Chart handoff:** load exported candle data into a separate DearCyFi demo;
  retain the standalone data-fetching script as the reference example.

## Knowledge-base checklist for every completed script

- [ ] Explain the purpose and link to the relevant upstream documentation.
- [ ] List the tested SDK/Python versions, dependencies, credential requirements,
  environment restrictions, and whether the script is read-only or dry-run.
- [ ] Include an exact PowerShell command runnable from the repository root.
- [ ] Describe configurable inputs, output fields, and redacted sample output.
- [ ] Document empty results, invalid symbols/accounts, missing permissions,
  closed-market or stale-data behavior where relevant, and network timeouts.
- [ ] Confirm clean resource shutdown and verify the expected output, not merely
  that the process started. Clearly distinguish partial data from complete data.
- [ ] Add the script's run instructions to this index when it is implemented.

Suggested first implementation batch: connection test, account listing,
balances, and positions. Then build the snapshot, quote-streaming, and
historical-candle demos before moving into options and order previews.
