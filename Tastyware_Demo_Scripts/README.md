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

The balance and position demos use the shared sync/async callable runner: it
awaits `Account.get_balances` and `Account.get_positions` in SDK 13.2.3 and also
supports synchronous implementations, with timeout enforcement, categorized
errors, and session cleanup.

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

The snapshot demo awaits SDK 13.2.3's async `get_market_data` through the shared
sync/async callable runner, preserving timeout enforcement, categorized errors,
and session cleanup. Synchronous implementations are also supported.

The quote-stream demo also awaits async `subscribe` and `unsubscribe` calls.
Each quote is printed and flushed as soon as it is received, without waiting
for collection to finish. Each line includes `delta_ms`, the local elapsed
milliseconds since the previous quote across all subscribed symbols, measured
with a monotonic clock (not exchange timestamps). The first quote shows `N/A`.
Each line also prints the feed's `event_time`, `bid_time`, `ask_time`, and
`time_nano_part` unchanged as raw integers, including any zero values. These
are separate from local arrival timing; no units or timezone conversions are
assumed for these raw fields.
The final summary reports symbols, event count, and
completion state without repeating the quotes. Collection remains bounded by
`TASTYTRADE_EVENT_LIMIT` (default 10) and the stream timeout.
It prints flushed timing checkpoints for individual imports, configuration,
session creation, streamer connection, subscription, collection, and cleanup.
The timestamps are elapsed seconds since the script's first timing checkpoint
setup; differences between consecutive timestamps show where time is spent.
Subscription, event collection, and unsubscription each use the configured
`TASTYTRADE_STREAM_TIMEOUT_SECONDS` budget separately; this is not a single
overall runtime limit. Unsubscription runs after collection completes, reaches
the event limit, times out, or fails, before the streamer context closes.

Historical candles and account-history requests also use the shared sync/async
callable runner. Candle cleanup awaits `unsubscribe_candle` with the same symbol,
interval, and extended-hours setting used for subscription. Candle subscription,
collection, and unsubscription each have a separate per-attempt timeout budget;
the overall collection deadline is checked between attempts.

### Phase 3 — Options and safe order preview (implemented)

| Script | Status | Command | Inputs | Expected output / failures |
|---|---|---|---|---|
| `10_option_chain.py` | ✅ implemented | `.\.venv\Scripts\python.exe .\Tastyware_Demo_Scripts\10_option_chain.py` | underlying/expiration/limit optional | Bounded option chain output; explicit unavailable-expiration handling. |
| `11_stream_option_greeks.py` | ✅ implemented | `.\.venv\Scripts\python.exe .\Tastyware_Demo_Scripts\11_stream_option_greeks.py` | underlying/expiration/contract-limit/event-limit/timeout | Streams bounded Greeks updates using contracts’ actual `streamer_symbol` values; explicit completion state and cleanup. |
| `12_order_dry_run.py` | ✅ implemented | `.\.venv\Scripts\python.exe .\Tastyware_Demo_Scripts\12_order_dry_run.py` | explicit account/symbol/quantity/Decimal limit price | Displays proposed order and dry-run preview effects. Always calls SDK preview with `dry_run=True`; no live-order option/path exists. |

Option-chain lookup, Greeks subscription/unsubscription, and order preview use
the shared sync/async callable runner to await SDK 13.2.3 operations with
categorized errors and bounded waits. Greeks subscription, collection, and
unsubscription each use a separate stream-timeout budget. Order preview always
passes `dry_run=True` explicitly.

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

## All-day SPY validation collector

[13_collect_spy_validation.py](./13_collect_spy_validation.py) collects a bounded
SPY options pilot for the external
[Macro_Ideas collector contract](https://github.com/khazaddim/Macro_Ideas/blob/main/short_put_replay/quote_collector_requirements.md).
It is a read-only collector, not an IV solver or a model-approval tool.

### Environment and startup

The collector uses **standard CPython**, not the free-threaded demo interpreter:
DuckDB has no compatible Windows `cp314t` wheel in the tested environment.
The existing `.venv` and demo lock are unchanged. The collector was tested with
CPython 3.14.0, tastytrade 13.2.3 and DuckDB 1.5.6. `pytz` is required by DuckDB's
Python TIMESTAMPTZ conversion. The collector lock constrains shared dependencies
to the existing demo lock.

From the repository root (the standard Python path below is this machine's):

```powershell
uv venv --python C:\Python314\python.exe .venv-collector
uv pip install --python .\.venv-collector\Scripts\python.exe --only-binary=:all: -r .\Tastyware_Demo_Scripts\requirements-collector.lock

# No authentication, subscriptions or database writes:
.\.venv-collector\Scripts\python.exe .\Tastyware_Demo_Scripts\13_collect_spy_validation.py --plan

# Set Tasty_SECRET and Tasty_Refresh locally as for the other demos.
$env:TASTYTRADE_ENV = 'production'
.\.venv-collector\Scripts\python.exe .\Tastyware_Demo_Scripts\13_collect_spy_validation.py --acknowledge-data-permissions
```

Only acknowledge permissions after verifying that your market-data agreement
allows retention and transfer to the validation repository. No authenticated
full-session pilot was run as part of implementation.

- Started before the open: waits for **today's** regular XNYS open.
- Started during the session: joins now; earlier missed cycles remain explicit.
- Started after close, on a weekend or on a holiday: exits without authentication.
  It does **not** wait for tomorrow. The date is determined in New York time.
- Holidays, DST and early closes come from the exchange calendar.
- Regular-session samples end at the calendar close. A default 15-second
  publication grace plus 30-second bounded candle reconciliation follows.
  This intentionally excludes any later SPY-option trading session.
- Ctrl+C closes the database and marks the run interrupted.

Default database: `Tastyware_Demo_Scripts\exports\spy_validation.duckdb`.
Use `--database <path.duckdb>` to change it. DuckDB databases, their WAL/temp
files, and Parquet files are Git-ignored throughout this repository. Keep export
directories under `Tastyware_Demo_Scripts\exports\` so their JSON manifests and
any other generated files are ignored too. For custom paths elsewhere, add an
ignore rule before collecting/exporting. Ignore rules do not protect files
already tracked by Git or prevent `git add -f`, and do not apply in other repos.
Only one collector may write a database at once.

### Sampling and quality configuration

The default universe selects nearest listed expirations to **0, 7, 14, 17,
28 and 56 calendar days**. Each expiry contributes up to nine puts using
adjacent strike ladders below strike/spot anchors **0.95, 0.975 and 1.0**, plus
three calls closest to spot. Selection never uses broker delta. Options must
be active, not past their stop-trading timestamp, have SPY roots and represent
100 shares. Complete deliverables/currency are not certified by that filter.

The universe refreshes every 30 minutes. Previously selected contracts remain
subscribed and retained, including after a same-day restart. The cumulative
cap is 96 options. An expansion exceeding that cap is rejected and diagnosed,
not silently truncated. Actual expirations, selected/rejected counts, selection
spot and effective universe revisions are stored.

If the feed omits SPY bid/ask source timestamps (zero values), universe selection
may use an otherwise valid quote received within `--quote-age` seconds. This is
explicitly logged and diagnosed; it does not replace source timestamps or relax
sample eligibility or option/underlying source-time alignment. Such samples
remain flagged and cannot support strict source-time-aligned validation.
Retries report the failing stage and sanitized exception types, not API secrets.

Relevant flags (`--help` lists all):

Add `--verbose` to your collection command for request/subscription progress and
feed heartbeats approximately every five seconds once universe setup completes.
Heartbeats show cumulative Quote/Trade/Greeks/Candle receipt counts, new events
since the previous report, time since the last event, cached quote count and
source-timestamp availability. Zero new events remain visible; blocking setup
requests can delay heartbeats. Buffer commit messages confirm candle/diagnostic
transactions, including every received market-data update; the normal minute
messages confirm sampled quote writes. Heartbeats also report committed and
buffered raw-update counts.
Receipt counts are not database row counts or proof of validation eligibility.
Verbose mode does not enable SDK debug logging or print credentials/raw payloads,
and does not change subscription, sampling or persistence behavior.

| Flag | Default | Meaning |
|---|---|---|
| `--verbose` | off | Safe feed heartbeats, request progress and buffer commit confirmations |
| `--sample-seconds` | 60 | Nominal cadence, anchored to today's open |
| `--quote-age` | 5 | Maximum age of both bid and ask, seconds |
| `--analytics-age` | 15 | Maximum analytics source age, seconds |
| `--max-skew` | 2 | Maximum backward option/underlying source-time skew, seconds |
| `--max-lateness` | 5 | Late snapshots remain stored but are quality-flagged |
| `--candle-grace` | 15 | Elapsed interval publication grace, seconds |
| `--max-gb` | 20 | Decimal GB stop threshold, including database and WAL |
| `--expiry-days` | `0,7,14,17,28,56` | Calendar-day expiry targets |
| `--put-moneyness` | `0.95,0.975,1` | Strike/spot anchors for adjacent put ladders |
| `--puts-per-expiry` / `--calls-per-expiry` | 9 / 3 | Pilot breadth |
| `--max-contracts` | 96 | Cumulative session option cap |
| `--refresh-seconds` | 1800 | Universe refresh |
| `--flush-seconds` | 1 | All-update/diagnostic batch transaction interval, including during setup |
| `--candle-refresh-seconds` / `--overlap-minutes` | 300 / 5 | Candle overlap re-request |
| `--reconnect-attempts` | 1000 | Unattended-session reconnect budget; exponential backoff capped at 30 seconds and bounded by today's session deadline |
| `--delay-status` | `unknown` | User-verified `realtime`, `delayed`, or unknown |
| `--clock-offset-ms` | omitted | User-measured local clock offset; omitted is unknown |

Verify clock synchronization and feed entitlement before supplying
`--clock-offset-ms` and `--delay-status realtime`; do not invent a zero offset
to bypass a quality flag. Unknown source timestamps, delay status or clock
offset do not count as verified freshness.

Quotes, last trades and Greeks stream continuously. **Every decoded update
received for subscribed symbols** is now retained, including duplicates, invalid
values and out-of-order events. This is every update delivered to this process,
not a guarantee of every exchange tick or coverage of every listed SPY option.
The existing bounded pilot universe remains unchanged.
Minute snapshots independently retain the latest received states and their
original payloads; they are not time-window averages.
The paired SPY quote is the latest retained quote with source time no later
than the option quote and receipt time no later than the sample. A bounded
SPY history supports that backward match; missing matches remain missing.
Quote source time is the later of bid/ask update times, while quote age checks
both sides. Source time, local processing/receipt time, scheduled time and
actual sampling time are separate. Pairing is at millisecond precision;
original integer timestamps and sub-millisecond fields remain in raw JSON.

The first scheduled sample is one interval after the open; the last is at
the close. Sampling is not atomic across instruments. A late current sample
is retained and flagged; whole missed intervals are recorded without
fabricating historical snapshots. Missing/stale Greeks do not discard quotes.
Zero-bid, crossed, locked, invalid and one-sided quotes remain available for
diagnosis. Last trade is never substituted for midpoint.

### Persistence, failures and the 20 GB stop

Each minute's samples and referenced raw payloads are committed together.
All-update batches, candle revisions and diagnostics are committed by a
supervised background task every configured flush interval (default one second)
while the event loop is responsive, including during subscription/chain requests
and reconnect waits. Storage failures stop collection explicitly. Checkpoints
occur approximately once per minute. Committed WAL data survives process
termination. Graceful cancellation flushes pending updates; abrupt termination
can lose the last uncommitted batch, and disk/size-limit failures may prevent
that final flush. Do not copy/delete the live `.wal` separately from the database.
Use local reliable storage, not a network share or a cloud-synchronized folder.

Restart with the same command/database to append a **new run**, not overwrite
earlier data. Unfinished runs are marked interrupted with unknown end time.
The earlier universe is restored, subscriptions are renewed, and the new run
does not claim to have captured pre-restart minutes. Raw payload hashes
deduplicate identical source content; distinct receipts and sampling attempts
retain their own IDs. Stream gaps cannot be repaired with candle backfills.

The storage threshold checks **database + WAL bytes before/after each bounded
transaction and checkpoint**. It is a stop trigger, not a hard filesystem
quota: one batch/checkpoint and the final run-status write can overshoot it.
No data is deleted to make room. Offline exports require additional space and
are not included in this capture threshold. DuckDB's memory budget is 512 MB;
pending event buffers are bounded and overflow is an explicit failure.
The default all-update buffer is 50,000 events; nothing is silently evicted.

Schema 1 databases are upgraded transactionally to schema 2 without deleting or
rewriting earlier samples. Complete update history starts with schema-2 runs;
missing updates from earlier runs or disconnected periods cannot be recovered.
Each run's capability manifest records its recording mode.

Exit codes: **0** session ended with recorded coverage (or no session today),
**2** partial coverage/storage stop, **1** fatal failure, **130** Ctrl+C.
Absent candles count as incomplete coverage, not proof that no trades occurred.
Even exit 0 does **not** approve feed semantics, broker/model agreement or
historical replay. Health counters are printed during collection.

### Database and export contract

| Table/view | Contents |
|---|---|
| `runs` | Config, capability manifest, code revision/file hashes, versions, session, status |
| `raw_records` | Content-addressed original requested market-data fields and reference provenance |
| `raw_updates` | One row per received update, including repeated payloads; kind/symbol, source/UTC receipt time, monotonic nanoseconds, per-run receive sequence, connection ID, observation/raw IDs |
| `raw_update_payloads` | All `raw_updates` columns plus original decoded JSON payload; convenient notebook view |
| `instruments`, `universe_revisions` | Contract terms, reference payloads, effective selection history |
| `quotes`, `trades`, `analytics` | Every received normalized observation, also referenced by minute samples; source/receipt times, unscaled numeric feed values, raw references |
| `cycles`, `samples` | Scheduled/actual samples, explicit quote/spot/analytics/trade joins, age/skew, quality |
| `candles`, `latest_candles` | All received candle revisions and deterministic latest-by-receipt view |
| `requests`, `diagnostics` | Request attempts, rate-limit/failure categories, gaps, clock jumps, event counters |
| `run_coverage` | Per-run received-update, cycle, sample, eligibility and diagnostic counts |

Normalized observation tables carry `schema_version`, `run_id`, `record_id`,
`source`, `received_at_utc` and `raw_record_id`. Administrative cycles and
diagnostics have no source payload and therefore null raw references.
Raw content may be shared across runs; join by raw ID, not by run ID.
Payloads preserve numeric strings/sentinels; normalized non-finite numbers
become null with quality reasons. Prices and Greeks are not multiplied by
contract size. Timestamps are UTC; Parquet uses typed nulls.

`quote_eligible` means the configured quote/pair timing checks passed, **not**
that all downstream BSM terms/dividend/rate policies are approved.
`analytics_fresh` checks independent analytics timing and fields.
`broker_comparison_ready` stays false because broker Greek units, pricing
model and inputs have not been independently verified.

The streaming interfaces do not provide a broker mark or analytics' underlying,
rate/dividend inputs. `analytics.price` remains separate from midpoint and is
not relabeled as mark. Candle price/adjustment basis and provider finality
remain unverified. `elapsed_with_grace` only describes receipt relative to
interval end. Replayed candles retain actual retrieval/receipt time; no
historical point-in-time availability is implied. Invalid/pending/removal
events remain flagged, not treated as finalized bars.

The adapter is pinned to tastytrade 13.2.3 and captures requested market-data
fields before SDK validators can drop invalid quotes or turn missing candle
prices into zero. It does not record authentication frames or enable SDK
debug logs. Revalidate this adapter before changing the SDK pin.

Stop the writer before querying/exporting from another process. Export:

```powershell
.\.venv-collector\Scripts\python.exe .\Tastyware_Demo_Scripts\13_collect_spy_validation.py `
  --export .\Tastyware_Demo_Scripts\exports\spy-pilot-export
```

The destination must not exist. Export writes Parquet tables/views and a JSON
manifest containing column schemas, row counts, SHA-256 checksums, timing/
quality summaries, and per-instrument candle coverage. Inspect `samples.quality`
and `runs.capabilities` before selecting rows for numerical comparison.
The report treats missing bars as unknown absence and reports unverified
capabilities rather than fabricating successful validation.

### Notebook analysis after stopping the writer

Use the collector environment (or another environment with DuckDB installed).
These queries do not require API credentials:

```python
import duckdb

con = duckdb.connect(
    r"C:\Chris\Tasty_Dearcyfi_Tests\Tastyware_Demo_Scripts\exports\spy_validation.duckdb",
    read_only=True,
)
con.execute("SET TimeZone='UTC'")

# Review recording modes and coverage before combining runs.
coverage = con.sql("SELECT * FROM run_coverage").df()
updates = con.sql("""
    SELECT run_id, connection_id, receive_sequence, received_at_utc,
           received_monotonic_ns, kind, symbol, source_time, payload
    FROM raw_update_payloads
    ORDER BY run_id, receive_sequence
    LIMIT 100
""").df()

# Notebook-friendly quotes with receipt ordering and retained bid/ask values.
quotes = con.sql("""
    SELECT u.run_id, u.connection_id, u.receive_sequence,
           u.received_at_utc, u.received_monotonic_ns,
           q.symbol, q.bid, q.ask, q.bid_size, q.ask_size, q.source_time
    FROM raw_updates u JOIN quotes q ON q.record_id=u.observation_id
    WHERE u.kind='Quote'
    ORDER BY u.run_id, u.receive_sequence
    LIMIT 1000
""").df()
con.close()
```

For receipt-time backward matching, match option updates to preceding SPY quote
updates within the **same run and connection**, ordered by `receive_sequence`.
Measure receipt gaps using `received_monotonic_ns`, not wall-clock differences;
only compare monotonic values within the same process/run. UTC receipt times
can be equal for multiple events decoded from one frame. These times describe
local callback processing, not exchange occurrence or measured network latency.
Receipt-time alignment must be explicitly labeled as such; it does not change
the minute samples' strict source-time eligibility. Earlier schema-1 quotes
remain in `quotes`, but do not have `raw_updates` delivery metadata.

Instrument metadata and broker Greeks are available in `instruments` and
`analytics`; use `raw_updates.observation_id` to locate received analytics.
Risk-free rates, dividends and broker pricing conventions are still external
inputs: this capture alone does not certify BSM/European-vs-American agreement.
Keep notebooks' data outputs and exports out of Git.

### Credential-free collector validation

```powershell
.\.venv-collector\Scripts\python.exe -m unittest -v Tastyware_Demo_Scripts.test_spy_collector
```

Tests cover calendar/DST/early-close behavior, raw full/compact framing,
invalid/missing/stale observations, backward matching and threshold boundaries,
minute sampling, closing reconciliation, sanitized failures, reconnects,
transaction rollback, size stops, actual subprocess-crash WAL recovery,
restart universe retention, candle revisions, and portable exports.
