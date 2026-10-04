## Context

`Tastyware_Demo_Scripts/README.md` records the intended SDK learning path but explicitly states that its proposed scripts do not exist. The repository needs concise, runnable references for the current asynchronous OAuth SDK before its applications or DearCyFi integrations depend on Tastytrade behavior.

The scripts handle sensitive financial-account context. They must be useful for real accounts without exposing credentials, tokens, full account numbers, or private exports, and they must never place a live order.

## Goals / Non-Goals

**Goals:**

- Deliver one independently executable script for each roadmap topic, organized by the existing numeric filenames.
- Make each script a ground-truth reference: explicit inputs, bounded network/streaming behavior, clear failure messages, documented expected output, and clean resource shutdown.
- Centralize only repeated mechanics (configuration, secret/account masking, session construction, timeouts, formatting, and CSV export) in small helpers so each script remains easy to read.
- Pin and document a tested Python/SDK combination before relying on SDK details, then validate behavior with credential-free unit tests and opt-in live smoke checks.

**Non-Goals:**

- Building GUI, charting, production trading, order submission, cancellation, or replacement workflows.
- Adding a general-purpose SDK abstraction layer, database, or application architecture.
- Promising complete historical data when the streamer’s backfill semantics or permissions cannot establish completeness.
- Storing account data, credentials, tokens, or sensitive output in the repository.

## Decisions

### Use the verified CPython 3.14t and tastytrade baseline

The initial supported and tested baseline is CPython 3.14.8 free-threaded
(`3.14t`, with `sys._is_gil_enabled() == False`) and `tastytrade==13.2.3`.
The reproducible dependency set is pinned in
`Tastyware_Demo_Scripts/requirements-3.14t.lock`.

### Use numbered standalone async entry points backed by minimal shared helpers

The knowledge base will retain the roadmap's `01_...py` through `12_...py` filenames and give every script an `async main()` invoked through `asyncio.run()`. A small local helper module will own common configuration parsing, secret/account masking, session lifecycle, timeout wrappers, output formatting, and optional CSV writing. Topic-specific SDK calls remain directly visible in each script.

This provides a consistent safety baseline without concealing the SDK APIs readers need to learn. Duplicating the safety logic in every script was rejected because it would drift; a broad application-style SDK wrapper was rejected because it would stop being a simple SDK reference.

### Make configuration explicit and fail closed

Credentials are read only from documented environment variables (`TASTYTRADE_CLIENT_SECRET` and `TASTYTRADE_REFRESH_TOKEN`); script-specific inputs use documented arguments or environment variables. Scripts validate required configuration before opening sessions, require an explicit account selection for account-specific actions, and surface actionable authentication, permissions, network, and timeout errors without printing sensitive values.

Sandbox mode remains explicit and uses its own credentials. Production-only streaming scripts must reject sandbox mode rather than silently changing environments.

There is no existing command-line argument convention to reuse for these
exploratory testing scripts. The demo suite will establish and document its own
consistent input conventions; exact argument names remain to be chosen during
implementation.

### Bound all externally driven work and label data honestly

Network requests use finite timeouts. Streaming scripts accept a maximum event count and timeout, unsubscribe and close in `finally` cleanup, and report whether collection ended by the configured limit, timeout, or error. Candle output is sorted, deduplicated, range-filtered, timezone-preserving, and labeled partial unless the tested SDK provides a reliable completion signal.

This favors reproducible demonstrations over convenience. Unbounded streaming and implicit defaults were rejected because neither produces a dependable reference result.

Prior experience suggests DXLink candle backfills can be throttled and may not emit
every requested candle before a deadline, particularly for large ranges. Until the
selected SDK exposes and passes a verified completion signal, a requested candle
range is therefore a timeout-bounded partial result rather than a complete dataset.
The candle helper will make that status visible and will later be evaluated for
reliable retrieval of a requested block without hiding streaming behavior.

### Evaluate bounded retry-based candle backfill in the helper

A repository backfill algorithm has not been established. The proposed helper
will request a time-bounded block, collect candles for a configured attempt
duration, then unsubscribe and close the streamer before retrying if completion
of the initial endpoint check has not been established. Each retry may use a revised start time based on the
observed coverage. Attempts will share a finite overall deadline and a configured
maximum attempt count, with bounded delays between retries.

Collected rows will be merged, sorted, and deduplicated across attempts. The
initial finish signal is receipt of at least one candle for each adjusted endpoint.
Use `pandas_market_calendars`, already present in the tested SDK dependency lock,
with the instrument's exchange/product calendar rather than a generic federal
holiday calendar. Move a closed-market start date forward to the next trading
session. Move today, or an explicitly historical end date, backward to the latest
session that has opened at or before the requested end time and the current time.
This avoids holiday/weekend endpoints and expecting today's candle before market
open. Preserve intraday range bounds, use the calendar's session labels and timezone
for endpoint matching, and report the selected calendar and adjusted endpoint dates.
An unknown calendar or a range containing no eligible session must produce an
actionable error rather than silently falling back to weekdays or widening the range.
A retry must retain the adjusted start target until its candle has
arrived; receipt of a recent candle alone does not satisfy both endpoints.

This endpoint check is deliberately a simple stopping heuristic, not proof that
every candle in the range arrived. Internal-gap detection and recovery are out of
scope initially; downstream gap handling remains with DearCyFi. Stronger coverage
checks are deferred. If an eligible target session has no
candle, the helper must still respect its attempt and time budgets rather than
silently treating the missing endpoint as satisfied.

The result will expose the attempt count, received coverage, stop reason, and
whether both endpoints were observed. Satisfying both endpoints stops collection
with an endpoint-coverage status; the dataset remains labeled partial unless a
tested SDK signal independently proves completeness. Exhausting the attempt or
time budget will also return an explicitly partial result.
Authentication, permission, and other non-retryable failures remain actionable
errors rather than being hidden by retries. This is a proposed recovery strategy
to evaluate, not a guarantee that provider throttling can be overcome.

### Make safety properties mechanically testable

The order script always passes `dry_run=True`, never exposes a live-order switch, and displays only a preview/validation result. Helpers are designed so credential-free tests can verify configuration validation, redaction, values sent to order construction, output schema, and bounded collector behavior. Live integration verification is opt-in, never part of normal automated tests, and requires users' local credentials.

### Keep documentation colocated with each script and indexed centrally

Each script begins with a concise module docstring containing purpose, upstream source link, tested versions, environment restriction, safety mode, exact PowerShell invocation, inputs, expected/redacted output, and common failures. The README remains the navigation index and phase checklist. Export data goes to one ignored local directory using predictable filenames and documented CSV columns.

## Risks / Trade-offs

- [SDK API/version drift] → Pin the first verified dependency range, include it in script output/docs, and update the scripts only after revalidating the affected live smoke check.
- [Authentication, market-data, or account permission differences] → Validate up front; distinguish empty results from authorization failures; document production-only restrictions.
- [Private information in console or exports] → Mask account identifiers by default, never print credential material, provide redacted sample output only, and ignore the local export directory.
- [Streaming resource leaks or hanging demos] → Enforce finite deadlines/event limits and clean up subscriptions, streamers, and sessions in `finally` blocks.
- [Misleading historical-data claims] → Preserve timezones and explicitly label bounded backfill as partial unless a documented completion signal proves otherwise.
- [Tests requiring real funds/accounts] → Keep normal tests pure and stubbed; make live checks explicit, read-only, and opt-in.

## Migration Plan

1. Establish the tested Python and `tastytrade` SDK baseline and add the minimal dependency declaration.
2. Implement helpers and the Phase 1 account-reference scripts with credential-free tests.
3. Add Phase 2 market-data/history scripts, exports, and bounded streaming tests.
4. Add Phase 3 options and dry-run order-preview scripts with a test that prevents accidental live submission.
5. Update the README as each script is implemented and run opt-in, read-only smoke checks with locally supplied credentials.

This is additive. If a script proves incompatible with the selected SDK, remove it from the index or restore the prior documentation-only state; no existing application behavior or stored data requires migration.

## Open Questions

None currently.
