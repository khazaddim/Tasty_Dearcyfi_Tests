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

### Use numbered standalone async entry points backed by minimal shared helpers

The knowledge base will retain the roadmap's `01_...py` through `12_...py` filenames and give every script an `async main()` invoked through `asyncio.run()`. A small local helper module will own common configuration parsing, secret/account masking, session lifecycle, timeout wrappers, output formatting, and optional CSV writing. Topic-specific SDK calls remain directly visible in each script.

This provides a consistent safety baseline without concealing the SDK APIs readers need to learn. Duplicating the safety logic in every script was rejected because it would drift; a broad application-style SDK wrapper was rejected because it would stop being a simple SDK reference.

### Make configuration explicit and fail closed

Credentials are read only from documented environment variables (`TASTYTRADE_CLIENT_SECRET` and `TASTYTRADE_REFRESH_TOKEN`); script-specific inputs use documented arguments or environment variables. Scripts validate required configuration before opening sessions, require an explicit account selection for account-specific actions, and surface actionable authentication, permissions, network, and timeout errors without printing sensitive values.

Sandbox mode remains explicit and uses its own credentials. Production-only streaming scripts must reject sandbox mode rather than silently changing environments.

### Bound all externally driven work and label data honestly

Network requests use finite timeouts. Streaming scripts accept a maximum event count and timeout, unsubscribe and close in `finally` cleanup, and report whether collection ended by the configured limit, timeout, or error. Candle output is sorted, deduplicated, range-filtered, timezone-preserving, and labeled partial unless the tested SDK provides a reliable completion signal.

This favors reproducible demonstrations over convenience. Unbounded streaming and implicit defaults were rejected because neither produces a dependable reference result.

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

- Which exact Python version and `tastytrade` SDK version will be the initial supported, tested baseline?
- Does the selected SDK expose a reliable historical-candle backfill completion signal, and what should the demo call it if it does not?
- Which command-line argument convention, if any, already exists elsewhere in the repository and should be reused for script inputs?
