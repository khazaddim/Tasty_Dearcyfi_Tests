## ADDED Requirements

### Requirement: Independently runnable reference scripts
The repository SHALL provide the twelve numbered Tastytrade SDK demo scripts documented in `Tastyware_Demo_Scripts/README.md`. Each script SHALL use the tested asynchronous OAuth API, run from PowerShell at the repository root without requiring another demo to execute first, and explain its purpose, upstream SDK reference, tested Python/SDK versions, exact run command, inputs, expected redacted output, environment restriction, and relevant failures.

#### Scenario: Running an account-reference demo
- **WHEN** a user provides the documented credentials and required input to a Phase 1 script
- **THEN** the script performs only its documented read-only account action and emits a labeled, redacted result or an actionable error

#### Scenario: Reading an individual script
- **WHEN** a developer opens a demo script
- **THEN** its documentation identifies the SDK behavior it demonstrates and how to run and validate it without consulting an unrelated script

### Requirement: Safe credential and account handling
The demo suite SHALL obtain credentials only from documented environment variables, SHALL never hard-code, print, or persist credentials or session tokens, and SHALL mask account numbers in normal output. Account-specific demos SHALL require an explicit account selection when multiple accounts are accessible and SHALL distinguish an empty result from authentication or authorization failure.

#### Scenario: Missing required credentials
- **WHEN** a user runs a demo without a required credential environment variable
- **THEN** the demo exits before creating a session and reports the missing variable name without revealing any supplied secret

#### Scenario: Multiple accessible accounts
- **WHEN** an account-specific demo has no explicit account selection and more than one account is available
- **THEN** the demo does not choose an account and reports how to provide the selection

### Requirement: Explicit environment and bounded external operations
Every authenticated demo SHALL require explicit production selection and SHALL reject sandbox selection; the suite SHALL NOT use sandbox credentials or claim sandbox compatibility. Every demo SHALL apply finite timeouts to network and streaming operations. Streaming demos SHALL accept a bounded collection policy, clean up subscriptions and resources, and state whether output is complete, bounded, timed out, or failed.

#### Scenario: Sandbox environment request
- **WHEN** a user selects sandbox for an authenticated demo
- **THEN** the demo rejects the request and explains that this knowledge base is tested against production only

#### Scenario: Streaming deadline expires
- **WHEN** a streaming demo reaches its configured timeout before its event limit
- **THEN** it cleans up its resources and reports the data as timeout-bounded rather than as a complete stream

### Requirement: Read-only market and historical-data references
The market-data demos SHALL separate snapshot, quote-streaming, candle-history, transaction-history, and account-value-history behavior. The candle demo SHALL require an explicit start time, preserve timezone information, sort and deduplicate OHLCV rows, filter rows to the requested range, and label results partial unless completion is established by a tested SDK signal. Optional CSV export SHALL use the documented predictable candle columns and write only to an ignored local output directory.

#### Scenario: Collecting candle history
- **WHEN** a user requests candles for a bounded date range
- **THEN** the demo outputs timezone-preserving, sorted, deduplicated rows within that range and discloses whether the result is partial

#### Scenario: Initial candle endpoint finish signal
- **WHEN** the candle helper receives a candle for both endpoint sessions selected using the instrument's exchange/product calendar, adjusting a closed-market start date forward to the next session and today (or an explicit end date) backward to the latest session opened by both the end time and current time
- **THEN** it stops collection, cleans up streaming resources, and reports the endpoint dates and endpoint coverage while retaining the partial-data label unless a tested SDK signal proves completeness
- **AND** it does not require internal-gap recovery to satisfy this initial finish signal

#### Scenario: Selecting calendar-adjusted candle endpoints
- **WHEN** a requested endpoint falls on a weekend, exchange holiday, or before the end session has opened
- **THEN** the helper uses `pandas_market_calendars` to select eligible endpoint sessions without widening the requested range, preserves intraday bounds, and reports the calendar and adjusted endpoint dates
- **AND** an unknown calendar or a range with no eligible session produces an actionable error rather than a silent weekday fallback

#### Scenario: Candle endpoint is unavailable
- **WHEN** either endpoint remains unobserved
- **THEN** the helper respects its finite attempt and time budgets and reports the missing endpoint and partial result rather than silently declaring endpoint coverage

#### Scenario: Missing market-data fields
- **WHEN** a market-data response omits a quote field or returns stale data
- **THEN** the demo identifies the value as unavailable or stale rather than presenting it as a current value

### Requirement: Safe option and order-preview references
The option-chain and Greeks demos SHALL use actual contract streamer symbols and bounded subscriptions. The order demo SHALL build and display a proposed order using explicit account, symbol, quantity, and decimal limit-price inputs, SHALL invoke the SDK only with `dry_run=True`, and SHALL report validation, buying-power, and fee information when available. The suite SHALL not implement a live-order submission, replacement, or cancellation path.

#### Scenario: Previewing a valid order
- **WHEN** a user supplies valid order-preview inputs
- **THEN** the demo displays the proposed dry-run order and available preview effects without transmitting a live order

#### Scenario: Attempting to enable live orders
- **WHEN** a user searches for or attempts to pass a live-order execution option to the order demo
- **THEN** no such option or execution path is available

### Requirement: Verifiable documentation and safety behavior
The repository SHALL provide automated tests for credential-free shared behavior, including required-configuration validation, output redaction, bounded collection logic, predictable export schema, and the enforced dry-run order invocation. Live credentialed checks SHALL be opt-in, read-only except for SDK dry-run validation, and excluded from routine automated test runs. Credentialed production smoke checks SHALL begin immediately after `01_test_connection.py` is implemented and be repeated incrementally for each completed phase before work proceeds to the next phase.

#### Scenario: Running routine tests without credentials
- **WHEN** the demo-suite automated tests run without Tastytrade credentials
- **THEN** they validate shared safety and formatting behavior without initiating a network request

#### Scenario: Validating credentials and scripts incrementally
- **WHEN** `01_test_connection.py` and then each subsequent phase are implemented
- **THEN** the developer can opt in to local-credential smoke checks at that point, beginning with an authenticated connection check and validating each completed phase before proceeding
- **AND** credential values remain local and verification results are redacted
- **AND** all credentialed checks target production; sandbox is not used because its behavior and feature availability differ

#### Scenario: Updating the script index
- **WHEN** a demo is implemented or its tested SDK compatibility changes
- **THEN** the README index records its current status, compatibility baseline, and run instructions
