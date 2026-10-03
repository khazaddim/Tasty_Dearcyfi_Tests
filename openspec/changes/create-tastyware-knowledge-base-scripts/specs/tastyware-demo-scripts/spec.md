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
Every demo SHALL require explicit sandbox or production selection, SHALL reject a sandbox configuration for production-only DXLink streaming, and SHALL apply finite timeouts to network and streaming operations. Streaming demos SHALL accept a bounded collection policy, clean up subscriptions and resources, and state whether output is complete, bounded, timed out, or failed.

#### Scenario: Sandbox quote-stream request
- **WHEN** a user invokes a DXLink quote, candle, or Greeks demo in sandbox mode
- **THEN** the demo rejects the request with an explanation that streaming requires the documented production session

#### Scenario: Streaming deadline expires
- **WHEN** a streaming demo reaches its configured timeout before its event limit
- **THEN** it cleans up its resources and reports the data as timeout-bounded rather than as a complete stream

### Requirement: Read-only market and historical-data references
The market-data demos SHALL separate snapshot, quote-streaming, candle-history, transaction-history, and account-value-history behavior. The candle demo SHALL require an explicit start time, preserve timezone information, sort and deduplicate OHLCV rows, filter rows to the requested range, and label results partial unless completion is established by a tested SDK signal. Optional CSV export SHALL use the documented predictable candle columns and write only to an ignored local output directory.

#### Scenario: Collecting candle history
- **WHEN** a user requests candles for a bounded date range
- **THEN** the demo outputs timezone-preserving, sorted, deduplicated rows within that range and discloses whether the result is partial

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
The repository SHALL provide automated tests for credential-free shared behavior, including required-configuration validation, output redaction, bounded collection logic, predictable export schema, and the enforced dry-run order invocation. Live credentialed checks SHALL be opt-in, read-only except for SDK dry-run validation, and excluded from routine automated test runs.

#### Scenario: Running routine tests without credentials
- **WHEN** the demo-suite automated tests run without Tastytrade credentials
- **THEN** they validate shared safety and formatting behavior without initiating a network request

#### Scenario: Updating the script index
- **WHEN** a demo is implemented or its tested SDK compatibility changes
- **THEN** the README index records its current status, compatibility baseline, and run instructions
