## 1. Establish the verified demo baseline

- [ ] 1.1 Inspect the current repository Python tooling and select a supported Python version plus a tested `tastytrade` SDK version/range that uses asynchronous OAuth.
- [ ] 1.2 Add the minimal dependency declaration and document the selected compatibility baseline in `Tastyware_Demo_Scripts/README.md`.
- [ ] 1.3 Define the `TASTYTRADE_CLIENT_SECRET`, `TASTYTRADE_REFRESH_TOKEN`, environment-selection, account-selection, timeout, and output-directory conventions in the README.
- [ ] 1.4 Add Git ignore coverage for the local demo export directory and verify no credential, token, account-data, or generated export path can be committed.

## 2. Build shared safety and documentation infrastructure

- [ ] 2.1 Create a small local helper module for required environment-variable validation, explicit sandbox/production configuration, and actionable failures that do not expose secret values.
- [ ] 2.2 Add shared account-number and sensitive-value redaction/formatting helpers and test representative values.
- [ ] 2.3 Add session and timeout lifecycle helpers that cleanly close resources and distinguish authentication, permission, network, and timeout failures.
- [ ] 2.4 Add bounded streaming-collection helpers that report event-limit, timeout, and failure completion states and clean up subscriptions in `finally`.
- [ ] 2.5 Add timezone-preserving candle normalization, sort/deduplication, range filtering, and CSV export helpers with the documented `symbol`, `timestamp`, `open`, `high`, `low`, `close`, and `volume` columns.
- [ ] 2.6 Create a concise module-docstring template containing the upstream link, versions, PowerShell command, inputs, safety mode, expected redacted output, and common failure guidance.

## 3. Implement Phase 1 connection and account references

- [ ] 3.1 Implement `01_test_connection.py` with an authenticated account request, SDK/version/environment reporting, and separate zero-account versus authentication failure handling.
- [ ] 3.2 Implement `02_list_accounts.py` with masked account summaries and explicit account-selection instructions.
- [ ] 3.3 Implement `03_account_balances.py` with explicit account selection and labeled balance, buying-power, net-liquidating-value, and available timestamp output.
- [ ] 3.4 Implement `04_current_positions.py` with empty-position handling and labeled symbol, instrument, quantity/direction, and average-open-price output.
- [ ] 3.5 Add credential-free unit tests for Phase 1 configuration validation, redaction, empty results, account-selection behavior, and output formatting.
- [ ] 3.6 Update the README index with Phase 1 status, exact PowerShell commands, inputs, expected output, and known failures.

## 4. Implement Phase 2 market and historical-data references

- [ ] 4.1 Implement `05_market_data_snapshot.py` with configurable symbol/instrument type and explicit unavailable or stale field reporting.
- [ ] 4.2 Implement `06_stream_quotes.py` with production-only validation, configurable symbols/event limit/timeout, and subscription cleanup.
- [ ] 4.3 Verify the selected SDK's candle backfill/completion semantics and document the verified behavior or partial-result limitation.
- [ ] 4.4 Implement `07_historical_candles.py` with explicit timezone-aware start/end inputs, production-only validation, bounded collection, normalized OHLCV output, partial-result labeling, and optional CSV export.
- [ ] 4.5 Implement `08_account_transaction_history.py` with explicit account/date inputs, transaction field formatting, and documented pagination behavior for the selected SDK.
- [ ] 4.6 Implement `09_account_value_history.py` with explicit account/lookback inputs, timestamp/value output, optional CSV export, and documentation that values can include deposits and withdrawals.
- [ ] 4.7 Add credential-free unit tests for snapshot missing/stale values, streaming completion states, candle normalization/export, and history pagination behavior.
- [ ] 4.8 Update the README index with Phase 2 status, commands, production-only restrictions, export location, and partial-data guidance.

## 5. Implement Phase 3 options and safe order-preview references

- [ ] 5.1 Implement `10_option_chain.py` with configurable underlying/expiration filtering, bounded output, and unavailable-expiration handling.
- [ ] 5.2 Implement `11_stream_option_greeks.py` using actual contract `streamer_symbol` values, production-only validation, bounded subscriptions, and cleanup.
- [ ] 5.3 Implement `12_order_dry_run.py` with explicit account, symbol, quantity, and `Decimal` limit-price inputs; display proposed order, validation, buying-power, and fee information when available.
- [ ] 5.4 Add a regression test that verifies the order-preview SDK call always receives `dry_run=True` and that no live-order option/path exists.
- [ ] 5.5 Add credential-free unit tests for option/Greek input validation, bounded streaming results, signed-price documentation, and dry-run output formatting.
- [ ] 5.6 Update the README index with Phase 3 status, commands, expected output, and the explicit live-order exclusion.

## 6. Verify and publish the knowledge base

- [ ] 6.1 Run the focused credential-free test suite without credentials and confirm it makes no network requests.
- [ ] 6.2 Run each script's syntax/type/lint checks using the repository's established tooling.
- [ ] 6.3 With locally supplied, non-committed credentials, execute opt-in read-only smoke checks for supported environment paths and record only redacted verification results in documentation.
- [ ] 6.4 Review every script and README entry against the knowledge-base checklist, confirming exact PowerShell invocation, input/output/failure documentation, resource cleanup, and accurate completion-state claims.
