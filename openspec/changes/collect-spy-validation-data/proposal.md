## Why

The bounded demos cannot produce the durable, contemporaneous SPY quote,
analytics, and candle sample required by Macro_Ideas' broker validation handoff.
A collector must survive interruptions without claiming complete coverage.

## What Changes

- Add a read-only collector for today's regular SPY session, waiting before
  the open and exiting on holidays, after close, or at a 10 GB storage threshold.
- Sample a bounded, moneyness-based multi-expiry option pilot once per minute.
- Persist source payloads, normalized quotes/analytics, candle revisions,
  configuration, universe revisions, and diagnostics in DuckDB transactions.
- Provide restart provenance, bounded recovery, portable exports, and tests.

## Capabilities

### New Capabilities

- `spy-validation-collector`: Scheduled, persistent market-data collection
  supporting the external quote collector requirements.

### Modified Capabilities

None.

## Impact

Adds a standalone script and focused collector helpers/tests without changing
existing demos. Adds DuckDB to the pinned dependencies. Uses only market-data
and instrument-reference SDK interfaces; no order or account mutations.
Reference: https://github.com/khazaddim/Macro_Ideas/blob/main/short_put_replay/quote_collector_requirements.md
