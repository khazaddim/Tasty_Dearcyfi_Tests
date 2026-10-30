## ADDED Requirements

### Requirement: Current-session scheduling
The collector SHALL wait for today's XNYS regular open, join an active session,
and stop after that session's bounded candle reconciliation. It SHALL NOT
roll holidays or closed sessions forward to another date.

#### Scenario: Started before an early-close session
- **WHEN** started before the open on an early-close trading day
- **THEN** it waits until the calendar open and uses that day's early close.

#### Scenario: Started after close or on a holiday
- **WHEN** today's regular session is unavailable or finished
- **THEN** it exits explicitly without authenticating or waiting for tomorrow.

### Requirement: Durable bounded storage
The collector SHALL commit sampled data and provenance incrementally to DuckDB,
preserve committed data across restart, and stop at the configured 10 GB
database-plus-WAL threshold, checked around every bounded write batch.

#### Scenario: Interrupted process
- **WHEN** a process exits without finalizing its run and the collector restarts
- **THEN** prior committed observations remain and the old run is interrupted.

#### Scenario: Storage threshold
- **WHEN** database plus WAL reaches the configured threshold
- **THEN** collection stops with a partial size-limit outcome; it does not
  silently delete data or claim a full session.

### Requirement: Reproducible pilot and observations
The collector SHALL select a bounded multi-expiry SPY pilot from contract terms
and spot, retain effective universe revisions, and persist minute-sampled
quotes, trades, analytics, raw payloads, source times, receipt times and joins.

#### Scenario: Missing analytics or invalid quotes
- **WHEN** quotes are one-sided, zero-bid, stale, or analytics are missing
- **THEN** observations remain with explicit quality reasons and independent
  quote and analytics eligibility.

### Requirement: Timing and capability honesty
The collector SHALL record numeric timing thresholds, provider/SDK versions,
delay status, supported fields, unknown conventions, and limitations.
Option/underlying pairings SHALL NOT silently use a future source quote.

#### Scenario: Unknown timestamps
- **WHEN** source time is absent or zero
- **THEN** freshness is unknown rather than derived from a successful receipt.

### Requirement: Candle revisions and coverage
The collector SHALL retain observed one-minute candle revisions, re-request
bounded overlap, reconcile at close, and report expected versus observed
coverage without synthesizing absent bars or declaring provider finality.

#### Scenario: Revised candle
- **WHEN** a previously observed candle changes
- **THEN** both versions remain and a deterministic latest view is available.

### Requirement: Operational failures and portable handoff
The collector SHALL perform bounded recovery, record sanitized failure/gap
diagnostics, expose partial outcomes, and support offline Parquet export with
schema and checksums. It MUST NOT place orders or persist authentication data.

#### Scenario: Failed subscription
- **WHEN** subscription fails or transport stops
- **THEN** bounded recovery is attempted, gaps are recorded, and exhaustion
  ends the run with a nonzero failure status.
