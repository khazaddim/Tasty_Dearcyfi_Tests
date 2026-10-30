## Context

The external Macro_Ideas collector contract requires contemporaneous quotes,
analytics, completed candles, provenance, and explicit capability limitations.
The user selected a bounded multi-expiry pilot, minute snapshots rather than
all ticks, and today's regular session only, with a 10 GB stop threshold.

## Goals / Non-Goals

**Goals:** Persist incremental DuckDB transactions, preserve original sampled
feed payloads, make gaps and unknown semantics visible, and produce portable
Parquet exports without credentials. Wait for today's open using XNYS; do not
roll forward to a different trading date.

**Non-Goals:** Orders, model approval, historical live-quote backfill, all-chain
capacity certification, and a claim that different feed events are atomic.

## Decisions

- Use the existing pinned SDK for authenticated DXLink connections. Capture
  market-data events before its validators: SDK 13.2.3 otherwise skips some
  invalid quotes and replaces invalid candle prices with zero. Isolate this
  version-specific adapter and test both full and compact event formats.
- Keep timestamped Quote, Trade, and Greeks state in memory; persist sampled
  raw states and explicit observation IDs once per minute. Flush candle
  revisions and diagnostics frequently. Use bounded caches/queues.
- Use one DuckDB writer, transactions, WAL recovery, and periodic checkpoints.
  New invocations have distinct run IDs; unfinished prior runs become
  interrupted, never silently completed. Check database plus WAL size before
  and after each batch. The threshold is a stop trigger, not a byte-perfect
  filesystem quota; one bounded batch/checkpoint can cross it.
- Initial universe: nearest available expirations to 0, 7, 14, 17, 28, and 56
  calendar days; adjacent near-ATM/OTM put strikes and selected nearby calls.
  Selection uses terms and SPY spot, never broker delta. Refresh periodically,
  retaining earlier selections subject to a configured cumulative contract cap.
- User-approved defaults: 5-second quote age, 15-second analytics age, 2-second
  option/underlying source skew, 5-second scheduling lateness, 15-second candle
  publication grace. Unknown source times remain unknown; retain failed cases.
- Store all observed candle revisions, mark elapsed-with-grace separately
  from provider finality, periodically re-request bounded overlap and perform
  a bounded closing reconciliation. Missing bars are unknown absence, not
  synthetic zero-volume bars.
- No snapshot cache survives reconnect as fresh data. Bounded reconnects
  retrieve new credentials through SDK session refresh and resubscribe.
- Portable export is an explicit offline operation, avoiding automatic export
  doubling disk use during capture. Export tables, schema, checksums, and
  comparison-readiness/coverage summaries.

## Risks / Trade-offs

- Unknown feed permissions, delays, analytics conventions and adjustment
  basis -> capability manifest and quality flags; no unverified normalization.
- SDK internal callback -> enforce pinned version, regression-test framing.
- Sparse option activity -> separate quote age, receipt age, and transport
  health; stale analytics do not discard usable quotes.
- Long-lived DuckDB writer -> separate processes cannot concurrently inspect
  the live database; print health summaries and export after shutdown.
- Process/power loss -> committed WAL is recoverable; pending minute state
  may be lost, and recovery records the interruption.
- Absolute clock synchronization cannot be established from feed arrivals ->
  retain user-reported offset or unknown status and detect local clock jumps.

## Migration Plan

DuckDB has no compatible cp314t wheel in this environment. Keep the existing
free-threaded demo environment unchanged and use an isolated standard CPython
3.14 collector environment. Its lock constrains shared packages to the existing
demo lock and adds DuckDB plus its runtime TIMESTAMPTZ dependency, pytz.
New schema version is checked before modifying an existing database; refuse
unknown versions. Existing scripts remain unchanged.

## Open Questions

Live capability/permission verification and a full-session pilot require
authenticated user execution. Tests cannot certify provider retention rights,
real-time entitlement, subscription capacity, or broker model conventions.
