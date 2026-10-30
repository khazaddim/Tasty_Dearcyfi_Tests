## 1. Collector core

- [x] 1.1 Add DuckDB dependency and calendar/configuration/universe helpers.
- [x] 1.2 Implement lossless sampled feed capture and explicit quality decisions.
- [x] 1.3 Implement transactional storage, recovery, size stop, and portable export.

## 2. Session orchestration

- [x] 2.1 Implement today's-session CLI, pre-open waiting, and bounded recovery.
- [x] 2.2 Wire quote/trade/Greeks sampling, candle revisions, overlap and reconciliation.
- [x] 2.3 Document invocation, schema, limitations, and comparison readiness.

## 3. Verification

- [x] 3.1 Test calendars, universe selection, alignment, invalid data and raw framing.
- [x] 3.2 Test crash recovery, size threshold, revisions, exports and failure paths.
- [x] 3.3 Run focused tests, existing compatibility tests, and OpenSpec validation.
