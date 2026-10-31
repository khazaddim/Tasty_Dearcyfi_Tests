# Macro Ideas handoff: preliminary SPY BSM fixture

Prepared October 31, 2026. **Research fixture, not numerical BSM approval or
proof of market-time synchronization.** Continue model work in Macro Ideas;
this repository supplies observations and timing/selection diagnostics.

## Files and access

Base directory on this computer:
`C:\Chris\Tasty_Dearcyfi_Tests\Tastyware_Demo_Scripts`

- [Source DuckDB](./exports/spy_validation.duckdb): required, unchanged capture.
  Open **read-only**. Raw updates, broker analytics, candles, reference revisions,
  and original minute samples remain here.
- [Validation Parquet](./exports/validation/bsm_validation_v1_73c47ea5ee194dd2981efa25f9e94800/validation_windows.parquet):
  all examined boundary windows, quote pairs where available, contract
  references, diagnostics, and `exploratory_candidate`.
- [Manifest](./exports/validation/bsm_validation_v1_73c47ea5ee194dd2981efa25f9e94800/manifest.json):
  schema, counts, exact settings, limitations, and Parquet/database checksums.
- [Exploration notebook](./14_explore_spy_capture.ipynb): Sections 10-11 explain
  the analysis and reproducible export.

This is **not self-contained**: original observation/raw/reference IDs resolve
in the shared database. Keep captures and private outputs out of Git, including
if copied to Macro Ideas.

## Scope and candidate selection

Run `73c47ea5ee194dd2981efa25f9e94800` captures part of October 30's regular
session, approximately 10:37-16:00 New York time, across **85 selected SPY
options**, not the full chain or a complete trading day.

The fixture retains **25,570 option-boundary windows across 322 boundaries**,
including rejected windows and **357 rows without a before-boundary pair**.
**1,184 windows (4.63%)** pass the illustrative quiet-window screen.
Repeated windows and contracts sharing SPY observations are not independent.

Defaults: symmetric **+/-5-second** local-receipt windows; backward SPY pairing
within **2 seconds**, on the same connection and in receive-sequence order.
Screening requires at least two distinct receipt times on each side for both
instruments, valid prices, boundary/window gaps no larger than two seconds,
and bid/ask/midpoint ranges no larger than **$0.02 for options / $0.05 for SPY**.
See the manifest for the full settings.

These are exploration thresholds, **not approved BSM tolerances**. Correlation
is diagnostic only, never a selection gate. Retain rejection reasons and
compare selected versus rejected windows.

## Limitations and compromises

- **Quote source bid/ask timestamps are unavailable** for SPY and options.
  Receipts are known; source freshness, transport delay, and market-time
  synchronization are not. Never replace null source times with receipt times.
- [tastyware/tastytrade#195](https://github.com/tastyware/tastytrade/issues/195)
  and [tastyware/tastytrade#119](https://github.com/tastyware/tastytrade/issues/119)
  reproduce the issue. The maintainer attributes zero fields to raw API data;
  a dxFeed-broker agreement limitation remains an **unconfirmed explanation**.
- Stability and correlation cannot prove synchronization or exclude stale
  data. Slower intended model use supports sensitivity-based preliminary work,
  but sharp moves and near-expiry contracts still warrant caution.
- The selected price pair is backward-looking, but the stability screen uses
  **after-boundary receipts**: retrospective selection, not a live/backtest
  signal. Symmetric windows exclude the regular-session close boundary.
- Candle/trade/Greeks source-time fields have different meanings. Analytics
  and candles are **not automatically aligned or attached** to fixture rows.
  Contract fields use the first recorded reference; later revisions remain
  available in DuckDB.
- Broker IV/Greeks are model references, not ground truth. Units and model
  inputs remain unverified; `analytics.price` is not a broker mark.
  Existing strict collector eligibility flags are not overridden by candidates.

## Next steps in Macro Ideas

1. Verify manifest checksums and load all fixture rows; use the candidate flag
   as a preliminary subset, not a blanket approval.
2. Resolve quote IDs through `quotes.record_id`; resolve contract references
   through `instruments.record_id`. Raw payloads join through `raw_record_id`.
   Choose explicit independent analytics and candle matching/revision policies;
   keep missing matches and their reasons.
3. Specify rates/dividends, valuation-time/expiry conventions and units.
   Standard BSM is European; SPY options are American, so exercise/dividend
   effects may explain genuine discrepancies.
4. Run independent known-input/reference tests, then bid/mid/ask and observed
   spot-range/pairing-offset sensitivity comparisons. Repricing with IV inferred
   from the same price is a consistency test, not independent validation.
5. Report failures, no-arbitrage/low-vega cases, exclusions, selection bias and
   unresolved discrepancies. No historical replay or broader market approval
   follows from this partial-session fixture.

The [original collector requirements](https://github.com/khazaddim/Macro_Ideas/blob/main/short_put_replay/quote_collector_requirements.md)
remain the strict reference; this handoff documents a preliminary compromise,
not a claim that their source-time alignment requirements were satisfied.
