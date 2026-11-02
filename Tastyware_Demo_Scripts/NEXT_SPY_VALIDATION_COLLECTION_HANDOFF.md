# Next SPY validation collection: implementation handoff

Prepared November 1, 2026 for the Tastytrade/Tastyworks collection repository.
This is a work request for improving the next bounded data capture. It is not
an approval record, a claim that broker analytics are ground truth, or
authorization to use the data for historical strategy replay.

## Objective

Improve [13_collect_spy_validation.py](./13_collect_spy_validation.py) and its
supporting files so the next SPY collection can measure the inputs most
relevant to the Macro_Ideas short-put entry research:

- Actual-time DTE from 15 through 20 days, inclusive.
- OTM puts around absolute option deltas 0.15, 0.20 and 0.25. For puts, the
  corresponding signed option deltas are negative.
- Enough neighboring strikes at each expiration to compare contracts and
  identify cases where bar- and quote-derived inputs select different
  contracts.
- Delta recomputation at later replay decision points, including around a
  potential roll trigger, plus the candidate universe needed to reselect a
  replacement option.
- Contemporaneous option and SPY quotes, broker IV/Greeks, and completed
  one-minute candles, with source/receipt times and quality limitations kept
  explicit.

The intended first research authorization is **SPY OTM entry selection plus
bounded replay-time delta monitoring and roll-trigger reselection**. It is not
a request to approve every lifecycle feature. The current Macro_Ideas approval
guide explicitly excludes rolling from its restricted entry scope; the
approval scope/gate must be reviewed and extended before claiming that this
use is authorized. This collection handoff does not change or issue that
approval.

The central gap in the previous capture is not simply its number of rows. The
October 30 capture used 14/28-calendar-day expiration buckets rather than the
strategy's 15-20 actual-time-DTE scope. The existing broker study found only
six competitive comparisons with at least two eligible strikes under each
method. The next collection should be designed to improve relevant expiration,
delta, and adjacent-strike coverage, not just increase repeated observations.

## Timing limitation and deliberate oversampling

The feed is asynchronous: option quotes, SPY quotes, and broker Greeks arrive
as separate stream updates, and quote source timestamps have been missing in
the observed capture. Receipt order/time can show when this collector received
an update, but cannot prove when the market observation was made or that two
instruments were observed simultaneously. **More data cannot repair a missing
source timestamp or establish market-time synchronization.**

Oversample to improve the chance and breadth of usable comparisons, while
keeping that limitation explicit:

- Increase the number of distinct candidate puts around the target-delta
  region and retain adjacent strikes. Contract breadth is more valuable than
  many repeats of a few options.
- In the pilot, consider a configurable 15-second decision-snapshot cadence
  instead of the current 60 seconds, while preserving all received stream
  updates. Check storage, quote freshness, and event-loop health before
  keeping the higher rate.
- Measure unique option-quote, SPY-quote, and Greek record IDs per comparison.
  Repeated snapshots that point to the same cached updates are repeated
  observations, not new independent market evidence.
- Preserve receipt-time skew and quote/analytics ages; analyze a predeclared
  range of receipt-time pairing windows as sensitivity diagnostics. Keep
  source-time-aligned eligibility separate, and leave it unknown/ineligible
  when source times are missing.
- If target coverage or eligible-pair counts are still inadequate, add
  complete sessions or targeted contracts rather than relaxing freshness,
  timestamp, or quality rules after seeing results.
- Ensure observations span distinct market sessions and option states,
  including contracts moving toward the proposed roll-trigger delta. More
  observations during the entry-selection state alone do not validate
  replay-time delta behavior.

Report two conclusions separately: what was measured in the
receipt-time/diagnostic sample, and what remains unverified because source
timestamps or documented broker conventions are unavailable. Do not describe
the first as source-time-synchronized validation.

## Pre-run blocker: permission flag is inverted

The README instructs the operator to start the collector with
`--acknowledge-data-permissions`. In the current `main()` implementation, the
branch checks `if args.acknowledge_data_permissions` and exits with a message
that permission is required. Thus the documented affirmative command returns
failure, while omitting the flag bypasses that check.

Before any authenticated collection:

1. Correct the condition so collection is blocked when acknowledgment is
   absent and allowed to proceed only when it is present.
2. Add credential-free regression tests for both cases: missing acknowledgment
   must not authenticate or collect; affirmative acknowledgment must pass the
   gate.
3. Keep the README command and help text consistent with the fixed behavior.
   Do not remove the permission confirmation as a workaround.

## Required collector improvements

### 1. Make the sampled universe match actual-time DTE and target delta

The current universe defaults to calendar expiry targets
`0,7,14,17,28,56`, selects the nearest listed expirations, and chooses puts
from strike/spot anchors `0.95, 0.975, 1.0`, up to nine puts per expiration.
It does not select or guarantee coverage around any target delta. The existing
strike/spot ladder is useful for avoiding delta-only selection bias, but it
does not guarantee that the target deltas or enough competing strikes will be
present.

Update the configuration/selection/reporting flow to:

- Prioritize listed expirations that produce **15-20 actual-time DTE at the
  observation timestamp**. Record the exact expiration timestamp/convention
  and calculate DTE from it; a calendar-day target is only a way to find
  candidate expirations, not proof of actual DTE eligibility.
- Keep the candidate universe grounded in option terms and strike/moneyness.
  Add delta-aware coverage as a supplemental requirement, not as the sole
  universe filter. First verify whether the pinned SDK/API provides usable
  chain Greeks before relying on them for pre-subscription selection.
- For each target absolute delta (0.15, 0.20, 0.25), retain the candidate
  strikes and broker-delta observations needed to report coverage, neighboring
  alternatives, and no-candidate outcomes. Do not silently drop a target or
  switch to another expiration when coverage is missing.
- Make candidate counts and the contract cap explicit. If the cap prevents
  target coverage, record a partial/incomplete result and the exact excluded
  candidates; do not truncate silently or describe the capture as target
  complete.
- Retain the universe-selection rule, inputs, effective time, and all
  revisions so the candidate set can be reproduced later.

Delta conventions in every report must distinguish signed option delta from
absolute target delta and short-position exposure. The target-delta selection
comparison should use the same common eligible universe for both input paths,
record each path's achieved delta/selected contract or rejection, and preserve
all rejection reasons.

### 2. Capture replay-time delta and roll-trigger reselection evidence

The replay needs to recalculate the held option's delta at later decision
points, not reuse its entry delta. At each sampled decision point, retain the
held contract identity and terms, option quote/candle inputs, paired SPY
observations, broker IV/Greeks when available, and enough rate/dividend/time
metadata or explicit unknowns to reproduce the replay model's delta. Preserve
the candidate set and terms for a possible replacement option at that same
decision point; collecting only the originally selected contract cannot test
reselection.

The collector and analysis should support:

- Delta trajectories for distinct held contracts, not only the initial entry
  snapshot, with signed put delta, absolute delta, and short-exposure sign
  labeled separately.
- Comparisons between replay-calculated delta and broker-reported delta at
  sampled decision points, along with quote/bar input paths, source/receipt
  timing, and rejected or unavailable observations.
- Roll-trigger evaluation and deterministic replacement selection using the
  inputs available at that replay point; retain trigger state, eligible
  replacements, selected replacement or rejection, and the reason.
- Coverage around trigger crossings, including observations before and after
  the crossing where the available stream permits. Do not interpolate a
  missing crossing or treat asynchronously received updates as a synchronous
  snapshot.

Initial roll-trigger error/tolerance scenarios are **2-5 delta points**.
Treat 0.02 and 0.05 absolute delta as sensitivity-reporting cases until the
exact trigger, tolerance definition, and acceptance rule are explicitly
agreed. Report how many observed decisions fall inside, outside, or cannot be
classified under each scenario; do not silently turn these candidate bands
into approval thresholds.

The roll replacement expiration/DTE policy, decision cadence, trigger
direction, and whether the trigger uses signed or absolute delta must be
specified before evaluating results. Entry's 15-20 actual-time-DTE policy
does not automatically define a later replacement contract's eligibility.

### 3. Make preflight and run coverage actionable

`--plan` currently reports the session and database but not the complete
effective selection configuration. Extend it to show the resolved, non-secret
settings needed to confirm a run before authenticating: expiry targets and
their actual-DTE mapping/policy, delta targets, moneyness/candidate rules,
contract cap, sampling cadence, quality limits, feed-delay status, database
path, and session schedule.

At run end, report at least:

- Requested versus actual session coverage, missed/unrecorded sample cycles,
  reconnect gaps, and run status.
- Distinct expirations and contracts, with actual-time-DTE distribution.
- Candidate and eligible contract counts by expiration and target-delta
  region, including target-level no-candidate/rejection counts.
- Replay-state delta coverage: distinct held contracts and decision points,
  delta range and change, observations near/crossing candidate roll triggers,
  candidate replacement coverage, and selection/rejection outcomes.
- Fresh paired option/underlying quote and analytics coverage, timestamp
  availability, age/skew distributions, and explicit unknown-source-time
  counts.
- Candle interval coverage, revisions/pending or invalid intervals, and the
  separate status of candle-derived comparisons.

The store currently writes `broker_comparison_ready=False` for samples and the
run report. Keep that conservative until a tested matching policy actually
establishes readiness; do not change the flag merely because the stream
received Greeks and quotes. Receipt counts are not paired, fresh, or
comparison-eligible observations.

### 4. Preserve evidence and limitations

For each retained option, keep the quote, trade, broker analytics and candle
records, linked SPY observations, raw-record references, source timestamps,
receipt timestamps, quote sizes, quality flags, contract reference revisions,
and run/configuration/code provenance. Keep unavailable timestamps/conventions
null or explicitly unknown; never substitute receipt time for source time.
Do not treat broker analytics as a broker mark or infer their model inputs
unless the provider documents them.

Retain the existing safeguards: production/read-only collection, no orders,
no credentials or authentication frames in exports, bounded storage/retries,
and explicit partial/failure outcomes. Verify data retention and transfer
rights before running. Keep the source DuckDB unchanged after capture; stop
the writer before exporting and use a new export directory with its manifest
and checksums.

## Suggested collection sequence

1. Run credential-free tests and `--plan`; confirm the plan shows the intended
   expirations, actual-DTE policy, target deltas, caps, and session.
2. Verify production feed entitlement, retention/export permission, local
   clock synchronization, and whether the feed is real-time or delayed.
   Record unknown status as unknown rather than assuming real-time or zero
   clock offset.
3. Collect one complete regular SPY session as a pilot. Inspect target DTE and
   delta coverage, candidate breadth, freshness/skew, missing timestamps,
   replay-state/roll-candidate coverage, candle quality, and storage before
   expanding.
4. If the pilot is healthy, collect additional complete sessions as needed to
   improve distinct-contract, target-delta, and eligible-pair coverage. Review
   each session before expanding. Do not count repeated cached snapshots or
   minute observations as independent market conditions.
5. Stop the writer, export to a new directory, verify manifest checksums, and
   hand off the run ID, code revision, config, manifest, and coverage summary.
   Keep raw data and private exports out of Git.

If the implementation cannot guarantee the target universe in time, collect
only with an explicit moneyness-based sampling description and label target
delta coverage as unverified. Do not claim the resulting sample answers the
target-delta question.

## What this collection can and cannot support

**Useful evidence:** quote-versus-bar input sensitivity; broker-IV/Greek
comparisons under documented or explicitly unknown conventions; entry and
replay-time delta behavior; roll-trigger/reselection sensitivity in the
observed candidate universe; collector field, timing, and failure diagnostics.

**Not established by this collection:** historical split/price-basis evidence
for the Macro_Ideas source lake; historical dividend-record availability or
the entry study's trailing-dividend policy; executable fills; American-option
model accuracy; broad market-regime or historical reliability; or full replay
management, sizing, assignment, settlement, rolls, and P&L. The proposed
roll-trigger delta/reselection comparisons are bounded evidence, not
validation of the entire lifecycle. Contract reference fields may help a
later audit, but the collector's standard-root / 100-share filter is not a
full adjusted-deliverable audit.

The Macro_Ideas approval guide keeps these evidence tracks separate. Broker
data can strengthen the input/Greek/selection comparisons, but it does not by
itself clear the split-basis or dividend blockers, issue a model approval, or
authorize full replay.
