# MM Planner TLA+

This directory contains the current TLA+ reference model for the
market-maker-local liquidity planner.

## Scope

The model is intentionally narrow:

- MM-only
- exactly two assets
- confirmed obligations only
- active outbound payout batches only
- pending settlements only after outbound success
- active rebalances only
- deterministic planner step

The model intentionally does not include:

- OTC protocol state
- quote-time reservation
- obligation history after an open obligation is dropped
- wallet reconciliation and restart recovery
- executor sub-states like committed vs broadcast
- fees, slippage, or rebalance cost
- a stored future drain plan

## Stored State

The TLA+ state matches the current reference model:

- `obligations`
- `available_inventory`
- `outbound_batches`
- `pending_settlements`
- `rebalances`

`outbound_batches`, `pending_settlements`, and `rebalances` contain only active
unresolved actions. They are not historical logs.

`available_inventory` means planner-usable inventory right now. It excludes
inventory already committed to active outbound batches or active rebalances, and
it excludes inventory still trapped in `pending_settlements`.

In the model, `id` provides a deterministic tie-break within the active
obligation set rather than trying to model a real UUID.

## Lifecycle

The current abstract lifecycle is:

1. `obligations`
2. `outbound_batches`
3. `pending_settlements`
4. `available_inventory`

More precisely:

1. An `open` obligation consumes `available_inventory` when it is committed into
   an `outbound_batch`.
2. When an `outbound_batch` succeeds, the outbound leg is complete and the
   obligation moves into `pending_settlement`.
3. When the settlement completes, the inbound asset becomes usable and is added
   back into `available_inventory`.

An `open` obligation may also be dropped before it is committed into an outbound
batch. Obligations already present in `outbound_batches` cannot be dropped.

For the checked inventory-monotonicity temporal property, the model now
constrains each obligation so `settlement_amount >= payout_amount`. That makes
successful outbound completion inventory-non-lossy in BTC-equivalent units.

## Planner Behavior

`PlannerStep` is the core derived decision rule:

1. Group open obligations by payout asset.
2. Within each asset lane, order obligations by `(created_at, id)`.
3. Start the maximal oldest-first outbound batch for each asset with no active
   outbound batch.
4. Subtract those payout amounts from `available_inventory` and mark those
   obligations `outbound`.
5. Recompute remaining open payout demand and post-batch available inventory.
6. Only consider rebalance if the larger post-batch available inventory is at
   least the configured trigger share of total post-batch available inventory.
7. Only consider rebalance if the smaller side still has blocked demand after
   the current outbound batches are started.
8. If those conditions hold, and no rebalance is already active, start one
   rebalance for the minimum of:
   - the amount needed to move the majority side back toward the configured
     target share
   - source surplus
   - minority-side blocked deficit

The model keeps the plan implicit. It checks the safety of the immediate
planner step rather than storing a separate future schedule.

`CreateObligation` allows equal timestamps. While the active obligation set is
non-empty, new obligations choose a `created_at` from the range
`[current_max_created_at, MaxCreatedAt]`, so multiple obligations may share the
same timestamp without making ordering ambiguous.

The default trigger is `70%`, encoded as integer arithmetic with
`RebalanceTriggerNumerator = 7` and `RebalanceTriggerDenominator = 10`. The
model uses `>=`, so an exact `70/30` split is treated as rebalance-eligible.

The default target is `50%`, encoded with `RebalanceTargetNumerator = 1` and
`RebalanceTargetDenominator = 2`. Because the model uses integer quantities,
the rebalance amount moves the majority side toward `50/50` conservatively
without forcing an overshoot when an exact split is impossible.

## Checked Invariants And Properties

The starter config checks:

- type correctness
- unique obligation ids
- active outbound batches only reference real obligations
- outbound batch records match the obligations they carry
- no obligation appears in multiple active outbound batches
- at most one active outbound batch per asset
- active pending settlements only reference real obligations
- pending settlements match the obligations they carry
- no obligation appears in multiple pending settlements
- at most one active rebalance
- rebalances always move between distinct assets
- every `outbound` obligation belongs to an active outbound batch
- every non-`outbound` obligation belongs to no active outbound batch
- every `pending_settlement` obligation belongs to an active pending settlement
- every non-`pending_settlement` obligation belongs to no active pending
  settlement
- active outbound batches respect oldest-first ordering within an asset lane
- total tracked inventory never decreases across steps, where tracked inventory
  means:
  - free `available_inventory`
  - active outbound batch payout inventory
  - active pending settlement claims
  - active rebalance commitments

## Running TLC

The repository does not vendor TLA+ tooling. Once `tla2tools.jar` is available,
you can run TLC with:

```bash
cd bin/market-maker/spec
java -cp /path/to/tla2tools.jar tlc2.TLC MMPlanner.tla -config MMPlanner.cfg
```

The default config is the broad bounded search profile:

- `MaxObligationId = 3`
- `MaxCreatedAt = 3`
- `MaxAmount = 2`
- `MaxInventory = 3`

Because payout and settlement amounts still branch, but now under the
non-lossy constraint `settlement_amount >= payout_amount`, this default run is
no longer a trivial smoke test. It is still the right broad model check, but it
should be treated as a longer-running verification pass.

For a deeper run with larger amount and inventory bounds:

```bash
cd bin/market-maker/spec
java -cp /path/to/tla2tools.jar tlc2.TLC MMPlanner.tla -config MMPlanner.deep.cfg
```

`MMPlanner.deep.cfg` uses:

- `MaxObligationId = 4`
- `MaxCreatedAt = 3`
- `MaxAmount = 3`
- `MaxInventory = 5`

The deeper config is slower still and is best treated as an explicit stress run.

For a targeted deep run that still uses larger amount and inventory bounds but
fixes the initial state to a non-lossy monotonicity scenario that terminates
much faster:

```bash
cd bin/market-maker/spec
java -cp /path/to/tla2tools.jar tlc2.TLC MMPlanner.tla -config MMPlanner.deep.targeted.cfg
```

`MMPlanner.deep.targeted.cfg` uses:

- `MaxObligationId = 2`
- `MaxCreatedAt = 1`
- `MaxAmount = 3`
- `MaxInventory = 5`

That config starts from a state with:

- one `AssetA` obligation with `payout_amount = 2` and `settlement_amount = 3`
- one blocked `AssetB` obligation with `payout_amount = 2`
- `5` units of free `AssetA` inventory and `0` units of free `AssetB`

So the first planner step can simultaneously:

- start an `AssetA` outbound batch
- start an `AssetA -> AssetB` rebalance

That makes it a good terminating check for the new tracked-inventory
monotonicity property without replacing the broader stress profile.

For a concrete concurrency scenario where a rebalance is already active, a new
opposite-side obligation arrives, and the planner starts an outbound batch
before the rebalance resolves:

```bash
cd bin/market-maker/spec
java -cp /path/to/tla2tools.jar tlc2.TLC MMPlanner.tla -config MMPlanner.interleaving.cfg
```

That config fixes the initial state and uses an action constraint to force this
two-step interleaving:

1. Start with an active `AssetA -> AssetB` rebalance and an open `AssetB`
   obligation.
2. Create a new `AssetA` obligation while the rebalance is still active.
3. Run `PlannerStep`, which starts an `AssetA` outbound batch concurrently with
   the still-active rebalance.

## Trace Validation

There is also a TLC-backed differential test path for validating real Rust
planner executions against the TLA+ model, rather than against another Rust
reference implementation.

Run it from the repo root with:

```bash
just mm-tla-trace-tests
```

That recipe:

- ensures `tla2tools.jar` is present in `.cache/`
- runs the ignored Rust test binary at
  `bin/market-maker/tests/planner_tla_trace_validation.rs`
- generates a concrete `MMPlannerTraceData.tla` module per test case
- checks the recorded Rust before-state, action label, and after-state against
  `MMPlannerTraceValidation.tla`

The current trace cases cover:

- rebalance fail/retry/succeed, then outbound success, then settlement
  completion
- outbound batch failure followed by dropping an open obligation
- an automatically generated stress corpus of Rust planner traces, each of
  which is checked by TLC

The generated stress case defaults to:

- `64` generated traces
- `12` max planner steps per trace
- RNG seed `1`

You can override those with:

- `MM_TLA_TRACE_CASES`
- `MM_TLA_TRACE_STEPS`
- `MM_TLA_TRACE_SEED`

This is a true external-oracle check: TLC only passes if each recorded Rust
transition is also a valid `MMPlanner` transition with the same observed state.

## Next Steps

Once this base model is stable, the next useful extensions are:

1. Recovery and reconciliation.
2. Scenario-specific model configs.
3. A derived future drain-plan operator for inspection.
4. Optional liveness properties after the safety model is trusted.
