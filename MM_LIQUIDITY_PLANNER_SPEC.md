# Deterministic MM Liquidity Planner Spec

Status:

- draft
- design note, not yet an implementation contract
- intended to capture current thinking rigorously enough to simulate and review

## Summary

This document defines a proposed market-maker-local liquidity planner for `tee-otc`.

The planner exists to replace loosely coupled quoting, payment batching, and
rebalancing heuristics with one deterministic controller that:

- owns the MM's view of liquidity
- decides what can be filled now
- decides what must wait
- decides when and how rebalancing should happen
- exposes the exact current queue and planned actions

The planner is explicitly MM-internal. OTC remains authoritative for swap
lifecycle, confirmations, refunds, and coordination. The planner is the MM's
internal control plane for inventory allocation and execution ordering.

## Problem Statement

Today, the MM has multiple partially independent loops making liquidity
decisions:

- quoting estimates available inventory from wallet balances and liquidity locks
- payment batching decides what can be sent from current wallet balance
- rebalancing decides what inventory to convert based on separate balance logic

This causes several problems:

- the same inventory can be implicitly promised by different components
- ordering under load is not fully explicit or deterministic
- opposite-side flow and rebalance decisions are not coordinated by one policy
- behavior after restart is hard to reason about as a single system
- operators cannot easily answer "what is the MM going to do next?"

The goal of this design is to make MM behavior explainable, deterministic, and
testable under stress.

## Goals

- Define one deterministic source of truth for MM liquidity decisions.
- Make queue ordering explicit and reviewable.
- Prevent double-allocation of inventory across fills and rebalances.
- Allow concurrent execution where safe without sacrificing determinism.
- Support replanning when new swaps arrive or settlements complete.
- Make the current plan inspectable at any point in time.
- Provide a clean model that can be simulated and model-checked before full
  implementation.

## Non-Goals

- OTC does not become aware of MM internals in v1.
- This design does not change OTC's authority over swap lifecycle or refunds.
- This design does not attempt to guarantee user-facing fill ETA in v1.
- This design does not require quote-time inventory holds in v1.
- This design does not assume a single action is processed globally at a time.
- This design does not optimize for maximum theoretical throughput at the cost
  of predictability.

## Core View Of The System

This system should be viewed as a deterministic event-driven planner.

More precisely, it is:

- a global MM state machine that consumes events
- a pure planning function over current facts
- a scheduler that emits commands for fill and rebalance executors

It is not best thought of as:

- one global FIFO queue
- one per-swap FSM with no global coordination
- three separate loops for quoting, batching, and rebalancing

There are two useful layers of state:

1. Per-swap lifecycle state
2. Global shared-liquidity planning state

The first answers "where is this swap in its lifecycle?"

The second answers:

- what inventory is currently available
- what inventory is already committed
- which swaps can be filled now
- which swaps are blocked
- what rebalances should happen
- what actions are already committed and in flight

The second layer is the focus of this document.

## Responsibility Split

### OTC

OTC remains responsible for:

- swap creation
- deposit confirmation policy
- refunds
- authoritative swap state
- notifying the MM when swaps become MM obligations

### MM Planner

The MM planner is responsible for:

- maintaining MM-local inventory state
- ordering confirmed obligations
- determining quoteable capacity
- deciding which fills can start now
- deciding which rebalances can start now
- reserving inventory for committed actions
- exposing the current plan and blocked reasons

### MM Executors

Executors are responsible for carrying out actions chosen by the planner:

- payout batch executor
- rebalance executor

Executors do not decide priority or inventory allocation. They only execute
planner-approved actions and report results back as events.

## Scope Boundary For V1

In v1, the planner begins at the point a swap becomes a real MM obligation.

That means the planner's primary entry event is:

- `SwapObligationCreated`

This should correspond to the point at which the user-side deposit is confirmed
enough that the MM is expected to fulfill the swap or participate in the
failure/refund path.

Quote-time reservation before the user deposit confirms is explicitly out of
scope for v1. In v1:

- quoting reads planner-derived available inventory
- quoting does not itself create durable reservations

This keeps the first version smaller while still giving deterministic behavior
for real obligations and rebalances.

## Definitions

### Asset Lane

An asset lane is the ordered queue of obligations that require payout in the
same asset.

Examples:

- all swaps that require payout in `cbBTC`
- all swaps that require payout in `BTC`

Fairness is primarily enforced within a payout-asset lane.

### Rebalance Route

A rebalance route is the specific path used to convert one controlled asset into
another.

At minimum it is identified by:

- `source_asset`
- `target_asset`
- `venue`
- `source_wallet`

Example:

- `BTC -> cbBTC via zero-fee internal venue from wallet W`

### Controlled Inventory

Controlled inventory is inventory the MM can actually spend now.

This document avoids assuming that inbound swap proceeds are automatically
controlled at the same moment the obligation is created. If inbound funds become
usable only later, that transition must be represented explicitly as an event.

### Current Reference Model

The current executable reference model uses five MM-local state variables:

- `obligations`
- `available_inventory`
- `outbound_batches`
- `pending_settlements`
- `rebalances`

Their intended lifecycle is:

1. An `open` obligation is committed into an `outbound_batch`, consuming
   `available_inventory`.
2. If the outbound batch succeeds, that obligation leaves the payout path and
   becomes a `pending_settlement`.
3. When the inbound settlement is realized, the corresponding asset is credited
   back into `available_inventory`.

This is intentionally smaller than the broader bucket taxonomy discussed below.
The bucket taxonomy still describes possible future refinement, but the current
model state should be understood in terms of the five variables above.

## Planner State

The planner state has four parts:

1. Inventory buckets per asset
2. Ordered obligations
3. Action records
4. Concurrency limits and policy

### 1. Inventory Buckets

In the current reference model, for each asset the planner explicitly tracks:

- `available_inventory`

The current reference model does not separately store bucketized reservations.
Instead, inventory already committed to payout or rebalance actions is made
explicit through:

- `outbound_batches`
- `pending_settlements`
- `rebalances`

A possible future refinement is to split that further into buckets such as:

- `controlled_free`
- `reserved_for_fill`
- `fill_in_flight`
- `reserved_for_rebalance`
- `rebalance_in_flight`

Optional future buckets:

- `held_for_open_swaps`
- `expected_inbound_not_yet_controlled`

For v1, the smaller `available_inventory` plus explicit action records is the
working abstraction.

### 2. Obligations

Each obligation record contains:

- `swap_id`
- `payout_asset`
- `payout_amount`
- `settlement_asset`
- `settlement_amount`
- `confirmed_at`
- `status`
- `blocked_reason`

Required statuses:

- `open`
- `outbound`
- `pending_settlement`
- `completed`

An `open` obligation may also be dropped before it is committed into an
`outbound_batch` if MM policy decides it is no longer fillable. Once an
obligation is in an `outbound_batch`, it is immutable from the planner's point
of view.

### 3. Actions

Each planner action contains:

- `action_id`
- `action_type`
- `status`
- `input_reservations`
- `expected_outputs`
- `dependency_reason`

Action types:

- `OutboundBatch`
- `Rebalance`

Action statuses:

- `proposed`
- `committed`
- `in_flight`
- `done`
- `failed`

Important distinction:

- `proposed` actions are tentative and may be replanned away
- `committed` and `in_flight` actions are hard and consume inventory

### 4. Concurrency Policy

The planner also tracks execution limits such as:

- maximum payout batches in flight per wallet or chain lane
- maximum rebalances in flight per route
- configured per-asset risk-buffer floors

Recommended v1 rule:

- at most one `rebalance_in_flight` per rebalance route

## Derived Quantities

### Quoteable Capacity

For each asset:

`quoteable = max(0, controlled_free - configured_risk_buffer)`

Because `controlled_free` already excludes hard reservations, quoting
automatically respects:

- committed fills
- fills already in flight
- committed rebalances
- rebalances already in flight

### Fillable Capacity

A swap is immediately fillable if:

- its payout asset lane has enough `controlled_free`
- using that free inventory does not violate any committed older reservation

### Rebalanceable Capacity

A rebalance is allowed only from:

- free source inventory
- minus configured risk buffer
- minus amounts needed for older committed obligations in that source asset lane

## Hard Invariants

These invariants should hold in every reachable planner state.

1. No double-allocation

For each asset, a unit of controlled inventory may be in exactly one bucket.

2. Conservation of controlled inventory

For each asset:

`observed_controlled_balance = controlled_free + reserved_for_fill + fill_in_flight + reserved_for_rebalance + rebalance_in_flight`

The configured risk buffer is a policy floor applied to `controlled_free`; it
is not a separate inventory bucket.

3. Unique obligation state

Every planner-tracked swap must be in exactly one obligation status.

4. Hard reservations cannot be stolen

Inventory reserved for a committed or in-flight action cannot be consumed by any
newer action.

5. Determinism

Given the same initial state and the same event stream, the planner must produce
the same resulting state and the same emitted commands.

6. Same-route rebalance limit

The system must never have more than the configured maximum number of in-flight
rebalances for the same route.

7. Explicit time semantics

All time-driven behavior must be modeled as explicit timer events. No hidden
background timing assumptions should affect correctness.

## Fairness And Ordering Policy

Fairness is enforced primarily per payout-asset lane.

Recommended v1 policy:

1. Order obligations oldest-first by `confirmed_at`
2. Tie-break by `swap_id`
3. Consume payout inventory in that order
4. Allow opposite-side swaps to use only truly free inventory
5. Do not let a newer swap use inventory already committed to help older swaps

This means:

- older `cbBTC` payout obligations are ordered relative to other `cbBTC`
  obligations
- a new `BTC` payout obligation does not automatically go behind them, because
  it is a different payout lane
- however, it may still be blocked if the `BTC` it wants is already hard
  reserved for an older action or an in-flight rebalance

## Concurrency Model

The planner is single-writer. Execution is concurrent.

This is a critical distinction.

What is serialized:

- event application
- reservation decisions
- planner state updates

What may be concurrent:

- multiple payout actions using disjoint reserved inventory
- payout execution while an unrelated rebalance is in flight
- independent rebalances on independent routes

What may not overlap:

- actions that would consume the same reserved source inventory
- same-route rebalances beyond policy

Therefore:

- the planner does not process one global action at a time
- it processes one event at a time and may approve multiple concurrent actions

## Event Model

The planner consumes explicit events.

Required v1 event set:

- `SwapObligationCreated`
- `FillCommitted`
- `FillBroadcast`
- `FillConfirmed`
- `FillFailed`
- `RebalanceCommitted`
- `RebalanceBroadcast`
- `RebalanceConfirmed`
- `RebalanceFailed`
- `WalletReconciled`
- `TimerFired`

### Event Semantics

#### `SwapObligationCreated`

A new confirmed MM obligation exists.

Required fields:

- `swap_id`
- `payout_asset`
- `payout_amount`
- `inbound_asset`
- `inbound_amount`
- `confirmed_at`

#### `FillCommitted`

The planner-issued fill action has been accepted by the fill executor and its
input inventory becomes hard-reserved.

#### `FillBroadcast`

The fill transaction or batch has actually been submitted. Reservation moves
from `reserved_for_fill` to `fill_in_flight`.

#### `FillConfirmed`

The payout action has completed successfully. The obligation transitions to
`completed`.

#### `FillFailed`

The fill did not complete. The planner must decide whether to:

- restore free inventory
- retry
- mark failed pending operator action

#### `RebalanceCommitted`

A rebalance action has been accepted by the rebalance executor and its source
inventory becomes hard-reserved.

#### `RebalanceBroadcast`

The rebalance has been submitted. Reservation moves from
`reserved_for_rebalance` to `rebalance_in_flight`.

#### `RebalanceConfirmed`

The rebalance has completed and its target inventory becomes `controlled_free`.

#### `RebalanceFailed`

The rebalance did not complete successfully. The planner must reconcile actual
wallet state and then replan.

#### `WalletReconciled`

Observed on-chain or wallet balances are refreshed. This is used for:

- recovery
- reconciliation
- detecting deviations from expected action effects

#### `TimerFired`

Represents an explicit time-based trigger for:

- low-frequency reconciliation
- stale action detection
- time-based replanning policies

## Planner Outputs

On each event, the planner produces:

- updated internal state
- an inspectable snapshot
- zero or more commands

Commands:

- `CommitFillBatch(...)`
- `CommitRebalance(...)`
- `NoOp`

The planner should not directly perform network or chain execution. Executors do
that work and report the results back as events.

## Planning Rules

### Rule 1: Replan On Every Event

After applying each event, the planner recomputes:

- which obligations are `ready`
- which obligations are `blocked`
- which future actions are `proposed`

Only `proposed` actions may be replaced by replanning.

### Rule 2: Preserve Hard Commitments

`committed` and `in_flight` actions are fixed unless they fail or reconcile as
failed.

The planner may not silently cancel them during replan.

### Rule 3: Fill Oldest Available Obligations First Within A Lane

For each payout asset lane:

1. sort by `confirmed_at`, then `swap_id`
2. walk obligations in that order
3. mark as `ready` until free payout inventory is exhausted
4. mark the rest `blocked`

### Rule 4: Rebalances Use Only True Surplus

Rebalance source inventory must come only from:

- `controlled_free`
- after honoring committed actions
- after honoring risk buffer
- after honoring fairness policy in the source asset lane

### Rule 5: Opposite-Side Flow May Change Future Plan

A new opposite-side swap may cause replanning, but only for soft future actions.

It may:

- consume free unreserved inventory immediately
- replace a merely proposed rebalance
- shrink a merely proposed rebalance

It may not:

- steal inventory from a committed fill
- steal inventory from an in-flight fill
- steal inventory from a committed or in-flight rebalance

### Rule 6: Same-Route Rebalances Should Coalesce

If a route already has an in-flight rebalance:

- the planner should generally not start a second in-flight rebalance on that
  same route
- additional need should be represented as remaining deficit and future planned
  work

### Rule 7: Quoteing Must Be A Read-Only View Of Planner State

Quoteing does not create special side effects in v1. It simply asks:

- how much payout inventory is currently quoteable?

The answer must come from planner state, not from a separate independent
liquidity view.

## Worked Scenarios

### Scenario A: One-Sided Backlog

State:

- 10 confirmed swaps
- each owes `100 cbBTC`
- MM has `500 cbBTC`
- MM has `0 BTC`

Planner result:

- swaps 1-5 are `ready`
- swaps 6-10 are `blocked` by insufficient `cbBTC`
- planner may propose or commit fill actions for swaps 1-5
- planner may also propose future rebalancing if a valid source of `BTC` later
  becomes controlled inventory

If no additional `BTC` is controlled yet, the planner must not pretend the last
five swaps are fillable.

### Scenario B: Opposite-Side Flow Arrives During Backlog

Initial additional state:

- MM has `100 BTC` as controlled free inventory

Suppose the planner has older blocked `cbBTC` payout obligations and is
considering a `BTC -> cbBTC` rebalance.

If a new user arrives wanting `cbBTC -> BTC`:

- if the `100 BTC` is still free and unreserved, the planner may choose to fill
  it immediately
- if the `100 BTC` is only part of a proposed rebalance, the planner may change
  that proposed rebalance before commit
- if the `100 BTC` is already committed to a rebalance, the new swap must not
  steal it

This is why the system must distinguish:

- proposed actions
- committed actions
- in-flight actions

### Scenario C: Long Rebalance Latency

If rebalances take 20 minutes on average:

- fills should not globally stop
- only inventory committed to the rebalance route is unavailable
- unrelated fill actions may continue
- the planner continues to accept events and replan soft future actions during
  the rebalance

Therefore the model is:

- serialized planning
- concurrent execution

not:

- one global action at a time

## Recovery Model

The planner should be reconstructable from durable facts plus wallet
reconciliation.

Required durable inputs:

- active MM obligations known at startup
- in-flight payout actions
- in-flight rebalance actions
- current wallet-controlled balances

Recovery approach:

1. load durable facts
2. reconcile wallet balances
3. reconstruct hard commitments
4. re-run planner
5. regenerate only soft future actions

Correctness should not depend on the in-memory presence of an old proposed plan.

## Inspectability Requirements

At any point in time, operators should be able to inspect:

- inventory by asset and bucket
- obligations in priority order
- blocked obligations and exact blocked reasons
- committed actions
- in-flight actions
- proposed future actions
- per-route rebalance state

The planner should always be able to answer:

- what is free?
- what is reserved?
- what is in flight?
- what is blocked?
- what is next?
- why?

## Verification Plan

Before implementation, this design should be validated with:

1. A pure reference model

- implement `next_state = apply_event(state, event)` in a small simulator

2. Explicit invariants

- no double-allocation
- no lost obligations
- deterministic replay
- no same-route rebalance overlap beyond policy
- no committed reservation theft

3. Randomized scenario simulation

- bursts of same-side swaps
- opposite-side swaps arriving during rebalance
- failed rebalances
- delayed fills
- restart and replay at arbitrary points

4. Optional model checking

- TLA+ or similar tooling for safety properties

## Open Questions

These are policy choices that should be decided before implementation:

1. Planner entry point

- start at confirmed MM obligations only in v1
- or also reserve for pre-confirmation open swaps

2. Risk buffer policy

- fixed percentage
- fixed per-asset floor
- dynamic by volatility or venue health

3. Rebalance aggressiveness

- rebalance only when no more fills are possible
- or rebalance proactively when projected deficit crosses a threshold

4. Opposite-side preference

- should natural cross-flow be preferred over external rebalance whenever
  possible?
- or only when it does not delay the oldest blocked obligations?

5. Concurrency limits

- how many payout batches may be in flight per wallet?
- should rebalance remain one-in-flight per route in all cases?

6. Failure semantics

- when should a failed action be retried automatically?
- when should it become operator-visible and blocked?

## Recommended V1 Shape

To keep the first implementation tractable:

- MM-local planner only
- no OTC awareness required
- no quote-time reservation in v1
- deterministic ordering by payout-asset lane
- explicit hard reservations for fills and rebalances
- single-writer planner actor
- concurrent executors
- one in-flight rebalance per route
- planner snapshot endpoint for inspection

This is enough to make MM behavior deterministic and inspectable without
immediately building a large persistent scheduling system.
