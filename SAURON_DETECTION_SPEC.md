# Sauron Initial Deposit Detection Spec

## Scope

This document defines a v1 architecture where:

- `Sauron` is responsible only for initial user deposit detection.
- `tee-otc` remains responsible for verification, confirmation tracking, refunds, MM coordination, and the rest of the swap lifecycle.
- `Sauron` loads the active watch set from a read-only Postgres replica or other read-only replicated Postgres endpoint.
- `Sauron` receives realtime watch-set invalidation signals via Postgres `NOTIFY` on that replicated Postgres endpoint.
- `Sauron` reports candidate deposits back to `tee-otc` through an internal authenticated endpoint.

## Goals

- Remove address-scale deposit discovery from `tee-otc`.
- Keep `tee-otc` authoritative for swap state and confirmation policy.
- Use a simple replica-backed read path plus Postgres-level change notifications instead of a custom watch-sync API in v1.
- Keep watch loading stateless from Sauron's point of view.
- Support hundreds to hundreds of thousands of active deposit vaults.
- Keep the system testable in the existing deep integration environment.

## Non-Goals

- `Sauron` does not decide whether a tx is sufficiently confirmed.
- `Sauron` does not mutate OTC database state directly.
- `Sauron` does not own refunds, MM coordination, or settlement.
- v1 does not require SSE, WebSockets, replayable delta logs, or durable watch cursors for watch-set transport.

## Responsibility Split

### Sauron

- Runs a replica-backed Postgres watch query for boot, reconnect, and refresh.
- Listens for Postgres `NOTIFY` signals that the watch set changed.
- Maintains an in-memory set of active deposit-vault watches.
- Ingests chain data once per supported chain.
- Detects candidate deposits to watched addresses.
- Submits candidate deposits to `tee-otc`.
- Optionally emits best-effort mempool detections before block inclusion.

### tee-otc

- Creates swaps and deposit vaults.
- Decides which swaps belong in the Sauron watch set.
- Maintains the authoritative swap state in primary Postgres.
- Exposes or documents a narrow replica-backed watch query for Sauron to read.
- Arranges for relevant replica-side table changes to emit `NOTIFY` events for Sauron.
- Verifies candidate deposits from Sauron.
- Stores accepted deposit tx identity.
- Tracks confirmations for accepted deposit txs.
- Transitions swap state after confirmation.
- Handles refunds, MM flow, and settlement.

### Postgres Replica

- Serves as a read-only snapshot source for the current Sauron watch set.
- Emits `NOTIFY` events when the watch set may have changed.
- May lag the primary database.
- Must never be treated as authoritative for writes or final swap state decisions.

Important note:

- this requires a logical-subscriber-style replica or another replicated Postgres endpoint that can execute triggers and `NOTIFY`
- a hot-standby physical replica cannot be used for `LISTEN`/`NOTIFY`

## Watch Eligibility

A swap is present in the Sauron watch set only when:

- swap status is `waiting_user_deposit_initiated`

A swap is removed from the Sauron watch set when:

- OTC accepts a deposit and transitions it to `waiting_user_deposit_confirmed`
- swap is refunded
- swap fails
- swap settles
- swap is otherwise no longer awaiting an initial user deposit

This keeps the Sauron watch set strictly address-based. Sauron v1 does not track tx confirmations.

## Watch Record Schema

Logical watch fields:

- `swapId`: UUID
- `chainId`: compact chain identifier
- `assetId`: compact asset identifier
- `address`: normalized deposit-vault address bytes
- `addressKind`: `bitcoin_address | evm_address | script_pubkey`
- `minAmount`: raw integer amount
- `maxAmount`: raw integer amount
- `depositDeadline`: unix millis
- `createdAt`: unix millis

Notes:

- `swapId` is the idempotency anchor when Sauron calls OTC.
- `assetId` must fully identify the deposit asset, not just the chain.
- `depositDeadline` allows Sauron to prune obviously stale watches locally.
- `minAmount` and `maxAmount` let Sauron prefilter nonsense before calling OTC.

## Replica-Backed Watch Source

Preferred v1 source:

- a normal read-only SQL query run against a Postgres replica
- optionally, a named SQL view such as `sauron_active_watches_v1` if we want a stable DB-level contract

The read path may be implemented as:

- a direct `SELECT` over replicated tables
- a SQL view over replicated tables
- a dedicated replicated table maintained by OTC
- another equivalent Postgres read model with the same logical columns

Sauron must depend on a stable query contract, not on ad hoc schema assumptions.

### Realtime Notification Contract

Preferred v1 channel:

- `sauron_watch_set_changed`

Notification semantics:

- a notification means "the watch set may have changed"
- the notification is a hint, not an authoritative state diff
- payloads may include `swap_id` or another lookup hint, but Sauron must remain correct if payloads are absent, duplicated, or coalesced

Replica-side trigger requirements:

- if the replica is a logical subscriber, subscriber-side notify triggers must be row triggers
- those triggers must be configured to fire under replication apply, preferably via `ENABLE REPLICA TRIGGER`
- statement triggers must not be relied on for logical replication apply

### SQL Query Contract

Recommended logical columns:

- `swap_id`
- `chain_id`
- `asset_id`
- `address`
- `address_kind`
- `min_amount`
- `max_amount`
- `deposit_deadline`
- `created_at`

In v1 these logical fields may be represented by database-native columns such as
`from_chain` and `from_token`, as defined in the build appendix.

Recommended query result shape:

- return only currently active watches
- return only the columns Sauron actually needs
- return rows with the logical fields listed above

If a named view is used, the query may simply be:

```sql
SELECT
  swap_id,
  chain_id,
  asset_id,
  address,
  address_kind,
  min_amount,
  max_amount,
  deposit_deadline,
  created_at
FROM sauron_active_watches_v1;
```

Notes:

- A named view is optional in v1.
- A direct query against replicated OTC tables is acceptable if it returns the same logical fields.
- Sauron should stream rows from the query if the client library supports it.
- Querying the replica is the full load path; realtime freshness comes from `NOTIFY`, not from a custom watch-stream API.

## Load And Notify Model

### Boot Sequence

1. Sauron opens a dedicated notification connection to the replica.
2. Sauron executes and commits `LISTEN sauron_watch_set_changed`.
3. In a new transaction, Sauron runs the full watch query against the replica.
4. Sauron builds a fresh in-memory watch map.
5. Sauron starts live chain ingestion.
6. Sauron treats subsequent `NOTIFY` messages as hints that the watch set changed.

This follows PostgreSQL's recommended LISTEN usage pattern: start listening first, then inspect the database state, then rely on notifications for subsequent changes.

### Realtime Refresh

- on each notification, Sauron refreshes watch state from the replica
- a targeted refresh keyed by notification payload is preferred when practical
- if payloads are not sufficient or the local state is suspect, Sauron reruns the full watch query and atomically replaces the watch map

### Periodic Reconciliation

- Sauron also reruns the full watch query on a low-frequency interval for durability
- the default v1 interval is `60s`

### Reconnect Sequence

1. If the notification connection drops, mark the notify path unhealthy.
2. Reopen the notification connection.
3. Execute and commit `LISTEN sauron_watch_set_changed`.
4. Rerun the full watch query.
5. Atomically replace the watch map.

Correctness depends on full query loads on boot and after notify-path failure, plus OTC verification. Notifications optimize freshness; they are not the source of truth.

### Freshness Model

Watch freshness is bounded by:

- replica lag relative to the primary
- notification delivery latency
- the time needed for Sauron to run the follow-up refresh query

This is acceptable in v1 because:

- OTC remains authoritative
- OTC ingest is idempotent
- stale detections are safe to reject

## In-Memory State in Sauron

Primary index:

- `addressWatchIndex[chainId][assetId][normalizedAddress] -> watch`

Optional secondary index:

- `swapId -> watch`

Use immutable replacement:

- build new maps from the latest full query result
- atomically swap pointers
- discard old maps

This avoids partial-update corruption during refresh.

## Chain Ingestion Model

Sauron must ingest chain data once per chain, not per address.

### Bitcoin

- Subscribe to mempool txs and new blocks.
- Parse outputs.
- Normalize output destination.
- Look up `(chainId, assetId, address)` in `addressWatchIndex`.
- For outputs matching watched addresses, emit candidate deposits.

### EVM ERC20

- Subscribe to new blocks or an indexer feed.
- Consume `Transfer(address,address,uint256)` logs for depositable token contracts.
- Normalize `to` address.
- Look up `(chainId, assetId, toAddress)` in `addressWatchIndex`.
- For matches, emit candidate deposits.

### EVM Native Asset

- Consume block txs or trace/indexer feed.
- Normalize recipient address.
- Look up `(chainId, assetId, toAddress)` in `addressWatchIndex`.

The expensive scaling problem is chain ingestion, not watch-set membership. Hash-map membership checks are cheap even at 100k scale.

## Chain Cursor State

Watch-set sync is stateless from the replica query perspective.

Sauron should not use a local durable cursor database in v1. Per-chain cursors
exist only in memory while the process is running.

On startup or restart:

1. Load the current watch query result from the replica.
2. Run the indexed lookup pass for all active watches.
3. Initialize each backend cursor to the current canonical tip.
4. Resume live ingestion from that tip forward.

Recovery from downtime comes from the indexed bootstrap pass plus OTC-side
idempotent verification, not from a persisted local cursor store.

## Deposit Detection Submission

Endpoint:

- single submission route: `POST /api/v1/swaps/:swap_id/deposit-observation`

Auth:

- auth is selected dynamically per request on the single endpoint
- trusted detector lane uses authenticated detector-client credentials
- trusted lane request headers: `X-API-ID`, `X-API-SECRET`
- trusted-lane credentials are provisioned in OTC runtime config as an allowlist of detector clients
- `Sauron` gets one trusted detector credential; other operator-managed detector services can get distinct credentials
- participant-signed lane uses an EIP-712 signature from one of the swap's allowed EVM participant addresses
- allowed participant signers are `refund_address` and/or `user_destination_address`, but only when that specific address is EVM-capable for the swap
- OTC supports both EOA signature recovery and ERC-1271 contract-wallet verification for the participant-signed lane
- `participantAuth.signedAt` must be a UTC timestamp no more than 10 minutes old and not in the future
- the participant-signed lane is meant for swap participants running their own tooling, not for arbitrary unaffiliated observers
- if trusted detector headers and `participantAuth` are both present, OTC rejects the request as ambiguous
- if neither trusted detector headers nor `participantAuth` is present, OTC rejects the request as unauthenticated

Payload:

```json
{
  "sourceChain": "bitcoin",
  "sourceToken": {
    "type": "Native"
  },
  "txHash": "0x...",
  "amount": "100000000",
  "transferIndex": 0,
  "address": "deposit-address",
  "observedAt": "2026-03-17T00:00:00Z"
}
```

Participant-signed lane additional auth payload:

```json
{
  "participantAuth": {
    "kind": "participant_eip712",
    "signer": "0x1234...",
    "signerChain": "ethereum",
    "signature": "0xabcd...",
    "signedAt": "2026-03-17T00:00:30Z"
  }
}
```

The EIP-712 payload signed by the participant binds exactly:

- `swap_id` from the route path
- `txHash` from the request body
- `signedAt` from `participantAuth`

For Bitcoin:

- `transferIndex` identifies the exact output, effectively `vout`

For EVM:

- `transferIndex` identifies the exact log index

Idempotency key:

- `(swapId, txHash, transferIndex)`

V1 note:

- this is the detector-side dedupe key for submissions
- OTC's accepted swap state remains single-valued per swap and persists one accepted deposit tx identity
- if OTC has already accepted the same deposit tx for that swap, it returns a duplicate-safe response rather than applying state again

Sauron may submit the same candidate more than once. OTC must treat duplicates as safe no-ops.

## OTC Verification Rules

On `deposit_detected`, OTC must verify:

- swap exists
- swap is still `waiting_user_deposit_initiated`
- swap is still deposit-eligible by time and state
- candidate chain and asset match swap expectations
- candidate destination address matches the swap deposit vault
- candidate amount is within valid bounds
- tx/log/output actually exists on-chain according to OTC's own chain access
- duplicate submissions are ignored safely

If verification passes:

1. OTC records the accepted deposit tx identity.
2. OTC transitions the swap to `waiting_user_deposit_confirmed`.
3. OTC removes that swap from the watch set by normal state advancement.
4. OTC's existing confirmation tracker takes over.

If verification fails:

- OTC returns a typed error.
- Sauron does not retry indefinitely on semantic rejection.
- Retries are allowed on transport failure.

## Confirmation Tracking

After OTC accepts a candidate deposit:

- Sauron is done with that swap for initial detection purposes.
- OTC tracks confirmations itself and remains the only authority that can advance
  the swap out of `waiting_user_deposit_confirmed`.

This keeps confirmation policy centralized in OTC.

### OTC User-Deposit Confirmation Scheduler Subspec

The existing short-interval OTC tx polling loop is not the desired long-term
design for user deposits.

The intended replacement is:

- primary path: block-driven confirmation scheduling
- fallback path: infrequent recovery sweep
- non-goal: move confirmation authority into Sauron

#### Scope

This subspec applies to user deposits first.

- `waiting_user_deposit_confirmed` moves to the new scheduler
- `waiting_mm_deposit_confirmed` may keep its current polling path for now
- Sauron remains out of confirmation tracking

#### Responsibilities

OTC remains responsible for:

- storing the accepted deposit tx identity
- deciding whether confirmations are sufficient
- transitioning the swap to `waiting_mm_deposit_initiated`
- rearming external discovery if the accepted tx disappears

Sauron is not responsible for:

- watching confirmation counts
- deciding whether a tx is sufficiently confirmed
- emitting confirmation-ready hints

#### Event Source

OTC should add a chain-head tracking subsystem that notices new canonical blocks
per chain and wakes due confirmation work from those chain-head changes.

Code-informed v1 note:

- the existing chain surface already exposes `get_block_height()` and
  `get_best_hash()`
- that is sufficient to build a head tracker without widening the current OTC
  chain trait surface
- if later the chain clients add native push subscriptions, the head tracker can
  swap transports without changing the confirmation scheduler contract

#### Scheduler Model

For each swap in `waiting_user_deposit_confirmed`, OTC should keep in-memory
tracking keyed by `swap_id`:

- `chain`
- `tx_hash`
- `required_confirmations`
- `next_recheck_height`

The scheduler should be rebuilt from Postgres on OTC startup by loading swaps in
`waiting_user_deposit_confirmed` and computing their first `next_recheck_height`
from live chain status.

#### Required Tx Status Enrichment

To compute the earliest meaningful future recheck exactly, OTC should enrich the
confirmed tx status model to include the tx inclusion height.

Required model change:

- add `inclusion_height: u64` to `ConfirmedTxStatus`

This is preferable to inferring inclusion height from chain-specific
confirmation arithmetic inside the scheduler.

#### Target Height Computation

For a known confirmed tx, OTC should compute the earliest chain head at which
the tx could first satisfy the configured chain threshold.

With current confirmation semantics:

- Bitcoin uses Bitcoin Core confirmation counts, where inclusion in the block
  already counts as confirmation `1`
- EVM uses `current_block - receipt.block_number`, where the inclusion block
  counts as confirmation `0`

So the target height formulas are:

- Bitcoin: `target_height = inclusion_height + required_confirmations - 1`
- EVM: `target_height = inclusion_height + required_confirmations`

If the tx is known but still pending / not yet mined:

- schedule the next recheck for the next observed head height on that chain

#### Runtime Behavior

When OTC accepts a deposit observation:

1. it stores the accepted deposit tx identity as it does today
2. it queries tx status once
3. if already sufficiently confirmed, it advances the swap immediately
4. otherwise it computes `next_recheck_height`
5. it registers that swap with the user-deposit confirmation scheduler

When the chain-head tracker observes a new canonical head for a chain:

1. update the chain's latest known height/hash
2. wake all scheduled user-deposit checks whose `next_recheck_height` is now due
3. re-query those txs only
4. for each result:
   - if sufficiently confirmed, advance the swap
   - if still underconfirmed but confirmed on-chain, compute a new exact target
   - if pending, reschedule for the next head
   - if not found, rearm external discovery for that swap

#### Recovery Sweep

OTC should retain a very low-frequency recovery sweep for user-deposit
confirmation tracking.

Recommended default:

- `30m`

Sweep responsibilities:

- reload swaps in `waiting_user_deposit_confirmed`
- repair any missing in-memory scheduled items
- recover from missed head events, reconnect bugs, or process-local state loss

The recovery sweep is not the primary confirmation mechanism.

#### What Gets Removed

For user deposits:

- remove the current short-interval confirmation polling from the main
  generic swap-monitoring pass
- remove the current `target_height OR safety poll every few seconds` due check
  as the primary path

The remaining MM-side monitoring service should:

- stay scoped to MM batch confirmation / settlement work
- run from a head-driven scheduler rather than short-interval full scans
- retain a low-frequency recovery sweep

#### Concrete OTC Code Impact

The user-deposit implementation work should be centered in:

- `bin/otc-server/src/services/user_deposit_confirmation_scheduler.rs`
- `bin/otc-server/src/services/market_maker_batch_settlement.rs`
- `bin/otc-server/src/server.rs`
- `crates/otc-models/src/chain.rs`
- `crates/otc-chains/src/bitcoin.rs`
- `crates/otc-chains/src/evm.rs`

Expected changes:

1. Remove user-deposit confirmation progression from the short-interval
   generic monitoring loop.
2. Add a dedicated OTC user-deposit confirmation scheduler service.
3. Add a chain-head tracker service that publishes canonical head changes per
   chain.
4. Enrich confirmed tx status with `inclusion_height`.
5. Recompute exact target heights from chain semantics rather than using
   `current_height + 1` for already-confirmed txs.
6. Keep `rearm_user_deposit_detection(...)` as the recovery path when the
   accepted tx disappears.

#### Testing Requirements

Add integration coverage for:

- user deposit accepted with `0` confirmations, then advanced only after the
  required future block height is reached
- OTC restart while swaps are in `waiting_user_deposit_confirmed`
- missed head event recovered by the `30m` sweep using a shortened test
  interval
- disappeared / reorged tx causing `rearm_user_deposit_detection(...)`

## Performance Guidance

V1 is expected to tolerate hundreds of thousands of active watches if the read path stays narrow and the query path is only used for boot, reconnect, and refresh after notifications.

Recommended practices:

- run the watch query against a replica, not the primary
- expose a narrow query or view instead of broad raw-table reads
- select only the columns Sauron needs
- use a partial and/or covering index under the query if necessary
- stream result rows into the in-memory map if the driver supports it
- use `NOTIFY` as an invalidation signal, not as the only source of truth
- benchmark actual end-to-end fetch time from Sauron, not just database-side explain plans

If payload-driven targeted refresh proves too complex, Sauron can always fall back to rerunning the full watch query after each notification.

## Failure Semantics

Replica lag:

- the watch set seen by Sauron may be slightly stale
- OTC may reject detections for swaps that already advanced on the primary
- this is expected and safe

Notification coalescing or duplication:

- PostgreSQL notifications are not a replay log
- duplicate notifications are safe
- repeated identical notifications within a transaction may be folded
- Sauron must treat notifications as hints and re-read state from SQL

Replica query failure:

- keep the last successfully loaded watch set in memory
- continue chain ingestion with current watches
- retry the full watch query until healthy
- once healthy, atomically replace the watch set from the fresh result

Notification connection failure:

- reconnect and re-establish `LISTEN`
- rerun the full watch query before trusting notifications again
- atomically replace the watch set from the fresh result

Sauron outage:

- on restart, load the full watch query result
- rerun indexed lookup for active watches
- reset in-memory chain cursors to current tip
- detect any deposits missed during outage

Late or stale detection:

- OTC may reject a candidate because the swap already advanced or closed
- this is expected and safe

## Operational Assumptions

v1 assumes:

- a logical-subscriber-style Postgres replica or equivalent replicated Postgres endpoint is available
- the replica exposes a stable watch query contract for Sauron
- the replica can emit `NOTIFY` for relevant watch-set changes
- replica lag remains within acceptable operational bounds
- OTC ingest endpoints are idempotent
- query result size remains modest enough to reload on boot, reconnect, notification-driven refresh, and periodic reconcile

If later payload-driven refresh proves too complex or too lossy, the system can add replayable deltas or a dedicated watch service. Those are transport optimizations, not part of correctness in v1.

## Testing Spec

SQLx-backed integration tests in `tee-otc` require the compose-managed test
database from the repo `justfile`. Do not assume an ambient local Postgres is
enough.

Required test workflow:

1. Start from a fresh test database with `just redb`.
2. Run the target integration test command.
3. Tear the test database down with `just clean-db`.

Useful existing recipes:

- `just start-db`: start the SQLx test database without dropping volumes first
- `just clean-db`: stop the test database and remove volumes
- `just redb`: recreate the test database from scratch
- `just test`: full suite wrapper that recreates the test DB, runs tests, then tears it down

Integration tests must cover:

1. Boot-time full watch-query load.
2. `LISTEN` is established before the initial full query.
3. Notification-driven refresh picks up newly active watches.
4. Notification-driven refresh removes watches that are no longer active.
5. Periodic full-query reconcile heals missed notifications.
6. Replica lag or stale reads leading to safe OTC rejection.
7. Replica query failure with last-known-watch-set fallback.
8. Notification connection drop and full-query recovery.
9. Duplicate or coalesced notifications.
10. Sauron outage and chain backfill recovery.
11. Duplicate `deposit_detected` submissions.
12. Stale detection against swaps that already advanced.
13. Bitcoin output matching.
14. ERC20 transfer matching.
15. Native EVM transfer matching if supported.

## Recommended v1 Implementation Order

1. Define the watch-query contract Sauron will read from the replica.
2. Optionally expose that contract as a named SQL view such as `sauron_active_watches_v1`.
3. Add OTC internal `deposit_detected` endpoint.
4. Add replica-side row triggers that emit `NOTIFY sauron_watch_set_changed`.
5. Restrict the watch query to `waiting_user_deposit_initiated`.
6. Implement the Sauron `LISTEN` connection, full-query loader, and atomic in-memory watch map.
7. Implement notification-driven refresh and reconnect recovery.
8. Implement the Bitcoin address-match adapter.
9. Implement the EVM token-transfer adapter.
10. Replace OTC's short-interval user-deposit confirmation polling with the
    block-driven confirmation scheduler defined in this spec.
11. Add replica-lag, notify-path failure, outage, and duplicate integration tests.

## Build Appendix

This appendix pins down the concrete v1 implementation choices so the system is
buildable without inventing more architecture during implementation.

### Reuse The Existing Replica Pattern

The intended operational model is the same one already used by the analytics
server work in the SDK:

- Railway-hosted logical subscriber Postgres
- primary reachability through a dedicated tunnel service
- replica-side `NOTIFY` trigger installed on subscriber tables
- application process uses `LISTEN` plus SQL reconciliation

Reference implementation assets already exist in the repo:

- Railway tee-otc replica bootstrap:
  [tee-otc-replica-db/bootstrap.sh](/home/alpinevm/Development/rift/periphery/sdk/deploy/railway/analytics/tee-otc-replica-db/bootstrap.sh)
- Railway tee-otc replica trigger install:
  [tee-otc-replica-db/install-notify-trigger.sql](/home/alpinevm/Development/rift/periphery/sdk/deploy/railway/analytics/tee-otc-replica-db/install-notify-trigger.sql)
- Replica deployment notes:
  [tee-otc-replica.md](/home/alpinevm/Development/rift/periphery/sdk/docs/analytics-server/infra/tee-otc-replica.md)

Sauron should reuse that model, but with its own subscriber and slot names.

### Concrete Service Topology

Railway services:

1. `tee-otc-primary-tunnel`
2. `tee-otc-sauron-replica-db`
3. `sauron-worker`

Responsibilities:

- `tee-otc-primary-tunnel` exposes the tee-otc primary over Railway private networking.
- `tee-otc-sauron-replica-db` is a dedicated logical subscriber for Sauron.
- `sauron-worker` queries and listens to the replica, ingests chains, and POSTs candidate deposits to OTC.

Do not reuse the analytics subscriber slot for Sauron. Sauron gets its own
subscription and slot so it can be rebuilt independently.

### Concrete Replica Bootstrap Values

Use:

- publication: `otc_all_tables`
- subscription: `otc_sauron_subscription`
- slot: `otc_sauron_slot`
- replica database name: `otc_db`
- tunnel host: `tee-otc-primary-tunnel.railway.internal`
- replication user: `replicator`

For local development, the same logical-replication shape can be exercised
through:

- [compose.replica.yml](/home/alpinevm/Development/rift/periphery/tee-otc/etc/compose.replica.yml)

For Railway production bootstrap, mirror the SDK workflow but substitute the
Sauron-specific service, subscription, and slot names above.

### Concrete Watch Query

V1 should use the database-native tee-otc schema directly rather than inventing
new compact `chainId` or `assetId` columns in the database.

Use this full watch query:

```sql
SELECT
  s.id::text AS swap_id,
  q.from_chain AS from_chain,
  q.from_token AS from_token,
  s.deposit_vault_address AS address,
  q.min_input AS min_amount,
  q.max_input AS max_amount,
  q.expires_at AS deposit_deadline,
  s.created_at AS created_at,
  s.updated_at AS updated_at
FROM public.swaps s
JOIN public.quotes q ON q.id = s.quote_id
WHERE s.status = 'waiting_user_deposit_initiated'
ORDER BY s.updated_at ASC, s.id ASC;
```

Use this targeted refresh query after notification:

```sql
SELECT
  s.id::text AS swap_id,
  q.from_chain AS from_chain,
  q.from_token AS from_token,
  s.deposit_vault_address AS address,
  q.min_input AS min_amount,
  q.max_input AS max_amount,
  q.expires_at AS deposit_deadline,
  s.created_at AS created_at,
  s.updated_at AS updated_at
FROM public.swaps s
JOIN public.quotes q ON q.id = s.quote_id
WHERE s.id = $1::uuid
  AND s.status = 'waiting_user_deposit_initiated';
```

Interpretation rules:

- `from_chain` and `from_token` are the concrete v1 asset identity from the DB.
- `address_kind` is derived by Sauron from `from_chain`:
  - Bitcoin -> `bitcoin_address`
  - Ethereum/Base -> `evm_address`
- if the targeted refresh returns no row, Sauron removes that swap from the in-memory watch set.

V1 assumption:

- the quote fields relevant to watching (`from_chain`, `from_token`, `min_input`, `max_input`, `expires_at`) are immutable after swap creation

If that immutability assumption changes, add a corresponding subscriber-side
trigger on `public.quotes` and treat quote updates as watch invalidations too.

### Concrete Replica-Side Notification Trigger

Install this on the logical subscriber database after the subscription is up:

```sql
CREATE OR REPLACE FUNCTION public.notify_sauron_watch_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  payload jsonb;
BEGIN
  payload := jsonb_build_object(
    'op', TG_OP,
    'swapId', NEW.id,
    'updatedAt', NEW.updated_at
  );

  PERFORM pg_notify('sauron_watch_set_changed', payload::text);
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS sauron_swaps_replica_notify ON public.swaps;

CREATE TRIGGER sauron_swaps_replica_notify
AFTER INSERT OR UPDATE ON public.swaps
FOR EACH ROW
EXECUTE FUNCTION public.notify_sauron_watch_change();

ALTER TABLE public.swaps ENABLE REPLICA TRIGGER sauron_swaps_replica_notify;
```

Notes:

- this mirrors the proven analytics-server replica pattern
- the payload is intentionally minimal; Sauron does a targeted SQL read by `swapId`
- row trigger on `public.swaps` is sufficient in v1 because watch eligibility changes through swap row changes

### Concrete Sauron Runtime Configuration

Required environment:

- `OTC_REPLICA_DATABASE_URL`
- `OTC_REPLICA_DATABASE_NAME=otc_db`
- `OTC_REPLICA_NOTIFICATION_CHANNEL=sauron_watch_set_changed`
- `OTC_INTERNAL_BASE_URL`
- `OTC_DETECTOR_API_ID`
- `OTC_DETECTOR_API_SECRET`
- `ELECTRUM_HTTP_SERVER_URL`
- `EVM_RPC_URL`
- `ETHEREUM_TOKEN_INDEXER_URL`
- `ETHEREUM_ALLOWED_TOKEN`
- `BASE_RPC_URL`
- `BASE_TOKEN_INDEXER_URL`
- `BASE_ALLOWED_TOKEN`
- `SAURON_RECONCILE_INTERVAL_SECONDS=60`

Two Postgres connections are required:

- a normal query pool for full and targeted reads
- a dedicated `LISTEN` connection for `sauron_watch_set_changed`

### Concrete Rust Workspace Placement

Sauron should be built as a Rust binary inside this workspace:

- path: `bin/sauron`
- reuse the existing workspace crates for chain access, models, auth helpers, and shared utilities
- only extract new shared crates if code becomes useful to another binary

### Concrete OTC Submission Admission Model

OTC should support two admission lanes for deposit detections:

- trusted detector lane for first-party or operator-managed detector services such as Sauron
- participant-signed lane for swap participants who want to run their own tooling without pre-registering a detector service

Trusted detector lane:

- OTC maintains a runtime-configured allowlist of detector credentials
- each credential is a normal `PublicApiKeyRecord` with `id`, `tag`, and argon2 `hash`
- `bin/api-key-gen` should be reused to generate trusted detector credentials
- `Sauron` uses one trusted detector credential from that allowlist
- trusted detectors authenticate with `X-API-ID` and `X-API-SECRET`
- OTC attributes accepted/rejected trusted-lane submissions to the detector `tag`

Participant-signed lane:

- the request carries a typed EIP-712 signature over the exact candidate deposit payload
- the recovered signer must match one of the swap's allowed EVM participant addresses:
  - `refund_address` if the refund side of the swap is EVM
  - `user_destination_address` if the destination side of the swap is EVM
- if both participant addresses are EVM-capable, either signer is acceptable
- if neither participant address is EVM-capable, the participant-signed lane is unavailable for that swap in v1
- participant-signed submissions are intended for swap participants running their own tooling, not arbitrary third-party observers

ERC-1271 support:

- OTC must support ERC-1271 contract-wallet verification for the participant-signed lane
- implementation should prefer Alloy-native helpers if available at build time
- otherwise OTC should use Alloy-generated contract bindings or a direct provider `eth_call` to `isValidSignature(bytes32,bytes)` and require the ERC-1271 success magic value

Why this is the v1 choice:

- trusted detectors still get a clean operational path
- swap participants can use their own tooling without OTC-issued detector credentials
- admission is tied to actual swap participants instead of anonymous Internet callers
- it reduces spam from non-participants without forcing full public proof-of-work or payment

### Concrete OTC Runtime Configuration

Required new OTC environment:

- `PARTICIPANT_SIGNED_DETECTION_ENABLED=true`
- `PARTICIPANT_DETECTION_MIN_INTERVAL_SECONDS=60`

Configuration rules:

- OTC does not do built-in first-deposit discovery anymore
- OTC waits for trusted-detector or participant-signed submissions for the initial deposit observation
- the trusted detector lane uses the built-in `DETECTOR_API_KEY` record from `otc-auth::api_keys`
- at least one submission lane must be enabled and configured or OTC fails fast on startup
- `PARTICIPANT_DETECTION_MIN_INTERVAL_SECONDS` is an in-memory cooldown keyed by normalized participant signer address
- the cooldown is based on the last attempted participant-signed submission, not just the last successful one
- OTC should implement this cooldown as an expiry map keyed by signer address, storing `locked_until` rather than an unbounded history of past timestamps

Recommended rollout:

1. ship OTC with the single deposit-observation ingest route and dynamic auth
2. stand up Sauron and validate submissions in parallel in a non-authoritative environment
3. remove any remaining test or tooling assumptions that OTC auto-discovers deposits

### Concrete OTC Server Modification Appendix

OTC implementation changes for v1:

1. Add a detector credential store to `AppState`, separate from the market-maker key store.
2. Load detector credentials at startup from the built-in `DETECTOR_API_KEY` record in `otc-auth::api_keys`.
3. Add `POST /api/v1/swaps/:swap_id/deposit-observation`.
4. Dispatch auth dynamically inside the handler:
   - trusted detector lane when `X-API-ID` and/or `X-API-SECRET` headers are present
   - participant-signed lane when `participantAuth` is present in the body
   - reject ambiguous or missing-auth requests before chain verification
5. Authenticate the trusted lane using `X-API-ID` and `X-API-SECRET`.
6. Verify participant-signed lane requests with EIP-712 plus ERC-1271 support.
7. Accept detector-ingest submissions without request-size or inflight limiting.
8. Apply the participant-signed cooldown before chain verification.
9. Reuse the existing swap transition path for accepted deposits rather than creating a new write model.

Handler behavior:

1. Parse the payload.
2. Load `swap_id` from the route path.
3. Determine the submission lane from request auth material.
4. For the trusted detector lane, validate `X-API-ID` and `X-API-SECRET`.
5. Load the swap and quote from Postgres.
6. For the participant-signed lane, reject requests whose `signedAt` is older than 10 minutes or in the future.
7. Verify the EIP-712 payload signature against an allowed participant signer on the loaded swap.
8. Check duplicate-safe cases first:
   - if the swap is already `waiting_user_deposit_confirmed` with the same accepted deposit tx, return the duplicate response
9. Verify the swap is still eligible:
   - swap exists
   - status is `waiting_user_deposit_initiated`
   - deposit deadline has not expired
10. Verify the candidate matches the swap:
   - source chain matches the quote `from_chain`
   - source asset matches the quote `from_token`
   - destination address matches `deposit_vault_address`
   - amount is within `min_input` and `max_input`
11. Verify the tx/output/log exists using OTC's own chain access.
12. Compute realized amounts with the same `RealizedSwap::from_quote(...)` logic OTC already uses.
13. Call `db.swaps().user_deposit_detected(...)`.
14. Return a typed accepted or rejected response.

Participant-signed cooldown:

- OTC stores an in-memory expiry map keyed by normalized signer address
- each entry stores `locked_until` for that signer
- if another participant-signed submission arrives from the same signer before `PARTICIPANT_DETECTION_MIN_INTERVAL_SECONDS` elapses, OTC rejects it before chain verification
- once `locked_until` is in the past, the entry should be removed on access or by a lightweight periodic sweep
- this cooldown applies to attempts, not just successful detections
- v1 keeps this state in memory only; a process restart clears the cooldown map
- the cooldown map should only contain currently locked signers, so it does not grow without bound as historical signers accumulate

Exact internal reuse:

- accepted deposits should reuse `Swap::user_deposit_detected(...)` through [swap_repo.rs](/home/alpinevm/Development/rift/periphery/tee-otc/bin/otc-server/src/db/swap_repo.rs#L789)
- the state transition target remains `waiting_user_deposit_confirmed` in [swap_transitions.rs](/home/alpinevm/Development/rift/periphery/tee-otc/crates/otc-models/src/swap_transitions.rs#L23)
- confirmation authority remains in OTC, but user-deposit confirmation
  scheduling moves out of the old short-interval `WaitingUserDepositConfirmed`
  polling path and into [user_deposit_confirmation_scheduler.rs](/home/alpinevm/Development/rift/periphery/tee-otc/bin/otc-server/src/services/user_deposit_confirmation_scheduler.rs)

Required monitoring-service change:

- `MarketMakerBatchSettlementService` must not own any user-deposit discovery or confirmation scanning
- OTC must still keep confirmation ownership for swaps in `WaitingUserDepositConfirmed`
- OTC should move user-deposit confirmation checks into a dedicated
  block-driven scheduler keyed by chain head events
- OTC should add a low-frequency recovery sweep for
  `WaitingUserDepositConfirmed`
- OTC should enrich confirmed tx status with `inclusion_height` so exact target
  heights can be computed without brittle chain-specific inference
- OTC should only re-query a known user-deposit tx when the current chain head
  reaches that tx's computed target height
- if a known tx is seen but still pending, OTC should schedule the next recheck
  for the next observed head height
- if a known accepted tx disappears, OTC should call
  `rearm_user_deposit_detection(...)` and wait for external rediscovery rather
  than doing internal address-level re-search

This preserves the intended responsibility split:

- Sauron finds the first candidate deposit
- OTC verifies it and records it
- OTC keeps ownership of confirmation tracking and swap progression

### Concrete Detector Ingest Response Contract

Success response:

- status `202 Accepted`
- body:

```json
{
  "result": "accepted",
  "swapId": "uuid",
  "status": "waiting_user_deposit_confirmed"
}
```

Duplicate-safe response:

- status `200 OK`
- body:

```json
{
  "result": "duplicate",
  "swapId": "uuid",
  "status": "waiting_user_deposit_confirmed"
}
```

Semantic rejection response:

- status `409 Conflict`
- body:

```json
{
  "error": {
    "code": "amount_out_of_bounds",
    "message": "candidate amount is outside the swap bounds"
  }
}
```

Required rejection codes:

- `swap_not_found`
- `swap_not_waiting_user_deposit_initiated`
- `deposit_window_expired`
- `participant_signature_invalid`
- `participant_signer_not_allowed`
- `participant_submission_too_soon`
- `chain_mismatch`
- `asset_mismatch`
- `address_mismatch`
- `amount_out_of_bounds`
- `dust_output`
- `tx_not_found_on_chain`
- `transfer_not_found_in_tx`

Other response rules:

- malformed payload returns `400 Bad Request`
- auth failure returns `401 Unauthorized`
- participant cooldown rejection returns `429 Too Many Requests`
- transient chain/RPC/database failure returns `503 Service Unavailable`
- OTC must treat repeated submissions for the same already-accepted swap deposit as idempotent
- duplicate submissions must not produce duplicate side effects

### Concrete OTC Observability

OTC should expose at least:

- detector submissions accepted total by detector tag and chain
- detector submissions rejected total by detector tag and rejection code
- detector auth failures total by presented detector id
- participant-signed submissions accepted total by chain
- participant-signed submissions rejected total by rejection code
- participant-signed cooldown rejections total
- detector verification duration histogram
- current detector-ingest inflight requests

### Concrete OTC Data-Model Expectations

No new Postgres schema is required for v1.

The endpoint can reuse:

- existing swap rows
- existing quote rows
- existing user-deposit status fields
- existing realized amount calculation path

If operator audit history becomes necessary later, add a separate append-only
detector submission log table, but that is not required for the first build.

### Concrete Local State Choice

Sauron should not use SQLite or any other local durable database in v1.

- no persisted cursor store
- no local embedded DB dependency
- restart recovery comes from full watch reload plus indexed bootstrap

### Concrete Listener And Reconciliation Behavior

Startup order:

1. open the `LISTEN` connection
2. execute and commit `LISTEN sauron_watch_set_changed`
3. run the full watch query
4. build the in-memory watch map
5. start chain ingestion

Notification handling:

- parse payload
- coalesce by `swapId` in memory
- run the targeted refresh query for each pending `swapId`
- upsert or remove the watch based on the targeted query result

Durability behavior:

- run a full watch-query reconcile every `60s`
- run a full watch-query reconcile immediately after listener reconnect
- if targeted refresh fails, do not mutate the in-memory watch for that `swapId` until the next successful targeted or full reconcile

This matches the analytics-server pattern of:

- event-driven low-latency updates
- periodic reconciliation for durability

### Concrete Health Signals

Sauron should expose at least:

- replica listener connected / disconnected
- last successful full reconcile timestamp
- current watch-set size
- last notification received timestamp
- chain cursor age per chain
- candidate deposit submissions accepted / rejected

### Concrete Out-Of-Scope For V1

The following are intentionally not part of the first Sauron build:

- subscriber-side materialized watch tables
- replayable queue semantics for notifications
- Sauron-owned confirmation tracking
- writing any watch state back into OTC Postgres

## Summary

The v1 design is:

- Sauron only handles initial deposit detection.
- Sauron reads active watches from a Postgres replica via a normal query, with an optional named view.
- Sauron uses Postgres `NOTIFY` on the replica as a realtime invalidation signal.
- OTC accepts detections through a trusted detector lane and a participant-signed lane.
- OTC keeps confirmation tracking and all authoritative swap-state decisions.
- Correctness comes from boot/reconnect full-query loads plus OTC verification; notifications provide freshness, not authority.
- Chain ingestion uses in-memory tip cursors plus indexed bootstrap on restart, not persistent local cursors.
