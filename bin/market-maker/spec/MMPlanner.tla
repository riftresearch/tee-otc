---- MODULE MMPlanner ----
EXTENDS Naturals, FiniteSets, TLC

\* Minimal MM-only planner model.
\*
\* Stored state:
\* - obligations: confirmed payout obligations
\* - available_inventory: planner-usable inventory not committed anywhere
\* - outbound_batches: active committed payout batches
\* - pending_settlements: outbound-complete, inbound-not-yet-realized claims
\* - rebalances: active rebalances
\*
\* This first pass intentionally omits:
\* - quote-time reservations
\* - obligation history after an open obligation is dropped
\* - OTC protocol state
\* - executor sub-states like "committed" vs "broadcast"
\* - wallet reconciliation and recovery
\* - explicit future drain-plan storage

CONSTANTS
    AssetA,
    AssetB,
    MaxObligationId,
    MaxCreatedAt,
    MaxAmount,
    MaxInventory,
    RebalanceTriggerNumerator,
    RebalanceTriggerDenominator,
    RebalanceTargetNumerator,
    RebalanceTargetDenominator

Assets == {AssetA, AssetB}
ObligationIds == 1..MaxObligationId
CreatedAts == 1..MaxCreatedAt
Amounts == 1..MaxAmount
InventoryCap == (2 * MaxInventory) + (MaxObligationId * MaxAmount)
InitialInventories == 0..MaxInventory
Inventories == 0..InventoryCap
CommittedAmounts == 1..InventoryCap
ObligationStates == {"open", "outbound", "pending_settlement", "done"}

OtherAsset(asset) == IF asset = AssetA THEN AssetB ELSE AssetA

ObligationType ==
    [ id                : ObligationIds,
      payout_asset      : Assets,
      payout_amount     : Amounts,
      settlement_asset  : Assets,
      settlement_amount : Amounts,
      created_at        : CreatedAts,
      state             : ObligationStates ]

OutboundBatchType ==
    [ payout_asset      : Assets,
      payout_amount     : CommittedAmounts,
      settlement_asset  : Assets,
      settlement_amount : CommittedAmounts,
      obligation_ids    : SUBSET ObligationIds ]

PendingSettlementType ==
    [ asset          : Assets,
      amount         : CommittedAmounts,
      obligation_ids : SUBSET ObligationIds ]

RebalanceType ==
    [ src_asset : Assets,
      dst_asset : Assets,
      amount    : CommittedAmounts ]

VARIABLES
    obligations,
    available_inventory,
    outbound_batches,
    pending_settlements,
    rebalances

vars ==
    << obligations,
       available_inventory,
       outbound_batches,
       pending_settlements,
       rebalances >>

RECURSIVE ObligationPayoutSum(_)
RECURSIVE ObligationSettlementSum(_)
RECURSIVE OutboundBatchPayoutSum(_)
RECURSIVE PendingSettlementAmountSum(_)
RECURSIVE RebalanceAmountSum(_)

ObligationPayoutSum(obs) ==
    IF obs = {} THEN
        0
    ELSE
        LET o == CHOOSE x \in obs : TRUE
        IN o.payout_amount + ObligationPayoutSum(obs \ {o})

ObligationSettlementSum(obs) ==
    IF obs = {} THEN
        0
    ELSE
        LET o == CHOOSE x \in obs : TRUE
        IN o.settlement_amount + ObligationSettlementSum(obs \ {o})

OutboundBatchPayoutSum(bs) ==
    IF bs = {} THEN
        0
    ELSE
        LET b == CHOOSE x \in bs : TRUE
        IN b.payout_amount + OutboundBatchPayoutSum(bs \ {b})

PendingSettlementAmountSum(ps) ==
    IF ps = {} THEN
        0
    ELSE
        LET s == CHOOSE x \in ps : TRUE
        IN s.amount + PendingSettlementAmountSum(ps \ {s})

RebalanceAmountSum(rs) ==
    IF rs = {} THEN
        0
    ELSE
        LET r == CHOOSE x \in rs : TRUE
        IN r.amount + RebalanceAmountSum(rs \ {r})

Min2(x, y) == IF x < y THEN x ELSE y

Min3(x, y, z) == Min2(x, Min2(y, z))

CeilDiv(n, d) == (n + d - 1) \div d

MaxNat(S) ==
    IF S = {} THEN
        0
    ELSE
        CHOOSE x \in S : \A y \in S : x >= y

UsedIds ==
    {o.id : o \in obligations}

NextId ==
    MaxNat(UsedIds) + 1

CurrentMaxCreatedAt ==
    MaxNat({o.created_at : o \in obligations})

AllowedCreatedAts ==
    IF obligations = {} THEN
        CreatedAts
    ELSE
        CurrentMaxCreatedAt..MaxCreatedAt

LookupObligation(id, obs) ==
    CHOOSE o \in obs : o.id = id

Older(left, right) ==
    (left.created_at < right.created_at)
        \/ (left.created_at = right.created_at /\ left.id < right.id)

OlderOrEqual(left, right) ==
    Older(left, right) \/ left.id = right.id

LaneOpen(asset, obs) ==
    {o \in obs : o.state = "open" /\ o.payout_asset = asset}

PrefixThrough(asset, anchor, obs) ==
    {o \in LaneOpen(asset, obs) : OlderOrEqual(o, anchor)}

FillablePrefix(asset) ==
    {o \in LaneOpen(asset, obligations) :
        ObligationPayoutSum(PrefixThrough(asset, o, obligations)) <= available_inventory[asset]}

HasActiveOutboundBatch(asset) ==
    \E b \in outbound_batches : b.payout_asset = asset

StartableOutboundBatch(asset) ==
    /\ ~HasActiveOutboundBatch(asset)
    /\ FillablePrefix(asset) # {}

OutboundBatchRecord(asset) ==
    [ payout_asset      |-> asset,
      payout_amount     |-> ObligationPayoutSum(FillablePrefix(asset)),
      settlement_asset  |-> OtherAsset(asset),
      settlement_amount |-> ObligationSettlementSum(FillablePrefix(asset)),
      obligation_ids    |-> {o.id : o \in FillablePrefix(asset)} ]

NewOutboundBatchAssets ==
    {a \in Assets : StartableOutboundBatch(a)}

NewOutboundBatches ==
    {OutboundBatchRecord(a) : a \in NewOutboundBatchAssets}

StartedOutboundIds ==
    UNION {b.obligation_ids : b \in NewOutboundBatches}

ObligationsAfterOutboundCommit ==
    {IF o.id \in StartedOutboundIds
        THEN [o EXCEPT !.state = "outbound"]
        ELSE o : o \in obligations}

AvailableInventoryAfterOutboundBatches ==
    [a \in Assets |->
        available_inventory[a] -
            (IF a \in NewOutboundBatchAssets
                THEN OutboundBatchRecord(a).payout_amount
                ELSE 0)]

OpenAfterOutboundBatches(asset) ==
    LaneOpen(asset, ObligationsAfterOutboundCommit)

DemandAfterOutboundBatches(asset) ==
    ObligationPayoutSum(OpenAfterOutboundBatches(asset))

SurplusAfterOutboundBatches(asset) ==
    IF AvailableInventoryAfterOutboundBatches[asset] > DemandAfterOutboundBatches(asset) THEN
        AvailableInventoryAfterOutboundBatches[asset] - DemandAfterOutboundBatches(asset)
    ELSE
        0

DeficitAfterOutboundBatches(asset) ==
    IF DemandAfterOutboundBatches(asset) > AvailableInventoryAfterOutboundBatches[asset] THEN
        DemandAfterOutboundBatches(asset) - AvailableInventoryAfterOutboundBatches[asset]
    ELSE
        0

TotalAvailableInventoryAfterOutboundBatches ==
    AvailableInventoryAfterOutboundBatches[AssetA] +
        AvailableInventoryAfterOutboundBatches[AssetB]

MajorityAssetAfterOutboundBatches ==
    IF AvailableInventoryAfterOutboundBatches[AssetA] >=
        AvailableInventoryAfterOutboundBatches[AssetB]
    THEN
        AssetA
    ELSE
        AssetB

MinorityAssetAfterOutboundBatches ==
    OtherAsset(MajorityAssetAfterOutboundBatches)

MajorityAvailableInventoryAfterOutboundBatches ==
    AvailableInventoryAfterOutboundBatches[MajorityAssetAfterOutboundBatches]

MinorityBlockedDemand ==
    DeficitAfterOutboundBatches(MinorityAssetAfterOutboundBatches) > 0

TargetMajorityCap ==
    CeilDiv(
        RebalanceTargetNumerator * TotalAvailableInventoryAfterOutboundBatches,
        RebalanceTargetDenominator
    )

RebalanceAmountToTarget ==
    IF MajorityAvailableInventoryAfterOutboundBatches > TargetMajorityCap THEN
        MajorityAvailableInventoryAfterOutboundBatches - TargetMajorityCap
    ELSE
        0

RebalanceThresholdReached ==
    /\ TotalAvailableInventoryAfterOutboundBatches > 0
    /\ RebalanceTriggerDenominator * MajorityAvailableInventoryAfterOutboundBatches >=
        RebalanceTriggerNumerator * TotalAvailableInventoryAfterOutboundBatches

HasSurplusDeficitPair ==
    ((SurplusAfterOutboundBatches(AssetA) > 0 /\ DeficitAfterOutboundBatches(AssetB) > 0)
        \/ (SurplusAfterOutboundBatches(AssetB) > 0 /\ DeficitAfterOutboundBatches(AssetA) > 0))

CanStartRebalance ==
    /\ rebalances = {}
    /\ RebalanceThresholdReached
    /\ MinorityBlockedDemand
    /\ HasSurplusDeficitPair
    /\ RebalanceAmountToTarget > 0

RebalanceSource ==
    IF SurplusAfterOutboundBatches(AssetA) > 0 /\ DeficitAfterOutboundBatches(AssetB) > 0 THEN
        AssetA
    ELSE
        AssetB

RebalanceTarget ==
    OtherAsset(RebalanceSource)

RebalanceAmount ==
    IF RebalanceSource = AssetA THEN
        Min3(
            RebalanceAmountToTarget,
            SurplusAfterOutboundBatches(AssetA),
            DeficitAfterOutboundBatches(AssetB)
        )
    ELSE
        Min3(
            RebalanceAmountToTarget,
            SurplusAfterOutboundBatches(AssetB),
            DeficitAfterOutboundBatches(AssetA)
        )

NewRebalances ==
    IF CanStartRebalance THEN
        {
            [ src_asset |-> RebalanceSource,
              dst_asset |-> RebalanceTarget,
              amount    |-> RebalanceAmount ]
        }
    ELSE
        {}

AvailableInventoryAfterPlanner ==
    [a \in Assets |->
        IF CanStartRebalance /\ a = RebalanceSource THEN
            AvailableInventoryAfterOutboundBatches[a] - RebalanceAmount
        ELSE
            AvailableInventoryAfterOutboundBatches[a]]

PlannerHasWork ==
    NewOutboundBatches # {} \/ CanStartRebalance

ActiveOutboundIds ==
    UNION {b.obligation_ids : b \in outbound_batches}

ActivePendingSettlementIds ==
    UNION {s.obligation_ids : s \in pending_settlements}

PendingSettlementRecord(batch) ==
    [ asset          |-> batch.settlement_asset,
      amount         |-> batch.settlement_amount,
      obligation_ids |-> batch.obligation_ids ]

FreeInventoryTotal ==
    available_inventory[AssetA] + available_inventory[AssetB]

TotalTrackedInventory ==
    FreeInventoryTotal
        + OutboundBatchPayoutSum(outbound_batches)
        + PendingSettlementAmountSum(pending_settlements)
        + RebalanceAmountSum(rebalances)

Init ==
    /\ obligations = {}
    /\ outbound_batches = {}
    /\ pending_settlements = {}
    /\ rebalances = {}
    /\ available_inventory \in [Assets -> InitialInventories]

CreateObligation ==
    /\ NextId <= MaxObligationId
    /\ \E asset \in Assets :
        \E payoutAmount \in Amounts :
            \E settlementAmount \in payoutAmount..MaxAmount :
                \E createdAt \in AllowedCreatedAts :
                    /\ obligations' =
                        obligations \cup {
                            [ id                |-> NextId,
                              payout_asset      |-> asset,
                              payout_amount     |-> payoutAmount,
                              settlement_asset  |-> OtherAsset(asset),
                              settlement_amount |-> settlementAmount,
                              created_at        |-> createdAt,
                              state             |-> "open" ]
                        }
                    /\ UNCHANGED
                        << available_inventory,
                           outbound_batches,
                           pending_settlements,
                           rebalances >>

DropOpenObligation ==
    \E o \in obligations :
        /\ o.state = "open"
        /\ obligations' = obligations \ {o}
        /\ UNCHANGED
            << available_inventory,
               outbound_batches,
               pending_settlements,
               rebalances >>

PlannerStep ==
    /\ PlannerHasWork
    /\ obligations' = ObligationsAfterOutboundCommit
    /\ available_inventory' = AvailableInventoryAfterPlanner
    /\ outbound_batches' = outbound_batches \cup NewOutboundBatches
    /\ pending_settlements' = pending_settlements
    /\ rebalances' = rebalances \cup NewRebalances

OutboundBatchSucceeds ==
    \E b \in outbound_batches :
        /\ obligations' =
            {IF o.id \in b.obligation_ids
                THEN [o EXCEPT !.state = "pending_settlement"]
                ELSE o : o \in obligations}
        /\ outbound_batches' = outbound_batches \ {b}
        /\ pending_settlements' =
            pending_settlements \cup {PendingSettlementRecord(b)}
        /\ UNCHANGED << available_inventory, rebalances >>

OutboundBatchFails ==
    \E b \in outbound_batches :
        /\ obligations' =
            {IF o.id \in b.obligation_ids
                THEN [o EXCEPT !.state = "open"]
                ELSE o : o \in obligations}
        /\ available_inventory' =
            [available_inventory EXCEPT ![b.payout_asset] = @ + b.payout_amount]
        /\ outbound_batches' = outbound_batches \ {b}
        /\ UNCHANGED << pending_settlements, rebalances >>

SettlementCompletes ==
    \E s \in pending_settlements :
        /\ obligations' =
            {IF o.id \in s.obligation_ids
                THEN [o EXCEPT !.state = "done"]
                ELSE o : o \in obligations}
        /\ available_inventory' = [available_inventory EXCEPT ![s.asset] = @ + s.amount]
        /\ pending_settlements' = pending_settlements \ {s}
        /\ UNCHANGED << outbound_batches, rebalances >>

RebalanceSucceeds ==
    \E r \in rebalances :
        /\ available_inventory' = [available_inventory EXCEPT ![r.dst_asset] = @ + r.amount]
        /\ rebalances' = rebalances \ {r}
        /\ UNCHANGED << obligations, outbound_batches, pending_settlements >>

RebalanceFails ==
    \E r \in rebalances :
        /\ available_inventory' = [available_inventory EXCEPT ![r.src_asset] = @ + r.amount]
        /\ rebalances' = rebalances \ {r}
        /\ UNCHANGED << obligations, outbound_batches, pending_settlements >>

Next ==
    \/ CreateObligation
    \/ DropOpenObligation
    \/ PlannerStep
    \/ OutboundBatchSucceeds
    \/ OutboundBatchFails
    \/ SettlementCompletes
    \/ RebalanceSucceeds
    \/ RebalanceFails

Spec ==
    Init /\ [][Next]_vars

NonDecreasingTotalTrackedInventory ==
    [][TotalTrackedInventory' >= TotalTrackedInventory]_vars

TypeOK ==
    /\ obligations \subseteq ObligationType
    /\ available_inventory \in [Assets -> Inventories]
    /\ outbound_batches \subseteq OutboundBatchType
    /\ pending_settlements \subseteq PendingSettlementType
    /\ rebalances \subseteq RebalanceType
    /\ RebalanceTriggerDenominator > 0
    /\ RebalanceTriggerNumerator >= 0
    /\ RebalanceTriggerNumerator <= RebalanceTriggerDenominator
    /\ RebalanceTargetDenominator > 0
    /\ RebalanceTargetNumerator >= 0
    /\ RebalanceTargetNumerator <= RebalanceTargetDenominator
    /\ RebalanceTargetNumerator * RebalanceTriggerDenominator <=
        RebalanceTriggerNumerator * RebalanceTargetDenominator

UniqueObligationIds ==
    \A o1 \in obligations :
        \A o2 \in obligations :
            o1.id = o2.id => o1 = o2

ActiveOutboundIdsExist ==
    \A b \in outbound_batches :
        /\ b.obligation_ids # {}
        /\ \A id \in b.obligation_ids : \E o \in obligations : o.id = id

OutboundBatchStateConsistency ==
    \A b \in outbound_batches :
        /\ b.payout_amount =
            ObligationPayoutSum({o \in obligations : o.id \in b.obligation_ids})
        /\ b.settlement_amount =
            ObligationSettlementSum({o \in obligations : o.id \in b.obligation_ids})
        /\ b.settlement_asset = OtherAsset(b.payout_asset)
        /\ \A id \in b.obligation_ids :
            LET o == LookupObligation(id, obligations)
            IN /\ o.state = "outbound"
               /\ o.payout_asset = b.payout_asset
               /\ o.settlement_asset = b.settlement_asset

NoObligationInMultipleOutboundBatches ==
    \A id \in ObligationIds :
        Cardinality({b \in outbound_batches : id \in b.obligation_ids}) <= 1

AtMostOneOutboundBatchPerAsset ==
    \A a \in Assets :
        Cardinality({b \in outbound_batches : b.payout_asset = a}) <= 1

ActivePendingSettlementIdsExist ==
    \A s \in pending_settlements :
        /\ s.obligation_ids # {}
        /\ \A id \in s.obligation_ids : \E o \in obligations : o.id = id

PendingSettlementConsistency ==
    \A s \in pending_settlements :
        /\ s.amount =
            ObligationSettlementSum({o \in obligations : o.id \in s.obligation_ids})
        /\ \A id \in s.obligation_ids :
            LET o == LookupObligation(id, obligations)
            IN /\ o.state = "pending_settlement"
               /\ o.settlement_asset = s.asset

NoObligationInMultiplePendingSettlements ==
    \A id \in ObligationIds :
        Cardinality({s \in pending_settlements : id \in s.obligation_ids}) <= 1

AtMostOneRebalance ==
    Cardinality(rebalances) <= 1

ValidRebalances ==
    \A r \in rebalances :
        /\ r.src_asset \in Assets
        /\ r.dst_asset \in Assets
        /\ r.src_asset # r.dst_asset
        /\ r.amount > 0

OutboundObligationsHaveActiveOutboundBatch ==
    \A o \in obligations :
        o.state = "outbound" => o.id \in ActiveOutboundIds

NonOutboundObligationsHaveNoActiveOutboundBatch ==
    \A o \in obligations :
        o.state # "outbound" => o.id \notin ActiveOutboundIds

PendingSettlementObligationsHaveActivePendingSettlement ==
    \A o \in obligations :
        o.state = "pending_settlement" => o.id \in ActivePendingSettlementIds

NonPendingSettlementObligationsHaveNoActivePendingSettlement ==
    \A o \in obligations :
        o.state # "pending_settlement" => o.id \notin ActivePendingSettlementIds

OldestFirstActiveOutboundBatches ==
    \A b \in outbound_batches :
        \A chosenId \in b.obligation_ids :
            LET chosen == LookupObligation(chosenId, obligations)
            IN \A olderOpen \in LaneOpen(b.payout_asset, obligations) :
                ~Older(olderOpen, chosen)

\* Scenario helpers for a concrete interleaving:
\* - a rebalance into AssetB is already active
\* - a new AssetA obligation arrives while that rebalance is active
\* - the planner starts an AssetA outbound batch before the rebalance resolves

\* Scenario helper for a targeted deep monotonicity run:
\* - AssetA starts with enough free inventory to pay one outbound obligation
\* - AssetB starts blocked
\* - the first planner step can start both an AssetA outbound batch and an
\*   AssetA -> AssetB rebalance
\* - successful outbound completion strictly increases tracked inventory
\*   because settlement_amount > payout_amount for obligation 1

TargetedDeepMonotonicityInit ==
    /\ obligations =
        {
            [ id                |-> 1,
              payout_asset      |-> AssetA,
              payout_amount     |-> 2,
              settlement_asset  |-> AssetB,
              settlement_amount |-> 3,
              created_at        |-> 1,
              state             |-> "open" ],
            [ id                |-> 2,
              payout_asset      |-> AssetB,
              payout_amount     |-> 2,
              settlement_asset  |-> AssetA,
              settlement_amount |-> 2,
              created_at        |-> 1,
              state             |-> "open" ]
        }
    /\ available_inventory = [a \in Assets |-> IF a = AssetA THEN 5 ELSE 0]
    /\ outbound_batches = {}
    /\ pending_settlements = {}
    /\ rebalances = {}

InterleavingScenarioInit ==
    /\ obligations =
        {
            [ id                |-> 1,
              payout_asset      |-> AssetB,
              payout_amount     |-> 2,
              settlement_asset  |-> AssetA,
              settlement_amount |-> 2,
              created_at        |-> 1,
              state             |-> "open" ]
        }
    /\ available_inventory = [a \in Assets |-> IF a = AssetA THEN 1 ELSE 0]
    /\ outbound_batches = {}
    /\ pending_settlements = {}
    /\ rebalances =
        {
            [ src_asset |-> AssetA,
              dst_asset |-> AssetB,
              amount    |-> 2 ]
        }

InterleavingScenarioAfterCreate ==
    /\ obligations =
        {
            [ id                |-> 1,
              payout_asset      |-> AssetB,
              payout_amount     |-> 2,
              settlement_asset  |-> AssetA,
              settlement_amount |-> 2,
              created_at        |-> 1,
              state             |-> "open" ],
            [ id                |-> 2,
              payout_asset      |-> AssetA,
              payout_amount     |-> 1,
              settlement_asset  |-> AssetB,
              settlement_amount |-> 1,
              created_at        |-> 1,
              state             |-> "open" ]
        }
    /\ available_inventory = [a \in Assets |-> IF a = AssetA THEN 1 ELSE 0]
    /\ outbound_batches = {}
    /\ pending_settlements = {}
    /\ rebalances =
        {
            [ src_asset |-> AssetA,
              dst_asset |-> AssetB,
              amount    |-> 2 ]
        }

InterleavingScenarioAfterPlanner ==
    /\ obligations =
        {
            [ id                |-> 1,
              payout_asset      |-> AssetB,
              payout_amount     |-> 2,
              settlement_asset  |-> AssetA,
              settlement_amount |-> 2,
              created_at        |-> 1,
              state             |-> "open" ],
            [ id                |-> 2,
              payout_asset      |-> AssetA,
              payout_amount     |-> 1,
              settlement_asset  |-> AssetB,
              settlement_amount |-> 1,
              created_at        |-> 1,
              state             |-> "outbound" ]
        }
    /\ available_inventory = [a \in Assets |-> 0]
    /\ outbound_batches =
        {
            [ payout_asset      |-> AssetA,
              payout_amount     |-> 1,
              settlement_asset  |-> AssetB,
              settlement_amount |-> 1,
              obligation_ids    |-> {2} ]
        }
    /\ pending_settlements = {}
    /\ rebalances =
        {
            [ src_asset |-> AssetA,
              dst_asset |-> AssetB,
              amount    |-> 2 ]
        }

InterleavingScenarioActionConstraint ==
    \/ /\ InterleavingScenarioInit
       /\ InterleavingScenarioAfterCreate'
    \/ /\ InterleavingScenarioAfterCreate
       /\ InterleavingScenarioAfterPlanner'
    \/ ~InterleavingScenarioInit /\ ~InterleavingScenarioAfterCreate

====
