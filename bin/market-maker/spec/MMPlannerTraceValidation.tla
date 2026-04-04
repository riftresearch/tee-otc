---- MODULE MMPlannerTraceValidation ----
EXTENDS MMPlanner, Sequences, MMPlannerTraceData

VARIABLE trace_index

trace_vars ==
    << obligations,
       available_inventory,
       outbound_batches,
       pending_settlements,
       rebalances,
       trace_index >>

TraceLen == Len(TraceStates)

StateAt(i) == TraceStates[i]

StateMatches(i) ==
    /\ obligations = StateAt(i).obligations
    /\ available_inventory = StateAt(i).available_inventory
    /\ outbound_batches = StateAt(i).outbound_batches
    /\ pending_settlements = StateAt(i).pending_settlements
    /\ rebalances = StateAt(i).rebalances

StateMatchesNext(i) ==
    /\ obligations' = StateAt(i).obligations
    /\ available_inventory' = StateAt(i).available_inventory
    /\ outbound_batches' = StateAt(i).outbound_batches
    /\ pending_settlements' = StateAt(i).pending_settlements
    /\ rebalances' = StateAt(i).rebalances

ActionAllowed(action) ==
    CASE action = "CreateObligation" -> CreateObligation
        [] action = "DropOpenObligation" -> DropOpenObligation
        [] action = "PlannerStep" -> PlannerStep
        [] action = "OutboundBatchSucceeds" -> OutboundBatchSucceeds
        [] action = "OutboundBatchFails" -> OutboundBatchFails
        [] action = "SettlementCompletes" -> SettlementCompletes
        [] action = "RebalanceSucceeds" -> RebalanceSucceeds
        [] action = "RebalanceFails" -> RebalanceFails
        [] OTHER -> FALSE

TraceInit ==
    /\ TraceLen > 0
    /\ Len(TraceActions) = TraceLen - 1
    /\ Init
    /\ StateMatches(1)
    /\ trace_index = 1

TraceAdvance ==
    /\ trace_index < TraceLen
    /\ ActionAllowed(TraceActions[trace_index])
    /\ StateMatchesNext(trace_index + 1)
    /\ trace_index' = trace_index + 1

TraceDone ==
    /\ trace_index = TraceLen
    /\ UNCHANGED trace_vars

TraceSpec ==
    TraceInit /\ [][TraceAdvance \/ TraceDone]_trace_vars

====
