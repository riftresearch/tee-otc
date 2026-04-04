use std::{
    collections::BTreeSet,
    env,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use market_maker::planner::{
    Obligation, ObligationState, OutboundBatch, PendingSettlement, PlannerAsset, PlannerPolicy,
    PlannerState, Rebalance,
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use tempfile::TempDir;

#[derive(Clone, Copy)]
enum TraceAction {
    CreateObligation,
    DropOpenObligation,
    PlannerStep,
    OutboundBatchSucceeds,
    OutboundBatchFails,
    SettlementCompletes,
    RebalanceSucceeds,
    RebalanceFails,
}

impl TraceAction {
    fn as_tla(self) -> &'static str {
        match self {
            Self::CreateObligation => "CreateObligation",
            Self::DropOpenObligation => "DropOpenObligation",
            Self::PlannerStep => "PlannerStep",
            Self::OutboundBatchSucceeds => "OutboundBatchSucceeds",
            Self::OutboundBatchFails => "OutboundBatchFails",
            Self::SettlementCompletes => "SettlementCompletes",
            Self::RebalanceSucceeds => "RebalanceSucceeds",
            Self::RebalanceFails => "RebalanceFails",
        }
    }
}

struct PlannerTrace {
    states: Vec<PlannerState>,
    actions: Vec<TraceAction>,
}

impl PlannerTrace {
    fn new(initial_state: PlannerState) -> Self {
        Self {
            states: vec![initial_state],
            actions: Vec::new(),
        }
    }

    fn record(&mut self, action: TraceAction, next_state: &PlannerState) {
        self.actions.push(action);
        self.states.push(next_state.clone());
    }

    fn max_obligation_id(&self) -> u64 {
        self.states
            .iter()
            .flat_map(|state| state.obligations.keys().copied())
            .max()
            .unwrap_or(1)
    }

    fn max_created_at(&self) -> u64 {
        self.states
            .iter()
            .flat_map(|state| {
                state
                    .obligations
                    .values()
                    .map(|obligation| obligation.created_at)
            })
            .max()
            .unwrap_or(1)
    }

    fn max_amount(&self) -> u64 {
        self.states
            .iter()
            .flat_map(|state| {
                state
                    .obligations
                    .values()
                    .flat_map(|obligation| [obligation.payout_amount, obligation.settlement_amount])
            })
            .max()
            .unwrap_or(1)
    }

    fn max_inventory(&self) -> u64 {
        self.states
            .iter()
            .flat_map(|state| {
                PlannerAsset::ALL.into_iter().map(|asset| {
                    state
                        .available_inventory
                        .get(&asset)
                        .copied()
                        .unwrap_or_default()
                })
            })
            .max()
            .unwrap_or(0)
    }

    fn write_validation_inputs(&self, dir: &Path) {
        let mut data_module = String::new();
        writeln!(&mut data_module, "---- MODULE MMPlannerTraceData ----").unwrap();
        writeln!(&mut data_module, "EXTENDS MMPlanner, Sequences").unwrap();
        writeln!(&mut data_module).unwrap();
        writeln!(&mut data_module, "TraceStates ==").unwrap();
        writeln!(
            &mut data_module,
            "    <<{}>>",
            self.states
                .iter()
                .map(render_state)
                .collect::<Vec<_>>()
                .join(", ")
        )
        .unwrap();
        writeln!(&mut data_module).unwrap();
        writeln!(&mut data_module, "TraceActions ==").unwrap();
        writeln!(
            &mut data_module,
            "    <<{}>>",
            self.actions
                .iter()
                .map(|action| format!("\"{}\"", action.as_tla()))
                .collect::<Vec<_>>()
                .join(", ")
        )
        .unwrap();
        writeln!(&mut data_module).unwrap();
        writeln!(&mut data_module, "====").unwrap();

        fs::write(dir.join("MMPlannerTraceData.tla"), data_module).unwrap();

        let config = format!(
            r#"SPECIFICATION TraceSpec

CONSTANTS
    AssetA = AssetA
    AssetB = AssetB
    MaxObligationId = {max_obligation_id}
    MaxCreatedAt = {max_created_at}
    MaxAmount = {max_amount}
    MaxInventory = {max_inventory}
    RebalanceTriggerNumerator = 7000
    RebalanceTriggerDenominator = 10000
    RebalanceTargetNumerator = 5000
    RebalanceTargetDenominator = 10000

INVARIANT TypeOK
INVARIANT UniqueObligationIds
INVARIANT ActiveOutboundIdsExist
INVARIANT OutboundBatchStateConsistency
INVARIANT NoObligationInMultipleOutboundBatches
INVARIANT AtMostOneOutboundBatchPerAsset
INVARIANT ActivePendingSettlementIdsExist
INVARIANT PendingSettlementConsistency
INVARIANT NoObligationInMultiplePendingSettlements
INVARIANT AtMostOneRebalance
INVARIANT ValidRebalances
INVARIANT OutboundObligationsHaveActiveOutboundBatch
INVARIANT NonOutboundObligationsHaveNoActiveOutboundBatch
INVARIANT PendingSettlementObligationsHaveActivePendingSettlement
INVARIANT NonPendingSettlementObligationsHaveNoActivePendingSettlement
INVARIANT OldestFirstActiveOutboundBatches
"#,
            max_obligation_id = self.max_obligation_id(),
            max_created_at = self.max_created_at(),
            max_amount = self.max_amount(),
            max_inventory = self.max_inventory(),
        );
        fs::write(dir.join("MMPlannerTraceValidation.cfg"), config).unwrap();
    }
}

fn planner_asset_literal(asset: PlannerAsset) -> &'static str {
    match asset {
        PlannerAsset::AssetA => "AssetA",
        PlannerAsset::AssetB => "AssetB",
    }
}

fn obligation_state_literal(state: ObligationState) -> &'static str {
    match state {
        ObligationState::Open => "\"open\"",
        ObligationState::Outbound => "\"outbound\"",
        ObligationState::PendingSettlement => "\"pending_settlement\"",
        ObligationState::Done => "\"done\"",
    }
}

fn tla_set(items: Vec<String>) -> String {
    if items.is_empty() {
        "{}".to_string()
    } else {
        format!("{{{}}}", items.join(", "))
    }
}

fn render_u64_set(values: &BTreeSet<u64>) -> String {
    tla_set(values.iter().map(u64::to_string).collect())
}

fn render_obligation(obligation: &Obligation) -> String {
    format!(
        "[ id |-> {id}, payout_asset |-> {payout_asset}, payout_amount |-> {payout_amount}, settlement_asset |-> {settlement_asset}, settlement_amount |-> {settlement_amount}, created_at |-> {created_at}, state |-> {state} ]",
        id = obligation.id,
        payout_asset = planner_asset_literal(obligation.payout_asset),
        payout_amount = obligation.payout_amount,
        settlement_asset = planner_asset_literal(obligation.settlement_asset),
        settlement_amount = obligation.settlement_amount,
        created_at = obligation.created_at,
        state = obligation_state_literal(obligation.state),
    )
}

fn render_outbound_batch(batch: &OutboundBatch) -> String {
    format!(
        "[ payout_asset |-> {payout_asset}, payout_amount |-> {payout_amount}, settlement_asset |-> {settlement_asset}, settlement_amount |-> {settlement_amount}, obligation_ids |-> {obligation_ids} ]",
        payout_asset = planner_asset_literal(batch.payout_asset),
        payout_amount = batch.payout_amount,
        settlement_asset = planner_asset_literal(batch.settlement_asset),
        settlement_amount = batch.settlement_amount,
        obligation_ids = render_u64_set(&batch.obligation_ids),
    )
}

fn render_pending_settlement(settlement: &PendingSettlement) -> String {
    format!(
        "[ asset |-> {asset}, amount |-> {amount}, obligation_ids |-> {obligation_ids} ]",
        asset = planner_asset_literal(settlement.asset),
        amount = settlement.amount,
        obligation_ids = render_u64_set(&settlement.obligation_ids),
    )
}

fn render_rebalance(rebalance: Rebalance) -> String {
    format!(
        "[ src_asset |-> {src_asset}, dst_asset |-> {dst_asset}, amount |-> {amount} ]",
        src_asset = planner_asset_literal(rebalance.src_asset),
        dst_asset = planner_asset_literal(rebalance.dst_asset),
        amount = rebalance.amount,
    )
}

fn render_inventory(state: &PlannerState) -> String {
    format!(
        "[AssetA |-> {asset_a}, AssetB |-> {asset_b}]",
        asset_a = state
            .available_inventory
            .get(&PlannerAsset::AssetA)
            .copied()
            .unwrap_or_default(),
        asset_b = state
            .available_inventory
            .get(&PlannerAsset::AssetB)
            .copied()
            .unwrap_or_default(),
    )
}

fn render_state(state: &PlannerState) -> String {
    let obligations = tla_set(state.obligations.values().map(render_obligation).collect());
    let outbound_batches = tla_set(
        state
            .outbound_batches
            .values()
            .map(render_outbound_batch)
            .collect(),
    );
    let pending_settlements = tla_set(
        state
            .pending_settlements
            .values()
            .map(render_pending_settlement)
            .collect(),
    );
    let rebalances = match state.rebalance {
        Some(rebalance) => tla_set(vec![render_rebalance(rebalance)]),
        None => "{}".to_string(),
    };

    format!(
        "[ obligations |-> {obligations}, available_inventory |-> {available_inventory}, outbound_batches |-> {outbound_batches}, pending_settlements |-> {pending_settlements}, rebalances |-> {rebalances} ]",
        obligations = obligations,
        available_inventory = render_inventory(state),
        outbound_batches = outbound_batches,
        pending_settlements = pending_settlements,
        rebalances = rebalances,
    )
}

fn open_obligation(
    id: u64,
    payout_asset: PlannerAsset,
    payout_amount: u64,
    settlement_amount: u64,
    created_at: u64,
) -> Obligation {
    Obligation {
        id,
        payout_asset,
        payout_amount,
        settlement_asset: payout_asset.other(),
        settlement_amount,
        created_at,
        state: ObligationState::Open,
    }
}

fn market_maker_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn repo_root() -> PathBuf {
    market_maker_root()
        .parent()
        .and_then(Path::parent)
        .expect("market-maker crate should live under repo/bin/")
        .to_path_buf()
}

fn tla_jar_path() -> PathBuf {
    std::env::var_os("TLA_JAR")
        .map(PathBuf::from)
        .unwrap_or_else(|| repo_root().join(".cache/tla2tools.jar"))
}

fn run_tlc_validation(trace: &PlannerTrace, case_name: &str) {
    let tempdir = TempDir::new().unwrap();
    let spec_dir = market_maker_root().join("spec");
    fs::copy(
        spec_dir.join("MMPlanner.tla"),
        tempdir.path().join("MMPlanner.tla"),
    )
    .unwrap();
    fs::copy(
        spec_dir.join("MMPlannerTraceValidation.tla"),
        tempdir.path().join("MMPlannerTraceValidation.tla"),
    )
    .unwrap();
    trace.write_validation_inputs(tempdir.path());

    let tla_jar = tla_jar_path();
    assert!(
        tla_jar.exists(),
        "missing tla2tools.jar at {}. Run `just mm-tla-trace-tests` or set TLA_JAR.",
        tla_jar.display()
    );

    let output = Command::new("java")
        .current_dir(tempdir.path())
        .arg("-cp")
        .arg(&tla_jar)
        .arg("tlc2.TLC")
        .arg("MMPlannerTraceValidation.tla")
        .arg("-config")
        .arg("MMPlannerTraceValidation.cfg")
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "TLC trace validation failed for {case_name}\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn random_asset(rng: &mut StdRng) -> PlannerAsset {
    if rng.gen_bool(0.5) {
        PlannerAsset::AssetA
    } else {
        PlannerAsset::AssetB
    }
}

fn generate_random_trace(rng: &mut StdRng, max_steps: usize) -> PlannerTrace {
    const MAX_OBLIGATIONS: u64 = 5;

    let asset_a_inventory = rng.gen_range(0..=6);
    let asset_b_inventory = rng.gen_range(0..=6);
    let mut state = PlannerState::new(asset_a_inventory, asset_b_inventory);
    let mut trace = PlannerTrace::new(state.clone());
    let mut next_obligation_id = 1u64;
    let mut current_created_at = 0u64;

    for _ in 0..max_steps {
        let mut actions = Vec::new();

        if next_obligation_id <= MAX_OBLIGATIONS {
            actions.push(TraceAction::CreateObligation);
        }

        if !state.next_actions(PlannerPolicy::default()).is_empty() {
            actions.push(TraceAction::PlannerStep);
        }

        if !state.outbound_batches.is_empty() {
            actions.push(TraceAction::OutboundBatchSucceeds);
            actions.push(TraceAction::OutboundBatchFails);
        }

        if !state.pending_settlements.is_empty() {
            actions.push(TraceAction::SettlementCompletes);
        }

        if state.rebalance.is_some() {
            actions.push(TraceAction::RebalanceSucceeds);
            actions.push(TraceAction::RebalanceFails);
        }

        if state
            .obligations
            .values()
            .any(|obligation| obligation.state == ObligationState::Open)
        {
            actions.push(TraceAction::DropOpenObligation);
        }

        if actions.is_empty() {
            break;
        }

        let chosen = actions[rng.gen_range(0..actions.len())];
        match chosen {
            TraceAction::CreateObligation => {
                let payout_asset = random_asset(rng);
                let payout_amount = rng.gen_range(1..=3);
                let settlement_amount = rng.gen_range(payout_amount..=3);
                if rng.gen_bool(0.7) {
                    current_created_at = current_created_at.saturating_add(1);
                } else {
                    current_created_at = current_created_at.max(1);
                }
                let obligation = open_obligation(
                    next_obligation_id,
                    payout_asset,
                    payout_amount,
                    settlement_amount,
                    current_created_at.max(1),
                );
                state.insert_obligation(obligation);
                next_obligation_id = next_obligation_id.saturating_add(1);
                trace.record(TraceAction::CreateObligation, &state);
            }
            TraceAction::PlannerStep => {
                let actions = state.commit_next_actions(PlannerPolicy::default());
                assert!(!actions.is_empty());
                trace.record(TraceAction::PlannerStep, &state);
            }
            TraceAction::OutboundBatchSucceeds => {
                let assets = state.outbound_batches.keys().copied().collect::<Vec<_>>();
                let asset = assets[rng.gen_range(0..assets.len())];
                state
                    .mark_outbound_batch_succeeded(asset)
                    .expect("generated trace expected outbound batch");
                trace.record(TraceAction::OutboundBatchSucceeds, &state);
            }
            TraceAction::OutboundBatchFails => {
                let assets = state.outbound_batches.keys().copied().collect::<Vec<_>>();
                let asset = assets[rng.gen_range(0..assets.len())];
                state
                    .mark_outbound_batch_failed(asset)
                    .expect("generated trace expected outbound batch");
                trace.record(TraceAction::OutboundBatchFails, &state);
            }
            TraceAction::SettlementCompletes => {
                let keys = state
                    .pending_settlements
                    .keys()
                    .cloned()
                    .collect::<Vec<BTreeSet<u64>>>();
                let key = &keys[rng.gen_range(0..keys.len())];
                state
                    .complete_pending_settlement(key)
                    .expect("generated trace expected pending settlement");
                trace.record(TraceAction::SettlementCompletes, &state);
            }
            TraceAction::RebalanceSucceeds => {
                state
                    .mark_rebalance_succeeded()
                    .expect("generated trace expected rebalance");
                trace.record(TraceAction::RebalanceSucceeds, &state);
            }
            TraceAction::RebalanceFails => {
                state
                    .mark_rebalance_failed()
                    .expect("generated trace expected rebalance");
                trace.record(TraceAction::RebalanceFails, &state);
            }
            TraceAction::DropOpenObligation => {
                let open_ids = state
                    .obligations
                    .values()
                    .filter(|obligation| obligation.state == ObligationState::Open)
                    .map(|obligation| obligation.id)
                    .collect::<Vec<_>>();
                let obligation_id = open_ids[rng.gen_range(0..open_ids.len())];
                assert!(state.drop_open_obligation(obligation_id));
                trace.record(TraceAction::DropOpenObligation, &state);
            }
        }
    }

    trace
}

#[test]
#[ignore = "requires Java and tla2tools.jar; run via `just mm-tla-trace-tests`"]
fn validates_real_planner_trace_with_rebalance_and_settlement_success() {
    let mut state = PlannerState::new(7, 3);
    let mut trace = PlannerTrace::new(state.clone());

    state.insert_obligation(open_obligation(1, PlannerAsset::AssetB, 4, 4, 1));
    trace.record(TraceAction::CreateObligation, &state);

    let actions = state.commit_next_actions(PlannerPolicy::default());
    assert!(actions.outbound_batches.is_empty());
    assert!(actions.rebalance.is_some());
    trace.record(TraceAction::PlannerStep, &state);

    state.mark_rebalance_failed().expect("expected rebalance");
    trace.record(TraceAction::RebalanceFails, &state);

    let actions = state.commit_next_actions(PlannerPolicy::default());
    assert!(actions.outbound_batches.is_empty());
    assert!(actions.rebalance.is_some());
    trace.record(TraceAction::PlannerStep, &state);

    state
        .mark_rebalance_succeeded()
        .expect("expected rebalance success");
    trace.record(TraceAction::RebalanceSucceeds, &state);

    let actions = state.commit_next_actions(PlannerPolicy::default());
    assert_eq!(actions.outbound_batches.len(), 1);
    assert!(actions.rebalance.is_none());
    trace.record(TraceAction::PlannerStep, &state);

    state
        .mark_outbound_batch_succeeded(PlannerAsset::AssetB)
        .expect("expected outbound success");
    trace.record(TraceAction::OutboundBatchSucceeds, &state);

    let settlement_key = BTreeSet::from([1]);
    state
        .complete_pending_settlement(&settlement_key)
        .expect("expected settlement completion");
    trace.record(TraceAction::SettlementCompletes, &state);

    run_tlc_validation(&trace, "rebalance_and_settlement_success");
}

#[test]
#[ignore = "requires Java and tla2tools.jar; run via `just mm-tla-trace-tests`"]
fn validates_real_planner_trace_with_outbound_failure_and_drop() {
    let mut state = PlannerState::new(5, 0);
    let mut trace = PlannerTrace::new(state.clone());

    state.insert_obligation(open_obligation(1, PlannerAsset::AssetA, 2, 2, 1));
    trace.record(TraceAction::CreateObligation, &state);

    state.insert_obligation(open_obligation(2, PlannerAsset::AssetA, 3, 3, 2));
    trace.record(TraceAction::CreateObligation, &state);

    state.insert_obligation(open_obligation(3, PlannerAsset::AssetA, 1, 1, 3));
    trace.record(TraceAction::CreateObligation, &state);

    let actions = state.commit_next_actions(PlannerPolicy::default());
    assert_eq!(actions.outbound_batches.len(), 1);
    trace.record(TraceAction::PlannerStep, &state);

    state
        .mark_outbound_batch_failed(PlannerAsset::AssetA)
        .expect("expected outbound failure");
    trace.record(TraceAction::OutboundBatchFails, &state);

    assert!(state.drop_open_obligation(3));
    trace.record(TraceAction::DropOpenObligation, &state);

    run_tlc_validation(&trace, "outbound_failure_and_drop");
}

#[test]
#[ignore = "requires Java and tla2tools.jar; run via `just mm-tla-trace-tests`"]
fn validates_many_generated_planner_traces_against_tla() {
    let cases = env_usize("MM_TLA_TRACE_CASES", 64);
    let max_steps = env_usize("MM_TLA_TRACE_STEPS", 12);
    let seed = env_u64("MM_TLA_TRACE_SEED", 1);

    let mut rng = StdRng::seed_from_u64(seed);
    for case_idx in 0..cases {
        let trace = generate_random_trace(&mut rng, max_steps);
        run_tlc_validation(&trace, &format!("generated_case_{case_idx}"));
    }
}
