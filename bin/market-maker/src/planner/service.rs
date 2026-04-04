use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, OnceLock},
};

use alloy::primitives::U256;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use otc_chains::traits::MarketMakerQueuedPayment;
use otc_models::{constants::CB_BTC_CONTRACT_ADDRESS, ChainType, Currency, Lot, TokenIdentifier};
use otc_protocols::mm::ActiveObligation;
use serde::{Deserialize, Serialize};
use snafu::{location, Location, OptionExt, ResultExt, Snafu};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    batch_monitor::BatchStatusListener,
    db::{AssociatedSwapDeposit, BatchStatus, DepositRepository, StoredBatch},
    liquidity_lock::LockedLiquidity,
    payment_manager::PaymentManager,
    rebalancer_actor::{RebalanceDispatcher, RebalanceStateSink},
    wallet::{Wallet, WalletError},
};

use super::{
    ObligationState, PlannerActions, PlannerAsset, PlannerPolicy, PlannerState, Rebalance,
};

#[derive(Debug, Snafu)]
pub enum PlannerServiceError {
    #[snafu(display("Missing planner obligation for swap {swap_id} at {loc}"))]
    MissingObligation {
        swap_id: Uuid,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Missing planner context for obligation {obligation_id} at {loc}"))]
    MissingContext {
        obligation_id: u64,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Missing planner obligation {obligation_id} in runtime state at {loc}"))]
    MissingRuntimeObligation {
        obligation_id: u64,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display(
        "Missing pending settlement membership for obligation {obligation_id} at {loc}"
    ))]
    MissingPendingSettlement {
        obligation_id: u64,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Unsupported planner currency {currency:?} at {loc}"))]
    UnsupportedCurrency {
        currency: Currency,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Amount {value} for {field} does not fit in u64 at {loc}"))]
    InvalidAmount {
        field: &'static str,
        value: U256,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Timestamp {value} for {field} is negative at {loc}"))]
    InvalidTimestamp {
        field: &'static str,
        value: i64,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display(
        "Obligation for swap {swap_id} is inventory-lossy: settlement {settlement_amount} < payout {payout_amount} at {loc}"
    ))]
    LossyObligation {
        swap_id: Uuid,
        payout_amount: u64,
        settlement_amount: u64,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display(
        "Confirmed deposit details for swap {swap_id} did not match local quote lock: {reason} at {loc}"
    ))]
    ConfirmedDepositMismatch {
        swap_id: Uuid,
        reason: String,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display(
        "Planner inventory decreased across {transition}: before={before}, after={after} at {loc}"
    ))]
    InventoryDecreased {
        transition: &'static str,
        before: u64,
        after: u64,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display(
        "Settlement lot for swap {swap_id} did not match planner obligation: {reason} at {loc}"
    ))]
    SettlementMismatch {
        swap_id: Uuid,
        reason: String,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Wallet operation failed: {source}"))]
    Wallet {
        source: WalletError,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Payment repository operation failed: {source}"))]
    PaymentRepository {
        source: crate::db::PaymentRepositoryError,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Deposit repository operation failed: {source}"))]
    DepositRepository {
        source: crate::db::DepositRepositoryError,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Rebalance inventory already active at {loc}"))]
    RebalanceAlreadyActive {
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Rebalance restore would overdraw available inventory at {loc}"))]
    RebalanceRestoreOverdraw {
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display(
        "Snapshot batch {txid} referenced swap {swap_id} without recoverable obligation data at {loc}"
    ))]
    MissingSnapshotObligation {
        txid: String,
        swap_id: Uuid,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display(
        "Snapshot batch {txid} contained inconsistent {field} across obligations at {loc}"
    ))]
    InconsistentSnapshotBatch {
        txid: String,
        field: &'static str,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display(
        "Snapshot settlement deposit for swap {swap_id} did not match obligation data: {reason} at {loc}"
    ))]
    SnapshotDepositMismatch {
        swap_id: Uuid,
        reason: String,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display(
        "Snapshot free inventory for {asset:?} underflowed: wallet_total={wallet_total}, reserved={reserved} at {loc}"
    ))]
    SnapshotInventoryUnderflow {
        asset: PlannerAsset,
        wallet_total: u64,
        reserved: u64,
        #[snafu(implicit)]
        loc: Location,
    },
}

type Result<T, E = PlannerServiceError> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct PlannerService {
    inner: Arc<PlannerServiceInner>,
}

struct PlannerServiceInner {
    runtime: Mutex<PlannerRuntime>,
    payment_manager: Arc<PaymentManager>,
    bitcoin_wallet: Arc<dyn Wallet>,
    evm_wallet: Arc<dyn Wallet>,
    policy: PlannerPolicy,
    evm_chain: ChainType,
    rebalance_dispatcher: OnceLock<Arc<dyn RebalanceDispatcher>>,
    cancellation_token: CancellationToken,
}

struct PlannerRuntime {
    planner_state: PlannerState,
    next_obligation_id: u64,
    swap_to_obligation_id: BTreeMap<Uuid, u64>,
    obligation_contexts: BTreeMap<u64, ObligationContext>,
    batch_txid_to_asset: BTreeMap<String, PlannerAsset>,
    pending_settlement_keys_by_obligation: BTreeMap<u64, BTreeSet<u64>>,
    settlement_completion_progress: BTreeMap<BTreeSet<u64>, BTreeSet<u64>>,
}

#[derive(Debug, Clone)]
struct ObligationContext {
    swap_id: Uuid,
    quote_id: Uuid,
    expected_lot: Lot,
    user_destination_address: String,
    mm_nonce: [u8; 16],
    protocol_fee: U256,
    user_deposit_confirmed_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug)]
struct DispatchPlan {
    outbound_batches: Vec<ScheduledOutboundBatch>,
    rebalance: Option<Rebalance>,
}

#[derive(Debug)]
struct ScheduledOutboundBatch {
    asset: PlannerAsset,
    payments: Vec<MarketMakerQueuedPayment>,
}

#[derive(Debug, Clone)]
struct SnapshotObligation {
    obligation: super::Obligation,
    status: otc_models::SwapStatus,
    swap_id: Uuid,
    quote_id: Uuid,
    user_destination_address: String,
    mm_nonce: [u8; 16],
    protocol_fee: U256,
    user_deposit_confirmed_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Copy)]
struct CompletedSettlement {
    asset: PlannerAsset,
    amount: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeterministicDrainPlan {
    pub assumptions: DeterministicDrainPlanAssumptions,
    pub max_rounds: usize,
    pub truncated: bool,
    pub fully_drained: bool,
    pub current_state: DeterministicPlannerStateSummary,
    pub current_in_flight: DeterministicInFlightWork,
    pub projected_final_state: DeterministicPlannerStateSummary,
    pub rounds: Vec<DeterministicDrainPlanRound>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeterministicDrainPlanAssumptions {
    pub no_new_obligations: bool,
    pub outbound_batches_succeed: bool,
    pub settlements_complete: bool,
    pub rebalances_succeed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeterministicPlannerStateSummary {
    pub total_tracked_inventory: u64,
    pub open_obligations: usize,
    pub outbound_obligations: usize,
    pub pending_settlement_obligations: usize,
    pub done_obligations: usize,
    pub active_outbound_batches: usize,
    pub active_pending_settlements: usize,
    pub active_rebalance: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct DeterministicInFlightWork {
    pub outbound_batches: Vec<DeterministicOutboundBatchPlan>,
    pub pending_settlements: Vec<DeterministicPendingSettlementPlan>,
    pub rebalance: Option<DeterministicRebalancePlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeterministicDrainPlanRound {
    pub round: usize,
    pub outbound_batches: Vec<DeterministicOutboundBatchPlan>,
    pub rebalance: Option<DeterministicRebalancePlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeterministicOutboundBatchPlan {
    pub payout_lot: Lot,
    pub settlement_lot: Lot,
    pub obligations: Vec<DeterministicObligationPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeterministicPendingSettlementPlan {
    pub settlement_lot: Lot,
    pub obligations: Vec<DeterministicObligationPlan>,
    pub completed_swap_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeterministicObligationPlan {
    pub obligation_id: u64,
    pub swap_id: Uuid,
    pub quote_id: Uuid,
    pub created_at: u64,
    pub user_deposit_confirmed_at: DateTime<Utc>,
    pub payout_lot: Lot,
    pub settlement_lot: Lot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeterministicRebalancePlan {
    pub source_lot: Lot,
    pub destination_lot: Lot,
}

impl PlannerService {
    pub async fn new(
        payment_manager: Arc<PaymentManager>,
        bitcoin_wallet: Arc<dyn Wallet>,
        evm_wallet: Arc<dyn Wallet>,
        policy: PlannerPolicy,
        evm_chain: ChainType,
        cancellation_token: CancellationToken,
    ) -> Result<Self> {
        let btc_inventory = balance_to_u64(
            "bitcoin inventory",
            bitcoin_wallet
                .balance(&TokenIdentifier::Native)
                .await
                .context(WalletSnafu)?
                .total_balance,
        )?;
        let cbbtc_inventory = balance_to_u64(
            "cbbtc inventory",
            evm_wallet
                .balance(&cbbtc_token())
                .await
                .context(WalletSnafu)?
                .total_balance,
        )?;

        Ok(Self {
            inner: Arc::new(PlannerServiceInner {
                runtime: Mutex::new(PlannerRuntime {
                    planner_state: PlannerState::new(btc_inventory, cbbtc_inventory),
                    next_obligation_id: 1,
                    swap_to_obligation_id: BTreeMap::new(),
                    obligation_contexts: BTreeMap::new(),
                    batch_txid_to_asset: BTreeMap::new(),
                    pending_settlement_keys_by_obligation: BTreeMap::new(),
                    settlement_completion_progress: BTreeMap::new(),
                }),
                payment_manager,
                bitcoin_wallet,
                evm_wallet,
                policy,
                evm_chain,
                rebalance_dispatcher: OnceLock::new(),
                cancellation_token,
            }),
        })
    }

    pub fn attach_rebalance_dispatcher(&self, dispatcher: Arc<dyn RebalanceDispatcher>) {
        let _ = self.inner.rebalance_dispatcher.set(dispatcher);
    }

    pub async fn sync_from_authoritative_snapshot(
        &self,
        obligations: Vec<ActiveObligation>,
        deposit_repository: &DepositRepository,
    ) -> Result<()> {
        let existing_rebalance = {
            let runtime = self.inner.runtime.lock().await;
            if !runtime.swap_to_obligation_id.is_empty()
                || !runtime.planner_state.outbound_batches.is_empty()
                || !runtime.planner_state.pending_settlements.is_empty()
            {
                info!(
                    snapshot_obligation_count = obligations.len(),
                    "skipping authoritative obligation snapshot because planner already has active state"
                );
                return Ok(());
            }
            runtime.planner_state.rebalance
        };

        let payment_repository = self.inner.payment_manager.payment_repository();
        let created_batches = payment_repository
            .get_batches_by_status(BatchStatus::Created)
            .await
            .context(PaymentRepositorySnafu)?;
        let confirmed_batches = payment_repository
            .get_batches_by_status(BatchStatus::Confirmed)
            .await
            .context(PaymentRepositorySnafu)?;
        let mut swap_ids = obligations
            .iter()
            .map(|obligation| obligation.swap_id)
            .collect::<BTreeSet<_>>();
        for batch in created_batches.iter().chain(confirmed_batches.iter()) {
            swap_ids.extend(batch.swap_ids.iter().copied());
        }
        let deposits = deposit_repository
            .list_deposits_by_swap_ids(&swap_ids.into_iter().collect::<Vec<_>>())
            .await
            .context(DepositRepositorySnafu)?;

        let btc_inventory = balance_to_u64(
            "snapshot bitcoin inventory",
            self.inner
                .bitcoin_wallet
                .balance(&TokenIdentifier::Native)
                .await
                .context(WalletSnafu)?
                .total_balance,
        )?;
        let cbbtc_inventory = balance_to_u64(
            "snapshot cbbtc inventory",
            self.inner
                .evm_wallet
                .balance(&cbbtc_token())
                .await
                .context(WalletSnafu)?
                .total_balance,
        )?;

        let (runtime, dispatch_plan) = build_runtime_from_authoritative_snapshot(
            obligations,
            created_batches,
            confirmed_batches,
            deposits,
            btc_inventory,
            cbbtc_inventory,
            existing_rebalance,
            self.inner.evm_chain,
            self.inner.policy,
        )?;

        let mut current = self.inner.runtime.lock().await;
        *current = runtime;
        drop(current);

        self.dispatch_plan(dispatch_plan);
        Ok(())
    }

    pub async fn deterministic_drain_plan(
        &self,
        max_rounds: usize,
    ) -> Result<DeterministicDrainPlan> {
        let (planner_state, obligation_contexts, settlement_completion_progress) = {
            let runtime = self.inner.runtime.lock().await;
            (
                runtime.planner_state.clone(),
                runtime.obligation_contexts.clone(),
                runtime.settlement_completion_progress.clone(),
            )
        };

        build_deterministic_drain_plan(
            &planner_state,
            &obligation_contexts,
            &settlement_completion_progress,
            self.inner.evm_chain,
            self.inner.policy,
            max_rounds,
        )
    }

    pub async fn register_confirmed_obligation(
        &self,
        swap_id: Uuid,
        quote_id: Uuid,
        user_destination_address: String,
        mm_nonce: [u8; 16],
        expected_lot: Lot,
        settlement_lot: Lot,
        user_deposit_confirmed_at: chrono::DateTime<chrono::Utc>,
        protocol_fee: U256,
        locked_liquidity: Option<LockedLiquidity>,
    ) -> Result<()> {
        let payout_asset =
            planner_asset_for_currency(&expected_lot.currency, self.inner.evm_chain)?;
        let settlement_asset =
            planner_asset_for_currency(&settlement_lot.currency, self.inner.evm_chain)?;
        if payout_asset.other() != settlement_asset {
            return UnsupportedCurrencySnafu {
                currency: settlement_lot.currency.clone(),
            }
            .fail();
        }

        let payout_amount = balance_to_u64("expected_lot.amount", expected_lot.amount)?;
        let settlement_amount = balance_to_u64("settlement_lot.amount", settlement_lot.amount)?;

        if let Some(locked_liquidity) = locked_liquidity.as_ref() {
            let locked_asset =
                planner_asset_for_currency(&locked_liquidity.from, self.inner.evm_chain)?;
            let locked_amount = balance_to_u64("locked_liquidity.amount", locked_liquidity.amount)?;
            if locked_asset != settlement_asset || locked_amount != settlement_amount {
                return ConfirmedDepositMismatchSnafu {
                    swap_id,
                    reason: format!(
                        "lock asset/amount {:?}/{} differed from confirmed settlement {:?}/{}",
                        locked_asset, locked_amount, settlement_asset, settlement_amount
                    ),
                }
                .fail();
            }
        }

        if settlement_amount < payout_amount {
            return LossyObligationSnafu {
                swap_id,
                payout_amount,
                settlement_amount,
            }
            .fail();
        }

        let created_at = timestamp_to_u64(
            "user_deposit_confirmed_at",
            user_deposit_confirmed_at.timestamp_micros(),
        )?;

        let dispatch_plan = {
            let mut runtime = self.inner.runtime.lock().await;
            if runtime.swap_to_obligation_id.contains_key(&swap_id) {
                return Ok(());
            }

            let obligation_id = runtime.next_obligation_id;
            runtime.next_obligation_id = runtime.next_obligation_id.saturating_add(1);

            let before = runtime.planner_state.total_tracked_inventory();
            runtime.planner_state.insert_obligation(super::Obligation {
                id: obligation_id,
                payout_asset,
                payout_amount,
                settlement_asset,
                settlement_amount,
                created_at,
                state: ObligationState::Open,
            });
            runtime.swap_to_obligation_id.insert(swap_id, obligation_id);
            runtime.obligation_contexts.insert(
                obligation_id,
                ObligationContext {
                    swap_id,
                    quote_id,
                    expected_lot,
                    user_destination_address,
                    mm_nonce,
                    protocol_fee,
                    user_deposit_confirmed_at,
                },
            );
            let actions = runtime.planner_state.commit_next_actions(self.inner.policy);
            let after = runtime.planner_state.total_tracked_inventory();
            ensure_inventory_non_decreasing("register_confirmed_obligation", before, after)?;

            dispatch_plan_from_actions(&runtime, actions)?
        };

        self.dispatch_plan(dispatch_plan);
        Ok(())
    }

    pub async fn record_swap_complete(&self, swap_id: Uuid, settlement_lot: &Lot) -> Result<()> {
        let settlement_asset =
            planner_asset_for_currency(&settlement_lot.currency, self.inner.evm_chain)?;
        let settlement_amount = balance_to_u64("swap_complete.amount", settlement_lot.amount)?;

        let dispatch_plan = {
            let mut runtime = self.inner.runtime.lock().await;
            let obligation_id = *runtime
                .swap_to_obligation_id
                .get(&swap_id)
                .context(MissingObligationSnafu { swap_id })?;
            let obligation = runtime
                .planner_state
                .obligations
                .get(&obligation_id)
                .context(MissingContextSnafu { obligation_id })?;
            let obligation_state = obligation.state;
            let expected_settlement_asset = obligation.settlement_asset;
            let expected_settlement_amount = obligation.settlement_amount;
            let _ = obligation;

            if obligation_state == ObligationState::Done {
                return Ok(());
            }

            if obligation_state != ObligationState::PendingSettlement {
                return SettlementMismatchSnafu {
                    swap_id,
                    reason: format!(
                        "expected pending_settlement state, found {:?}",
                        obligation_state
                    ),
                }
                .fail();
            }

            if expected_settlement_asset != settlement_asset {
                return SettlementMismatchSnafu {
                    swap_id,
                    reason: format!(
                        "expected settlement asset {:?}, got {:?}",
                        expected_settlement_asset, settlement_asset
                    ),
                }
                .fail();
            }

            if expected_settlement_amount != settlement_amount {
                return SettlementMismatchSnafu {
                    swap_id,
                    reason: format!(
                        "expected settlement amount {}, got {}",
                        expected_settlement_amount, settlement_amount
                    ),
                }
                .fail();
            }

            let settlement_key = runtime
                .pending_settlement_keys_by_obligation
                .get(&obligation_id)
                .cloned()
                .context(MissingPendingSettlementSnafu { obligation_id })?;
            let completed_ids = runtime
                .settlement_completion_progress
                .entry(settlement_key.clone())
                .or_default();
            completed_ids.insert(obligation_id);

            if *completed_ids != settlement_key {
                DispatchPlan {
                    outbound_batches: Vec::new(),
                    rebalance: None,
                }
            } else {
                let before = runtime.planner_state.total_tracked_inventory();
                runtime
                    .planner_state
                    .complete_pending_settlement(&settlement_key)
                    .expect("pending settlement must exist");
                runtime
                    .settlement_completion_progress
                    .remove(&settlement_key);
                for completed_id in &settlement_key {
                    runtime
                        .pending_settlement_keys_by_obligation
                        .remove(completed_id);
                }
                let actions = runtime.planner_state.commit_next_actions(self.inner.policy);
                let after = runtime.planner_state.total_tracked_inventory();
                ensure_inventory_non_decreasing("complete_pending_settlement", before, after)?;
                dispatch_plan_from_actions(&runtime, actions)?
            }
        };

        self.dispatch_plan(dispatch_plan);
        Ok(())
    }

    async fn record_batch_broadcast(&self, txid: String, asset: PlannerAsset) {
        let mut runtime = self.inner.runtime.lock().await;
        if let Some(previous_asset) = runtime.batch_txid_to_asset.insert(txid.clone(), asset) {
            warn!(
                txid = %txid,
                previous_asset = ?previous_asset,
                new_asset = ?asset,
                "planner batch txid mapping was replaced"
            );
        }
    }

    async fn mark_outbound_batch_confirmed(&self, txid: &str) -> Result<()> {
        let dispatch_plan = {
            let mut runtime = self.inner.runtime.lock().await;
            let Some(asset) = runtime.batch_txid_to_asset.remove(txid) else {
                warn!(txid = %txid, "ignoring confirmed batch that is not tracked by planner");
                return Ok(());
            };

            let before = runtime.planner_state.total_tracked_inventory();
            let pending_settlement = match runtime
                .planner_state
                .mark_outbound_batch_succeeded(asset)
            {
                Some(pending_settlement) => pending_settlement,
                None => {
                    warn!(txid = %txid, asset = ?asset, "planner had no active outbound batch for confirmed tx");
                    return Ok(());
                }
            };

            let settlement_key = pending_settlement.obligation_ids.clone();
            runtime
                .settlement_completion_progress
                .insert(settlement_key.clone(), BTreeSet::new());
            for obligation_id in &settlement_key {
                runtime
                    .pending_settlement_keys_by_obligation
                    .insert(*obligation_id, settlement_key.clone());
            }

            let actions = runtime.planner_state.commit_next_actions(self.inner.policy);
            let after = runtime.planner_state.total_tracked_inventory();
            ensure_inventory_non_decreasing("mark_outbound_batch_confirmed", before, after)?;
            dispatch_plan_from_actions(&runtime, actions)?
        };

        self.dispatch_plan(dispatch_plan);
        Ok(())
    }

    async fn mark_outbound_batch_cancelled(&self, txid: &str) -> Result<()> {
        let dispatch_plan = {
            let mut runtime = self.inner.runtime.lock().await;
            let Some(asset) = runtime.batch_txid_to_asset.remove(txid) else {
                warn!(txid = %txid, "ignoring cancelled batch that is not tracked by planner");
                return Ok(());
            };

            let before = runtime.planner_state.total_tracked_inventory();
            if runtime
                .planner_state
                .mark_outbound_batch_failed(asset)
                .is_none()
            {
                warn!(txid = %txid, asset = ?asset, "planner had no active outbound batch for cancelled tx");
                return Ok(());
            }
            let actions = runtime.planner_state.commit_next_actions(self.inner.policy);
            let after = runtime.planner_state.total_tracked_inventory();
            ensure_inventory_non_decreasing("mark_outbound_batch_cancelled", before, after)?;
            dispatch_plan_from_actions(&runtime, actions)?
        };

        self.dispatch_plan(dispatch_plan);
        Ok(())
    }

    async fn mark_outbound_batch_submission_failed(&self, asset: PlannerAsset) -> Result<()> {
        let dispatch_plan = {
            let mut runtime = self.inner.runtime.lock().await;
            let before = runtime.planner_state.total_tracked_inventory();
            if runtime
                .planner_state
                .mark_outbound_batch_failed(asset)
                .is_none()
            {
                return Ok(());
            }
            let actions = runtime.planner_state.commit_next_actions(self.inner.policy);
            let after = runtime.planner_state.total_tracked_inventory();
            ensure_inventory_non_decreasing(
                "mark_outbound_batch_submission_failed",
                before,
                after,
            )?;
            dispatch_plan_from_actions(&runtime, actions)?
        };

        self.dispatch_plan(dispatch_plan);
        Ok(())
    }

    pub async fn restore_active_rebalance(&self, rebalance: Rebalance) -> Result<()> {
        let mut runtime = self.inner.runtime.lock().await;
        if runtime.planner_state.rebalance == Some(rebalance) {
            return Ok(());
        }
        if runtime.planner_state.rebalance.is_some() {
            return RebalanceAlreadyActiveSnafu.fail();
        }
        let before = runtime.planner_state.total_tracked_inventory();
        let available_before = runtime
            .planner_state
            .available_inventory(rebalance.src_asset);
        if available_before < rebalance.amount {
            return RebalanceRestoreOverdrawSnafu.fail();
        }
        runtime
            .planner_state
            .set_available_inventory(rebalance.src_asset, available_before - rebalance.amount);
        let previous = runtime.planner_state.rebalance.replace(rebalance);
        assert!(previous.is_none(), "rebalance already active");
        let after = runtime.planner_state.total_tracked_inventory();
        ensure_inventory_non_decreasing("restore_active_rebalance", before, after)?;
        Ok(())
    }

    pub async fn rebalance_succeeded(&self) -> Result<()> {
        let dispatch_plan = {
            let mut runtime = self.inner.runtime.lock().await;
            let before = runtime.planner_state.total_tracked_inventory();
            if runtime.planner_state.mark_rebalance_succeeded().is_none() {
                return Ok(());
            }
            let actions = runtime.planner_state.commit_next_actions(self.inner.policy);
            let after = runtime.planner_state.total_tracked_inventory();
            ensure_inventory_non_decreasing("mark_rebalance_succeeded", before, after)?;
            dispatch_plan_from_actions(&runtime, actions)?
        };

        self.dispatch_plan(dispatch_plan);
        Ok(())
    }

    pub async fn rebalance_failed(&self) -> Result<()> {
        let dispatch_plan = {
            let mut runtime = self.inner.runtime.lock().await;
            let before = runtime.planner_state.total_tracked_inventory();
            if runtime.planner_state.mark_rebalance_failed().is_none() {
                return Ok(());
            }
            let actions = runtime.planner_state.commit_next_actions(self.inner.policy);
            let after = runtime.planner_state.total_tracked_inventory();
            ensure_inventory_non_decreasing("mark_rebalance_failed", before, after)?;
            dispatch_plan_from_actions(&runtime, actions)?
        };

        self.dispatch_plan(dispatch_plan);
        Ok(())
    }

    fn dispatch_plan(&self, dispatch_plan: DispatchPlan) {
        for outbound_batch in dispatch_plan.outbound_batches {
            let service = self.clone();
            tokio::spawn(async move {
                if service.inner.cancellation_token.is_cancelled() {
                    return;
                }

                match service
                    .inner
                    .payment_manager
                    .execute_planned_batch(outbound_batch.payments.clone())
                    .await
                {
                    Ok(txid) => {
                        info!(
                            txid = %txid,
                            asset = ?outbound_batch.asset,
                            "planner broadcast outbound batch"
                        );
                        service
                            .record_batch_broadcast(txid, outbound_batch.asset)
                            .await;
                    }
                    Err(error) => {
                        let swap_ids = outbound_batch
                            .payments
                            .iter()
                            .map(|payment| payment.swap_id)
                            .collect::<Vec<_>>();
                        error!(
                            asset = ?outbound_batch.asset,
                            error = %error,
                            "planner outbound batch execution failed"
                        );
                        if service
                            .inner
                            .payment_manager
                            .has_durable_batch_for_swaps(&swap_ids)
                            .await
                        {
                            error!(
                                asset = ?outbound_batch.asset,
                                ?swap_ids,
                                "planner detected a durable local batch after outbound submission failure; preserving planner state and forcing restart for recovery"
                            );
                            service.inner.cancellation_token.cancel();
                            return;
                        }
                        if let Err(mark_error) = service
                            .mark_outbound_batch_submission_failed(outbound_batch.asset)
                            .await
                        {
                            error!(
                                asset = ?outbound_batch.asset,
                                error = %mark_error,
                                "planner failed to unwind outbound batch after submission error"
                            );
                        }
                    }
                }
            });
        }

        if let Some(rebalance) = dispatch_plan.rebalance {
            let dispatcher = self
                .inner
                .rebalance_dispatcher
                .get()
                .expect("planner rebalance dispatcher must be attached before use")
                .clone();
            let service = self.clone();
            tokio::spawn(async move {
                if let Err(error) = dispatcher.dispatch_rebalance(rebalance).await {
                    error!(
                        error = %error,
                        ?rebalance,
                        "planner failed to dispatch durable rebalance"
                    );
                    if let Err(mark_error) = service.rebalance_failed().await {
                        error!(
                            error = %mark_error,
                            ?rebalance,
                            "planner failed to unwind rebalance after dispatch error"
                        );
                    }
                }
            });
        }
    }
}

#[async_trait]
impl BatchStatusListener for PlannerService {
    async fn batch_confirmed(&self, batch: &StoredBatch) {
        if let Err(error) = self.mark_outbound_batch_confirmed(&batch.txid).await {
            error!(txid = %batch.txid, error = %error, "planner failed to apply confirmed batch");
        }
    }

    async fn batch_cancelled(&self, batch: &StoredBatch) {
        if let Err(error) = self.mark_outbound_batch_cancelled(&batch.txid).await {
            error!(txid = %batch.txid, error = %error, "planner failed to apply cancelled batch");
        }
    }
}

#[async_trait]
impl RebalanceStateSink for PlannerService {
    async fn restore_active_rebalance(
        &self,
        rebalance: Rebalance,
    ) -> crate::rebalancer_actor::Result<()> {
        PlannerService::restore_active_rebalance(self, rebalance)
            .await
            .map_err(
                |error| crate::rebalancer_actor::RebalanceActorError::PlannerCallback {
                    reason: error.to_string(),
                    loc: location!(),
                },
            )
    }

    async fn rebalance_succeeded(&self) -> crate::rebalancer_actor::Result<()> {
        PlannerService::rebalance_succeeded(self)
            .await
            .map_err(
                |error| crate::rebalancer_actor::RebalanceActorError::PlannerCallback {
                    reason: error.to_string(),
                    loc: location!(),
                },
            )
    }

    async fn rebalance_failed(&self) -> crate::rebalancer_actor::Result<()> {
        PlannerService::rebalance_failed(self)
            .await
            .map_err(
                |error| crate::rebalancer_actor::RebalanceActorError::PlannerCallback {
                    reason: error.to_string(),
                    loc: location!(),
                },
            )
    }
}

fn build_runtime_from_authoritative_snapshot(
    authoritative_obligations: Vec<ActiveObligation>,
    created_batches: Vec<StoredBatch>,
    confirmed_batches: Vec<StoredBatch>,
    deposits: Vec<AssociatedSwapDeposit>,
    btc_inventory: u64,
    cbbtc_inventory: u64,
    active_rebalance: Option<Rebalance>,
    evm_chain: ChainType,
    policy: PlannerPolicy,
) -> Result<(PlannerRuntime, DispatchPlan)> {
    let mut runtime = PlannerRuntime {
        planner_state: PlannerState::new(0, 0),
        next_obligation_id: 1,
        swap_to_obligation_id: BTreeMap::new(),
        obligation_contexts: BTreeMap::new(),
        batch_txid_to_asset: BTreeMap::new(),
        pending_settlement_keys_by_obligation: BTreeMap::new(),
        settlement_completion_progress: BTreeMap::new(),
    };

    let mut authoritative_obligations = authoritative_obligations;
    authoritative_obligations
        .sort_by_key(|obligation| (obligation.user_deposit_confirmed_at, obligation.swap_id));

    let mut obligation_by_swap = BTreeMap::new();
    for obligation in authoritative_obligations {
        let payout_asset =
            planner_asset_for_currency(&obligation.expected_lot.currency, evm_chain)?;
        let settlement_asset =
            planner_asset_for_currency(&obligation.settlement_lot.currency, evm_chain)?;
        if payout_asset.other() != settlement_asset {
            return UnsupportedCurrencySnafu {
                currency: obligation.settlement_lot.currency.clone(),
            }
            .fail();
        }

        let payout_amount = balance_to_u64("expected_lot.amount", obligation.expected_lot.amount)?;
        let settlement_amount =
            balance_to_u64("settlement_lot.amount", obligation.settlement_lot.amount)?;
        if settlement_amount < payout_amount {
            return LossyObligationSnafu {
                swap_id: obligation.swap_id,
                payout_amount,
                settlement_amount,
            }
            .fail();
        }

        let obligation_id = runtime.next_obligation_id;
        runtime.next_obligation_id = runtime.next_obligation_id.saturating_add(1);
        let created_at = timestamp_to_u64(
            "user_deposit_confirmed_at",
            obligation.user_deposit_confirmed_at.timestamp_micros(),
        )?;

        let snapshot = SnapshotObligation {
            obligation: super::Obligation {
                id: obligation_id,
                payout_asset,
                payout_amount,
                settlement_asset,
                settlement_amount,
                created_at,
                state: ObligationState::Open,
            },
            status: obligation.status,
            swap_id: obligation.swap_id,
            quote_id: obligation.quote_id,
            user_destination_address: obligation.user_destination_address.clone(),
            mm_nonce: obligation.mm_nonce,
            protocol_fee: obligation.protocol_fee,
            user_deposit_confirmed_at: obligation.user_deposit_confirmed_at,
        };

        runtime
            .planner_state
            .insert_obligation(snapshot.obligation.clone());
        runtime
            .swap_to_obligation_id
            .insert(snapshot.swap_id, snapshot.obligation.id);
        runtime.obligation_contexts.insert(
            snapshot.obligation.id,
            ObligationContext {
                swap_id: snapshot.swap_id,
                quote_id: snapshot.quote_id,
                expected_lot: obligation.expected_lot,
                user_destination_address: snapshot.user_destination_address.clone(),
                mm_nonce: snapshot.mm_nonce,
                protocol_fee: snapshot.protocol_fee,
                user_deposit_confirmed_at: snapshot.user_deposit_confirmed_at,
            },
        );
        obligation_by_swap.insert(snapshot.swap_id, snapshot);
    }

    let completed_settlements = aggregate_snapshot_deposits(deposits, evm_chain)?;

    let mut created_batch_by_swap = BTreeMap::new();
    let mut confirmed_batch_by_swap = BTreeMap::new();
    for batch in &created_batches {
        track_snapshot_batch_membership(&mut created_batch_by_swap, batch)?;
    }
    for batch in &confirmed_batches {
        track_snapshot_batch_membership(&mut confirmed_batch_by_swap, batch)?;
    }
    for swap_id in created_batch_by_swap.keys() {
        if confirmed_batch_by_swap.contains_key(swap_id) {
            let txid = created_batch_by_swap
                .get(swap_id)
                .expect("created batch mapping must exist")
                .txid
                .clone();
            return InconsistentSnapshotBatchSnafu {
                txid,
                field: "status overlap",
            }
            .fail();
        }
    }

    let mut assigned = BTreeSet::new();
    let mut withheld_completed_pending_inventory =
        BTreeMap::from([(PlannerAsset::AssetA, 0u64), (PlannerAsset::AssetB, 0u64)]);

    for batch in &created_batches {
        let tracked = tracked_snapshot_batch_members(batch, &obligation_by_swap);
        if tracked.len() != batch.swap_ids.len() {
            let missing_swap_id = batch
                .swap_ids
                .iter()
                .find(|swap_id| !obligation_by_swap.contains_key(*swap_id))
                .copied()
                .expect("missing swap must exist");
            return MissingSnapshotObligationSnafu {
                txid: batch.txid.clone(),
                swap_id: missing_swap_id,
            }
            .fail();
        }

        let outbound_batch = snapshot_outbound_batch(batch, &tracked, evm_chain)?;
        runtime
            .planner_state
            .outbound_batches
            .insert(outbound_batch.payout_asset, outbound_batch.clone());
        runtime
            .batch_txid_to_asset
            .insert(batch.txid.clone(), outbound_batch.payout_asset);
        for obligation_id in &outbound_batch.obligation_ids {
            let obligation = runtime
                .planner_state
                .obligations
                .get_mut(obligation_id)
                .expect("snapshot obligation must exist");
            obligation.state = ObligationState::Outbound;
            assigned.insert(*obligation_id);
        }
    }

    for batch in &confirmed_batches {
        let batch_asset = planner_asset_for_batch_chain(batch.chain, evm_chain)?;
        let mut member_ids = BTreeSet::new();
        let mut completed_ids = BTreeSet::new();
        let mut payout_amount = 0u64;
        let mut settlement_amount = 0u64;
        let mut settlement_asset: Option<PlannerAsset> = None;

        for swap_id in &batch.swap_ids {
            let completed = completed_settlements.get(swap_id).copied();

            let obligation_id = if let Some(snapshot) = obligation_by_swap.get(swap_id) {
                if let Some(completed) = completed {
                    ensure_snapshot_deposit_matches(snapshot, completed)?;
                }
                payout_amount = payout_amount.saturating_add(snapshot.obligation.payout_amount);
                settlement_amount =
                    settlement_amount.saturating_add(snapshot.obligation.settlement_amount);
                settlement_asset.get_or_insert(snapshot.obligation.settlement_asset);
                if settlement_asset != Some(snapshot.obligation.settlement_asset) {
                    return InconsistentSnapshotBatchSnafu {
                        txid: batch.txid.clone(),
                        field: "settlement asset",
                    }
                    .fail();
                }
                snapshot.obligation.id
            } else if let Some(completed) = completed {
                let obligation_id = runtime.next_obligation_id;
                runtime.next_obligation_id = runtime.next_obligation_id.saturating_add(1);
                payout_amount = payout_amount.saturating_add(0);
                settlement_amount = settlement_amount.saturating_add(completed.amount);
                settlement_asset.get_or_insert(completed.asset);
                if settlement_asset != Some(completed.asset) {
                    return InconsistentSnapshotBatchSnafu {
                        txid: batch.txid.clone(),
                        field: "settlement asset",
                    }
                    .fail();
                }

                let created_at =
                    timestamp_to_u64("batch.created_at", batch.created_at.timestamp_micros())?;
                runtime.planner_state.insert_obligation(super::Obligation {
                    id: obligation_id,
                    payout_asset: batch_asset,
                    payout_amount: 0,
                    settlement_asset: completed.asset,
                    settlement_amount: completed.amount,
                    created_at,
                    state: ObligationState::Open,
                });
                runtime
                    .swap_to_obligation_id
                    .insert(*swap_id, obligation_id);
                obligation_id
            } else {
                return MissingSnapshotObligationSnafu {
                    txid: batch.txid.clone(),
                    swap_id: *swap_id,
                }
                .fail();
            };

            member_ids.insert(obligation_id);
            if completed.is_some() {
                completed_ids.insert(obligation_id);
            }
        }

        if completed_ids == member_ids {
            for obligation_id in member_ids {
                let obligation = runtime
                    .planner_state
                    .obligations
                    .get_mut(&obligation_id)
                    .expect("snapshot obligation must exist");
                obligation.state = ObligationState::Done;
            }
            continue;
        }

        let pending = super::PendingSettlement {
            asset: settlement_asset.expect("confirmed batch must have settlement asset"),
            amount: settlement_amount,
            obligation_ids: member_ids.clone(),
        };
        runtime
            .planner_state
            .pending_settlements
            .insert(member_ids.clone(), pending.clone());
        runtime
            .settlement_completion_progress
            .insert(member_ids.clone(), completed_ids.clone());
        for obligation_id in &member_ids {
            let obligation = runtime
                .planner_state
                .obligations
                .get_mut(obligation_id)
                .expect("snapshot obligation must exist");
            obligation.state = ObligationState::PendingSettlement;
            runtime
                .pending_settlement_keys_by_obligation
                .insert(*obligation_id, member_ids.clone());
            assigned.insert(*obligation_id);
        }

        let withheld = completed_ids
            .iter()
            .map(|obligation_id| {
                runtime
                    .planner_state
                    .obligations
                    .get(obligation_id)
                    .expect("snapshot obligation must exist")
                    .settlement_amount
            })
            .sum::<u64>();
        *withheld_completed_pending_inventory
            .get_mut(&pending.asset)
            .expect("asset bucket must exist") += withheld;
    }

    for snapshot in obligation_by_swap.values() {
        if assigned.contains(&snapshot.obligation.id) {
            continue;
        }

        let obligation_id = snapshot.obligation.id;
        let obligation = runtime
            .planner_state
            .obligations
            .get_mut(&obligation_id)
            .expect("snapshot obligation must exist");

        match snapshot.status {
            otc_models::SwapStatus::WaitingMMDepositInitiated => {
                obligation.state = ObligationState::Open;
            }
            otc_models::SwapStatus::WaitingMMDepositConfirmed => {
                let settlement_key = BTreeSet::from([obligation_id]);
                let pending = super::PendingSettlement {
                    asset: obligation.settlement_asset,
                    amount: obligation.settlement_amount,
                    obligation_ids: settlement_key.clone(),
                };
                obligation.state = ObligationState::PendingSettlement;
                runtime
                    .planner_state
                    .pending_settlements
                    .insert(settlement_key.clone(), pending);
                runtime
                    .pending_settlement_keys_by_obligation
                    .insert(obligation_id, settlement_key);
            }
            other => {
                return InconsistentSnapshotBatchSnafu {
                    txid: format!("swap:{}", snapshot.swap_id),
                    field: match other {
                        otc_models::SwapStatus::Settled => "settled obligation in active snapshot",
                        otc_models::SwapStatus::RefundingUser => {
                            "refunding obligation in active snapshot"
                        }
                        otc_models::SwapStatus::Failed => "failed obligation in active snapshot",
                        otc_models::SwapStatus::WaitingUserDepositInitiated => {
                            "pre-deposit obligation in active snapshot"
                        }
                        otc_models::SwapStatus::WaitingUserDepositConfirmed => {
                            "unconfirmed obligation in active snapshot"
                        }
                        _ => "unexpected status in active snapshot",
                    },
                }
                .fail();
            }
        }
    }

    let mut available_inventory = BTreeMap::from([
        (PlannerAsset::AssetA, btc_inventory),
        (PlannerAsset::AssetB, cbbtc_inventory),
    ]);
    for batch in runtime.planner_state.outbound_batches.values() {
        checked_sub_inventory(
            &mut available_inventory,
            batch.payout_asset,
            batch.payout_amount,
        )?;
    }
    for (asset, withheld) in &withheld_completed_pending_inventory {
        if *withheld > 0 {
            checked_sub_inventory(&mut available_inventory, *asset, *withheld)?;
        }
    }
    if let Some(rebalance) = active_rebalance {
        checked_sub_inventory(
            &mut available_inventory,
            rebalance.src_asset,
            rebalance.amount,
        )?;
        runtime.planner_state.rebalance = Some(rebalance);
    }
    for asset in PlannerAsset::ALL {
        runtime.planner_state.set_available_inventory(
            asset,
            available_inventory.get(&asset).copied().unwrap_or_default(),
        );
    }

    let actions = runtime.planner_state.commit_next_actions(policy);
    let dispatch_plan = dispatch_plan_from_actions(&runtime, actions)?;
    Ok((runtime, dispatch_plan))
}

fn aggregate_snapshot_deposits(
    deposits: Vec<AssociatedSwapDeposit>,
    evm_chain: ChainType,
) -> Result<BTreeMap<Uuid, CompletedSettlement>> {
    let mut settlements = BTreeMap::new();

    for deposit in deposits {
        let asset = planner_asset_for_currency(&deposit.holdings.currency, evm_chain)?;
        let amount = balance_to_u64("snapshot settlement deposit", deposit.holdings.amount)?;
        let entry = settlements
            .entry(deposit.associated_swap_id)
            .or_insert(CompletedSettlement { asset, amount: 0 });
        if entry.asset != asset {
            return SnapshotDepositMismatchSnafu {
                swap_id: deposit.associated_swap_id,
                reason: "multiple settlement assets observed".to_string(),
            }
            .fail();
        }
        entry.amount = entry.amount.saturating_add(amount);
    }

    Ok(settlements)
}

fn ensure_snapshot_deposit_matches(
    snapshot: &SnapshotObligation,
    completed: CompletedSettlement,
) -> Result<()> {
    if snapshot.obligation.settlement_asset != completed.asset {
        return SnapshotDepositMismatchSnafu {
            swap_id: snapshot.swap_id,
            reason: format!(
                "expected asset {:?}, got {:?}",
                snapshot.obligation.settlement_asset, completed.asset
            ),
        }
        .fail();
    }
    if snapshot.obligation.settlement_amount != completed.amount {
        return SnapshotDepositMismatchSnafu {
            swap_id: snapshot.swap_id,
            reason: format!(
                "expected amount {}, got {}",
                snapshot.obligation.settlement_amount, completed.amount
            ),
        }
        .fail();
    }
    Ok(())
}

fn tracked_snapshot_batch_members<'a>(
    batch: &'a StoredBatch,
    obligation_by_swap: &'a BTreeMap<Uuid, SnapshotObligation>,
) -> Vec<&'a SnapshotObligation> {
    batch
        .swap_ids
        .iter()
        .filter_map(|swap_id| obligation_by_swap.get(swap_id))
        .collect()
}

fn snapshot_outbound_batch(
    batch: &StoredBatch,
    tracked: &[&SnapshotObligation],
    evm_chain: ChainType,
) -> Result<super::OutboundBatch> {
    let payout_asset = tracked
        .first()
        .map(|snapshot| snapshot.obligation.payout_asset)
        .expect("tracked batch members cannot be empty");
    let settlement_asset = tracked
        .first()
        .map(|snapshot| snapshot.obligation.settlement_asset)
        .expect("tracked batch members cannot be empty");
    let batch_asset = planner_asset_for_batch_chain(batch.chain, evm_chain)?;

    if payout_asset != batch_asset {
        return InconsistentSnapshotBatchSnafu {
            txid: batch.txid.clone(),
            field: "payout asset",
        }
        .fail();
    }
    if tracked
        .iter()
        .any(|snapshot| snapshot.obligation.payout_asset != payout_asset)
    {
        return InconsistentSnapshotBatchSnafu {
            txid: batch.txid.clone(),
            field: "payout asset",
        }
        .fail();
    }
    if tracked
        .iter()
        .any(|snapshot| snapshot.obligation.settlement_asset != settlement_asset)
    {
        return InconsistentSnapshotBatchSnafu {
            txid: batch.txid.clone(),
            field: "settlement asset",
        }
        .fail();
    }

    Ok(super::OutboundBatch {
        payout_asset,
        payout_amount: tracked
            .iter()
            .map(|snapshot| snapshot.obligation.payout_amount)
            .sum(),
        settlement_asset,
        settlement_amount: tracked
            .iter()
            .map(|snapshot| snapshot.obligation.settlement_amount)
            .sum(),
        obligation_ids: tracked
            .iter()
            .map(|snapshot| snapshot.obligation.id)
            .collect(),
    })
}

fn track_snapshot_batch_membership<'a>(
    map: &mut BTreeMap<Uuid, &'a StoredBatch>,
    batch: &'a StoredBatch,
) -> Result<()> {
    for swap_id in &batch.swap_ids {
        if let Some(previous) = map.insert(*swap_id, batch) {
            return InconsistentSnapshotBatchSnafu {
                txid: previous.txid.clone(),
                field: "swap membership",
            }
            .fail();
        }
    }
    Ok(())
}

fn planner_asset_for_batch_chain(chain: ChainType, evm_chain: ChainType) -> Result<PlannerAsset> {
    if chain == ChainType::Bitcoin {
        return Ok(PlannerAsset::AssetA);
    }
    if chain == evm_chain {
        return Ok(PlannerAsset::AssetB);
    }

    UnsupportedCurrencySnafu {
        currency: Currency {
            chain,
            token: TokenIdentifier::Native,
            decimals: 8,
        },
    }
    .fail()
}

fn checked_sub_inventory(
    available_inventory: &mut BTreeMap<PlannerAsset, u64>,
    asset: PlannerAsset,
    reserved: u64,
) -> Result<()> {
    let wallet_total = available_inventory.get(&asset).copied().unwrap_or_default();
    let Some(remaining) = wallet_total.checked_sub(reserved) else {
        return SnapshotInventoryUnderflowSnafu {
            asset,
            wallet_total,
            reserved,
        }
        .fail();
    };
    available_inventory.insert(asset, remaining);
    Ok(())
}

fn dispatch_plan_from_actions(
    runtime: &PlannerRuntime,
    actions: PlannerActions,
) -> Result<DispatchPlan> {
    let outbound_batches = actions
        .outbound_batches
        .into_iter()
        .map(|batch| {
            let mut ordered_obligations = batch
                .obligation_ids
                .iter()
                .map(|obligation_id| {
                    let obligation = runtime
                        .planner_state
                        .obligations
                        .get(obligation_id)
                        .context(MissingContextSnafu {
                            obligation_id: *obligation_id,
                        })?;
                    let context = runtime.obligation_contexts.get(obligation_id).context(
                        MissingContextSnafu {
                            obligation_id: *obligation_id,
                        },
                    )?;
                    Ok((obligation.created_at, obligation.id, context.clone()))
                })
                .collect::<Result<Vec<_>>>()?;
            ordered_obligations
                .sort_by_key(|(created_at, obligation_id, _)| (*created_at, *obligation_id));

            let payments = ordered_obligations
                .into_iter()
                .map(|(_, _, context)| MarketMakerQueuedPayment {
                    swap_id: context.swap_id,
                    quote_id: context.quote_id,
                    lot: context.expected_lot.clone(),
                    destination_address: context.user_destination_address.clone(),
                    mm_nonce: context.mm_nonce,
                    user_deposit_confirmed_at: Some(context.user_deposit_confirmed_at),
                    protocol_fee: context.protocol_fee,
                })
                .collect::<Vec<_>>();

            Ok(ScheduledOutboundBatch {
                asset: batch.payout_asset,
                payments,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(DispatchPlan {
        outbound_batches,
        rebalance: actions.rebalance,
    })
}

fn build_deterministic_drain_plan(
    planner_state: &PlannerState,
    obligation_contexts: &BTreeMap<u64, ObligationContext>,
    settlement_completion_progress: &BTreeMap<BTreeSet<u64>, BTreeSet<u64>>,
    evm_chain: ChainType,
    policy: PlannerPolicy,
    max_rounds: usize,
) -> Result<DeterministicDrainPlan> {
    let current_state = summarize_planner_state(planner_state);
    let current_in_flight = DeterministicInFlightWork {
        outbound_batches: planner_state
            .outbound_batches
            .values()
            .map(|batch| {
                build_outbound_batch_plan(
                    batch,
                    &planner_state.obligations,
                    obligation_contexts,
                    evm_chain,
                )
            })
            .collect::<Result<Vec<_>>>()?,
        pending_settlements: planner_state
            .pending_settlements
            .iter()
            .map(|(key, settlement)| {
                build_pending_settlement_plan(
                    settlement,
                    settlement_completion_progress.get(key),
                    &planner_state.obligations,
                    obligation_contexts,
                    evm_chain,
                )
            })
            .collect::<Result<Vec<_>>>()?,
        rebalance: planner_state
            .rebalance
            .map(|rebalance| build_rebalance_plan(rebalance, evm_chain)),
    };

    let mut simulated_state = planner_state.clone();
    let mut rounds = Vec::new();

    for round in 0..max_rounds {
        progress_active_work_assuming_success(&mut simulated_state);

        let actions = simulated_state.commit_next_actions(policy);
        if actions.is_empty() {
            break;
        }

        rounds.push(DeterministicDrainPlanRound {
            round: round + 1,
            outbound_batches: actions
                .outbound_batches
                .iter()
                .map(|batch| {
                    build_outbound_batch_plan(
                        batch,
                        &simulated_state.obligations,
                        obligation_contexts,
                        evm_chain,
                    )
                })
                .collect::<Result<Vec<_>>>()?,
            rebalance: actions
                .rebalance
                .map(|rebalance| build_rebalance_plan(rebalance, evm_chain)),
        });
    }

    // Resolve the final planned round before exposing the projected terminal state.
    progress_active_work_assuming_success(&mut simulated_state);

    let projected_final_state = summarize_planner_state(&simulated_state);
    let fully_drained = projected_final_state.open_obligations == 0
        && projected_final_state.outbound_obligations == 0
        && projected_final_state.pending_settlement_obligations == 0
        && !projected_final_state.active_rebalance;
    let truncated = !fully_drained && rounds.len() == max_rounds;

    Ok(DeterministicDrainPlan {
        assumptions: DeterministicDrainPlanAssumptions {
            no_new_obligations: true,
            outbound_batches_succeed: true,
            settlements_complete: true,
            rebalances_succeed: true,
        },
        max_rounds,
        truncated,
        fully_drained,
        current_state,
        current_in_flight,
        projected_final_state,
        rounds,
    })
}

fn summarize_planner_state(planner_state: &PlannerState) -> DeterministicPlannerStateSummary {
    let mut open_obligations = 0usize;
    let mut outbound_obligations = 0usize;
    let mut pending_settlement_obligations = 0usize;
    let mut done_obligations = 0usize;

    for obligation in planner_state.obligations.values() {
        match obligation.state {
            ObligationState::Open => open_obligations += 1,
            ObligationState::Outbound => outbound_obligations += 1,
            ObligationState::PendingSettlement => pending_settlement_obligations += 1,
            ObligationState::Done => done_obligations += 1,
        }
    }

    DeterministicPlannerStateSummary {
        total_tracked_inventory: planner_state.total_tracked_inventory(),
        open_obligations,
        outbound_obligations,
        pending_settlement_obligations,
        done_obligations,
        active_outbound_batches: planner_state.outbound_batches.len(),
        active_pending_settlements: planner_state.pending_settlements.len(),
        active_rebalance: planner_state.rebalance.is_some(),
    }
}

fn progress_active_work_assuming_success(planner_state: &mut PlannerState) {
    let outbound_assets = planner_state
        .outbound_batches
        .keys()
        .copied()
        .collect::<Vec<_>>();
    for asset in outbound_assets {
        planner_state.mark_outbound_batch_succeeded(asset);
    }

    let settlement_keys = planner_state
        .pending_settlements
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    for settlement_key in settlement_keys {
        planner_state.complete_pending_settlement(&settlement_key);
    }

    if planner_state.rebalance.is_some() {
        planner_state.mark_rebalance_succeeded();
    }
}

fn build_outbound_batch_plan(
    batch: &super::OutboundBatch,
    obligations: &BTreeMap<u64, super::Obligation>,
    obligation_contexts: &BTreeMap<u64, ObligationContext>,
    evm_chain: ChainType,
) -> Result<DeterministicOutboundBatchPlan> {
    Ok(DeterministicOutboundBatchPlan {
        payout_lot: planner_lot_for_asset(batch.payout_asset, batch.payout_amount, evm_chain),
        settlement_lot: planner_lot_for_asset(
            batch.settlement_asset,
            batch.settlement_amount,
            evm_chain,
        ),
        obligations: batch
            .obligation_ids
            .iter()
            .map(|obligation_id| {
                build_obligation_plan(*obligation_id, obligations, obligation_contexts, evm_chain)
            })
            .collect::<Result<Vec<_>>>()?,
    })
}

fn build_pending_settlement_plan(
    settlement: &super::PendingSettlement,
    completed_obligation_ids: Option<&BTreeSet<u64>>,
    obligations: &BTreeMap<u64, super::Obligation>,
    obligation_contexts: &BTreeMap<u64, ObligationContext>,
    evm_chain: ChainType,
) -> Result<DeterministicPendingSettlementPlan> {
    let completed_swap_ids = completed_obligation_ids
        .into_iter()
        .flat_map(|ids| ids.iter().copied())
        .map(|obligation_id| {
            obligation_contexts
                .get(&obligation_id)
                .context(MissingContextSnafu { obligation_id })
                .map(|context| context.swap_id)
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(DeterministicPendingSettlementPlan {
        settlement_lot: planner_lot_for_asset(settlement.asset, settlement.amount, evm_chain),
        obligations: settlement
            .obligation_ids
            .iter()
            .map(|obligation_id| {
                build_obligation_plan(*obligation_id, obligations, obligation_contexts, evm_chain)
            })
            .collect::<Result<Vec<_>>>()?,
        completed_swap_ids,
    })
}

fn build_obligation_plan(
    obligation_id: u64,
    obligations: &BTreeMap<u64, super::Obligation>,
    obligation_contexts: &BTreeMap<u64, ObligationContext>,
    evm_chain: ChainType,
) -> Result<DeterministicObligationPlan> {
    let obligation = obligations
        .get(&obligation_id)
        .context(MissingRuntimeObligationSnafu { obligation_id })?;
    let context = obligation_contexts
        .get(&obligation_id)
        .context(MissingContextSnafu { obligation_id })?;

    Ok(DeterministicObligationPlan {
        obligation_id,
        swap_id: context.swap_id,
        quote_id: context.quote_id,
        created_at: obligation.created_at,
        user_deposit_confirmed_at: context.user_deposit_confirmed_at,
        payout_lot: planner_lot_for_asset(
            obligation.payout_asset,
            obligation.payout_amount,
            evm_chain,
        ),
        settlement_lot: planner_lot_for_asset(
            obligation.settlement_asset,
            obligation.settlement_amount,
            evm_chain,
        ),
    })
}

fn build_rebalance_plan(rebalance: Rebalance, evm_chain: ChainType) -> DeterministicRebalancePlan {
    DeterministicRebalancePlan {
        source_lot: planner_lot_for_asset(rebalance.src_asset, rebalance.amount, evm_chain),
        destination_lot: planner_lot_for_asset(rebalance.dst_asset, rebalance.amount, evm_chain),
    }
}

fn planner_lot_for_asset(asset: PlannerAsset, amount: u64, evm_chain: ChainType) -> Lot {
    Lot {
        currency: planner_currency_for_asset(asset, evm_chain),
        amount: U256::from(amount),
    }
}

fn planner_currency_for_asset(asset: PlannerAsset, evm_chain: ChainType) -> Currency {
    match asset {
        PlannerAsset::AssetA => Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        },
        PlannerAsset::AssetB => Currency {
            chain: evm_chain,
            token: cbbtc_token(),
            decimals: 8,
        },
    }
}

fn planner_asset_for_currency(currency: &Currency, evm_chain: ChainType) -> Result<PlannerAsset> {
    if currency.chain == ChainType::Bitcoin && currency.token == TokenIdentifier::Native {
        return Ok(PlannerAsset::AssetA);
    }

    if currency.chain == evm_chain && currency.token.normalize() == cbbtc_token() {
        return Ok(PlannerAsset::AssetB);
    }

    UnsupportedCurrencySnafu {
        currency: currency.clone(),
    }
    .fail()
}

fn cbbtc_token() -> TokenIdentifier {
    TokenIdentifier::address(CB_BTC_CONTRACT_ADDRESS)
}

fn balance_to_u64(field: &'static str, value: U256) -> Result<u64> {
    if value > U256::from(u64::MAX) {
        return InvalidAmountSnafu { field, value }.fail();
    }

    Ok(value.to::<u64>())
}

fn timestamp_to_u64(field: &'static str, value: i64) -> Result<u64> {
    if value < 0 {
        return InvalidTimestampSnafu { field, value }.fail();
    }

    Ok(value as u64)
}

fn ensure_inventory_non_decreasing(
    transition: &'static str,
    before: u64,
    after: u64,
) -> Result<()> {
    if after < before {
        return InventoryDecreasedSnafu {
            transition,
            before,
            after,
        }
        .fail();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn btc_lot(amount: u64) -> Lot {
        Lot {
            currency: Currency {
                chain: ChainType::Bitcoin,
                token: TokenIdentifier::Native,
                decimals: 8,
            },
            amount: U256::from(amount),
        }
    }

    fn cbbtc_lot(amount: u64) -> Lot {
        Lot {
            currency: Currency {
                chain: ChainType::Base,
                token: cbbtc_token(),
                decimals: 8,
            },
            amount: U256::from(amount),
        }
    }

    #[test]
    fn build_runtime_from_authoritative_snapshot_rebuilds_outbound_and_partial_pending() {
        let outbound_swap = Uuid::now_v7();
        let pending_active_swap = Uuid::now_v7();
        let pending_completed_swap = Uuid::now_v7();

        let obligations = vec![
            ActiveObligation {
                swap_id: outbound_swap,
                quote_id: Uuid::now_v7(),
                status: otc_models::SwapStatus::WaitingMMDepositInitiated,
                user_destination_address: "dest-outbound".to_string(),
                mm_nonce: [1u8; 16],
                expected_lot: btc_lot(10),
                settlement_lot: cbbtc_lot(15),
                protocol_fee: U256::from(1u64),
                user_deposit_confirmed_at: utc::now(),
            },
            ActiveObligation {
                swap_id: pending_active_swap,
                quote_id: Uuid::now_v7(),
                status: otc_models::SwapStatus::WaitingMMDepositConfirmed,
                user_destination_address: "dest-pending-active".to_string(),
                mm_nonce: [2u8; 16],
                expected_lot: btc_lot(12),
                settlement_lot: cbbtc_lot(18),
                protocol_fee: U256::from(1u64),
                user_deposit_confirmed_at: utc::now(),
            },
        ];

        let created_batches = vec![StoredBatch {
            txid: "created-batch".to_string(),
            chain: ChainType::Bitcoin,
            swap_ids: vec![outbound_swap],
            batch_nonce_digest: [9u8; 32],
            aggregated_fee_sats: 0,
            fee_settlement_txid: None,
            created_at: utc::now(),
            status: BatchStatus::Created,
        }];
        let confirmed_batches = vec![StoredBatch {
            txid: "confirmed-batch".to_string(),
            chain: ChainType::Bitcoin,
            swap_ids: vec![pending_active_swap, pending_completed_swap],
            batch_nonce_digest: [8u8; 32],
            aggregated_fee_sats: 0,
            fee_settlement_txid: None,
            created_at: utc::now(),
            status: BatchStatus::Confirmed,
        }];
        let deposits = vec![AssociatedSwapDeposit {
            associated_swap_id: pending_completed_swap,
            holdings: cbbtc_lot(22),
        }];

        let (runtime, _dispatch_plan) = build_runtime_from_authoritative_snapshot(
            obligations,
            created_batches,
            confirmed_batches,
            deposits,
            50,
            40,
            Some(Rebalance {
                src_asset: PlannerAsset::AssetB,
                dst_asset: PlannerAsset::AssetA,
                amount: 5,
            }),
            ChainType::Base,
            PlannerPolicy::new(7_000, 5_000),
        )
        .unwrap();

        assert_eq!(runtime.planner_state.outbound_batches.len(), 1);
        assert_eq!(runtime.planner_state.pending_settlements.len(), 1);
        assert_eq!(runtime.obligation_contexts.len(), 2);
        assert_eq!(
            runtime
                .planner_state
                .available_inventory(PlannerAsset::AssetA),
            40
        );
        assert_eq!(
            runtime
                .planner_state
                .available_inventory(PlannerAsset::AssetB),
            13
        );
        assert_eq!(
            runtime.batch_txid_to_asset.get("created-batch"),
            Some(&PlannerAsset::AssetA)
        );

        let (pending_key, completed_ids) = runtime
            .settlement_completion_progress
            .iter()
            .next()
            .map(|(key, completed)| (key.clone(), completed.clone()))
            .unwrap();
        assert_eq!(pending_key.len(), 2);
        assert_eq!(completed_ids.len(), 1);
    }

    #[test]
    fn deterministic_drain_plan_projects_current_in_flight_work_and_future_rounds() {
        let mut planner_state = PlannerState::new(30, 10);

        let obligation_one = super::super::Obligation {
            id: 1,
            payout_asset: PlannerAsset::AssetA,
            payout_amount: 10,
            settlement_asset: PlannerAsset::AssetB,
            settlement_amount: 12,
            created_at: 1,
            state: ObligationState::Open,
        };
        let obligation_two = super::super::Obligation {
            id: 2,
            payout_asset: PlannerAsset::AssetA,
            payout_amount: 8,
            settlement_asset: PlannerAsset::AssetB,
            settlement_amount: 10,
            created_at: 2,
            state: ObligationState::Open,
        };
        let obligation_three = super::super::Obligation {
            id: 3,
            payout_asset: PlannerAsset::AssetB,
            payout_amount: 6,
            settlement_asset: PlannerAsset::AssetA,
            settlement_amount: 7,
            created_at: 3,
            state: ObligationState::Outbound,
        };
        let obligation_four = super::super::Obligation {
            id: 4,
            payout_asset: PlannerAsset::AssetA,
            payout_amount: 5,
            settlement_asset: PlannerAsset::AssetB,
            settlement_amount: 6,
            created_at: 4,
            state: ObligationState::PendingSettlement,
        };

        planner_state.insert_obligation(obligation_one.clone());
        planner_state.insert_obligation(obligation_two.clone());
        planner_state.insert_obligation(obligation_three.clone());
        planner_state.insert_obligation(obligation_four.clone());

        planner_state.outbound_batches.insert(
            PlannerAsset::AssetB,
            super::super::OutboundBatch {
                payout_asset: PlannerAsset::AssetB,
                payout_amount: 6,
                settlement_asset: PlannerAsset::AssetA,
                settlement_amount: 7,
                obligation_ids: BTreeSet::from([3]),
            },
        );
        planner_state.pending_settlements.insert(
            BTreeSet::from([4]),
            super::super::PendingSettlement {
                asset: PlannerAsset::AssetB,
                amount: 6,
                obligation_ids: BTreeSet::from([4]),
            },
        );
        planner_state.rebalance = Some(Rebalance {
            src_asset: PlannerAsset::AssetB,
            dst_asset: PlannerAsset::AssetA,
            amount: 4,
        });

        let context = |swap_id, quote_id, expected_lot, timestamp| ObligationContext {
            swap_id,
            quote_id,
            expected_lot,
            user_destination_address: "dest".to_string(),
            mm_nonce: [0u8; 16],
            protocol_fee: U256::ZERO,
            user_deposit_confirmed_at: timestamp,
        };

        let swap_one = Uuid::now_v7();
        let swap_two = Uuid::now_v7();
        let swap_three = Uuid::now_v7();
        let swap_four = Uuid::now_v7();

        let obligation_contexts = BTreeMap::from([
            (
                1,
                context(swap_one, Uuid::now_v7(), btc_lot(10), utc::now()),
            ),
            (2, context(swap_two, Uuid::now_v7(), btc_lot(8), utc::now())),
            (
                3,
                context(swap_three, Uuid::now_v7(), cbbtc_lot(6), utc::now()),
            ),
            (
                4,
                context(swap_four, Uuid::now_v7(), btc_lot(5), utc::now()),
            ),
        ]);

        let plan = build_deterministic_drain_plan(
            &planner_state,
            &obligation_contexts,
            &BTreeMap::new(),
            ChainType::Base,
            PlannerPolicy::new(7_000, 5_000),
            8,
        )
        .unwrap();

        assert_eq!(plan.current_state.open_obligations, 2);
        assert_eq!(plan.current_state.outbound_obligations, 1);
        assert_eq!(plan.current_state.pending_settlement_obligations, 1);
        assert!(plan.current_state.active_rebalance);
        assert_eq!(plan.current_in_flight.outbound_batches.len(), 1);
        assert_eq!(plan.current_in_flight.pending_settlements.len(), 1);

        assert_eq!(plan.rounds.len(), 1);
        assert_eq!(plan.rounds[0].round, 1);
        assert_eq!(plan.rounds[0].outbound_batches.len(), 1);
        assert!(plan.rounds[0].rebalance.is_none());
        let planned_swap_ids = plan.rounds[0].outbound_batches[0]
            .obligations
            .iter()
            .map(|obligation| obligation.swap_id)
            .collect::<Vec<_>>();
        assert_eq!(planned_swap_ids, vec![swap_one, swap_two]);

        assert!(plan.fully_drained);
        assert!(!plan.truncated);
        assert_eq!(plan.projected_final_state.open_obligations, 0);
        assert_eq!(plan.projected_final_state.outbound_obligations, 0);
        assert_eq!(plan.projected_final_state.pending_settlement_obligations, 0);
        assert!(!plan.projected_final_state.active_rebalance);
    }

    #[test]
    fn build_runtime_from_authoritative_snapshot_does_not_reopen_confirmed_obligation_without_local_batch(
    ) {
        let pending_swap = Uuid::now_v7();
        let obligations = vec![ActiveObligation {
            swap_id: pending_swap,
            quote_id: Uuid::now_v7(),
            status: otc_models::SwapStatus::WaitingMMDepositConfirmed,
            user_destination_address: "dest-pending".to_string(),
            mm_nonce: [4u8; 16],
            expected_lot: btc_lot(9),
            settlement_lot: cbbtc_lot(14),
            protocol_fee: U256::from(1u64),
            user_deposit_confirmed_at: utc::now(),
        }];

        let (runtime, dispatch_plan) = build_runtime_from_authoritative_snapshot(
            obligations,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            50,
            40,
            None,
            ChainType::Base,
            PlannerPolicy::new(7_000, 5_000),
        )
        .unwrap();

        assert!(dispatch_plan.outbound_batches.is_empty());

        let obligation_id = *runtime
            .swap_to_obligation_id
            .get(&pending_swap)
            .expect("snapshot obligation should be tracked");
        let obligation = runtime
            .planner_state
            .obligations
            .get(&obligation_id)
            .expect("snapshot obligation should exist");
        assert_eq!(obligation.state, ObligationState::PendingSettlement);
        assert_eq!(runtime.planner_state.pending_settlements.len(), 1);
        assert_eq!(
            runtime
                .pending_settlement_keys_by_obligation
                .get(&obligation_id),
            Some(&BTreeSet::from([obligation_id]))
        );
    }
}
