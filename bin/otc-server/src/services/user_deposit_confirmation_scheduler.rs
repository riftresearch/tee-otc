use crate::db::Database;
use crate::error::OtcServerError;
use crate::services::mm_registry::MMRegistry;
use dashmap::DashMap;
use metrics::{counter, gauge, histogram};
use otc_chains::ChainRegistry;
use otc_models::{ChainType, Lot, Swap, SwapStatus, TxStatus};
use serde::{Deserialize, Serialize};
use snafu::{location, prelude::*, Location};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tokio::time;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

const RECOVERY_SWEEP_INTERVAL: Duration = Duration::from_secs(30 * 60);
#[cfg(not(feature = "test-market-makers"))]
const BITCOIN_HEAD_POLL_INTERVAL: Duration = Duration::from_secs(30);
#[cfg(feature = "test-market-makers")]
const BITCOIN_HEAD_POLL_INTERVAL: Duration = Duration::from_secs(1);
#[cfg(not(feature = "test-market-makers"))]
const ETHEREUM_HEAD_POLL_INTERVAL: Duration = Duration::from_secs(12);
#[cfg(feature = "test-market-makers")]
const ETHEREUM_HEAD_POLL_INTERVAL: Duration = Duration::from_secs(1);
#[cfg(not(feature = "test-market-makers"))]
const BASE_HEAD_POLL_INTERVAL: Duration = Duration::from_secs(5);
#[cfg(feature = "test-market-makers")]
const BASE_HEAD_POLL_INTERVAL: Duration = Duration::from_secs(1);
pub const USER_DEPOSIT_CONFIRMATION_DURATION_METRIC: &str =
    "otc_user_deposit_confirmation_refresh_duration_seconds";
pub const ACTIVE_USER_DEPOSIT_CONFIRMATIONS_METRIC: &str =
    "otc_user_deposit_confirmations_scheduled";
pub const USER_DEPOSIT_CONFIRMATIONS_ADVANCED_TOTAL_METRIC: &str =
    "otc_user_deposit_confirmations_advanced_total";
pub const USER_DEPOSIT_REARMS_TOTAL_METRIC: &str = "otc_user_deposit_rearms_total";
pub const USER_DEPOSIT_HEAD_FETCH_FAILURES_TOTAL_METRIC: &str =
    "otc_user_deposit_head_fetch_failures_total";

#[derive(Debug, Snafu)]
pub enum SchedulerError {
    #[snafu(display("Database error: {source} at {loc}"))]
    Database {
        source: OtcServerError,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Chain operation error: {source} at {loc}"))]
    ChainOperation {
        source: otc_chains::Error,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Invalid scheduler state for swap {swap_id}: {reason} at {loc}"))]
    InvalidState {
        swap_id: Uuid,
        reason: String,
        #[snafu(implicit)]
        loc: Location,
    },
}

type SchedulerResult<T> = Result<T, SchedulerError>;

#[derive(Clone, Debug)]
struct ScheduledUserDeposit {
    chain: ChainType,
    next_recheck_height: u64,
}

#[derive(Clone, Debug)]
struct ChainHead {
    height: u64,
    hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UserDepositChainHeadStatus {
    pub height: u64,
    pub hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UserDepositSchedulerStatus {
    pub last_pass: u64,
    pub scheduled_swaps: usize,
    pub latest_heads: BTreeMap<String, UserDepositChainHeadStatus>,
}

pub struct UserDepositConfirmationScheduler {
    db: Database,
    chain_registry: Arc<ChainRegistry>,
    mm_registry: Arc<MMRegistry>,
    scheduled_swaps: DashMap<Uuid, ScheduledUserDeposit>,
    latest_heads: DashMap<ChainType, ChainHead>,
    inflight_swaps: DashMap<Uuid, ()>,
    last_confirmation_pass: AtomicU64,
}

impl UserDepositConfirmationScheduler {
    #[must_use]
    pub fn new(
        db: Database,
        chain_registry: Arc<ChainRegistry>,
        mm_registry: Arc<MMRegistry>,
    ) -> Self {
        Self {
            db,
            chain_registry,
            mm_registry,
            scheduled_swaps: DashMap::new(),
            latest_heads: DashMap::new(),
            inflight_swaps: DashMap::new(),
            last_confirmation_pass: AtomicU64::new(0),
        }
    }

    pub fn status_snapshot(&self) -> UserDepositSchedulerStatus {
        let latest_heads = self
            .latest_heads
            .iter()
            .map(|entry| {
                (
                    entry.key().to_db_string().to_string(),
                    UserDepositChainHeadStatus {
                        height: entry.value().height,
                        hash: entry.value().hash.clone(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();

        UserDepositSchedulerStatus {
            last_pass: self.last_confirmation_pass.load(Ordering::Relaxed),
            scheduled_swaps: self.scheduled_swaps.len(),
            latest_heads,
        }
    }

    pub async fn initialize(&self) {
        info!("Starting user deposit confirmation scheduler");
        if let Err(err) = self.reconcile_from_db().await {
            error!("Failed initial user-deposit scheduler reconcile: {}", err);
        }
    }

    pub fn spawn_tasks(self: &Arc<Self>, join_set: &mut JoinSet<crate::BackgroundTaskResult>) {
        for chain in self.chain_registry.supported_chains() {
            let scheduler = Arc::clone(self);
            join_set.spawn(async move {
                scheduler.chain_head_loop(chain).await;
                Ok(())
            });
        }

        join_set.spawn({
            let scheduler = Arc::clone(self);
            async move {
                scheduler.recovery_sweep_loop().await;
                Ok(())
            }
        });
    }

    pub async fn refresh_swap(&self, swap_id: Uuid) -> SchedulerResult<()> {
        if self.inflight_swaps.insert(swap_id, ()).is_some() {
            return Ok(());
        }

        let started = Instant::now();
        let result = self.refresh_swap_inner(swap_id).await;
        self.inflight_swaps.remove(&swap_id);
        histogram!(
            USER_DEPOSIT_CONFIRMATION_DURATION_METRIC,
            "outcome" => if result.is_ok() { "ok" } else { "error" },
        )
        .record(started.elapsed().as_secs_f64());
        self.note_scheduler_pass();
        result
    }

    async fn refresh_swap_inner(&self, swap_id: Uuid) -> SchedulerResult<()> {
        let swap = match self.db.swaps().get(swap_id).await {
            Ok(swap) => swap,
            Err(OtcServerError::NotFound) => {
                self.scheduled_swaps.remove(&swap_id);
                return Ok(());
            }
            Err(err) => {
                return Err(SchedulerError::Database {
                    source: err,
                    loc: location!(),
                })
            }
        };

        self.reconcile_loaded_swap(&swap).await
    }

    async fn reconcile_from_db(&self) -> SchedulerResult<()> {
        let waiting_confirmed_swaps = self
            .db
            .swaps()
            .get_active_swaps_with_status(SwapStatus::WaitingUserDepositConfirmed)
            .await
            .context(DatabaseSnafu)?;

        let active_swap_ids = waiting_confirmed_swaps
            .iter()
            .map(|swap| swap.id)
            .collect::<std::collections::HashSet<_>>();

        for swap in waiting_confirmed_swaps {
            if let Err(err) = self.refresh_swap(swap.id).await {
                warn!(
                    swap_id = %swap.id,
                    "Failed to refresh user-deposit confirmation schedule during reconcile: {}",
                    err
                );
            }
        }

        let stale_ids = self
            .scheduled_swaps
            .iter()
            .filter_map(|entry| (!active_swap_ids.contains(entry.key())).then_some(*entry.key()))
            .collect::<Vec<_>>();

        for swap_id in stale_ids {
            self.unschedule_swap(&swap_id);
        }

        self.note_scheduler_pass();

        Ok(())
    }

    async fn reconcile_loaded_swap(&self, swap: &Swap) -> SchedulerResult<()> {
        if swap.status != SwapStatus::WaitingUserDepositConfirmed {
            self.unschedule_swap(&swap.id);
            return Ok(());
        }

        let user_deposit =
            swap.user_deposit_status
                .as_ref()
                .ok_or_else(|| SchedulerError::InvalidState {
                    swap_id: swap.id,
                    reason: "missing user_deposit_status".to_string(),
                    loc: location!(),
                })?;

        let chain = swap.quote.from.currency.chain;
        let chain_ops =
            self.chain_registry
                .get(&chain)
                .ok_or_else(|| SchedulerError::ChainOperation {
                    source: otc_chains::Error::ChainNotSupported {
                        chain: format!("{:?}", chain),
                    },
                    loc: location!(),
                })?;
        let required_confirmations = chain_ops.minimum_block_confirmations() as u64;

        match chain_ops
            .get_tx_status(&user_deposit.tx_hash)
            .await
            .context(ChainOperationSnafu)?
        {
            TxStatus::Confirmed(status) => {
                self.db
                    .swaps()
                    .update_user_confirmations(swap.id, status.confirmations as u32)
                    .await
                    .context(DatabaseSnafu)?;

                if status.confirmations >= required_confirmations {
                    self.unschedule_swap(&swap.id);
                    self.advance_confirmed_swap(swap).await?;
                } else {
                    let next_recheck_height = confirmation_target_height(
                        chain,
                        status.inclusion_height,
                        required_confirmations,
                    );
                    self.schedule_swap(
                        swap.id,
                        ScheduledUserDeposit {
                            chain,
                            next_recheck_height,
                        },
                    );
                }
            }
            TxStatus::Pending(status) => {
                self.db
                    .swaps()
                    .update_user_confirmations(swap.id, 0)
                    .await
                    .context(DatabaseSnafu)?;
                self.schedule_swap(
                    swap.id,
                    ScheduledUserDeposit {
                        chain,
                        next_recheck_height: status.current_height.saturating_add(1),
                    },
                );
            }
            TxStatus::NotFound => {
                self.unschedule_swap(&swap.id);
                match self.db.swaps().rearm_user_deposit_detection(swap.id).await {
                    Ok(()) => {
                        counter!(
                            USER_DEPOSIT_REARMS_TOTAL_METRIC,
                            "chain" => chain.to_db_string(),
                        )
                        .increment(1);
                    }
                    Err(OtcServerError::InvalidState { .. }) => {
                        debug!(
                            swap_id = %swap.id,
                            "Swap changed state before user-deposit rearm could be applied"
                        );
                    }
                    Err(err) => {
                        return Err(SchedulerError::Database {
                            source: err,
                            loc: location!(),
                        })
                    }
                }
            }
        }

        Ok(())
    }

    async fn advance_confirmed_swap(&self, swap: &Swap) -> SchedulerResult<()> {
        let user_deposit =
            swap.user_deposit_status
                .as_ref()
                .ok_or_else(|| SchedulerError::InvalidState {
                    swap_id: swap.id,
                    reason: "missing user_deposit_status".to_string(),
                    loc: location!(),
                })?;
        let realized = swap
            .realized
            .as_ref()
            .ok_or_else(|| SchedulerError::InvalidState {
                swap_id: swap.id,
                reason: "missing realized amounts".to_string(),
                loc: location!(),
            })?;

        let user_deposit_confirmed_at = match self.db.swaps().user_deposit_confirmed(swap.id).await
        {
            Ok(timestamp) => timestamp,
            Err(OtcServerError::InvalidState { .. }) => {
                debug!(
                    swap_id = %swap.id,
                    "Swap changed state before user-deposit confirmation could be applied"
                );
                return Ok(());
            }
            Err(err) => {
                return Err(SchedulerError::Database {
                    source: err,
                    loc: location!(),
                })
            }
        };

        let expected_lot = Lot {
            currency: swap.quote.to.currency.clone(),
            amount: realized.mm_output,
        };
        let protocol_fee = realized.protocol_fee;
        let mm_registry = self.mm_registry.clone();
        let market_maker_id = swap.market_maker_id;
        let swap_id = swap.id;
        let quote_id = swap.quote.id;
        let user_destination_address = swap.user_destination_address.clone();
        let mm_nonce = swap.mm_nonce;

        tokio::spawn(async move {
            let _ = mm_registry
                .notify_user_deposit_confirmed(
                    &market_maker_id,
                    &swap_id,
                    &quote_id,
                    &user_destination_address,
                    mm_nonce,
                    &expected_lot,
                    protocol_fee,
                    user_deposit_confirmed_at,
                )
                .await;
        });

        info!(
            swap_id = %swap.id,
            tx_hash = %user_deposit.tx_hash,
            "User deposit reached required confirmations via scheduler"
        );
        counter!(
            USER_DEPOSIT_CONFIRMATIONS_ADVANCED_TOTAL_METRIC,
            "chain" => swap.quote.from.currency.chain.to_db_string(),
        )
        .increment(1);

        Ok(())
    }

    async fn chain_head_loop(self: Arc<Self>, chain: ChainType) {
        let poll_interval = match self.chain_registry.get(&chain) {
            Some(_) => head_poll_interval(chain),
            None => Duration::from_secs(5),
        };
        let mut interval = time::interval(poll_interval);

        loop {
            interval.tick().await;

            match self.fetch_chain_head(chain).await {
                Ok(head) => {
                    let changed = self.latest_heads.get(&chain).map_or(true, |existing| {
                        existing.height != head.height || existing.hash != head.hash
                    });
                    if !changed {
                        continue;
                    }

                    self.latest_heads.insert(chain, head.clone());

                    if let Err(err) = self.process_due_for_chain(chain, head.height).await {
                        error!(
                            chain = ?chain,
                            "Failed processing due user-deposit confirmations at head {}: {}",
                            head.height,
                            err
                        );
                    }
                    self.note_scheduler_pass();
                }
                Err(err) => {
                    counter!(
                        USER_DEPOSIT_HEAD_FETCH_FAILURES_TOTAL_METRIC,
                        "chain" => chain.to_db_string(),
                    )
                    .increment(1);
                    warn!(chain = ?chain, "Failed to fetch chain head for user-deposit scheduler: {}", err);
                }
            }
        }
    }

    async fn recovery_sweep_loop(self: Arc<Self>) {
        loop {
            time::sleep(RECOVERY_SWEEP_INTERVAL).await;

            if let Err(err) = self.reconcile_from_db().await {
                error!(
                    "User-deposit confirmation scheduler recovery sweep failed: {}",
                    err
                );
            }
        }
    }

    async fn fetch_chain_head(&self, chain: ChainType) -> SchedulerResult<ChainHead> {
        let chain_ops =
            self.chain_registry
                .get(&chain)
                .ok_or_else(|| SchedulerError::ChainOperation {
                    source: otc_chains::Error::ChainNotSupported {
                        chain: format!("{:?}", chain),
                    },
                    loc: location!(),
                })?;

        let height = chain_ops
            .get_block_height()
            .await
            .context(ChainOperationSnafu)?;
        let hash = chain_ops
            .get_best_hash()
            .await
            .context(ChainOperationSnafu)?;

        Ok(ChainHead { height, hash })
    }

    async fn process_due_for_chain(
        &self,
        chain: ChainType,
        current_height: u64,
    ) -> SchedulerResult<()> {
        let due_swap_ids = self
            .scheduled_swaps
            .iter()
            .filter_map(|entry| {
                let scheduled = entry.value();
                if scheduled.chain == chain && scheduled.next_recheck_height <= current_height {
                    Some(*entry.key())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for swap_id in due_swap_ids {
            if let Err(err) = self.refresh_swap(swap_id).await {
                warn!(
                    swap_id = %swap_id,
                    chain = ?chain,
                    "Failed refreshing due user-deposit confirmation: {}",
                    err
                );
            }
        }

        Ok(())
    }

    fn schedule_swap(&self, swap_id: Uuid, scheduled: ScheduledUserDeposit) {
        self.scheduled_swaps.insert(swap_id, scheduled);
        self.update_scheduled_swaps_metric();
    }

    fn unschedule_swap(&self, swap_id: &Uuid) {
        self.scheduled_swaps.remove(swap_id);
        self.update_scheduled_swaps_metric();
    }

    fn update_scheduled_swaps_metric(&self) {
        gauge!(ACTIVE_USER_DEPOSIT_CONFIRMATIONS_METRIC).set(self.scheduled_swaps.len() as f64);
    }

    fn note_scheduler_pass(&self) {
        self.last_confirmation_pass
            .store(utc::now().timestamp() as u64, Ordering::Relaxed);
    }
}

fn confirmation_target_height(
    chain: ChainType,
    inclusion_height: u64,
    required_confirmations: u64,
) -> u64 {
    match chain {
        ChainType::Bitcoin => {
            inclusion_height.saturating_add(required_confirmations.saturating_sub(1))
        }
        ChainType::Ethereum | ChainType::Base => {
            inclusion_height.saturating_add(required_confirmations)
        }
    }
}

fn head_poll_interval(chain: ChainType) -> Duration {
    match chain {
        ChainType::Bitcoin => BITCOIN_HEAD_POLL_INTERVAL,
        ChainType::Ethereum => ETHEREUM_HEAD_POLL_INTERVAL,
        ChainType::Base => BASE_HEAD_POLL_INTERVAL,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        confirmation_target_height, head_poll_interval, BASE_HEAD_POLL_INTERVAL,
        BITCOIN_HEAD_POLL_INTERVAL, ETHEREUM_HEAD_POLL_INTERVAL,
    };
    use otc_models::ChainType;

    #[test]
    fn bitcoin_target_height_counts_inclusion_block() {
        assert_eq!(confirmation_target_height(ChainType::Bitcoin, 100, 2), 101);
        assert_eq!(confirmation_target_height(ChainType::Bitcoin, 100, 6), 105);
    }

    #[test]
    fn evm_target_height_requires_head_after_inclusion_gap() {
        assert_eq!(confirmation_target_height(ChainType::Ethereum, 100, 4), 104);
        assert_eq!(confirmation_target_height(ChainType::Base, 100, 2), 102);
    }

    #[test]
    fn head_poll_interval_is_bounded() {
        assert_eq!(head_poll_interval(ChainType::Base), BASE_HEAD_POLL_INTERVAL);
        assert_eq!(
            head_poll_interval(ChainType::Ethereum),
            ETHEREUM_HEAD_POLL_INTERVAL
        );
        assert_eq!(
            head_poll_interval(ChainType::Bitcoin),
            BITCOIN_HEAD_POLL_INTERVAL
        );
    }
}
