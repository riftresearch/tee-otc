use crate::db::Database;
use crate::error::OtcServerError;
use crate::{config::Settings, services::mm_registry};
use dashmap::DashMap;
use metrics::{counter, gauge, histogram};
use otc_chains::traits::{MarketMakerQueuedPayment, MarketMakerQueuedPaymentExt};
use otc_chains::ChainRegistry;
use otc_models::{ChainType, Lot, MMDepositStatus, Swap, SwapStatus};
use serde::{Deserialize, Serialize};
use snafu::{location, prelude::*, Location};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tokio::time;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

pub const MARKET_MAKER_BATCH_SETTLEMENT_DURATION_METRIC: &str =
    "otc_market_maker_batch_settlement_duration_seconds";
pub const ACTIVE_MARKET_MAKER_BATCHES_METRIC: &str = "otc_market_maker_batches_active";
pub const MARKET_MAKER_BATCH_SETTLEMENT_EVENTS_TOTAL_METRIC: &str =
    "otc_market_maker_batch_settlement_events_total";
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

type BatchKey = (ChainType, String);

#[derive(Debug, Snafu)]
pub enum SettlementError {
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

    #[snafu(display(
        "Market maker batch verification failed: {context} for tx {tx_hash} at {loc}"
    ))]
    InvalidMarketMakerBatch {
        context: String,
        tx_hash: String,
        #[snafu(implicit)]
        loc: Location,
    },
}

pub type SettlementResult<T> = Result<T, SettlementError>;

#[derive(Clone, Debug)]
struct ScheduledBatch {
    next_recheck_height: u64,
}

#[derive(Clone, Debug)]
struct ChainHead {
    height: u64,
    hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MarketMakerBatchChainHeadStatus {
    pub height: u64,
    pub hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MarketMakerBatchSettlementStatus {
    pub last_pass: u64,
    pub scheduled_batches: usize,
    pub latest_heads: BTreeMap<String, MarketMakerBatchChainHeadStatus>,
}

pub struct MarketMakerBatchSettlementService {
    db: Database,
    settings: Arc<Settings>,
    chain_registry: Arc<ChainRegistry>,
    mm_registry: Arc<mm_registry::MMRegistry>,
    scheduled_batches: DashMap<BatchKey, ScheduledBatch>,
    latest_heads: DashMap<ChainType, ChainHead>,
    inflight_batches: DashMap<BatchKey, ()>,
    last_batch_settlement_pass: AtomicU64,
}

impl MarketMakerBatchSettlementService {
    #[must_use]
    pub fn new(
        db: Database,
        settings: Arc<Settings>,
        chain_registry: Arc<ChainRegistry>,
        mm_registry: Arc<mm_registry::MMRegistry>,
    ) -> Self {
        Self {
            db,
            settings,
            chain_registry,
            mm_registry,
            scheduled_batches: DashMap::new(),
            latest_heads: DashMap::new(),
            inflight_batches: DashMap::new(),
            last_batch_settlement_pass: AtomicU64::new(0),
        }
    }

    pub fn last_batch_settlement_pass(&self) -> u64 {
        self.last_batch_settlement_pass.load(Ordering::Relaxed)
    }

    pub fn status_snapshot(&self) -> MarketMakerBatchSettlementStatus {
        let latest_heads = self
            .latest_heads
            .iter()
            .map(|entry| {
                (
                    entry.key().to_db_string().to_string(),
                    MarketMakerBatchChainHeadStatus {
                        height: entry.value().height,
                        hash: entry.value().hash.clone(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();

        MarketMakerBatchSettlementStatus {
            last_pass: self.last_batch_settlement_pass(),
            scheduled_batches: self.scheduled_batches.len(),
            latest_heads,
        }
    }

    pub async fn initialize(&self) {
        info!("Starting market maker batch settlement service");
        if let Err(err) = self.reconcile_from_db().await {
            error!(
                "Failed initial market maker batch settlement reconcile: {}",
                err
            );
        }
    }

    pub fn spawn_tasks(self: &Arc<Self>, join_set: &mut JoinSet<crate::BackgroundTaskResult>) {
        for chain in self.chain_registry.supported_chains() {
            let service = Arc::clone(self);
            join_set.spawn(async move {
                service.chain_head_loop(chain).await;
                Ok(())
            });
        }

        join_set.spawn({
            let service = Arc::clone(self);
            async move {
                service.recovery_sweep_loop().await;
                Ok(())
            }
        });
    }

    pub async fn track_batch_payment(
        &self,
        market_maker_id: Uuid,
        tx_hash: &String,
        swap_ids: Vec<Uuid>,
        mm_sent_batch_nonce_digest: [u8; 32],
    ) -> SettlementResult<()> {
        let swaps = self
            .db
            .swaps()
            .get_swaps(&swap_ids)
            .await
            .context(DatabaseSnafu)?;

        if swaps.is_empty() {
            return Ok(());
        }

        let dest_currency = swaps[0].quote.to.currency.clone();

        if self
            .db
            .batches()
            .get_batch(&dest_currency.chain, tx_hash)
            .await
            .context(DatabaseSnafu)?
            .is_some()
        {
            info!(
                market_maker_id = %market_maker_id,
                tx_hash = %tx_hash,
                "Batch payment already tracked; skipping duplicate notification"
            );
            return Ok(());
        }

        for swap in &swaps {
            if swap.market_maker_id != market_maker_id {
                return Err(SettlementError::InvalidMarketMakerBatch {
                    context: format!(
                        "Unauthorized market maker id {market_maker_id} attempted to track batch payment for swap id {}",
                        swap.id
                    ),
                    tx_hash: tx_hash.clone(),
                    loc: location!(),
                });
            }
            if swap.quote.to.currency != dest_currency {
                return Err(SettlementError::InvalidMarketMakerBatch {
                    context: format!(
                        "Destination currency mismatch processing swap id {}",
                        swap.id
                    ),
                    tx_hash: tx_hash.clone(),
                    loc: location!(),
                });
            }
            if swap.status != SwapStatus::WaitingMMDepositInitiated {
                return Err(SettlementError::InvalidMarketMakerBatch {
                    context: format!(
                        "Swap id {} is not in a state where a batch payment update is allowed",
                        swap.id
                    ),
                    tx_hash: tx_hash.clone(),
                    loc: location!(),
                });
            }
            if swap.realized.is_none() {
                return Err(SettlementError::InvalidMarketMakerBatch {
                    context: format!(
                        "Swap id {} does not have realized amounts computed",
                        swap.id
                    ),
                    tx_hash: tx_hash.clone(),
                    loc: location!(),
                });
            }
        }

        let market_maker_batch = match swaps
            .iter()
            .map(MarketMakerQueuedPayment::from)
            .collect::<Vec<_>>()
            .to_market_maker_batch()
        {
            Some(batch) => batch,
            None => {
                return Err(SettlementError::InvalidMarketMakerBatch {
                    context: "Market maker batch creation failed".to_string(),
                    tx_hash: tx_hash.clone(),
                    loc: location!(),
                })
            }
        };

        if market_maker_batch.payment_verification.batch_nonce_digest != mm_sent_batch_nonce_digest
        {
            return Err(SettlementError::InvalidMarketMakerBatch {
                context: "Batch nonce digest mismatch".to_string(),
                tx_hash: tx_hash.clone(),
                loc: location!(),
            });
        }

        let mm_deposit_status_map = swaps
            .iter()
            .map(|swap| {
                let realized = swap.realized.as_ref().expect("realized must exist");
                (
                    swap.id,
                    MMDepositStatus {
                        tx_hash: tx_hash.clone(),
                        amount: realized.mm_output,
                        deposit_detected_at: utc::now(),
                        confirmations: 0,
                        last_checked: utc::now(),
                    },
                )
            })
            .collect::<HashMap<Uuid, MMDepositStatus>>();

        let chain = dest_currency.chain;
        let batch_swap_ids = swaps.iter().map(|s| s.id).collect::<Vec<_>>();
        self.db
            .batches()
            .add_batch(
                chain,
                tx_hash.clone(),
                &market_maker_batch,
                batch_swap_ids,
                market_maker_id,
            )
            .await
            .context(DatabaseSnafu)?;

        self.db
            .swaps()
            .batch_mm_deposit_detected(mm_deposit_status_map)
            .await
            .context(DatabaseSnafu)?;

        let batch_key = (chain, tx_hash.clone());
        self.record_event(chain, "tracked");
        self.schedule_batch(batch_key.clone(), 0);

        if let Err(err) = self.refresh_batch(batch_key.clone()).await {
            warn!(
                chain = ?batch_key.0,
                tx_hash = %batch_key.1,
                "Failed immediate market maker batch settlement refresh after track: {}",
                err
            );
        }

        Ok(())
    }

    pub async fn refresh_batch(&self, batch_key: BatchKey) -> SettlementResult<()> {
        if self
            .inflight_batches
            .insert(batch_key.clone(), ())
            .is_some()
        {
            return Ok(());
        }

        let start = Instant::now();
        let result = self.refresh_batch_inner(&batch_key).await;
        self.inflight_batches.remove(&batch_key);

        histogram!(MARKET_MAKER_BATCH_SETTLEMENT_DURATION_METRIC)
            .record(start.elapsed().as_secs_f64());
        if result.is_err() {
            self.record_event(batch_key.0, "refresh_error");
        }
        self.note_batch_settlement_pass();

        result
    }

    async fn refresh_batch_inner(&self, batch_key: &BatchKey) -> SettlementResult<()> {
        let (chain, tx_hash) = batch_key;
        let batch = self
            .db
            .batches()
            .get_batch(chain, tx_hash)
            .await
            .context(DatabaseSnafu)?;

        let Some((_, swap_ids)) = batch else {
            self.unschedule_batch(batch_key);
            return Ok(());
        };

        let swaps = self
            .db
            .swaps()
            .get_swaps(&swap_ids)
            .await
            .context(DatabaseSnafu)?;

        let waiting_swaps = swaps
            .into_iter()
            .filter(|swap| {
                swap.status == SwapStatus::WaitingMMDepositConfirmed
                    && swap.quote.to.currency.chain == *chain
                    && swap
                        .mm_deposit_status
                        .as_ref()
                        .map(|status| status.tx_hash == *tx_hash)
                        .unwrap_or(false)
            })
            .collect::<Vec<_>>();

        if waiting_swaps.is_empty() {
            self.unschedule_batch(batch_key);
            return Ok(());
        }

        self.check_batch_for_mm_deposit_confirmation(batch_key, &waiting_swaps)
            .await
    }

    async fn reconcile_from_db(&self) -> SettlementResult<()> {
        let active_swaps = self
            .db
            .swaps()
            .get_active_swaps_with_status(SwapStatus::WaitingMMDepositConfirmed)
            .await
            .context(DatabaseSnafu)?;
        let mut grouped_batches: HashMap<BatchKey, Vec<Swap>> = HashMap::new();

        for swap in active_swaps {
            let Some(mm_deposit_status) = swap.mm_deposit_status.as_ref() else {
                warn!(
                    swap_id = %swap.id,
                    "Skipping WaitingMMDepositConfirmed swap missing mm_deposit_status during reconcile"
                );
                continue;
            };

            let batch_key = (
                swap.quote.to.currency.chain,
                mm_deposit_status.tx_hash.clone(),
            );
            grouped_batches.entry(batch_key).or_default().push(swap);
        }

        let active_batch_keys = grouped_batches.keys().cloned().collect::<HashSet<_>>();

        for (batch_key, swaps) in grouped_batches {
            let start = Instant::now();
            let result = self.reconcile_loaded_batch(&batch_key, &swaps).await;
            histogram!(MARKET_MAKER_BATCH_SETTLEMENT_DURATION_METRIC)
                .record(start.elapsed().as_secs_f64());
            self.note_batch_settlement_pass();

            if let Err(err) = result {
                self.record_event(batch_key.0, "refresh_error");
                warn!(
                    chain = ?batch_key.0,
                    tx_hash = %batch_key.1,
                    "Failed to reconcile market maker batch settlement state: {}",
                    err
                );
            }
        }

        let stale_keys = self
            .scheduled_batches
            .iter()
            .filter_map(|entry| {
                let key = entry.key().clone();
                (!active_batch_keys.contains(&key)).then_some(key)
            })
            .collect::<Vec<_>>();

        for batch_key in stale_keys {
            self.unschedule_batch(&batch_key);
        }

        self.update_active_batches_metric();

        Ok(())
    }

    async fn reconcile_loaded_batch(
        &self,
        batch_key: &BatchKey,
        swaps: &[Swap],
    ) -> SettlementResult<()> {
        if swaps.is_empty() {
            self.unschedule_batch(batch_key);
            return Ok(());
        }

        self.check_batch_for_mm_deposit_confirmation(batch_key, swaps)
            .await
    }

    async fn check_batch_for_mm_deposit_confirmation(
        &self,
        batch_key: &BatchKey,
        swaps: &[Swap],
    ) -> SettlementResult<()> {
        if swaps.is_empty() {
            self.unschedule_batch(batch_key);
            return Ok(());
        }

        let (deposit_chain, mm_tx_hash) = batch_key;
        let chain_ops =
            self.chain_registry
                .get(deposit_chain)
                .ok_or(SettlementError::ChainOperation {
                    source: otc_chains::Error::ChainNotSupported {
                        chain: format!("{:?}", deposit_chain),
                    },
                    loc: location!(),
                })?;

        let current_height = self.current_chain_height(*deposit_chain).await?;
        let market_maker_batch = self
            .db
            .batches()
            .get_batch(deposit_chain, mm_tx_hash)
            .await
            .context(DatabaseSnafu)?;
        let (market_maker_batch, _) =
            market_maker_batch.ok_or(SettlementError::InvalidMarketMakerBatch {
                context: format!(
                    "Market maker batch not found in local db for tx hash {}",
                    mm_tx_hash
                ),
                tx_hash: mm_tx_hash.clone(),
                loc: location!(),
            })?;

        let tx_status = chain_ops
            .verify_market_maker_batch_transaction(mm_tx_hash, &market_maker_batch)
            .await
            .context(ChainOperationSnafu)?;

        match tx_status {
            Some(confirmations) => {
                let swap_ids: Vec<Uuid> = swaps.iter().map(|s| s.id).collect();

                debug!(
                    tx_hash = %mm_tx_hash,
                    confirmations,
                    swap_count = swaps.len(),
                    "Market maker batch confirmation refresh"
                );

                self.db
                    .swaps()
                    .batch_update_mm_confirmations(&swap_ids, confirmations as u32)
                    .await
                    .context(DatabaseSnafu)?;

                let required_confirmations = u64::from(chain_ops.minimum_block_confirmations());
                if confirmations >= required_confirmations {
                    self.record_event(*deposit_chain, "confirmed");
                    self.unschedule_batch(batch_key);
                    debug!(
                        tx_hash = %mm_tx_hash,
                        swap_count = swaps.len(),
                        "Market maker batch reached required confirmations; settling swaps"
                    );

                    let now = utc::now();
                    let swap_settlement_timestamp = self
                        .db
                        .swaps()
                        .batch_mm_deposit_confirmed(&swap_ids)
                        .await
                        .context(DatabaseSnafu)?;

                    self.db
                        .batches()
                        .mark_confirmed(deposit_chain, mm_tx_hash, now)
                        .await
                        .context(DatabaseSnafu)?;

                    let market_maker_id = swaps[0].market_maker_id;
                    let fee_sats: i64 = market_maker_batch
                        .payment_verification
                        .aggregated_fee
                        .to::<u64>()
                        .try_into()
                        .map_err(|_| SettlementError::InvalidMarketMakerBatch {
                            context: "Aggregated fee does not fit into i64".to_string(),
                            tx_hash: mm_tx_hash.clone(),
                            loc: location!(),
                        })?;
                    self.db
                        .fees()
                        .accrue_batch_fee(
                            market_maker_id,
                            market_maker_batch.payment_verification.batch_nonce_digest,
                            fee_sats,
                            now,
                        )
                        .await
                        .context(DatabaseSnafu)?;

                    for swap in swaps {
                        let user_chain_ops = self
                            .chain_registry
                            .get(&swap.quote.from.currency.chain)
                            .ok_or(SettlementError::ChainOperation {
                                source: otc_chains::Error::ChainNotSupported {
                                    chain: format!("{:?}", swap.quote.from.currency.chain),
                                },
                                loc: location!(),
                            })?;

                        let user_wallet = user_chain_ops
                            .derive_wallet(
                                &self.settings.master_key_bytes(),
                                &swap.deposit_vault_salt,
                            )
                            .context(ChainOperationSnafu)?;

                        let mm_registry = self.mm_registry.clone();
                        let market_maker_id = swap.market_maker_id;
                        let swap_id = swap.id;
                        let private_key = user_wallet.private_key().to_string();
                        let user_deposit_tx_hash =
                            swap.user_deposit_status.as_ref().unwrap().tx_hash.clone();

                        let actual_deposit_lot = Lot {
                            currency: swap.quote.from.currency.clone(),
                            amount: swap
                                .user_deposit_status
                                .as_ref()
                                .expect("user deposit status should be some")
                                .amount,
                        };

                        tokio::spawn(async move {
                            let _ = mm_registry
                                .notify_swap_complete(
                                    &market_maker_id,
                                    &swap_id,
                                    &private_key,
                                    &actual_deposit_lot,
                                    &user_deposit_tx_hash,
                                    &swap_settlement_timestamp,
                                )
                                .await;
                        });

                        self.db
                            .swaps()
                            .mark_private_key_sent(swap.id)
                            .await
                            .context(DatabaseSnafu)?;
                    }
                    self.record_event(*deposit_chain, "settled");
                } else if let Some(target_height) = next_confirmation_target_height(
                    current_height,
                    confirmations,
                    required_confirmations,
                ) {
                    self.record_event(*deposit_chain, "pending");
                    self.schedule_batch(batch_key.clone(), target_height);
                }
            }
            None => {
                debug!(
                    tx_hash = %mm_tx_hash,
                    chain = ?deposit_chain,
                    swap_count = swaps.len(),
                    "Market maker batch tx not found on chain; waiting for next head"
                );
                self.record_event(*deposit_chain, "not_found");
                self.schedule_batch(batch_key.clone(), current_height.saturating_add(1));
            }
        }

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
                            height = head.height,
                            "Failed processing due market maker batch settlements: {}",
                            err
                        );
                    }
                    self.note_batch_settlement_pass();
                }
                Err(err) => {
                    warn!(
                        chain = ?chain,
                        "Failed to fetch chain head for market maker batch settlement service: {}",
                        err
                    );
                }
            }
        }
    }

    async fn recovery_sweep_loop(self: Arc<Self>) {
        loop {
            time::sleep(RECOVERY_SWEEP_INTERVAL).await;

            if let Err(err) = self.reconcile_from_db().await {
                error!(
                    "Market maker batch settlement recovery sweep failed: {}",
                    err
                );
            }
        }
    }

    async fn fetch_chain_head(&self, chain: ChainType) -> SettlementResult<ChainHead> {
        let chain_ops =
            self.chain_registry
                .get(&chain)
                .ok_or_else(|| SettlementError::ChainOperation {
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
    ) -> SettlementResult<()> {
        let due_batch_keys = self
            .scheduled_batches
            .iter()
            .filter_map(|entry| {
                let (scheduled_chain, tx_hash) = entry.key();
                if *scheduled_chain == chain && entry.value().next_recheck_height <= current_height
                {
                    Some((chain, tx_hash.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for batch_key in due_batch_keys {
            if let Err(err) = self.refresh_batch(batch_key.clone()).await {
                warn!(
                    chain = ?batch_key.0,
                    tx_hash = %batch_key.1,
                    "Failed refreshing due market maker batch settlement: {}",
                    err
                );
            }
        }

        Ok(())
    }

    async fn current_chain_height(&self, chain: ChainType) -> SettlementResult<u64> {
        if let Some(head) = self.latest_heads.get(&chain) {
            return Ok(head.height);
        }

        let chain_ops =
            self.chain_registry
                .get(&chain)
                .ok_or_else(|| SettlementError::ChainOperation {
                    source: otc_chains::Error::ChainNotSupported {
                        chain: format!("{:?}", chain),
                    },
                    loc: location!(),
                })?;

        let height = chain_ops
            .get_block_height()
            .await
            .context(ChainOperationSnafu)?;
        self.latest_heads.insert(
            chain,
            ChainHead {
                height,
                hash: String::new(),
            },
        );
        Ok(height)
    }

    fn schedule_batch(&self, batch_key: BatchKey, next_recheck_height: u64) {
        self.scheduled_batches.insert(
            batch_key,
            ScheduledBatch {
                next_recheck_height,
            },
        );
        self.update_active_batches_metric();
    }

    fn unschedule_batch(&self, batch_key: &BatchKey) {
        self.scheduled_batches.remove(batch_key);
        self.update_active_batches_metric();
    }

    fn update_active_batches_metric(&self) {
        gauge!(ACTIVE_MARKET_MAKER_BATCHES_METRIC).set(self.scheduled_batches.len() as f64);
    }

    fn note_batch_settlement_pass(&self) {
        self.last_batch_settlement_pass
            .store(utc::now().timestamp() as u64, Ordering::Relaxed);
    }

    fn record_event(&self, chain: ChainType, event: &'static str) {
        counter!(
            MARKET_MAKER_BATCH_SETTLEMENT_EVENTS_TOTAL_METRIC,
            "chain" => chain.to_db_string(),
            "event" => event,
        )
        .increment(1);
    }
}

fn next_confirmation_target_height(
    current_height: u64,
    confirmations: u64,
    min_confirmations: u64,
) -> Option<u64> {
    if confirmations >= min_confirmations {
        None
    } else {
        Some(current_height.saturating_add(1))
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
        head_poll_interval, next_confirmation_target_height, BASE_HEAD_POLL_INTERVAL,
        BITCOIN_HEAD_POLL_INTERVAL, ETHEREUM_HEAD_POLL_INTERVAL,
    };
    use otc_models::ChainType;

    #[test]
    fn already_confirmed_has_no_future_target() {
        assert_eq!(next_confirmation_target_height(100, 4, 4), None);
    }

    #[test]
    fn next_target_height_is_the_next_observed_head() {
        assert_eq!(next_confirmation_target_height(100, 0, 4), Some(101));
        assert_eq!(next_confirmation_target_height(100, 1, 4), Some(101));
        assert_eq!(next_confirmation_target_height(100, 3, 4), Some(101));
        assert_eq!(next_confirmation_target_height(100, 4, 4), None);
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
