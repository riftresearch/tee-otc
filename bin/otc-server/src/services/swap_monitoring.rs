use crate::db::Database;
use crate::error::OtcServerError;
use crate::{config::Settings, services::mm_registry};
use metrics::{gauge, histogram};
use otc_chains::traits::{MarketMakerQueuedPayment, MarketMakerQueuedPaymentExt};
use otc_chains::ChainRegistry;
use otc_models::{MMDepositStatus, Swap, SwapStatus, TxStatus, UserDepositStatus};
use snafu::{location, prelude::*, Location};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::time;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

pub const SWAP_MONITORING_DURATION_METRIC: &str = "otc_swap_monitoring_duration_seconds";
pub const ACTIVE_INDIVIDUAL_SWAPS_METRIC: &str = "otc_swaps_active_individual";
pub const ACTIVE_SWAP_BATCHES_METRIC: &str = "otc_swap_batches_active";

#[derive(Debug, Snafu)]
pub enum MonitoringError {
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

    #[snafu(display("Invalid state transition from {:?} at {loc}", current_state))]
    InvalidTransition {
        current_state: SwapStatus,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display(
        "Unauthorized market maker id: expected {expected} but got {actual} at {loc}"
    ))]
    UnauthorizedMarketMakerId {
        expected: Uuid,
        actual: Uuid,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display(
        "Invalid destination currency: expected {expected:?} but got {actual:?} at {loc}"
    ))]
    InvalidDestinationCurrency {
        expected: otc_models::Currency,
        actual: otc_models::Currency,
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

pub type MonitoringResult<T> = Result<T, MonitoringError>;

/// Background service that monitors all active swaps for:
/// - Incoming deposits (user and MM)
/// - Confirmation tracking
/// - Settlement completion
pub struct SwapMonitoringService {
    db: Database,
    settings: Arc<Settings>,
    chain_registry: Arc<ChainRegistry>,
    mm_registry: Arc<mm_registry::MMRegistry>,
    chain_monitor_interval_seconds: u64,
    max_concurrent_swaps: usize,
}

impl SwapMonitoringService {
    #[must_use]
    pub fn new(
        db: Database,
        settings: Arc<Settings>,
        chain_registry: Arc<ChainRegistry>,
        mm_registry: Arc<mm_registry::MMRegistry>,
        chain_monitor_interval_seconds: u64,
        max_concurrent_swaps: usize,
    ) -> Self {
        Self {
            db,
            settings,
            chain_registry,
            mm_registry,
            chain_monitor_interval_seconds,
            max_concurrent_swaps,
        }
    }

    /// Start the monitoring service
    pub async fn run(self: Arc<Self>) {
        info!("Starting swap monitoring service");

        let interval = Duration::from_secs(self.chain_monitor_interval_seconds);
        info!(
            "Starting swap monitoring service with interval: {:?}",
            interval
        );
        let mut interval = time::interval(interval);

        loop {
            let service = Arc::clone(&self);
            let handle = tokio::spawn(async move {
                service.monitor_all_swaps().await
            });

            match handle.await {
                Ok(Ok(())) => {
                    // Success
                }
                Ok(Err(e)) => {
                    error!("Error monitoring swaps: {}", e);
                }
                Err(e) if e.is_panic() => {
                    error!("CRITICAL: Swap monitoring PANICKED: {:?}", e);
                }
                Err(e) => {
                    error!("Swap monitoring task cancelled: {:?}", e);
                }
            }

            interval.tick().await;
        }
    }

    /// Monitor all active swaps
    async fn monitor_all_swaps(self: &Arc<Self>) -> MonitoringResult<()> {
        let start = Instant::now();

        let active_swaps = self.db.swaps().get_active().await.context(DatabaseSnafu)?;
        info!("Monitoring {} active swaps", active_swaps.len());

        let mut join_set = tokio::task::JoinSet::new();

        // there's truly two meta-states where a swap can be in the happy-path
        // 1. waiting for a user deposit to ultimately confirm (initiated -> confirmed)
        // 2. waiting for a market maker deposit to ultimately confirm (initiated -> confirmed)
        // As of the batch rewrite, it's highly inefficient to monitor each swap individually once
        // the swap gets to stage 2.
        // So solution is to first split them up into two groups
        // if the state of a swap is:
        // waiting_mm_deposit_confirmed these swaps should be split into groups of batches.
        // and monitored as a batch that share the same mm_tx_hash (vec<vec<Swap>>)
        // otherwise, they should be monitored individually (vec<Swap>)

        // Split into individual swaps vs those waiting for MM deposit confirmation
        let (individual_swaps, waiting_mm_confirmed): (Vec<_>, Vec<_>) = active_swaps
            .into_iter()
            .partition(|swap| swap.status != SwapStatus::WaitingMMDepositConfirmed);

        // Group the MM-waiting swaps by their shared mm_tx_hash into batches
        let batched_swaps: HashMap<String, Vec<Swap>> = waiting_mm_confirmed.into_iter().fold(
            HashMap::<String, Vec<Swap>>::new(),
            |mut acc, swap| {
                if let Some(ref mm_deposit) = swap.mm_deposit_status {
                    acc.entry(mm_deposit.tx_hash.clone())
                        .or_default()
                        .push(swap);
                }
                acc
            },
        );

        gauge!(ACTIVE_INDIVIDUAL_SWAPS_METRIC).set(individual_swaps.len() as f64);
        gauge!(ACTIVE_SWAP_BATCHES_METRIC).set(batched_swaps.len() as f64);

        let semaphore = Arc::new(Semaphore::new(self.max_concurrent_swaps));

        for swap in individual_swaps {
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(e) => {
                    error!("Failed to acquire semaphore permit for swap {}: {}", swap.id, e);
                    continue;
                }
            };
            let service = Arc::clone(self);

            join_set.spawn(async move {
                let _permit = permit;
                if let Err(e) = service.monitor_swap(&swap).await {
                    error!("Error monitoring swap {}: {}", swap.id, e);
                }
            });
        }

        for (mm_tx_hash, swaps) in batched_swaps {
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(e) => {
                    error!("Failed to acquire semaphore permit for batch {}: {}", mm_tx_hash, e);
                    continue;
                }
            };
            let service = Arc::clone(self);

            join_set.spawn(async move {
                let _permit = permit;
                if let Err(e) = service
                    .check_batch_for_mm_deposit_confirmation(&mm_tx_hash, &swaps)
                    .await
                {
                    error!(
                        "Error monitoring batch for MM deposit {}: {}",
                        mm_tx_hash, e
                    );
                }
            });
        }

        while let Some(result) = join_set.join_next().await {
            if let Err(e) = result {
                error!("Swap monitoring task panicked: {}", e);
            }
        }

        histogram!(SWAP_MONITORING_DURATION_METRIC).record(start.elapsed().as_secs_f64());

        Ok(())
    }

    /// Monitor a single swap based on its current state
    async fn monitor_swap(&self, swap: &Swap) -> MonitoringResult<()> {
        debug!(
            "Monitoring swap {} status: {:?}, user deposit status: {:?}",
            swap.id, swap.status, swap.user_deposit_status
        );
        // Check for timeout first
        if swap.failure_at.is_some() {
            warn!("Swap {} has failed", swap.id);
            return Ok(());
        }

        debug!(
            "Monitoring swap {} status: {:?}, user deposit status: {:?}",
            swap.id, swap.status, swap.user_deposit_status
        );

        match swap.status {
            SwapStatus::WaitingUserDepositInitiated => {
                self.check_user_deposit(swap, None).await?;
            }
            SwapStatus::WaitingUserDepositConfirmed => {
                self.check_user_deposit_confirmation(swap).await?;
            }
            SwapStatus::WaitingMMDepositInitiated => {
                // Market makers will ping us to initiate their deposit, so nothing to monitor
            }
            SwapStatus::WaitingMMDepositConfirmed => {
                // Swaps in this state are monitored by a batch monitoring method
            }
            SwapStatus::Settled => {
                // Settlement already complete, nothing to monitor
            }
            SwapStatus::RefundingUser => {
                // Refunding user, nothing to monitor
            }
            SwapStatus::Failed => {
                // Failed, nothing to monitor
            }
        }

        Ok(())
    }

    /// Check for user deposit
    async fn check_user_deposit(&self, swap: &Swap, existent_tx_hash: Option<String>) -> MonitoringResult<()> {
        // Get the quote to know what token/chain to check
        let quote = &swap.quote;

        // Get the chain operations for the user's deposit chain (from = user sends)
        let chain_ops = self.chain_registry.get(&quote.from.currency.chain).ok_or(
            MonitoringError::ChainOperation {
                source: otc_chains::Error::ChainNotSupported {
                    chain: format!("{:?}", quote.from.currency.chain),
                },
                loc: location!(),
            },
        )?;

        // Derive the user deposit address
        let user_wallet = chain_ops
            .derive_wallet(&self.settings.master_key_bytes(), &swap.user_deposit_salt)
            .context(ChainOperationSnafu)?;


        // Check for deposit from the user's wallet
        let deposit_info = chain_ops
            .search_for_transfer(&user_wallet.address, &quote.from, None)
            .await
            .context(ChainOperationSnafu)?;

        if let Some(deposit) = deposit_info {
            debug!(
                "User deposit detected for swap {}: {} on chain {:?}",
                swap.id, deposit.tx_hash, quote.from.currency.chain
            );

            if let Some(existing_hash) = existent_tx_hash {
                if existing_hash == deposit.tx_hash {
                    info!("User deposit tx {} for swap {} already detected, skipping", deposit.tx_hash, swap.id);
                    return Ok(());
                }
            }

            // Update swap state
            let user_deposit_status = UserDepositStatus {
                tx_hash: deposit.tx_hash.clone(),
                amount: deposit.amount,
                deposit_detected_at: utc::now(),
                confirmed_at: None,
                confirmations: 0, // Initial detection
                last_checked: utc::now(),
            };

            self.db
                .swaps()
                .user_deposit_detected(swap.id, user_deposit_status)
                .await
                .context(DatabaseSnafu)?;

            // Notify MM about user deposit
            let mm_registry = self.mm_registry.clone();
            let market_maker_id = swap.market_maker_id;
            let swap_id = swap.id;
            let quote_id = swap.quote.id;
            let user_deposit_address = swap.user_deposit_address.clone();
            let tx_hash = deposit.tx_hash.clone();
            tokio::spawn(async move {
                let _ = mm_registry
                    .notify_user_deposit(
                        &market_maker_id,
                        &swap_id,
                        &quote_id,
                        &user_deposit_address,
                        &tx_hash,
                    )
                    .await;
            });
        }

        Ok(())
    }

    /// Check user deposit confirmations
    async fn check_user_deposit_confirmation(&self, swap: &Swap) -> MonitoringResult<()> {
        let quote = &swap.quote;
        let user_deposit =
            swap.user_deposit_status
                .as_ref()
                .ok_or(MonitoringError::InvalidTransition {
                    current_state: swap.status,
                    loc: location!(),
                })?;

        // Get the chain operations for the user's deposit chain
        let chain_ops = self.chain_registry.get(&quote.from.currency.chain).ok_or(
            MonitoringError::ChainOperation {
                source: otc_chains::Error::ChainNotSupported {
                    chain: format!("{:?}", quote.from.currency.chain),
                },
                loc: location!(),
            },
        )?;

        // Check confirmation status
        let tx_status = chain_ops
            .get_tx_status(&user_deposit.tx_hash)
            .await
            .context(ChainOperationSnafu)?;

        match tx_status {
            TxStatus::Confirmed(confirmations) => {
                debug!(
                    "User deposit for swap {} has {} confirmations",
                    swap.id, confirmations
                );

                // Update confirmations
                self.db
                    .swaps()
                    .update_user_confirmations(swap.id, confirmations as u32)
                    .await
                    .context(DatabaseSnafu)?;

                // Check if we have enough confirmations
                if confirmations >= chain_ops.minimum_block_confirmations().into() {
                    info!(
                        "User deposit for swap {} has reached required confirmations",
                        swap.id
                    );

                    // Transition to waiting for MM deposit
                    let user_deposit_confirmed_at = self
                        .db
                        .swaps()
                        .user_deposit_confirmed(swap.id)
                        .await
                        .context(DatabaseSnafu)?;

                    // Notify MM to send their deposit
                    let mm_registry = self.mm_registry.clone();
                    let market_maker_id = swap.market_maker_id;
                    let swap_id = swap.id;
                    let quote_id = swap.quote.id;
                    let user_destination_address = swap.user_destination_address.clone();
                    let mm_nonce = swap.mm_nonce;
                    let expected_lot = swap.quote.to.clone();

                    tokio::spawn(async move {
                        let _ = mm_registry
                            .notify_user_deposit_confirmed(
                                &market_maker_id,
                                &swap_id,
                                &quote_id,
                                &user_destination_address,
                                mm_nonce,
                                &expected_lot,
                                user_deposit_confirmed_at,
                            )
                            .await;
                    });
                }
            }
            TxStatus::NotFound => {
                warn!(
                    "User deposit tx {} for swap {} not found on chain {:?}",
                    user_deposit.tx_hash, swap.id, quote.from.currency.chain
                );
                // If it's not found, it's possible the user deposit was replaced with a different tx, 
                // so we need to check for a new deposit
                self.check_user_deposit(swap, Some(user_deposit.tx_hash.clone())).await?;
            }
        }

        Ok(())
    }

    /// Market makers send notifications of batch payments directly to us, so we can track the batch payment
    /// this method will validate the expected nonce and fee amounts along with minifying the data necessary to verify this batch of payments
    /// once the transaction is confirmed later on
    pub async fn track_batch_payment(
        &self,
        market_maker_id: Uuid,
        tx_hash: &String,
        swap_ids: Vec<Uuid>,
        mm_sent_batch_nonce_digest: [u8; 32],
    ) -> MonitoringResult<()> {
        // first get all swaps that are in this mm's requested batch
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

        // Swap state verification
        for swap in &swaps {
            // validate the market maker id is the same for all swaps
            if swap.market_maker_id != market_maker_id {
                return Err(MonitoringError::InvalidMarketMakerBatch {
                    context: format!("Unauthorized market maker id {market_maker_id} attempted to track batch payment for swap id {}", swap.id),
                    tx_hash: tx_hash.clone(),
                    loc: location!(),
                });
            }
            // validate the destination currency is the same for all swaps
            if swap.quote.to.currency != dest_currency {
                return Err(MonitoringError::InvalidMarketMakerBatch {
                    context: format!(
                        "Destination currency mismatch processing swap id {}",
                        swap.id
                    ),
                    tx_hash: tx_hash.clone(),
                    loc: location!(),
                });
            }
            // can only update batch_payment for a swap if it's still waiting for MM initial deposit
            if swap.status != SwapStatus::WaitingMMDepositInitiated {
                return Err(MonitoringError::InvalidMarketMakerBatch {
                    context: format!(
                        "Swap id {} is not in a state where a batch payment update is allowed",
                        swap.id
                    ),
                    tx_hash: tx_hash.clone(),
                    loc: location!(),
                });
            }
        }

        // Now create the MarketMakerBatch to cache the relevant data for this batch payment
        let market_maker_batch = match swaps
            .iter()
            .map(MarketMakerQueuedPayment::from)
            .collect::<Vec<_>>()
            .to_market_maker_batch()
        {
            Some(batch) => batch,
            None => {
                return Err(MonitoringError::InvalidMarketMakerBatch {
                    context: "Market maker batch creation failed".to_string(),
                    tx_hash: tx_hash.clone(),
                    loc: location!(),
                })
            }
        };

        if market_maker_batch.payment_verification.batch_nonce_digest != mm_sent_batch_nonce_digest
        {
            return Err(MonitoringError::InvalidMarketMakerBatch {
                context: "Batch nonce digest mismatch".to_string(),
                tx_hash: tx_hash.clone(),
                loc: location!(),
            });
        }

        // It's unlikely the batch payment will be found onchain, so
        // we'll take the MMs word that the batch payment was sent, no risk if they lie, b/c the tx_hash will then never confirm

        // Create a map of swap id => MMDepositStatus structs
        let mm_deposit_status_map = swaps
            .iter()
            .map(|swap| {
                (
                    swap.id,
                    MMDepositStatus {
                        tx_hash: tx_hash.clone(),
                        amount: swap.quote.to.amount, // Note: actual amount sent could be more than expected
                        deposit_detected_at: utc::now(),
                        confirmations: 0,
                        last_checked: utc::now(),
                    },
                )
            })
            .collect::<HashMap<Uuid, MMDepositStatus>>();

        // Store the batch payment in the batch database
        let chain_type = dest_currency.chain;
        let swap_ids = swaps.iter().map(|s| s.id).collect::<Vec<_>>();
        self.db
            .batches()
            .add_batch(
                chain_type,
                tx_hash.clone(),
                &market_maker_batch,
                swap_ids,
                market_maker_id,
            )
            .await
            .context(DatabaseSnafu)?;

        // Atomically update all swaps in the batch
        self.db
            .swaps()
            .batch_mm_deposit_detected(mm_deposit_status_map)
            .await
            .context(DatabaseSnafu)?;

        Ok(())
    }

    /// Check MM deposit confirmations for a batch of swaps sharing the same transaction
    async fn check_batch_for_mm_deposit_confirmation(
        &self,
        mm_tx_hash: &String,
        swaps: &[Swap],
    ) -> MonitoringResult<()> {
        if swaps.is_empty() {
            return Ok(());
        }

        // All swaps in a batch share the same destination chain/currency, use the first one
        let first_swap = &swaps[0];
        let deposit_chain = &first_swap.quote.to.currency.chain;

        // Get the chain operations for the MM's deposit chain
        let chain_ops =
            self.chain_registry
                .get(deposit_chain)
                .ok_or(MonitoringError::ChainOperation {
                    source: otc_chains::Error::ChainNotSupported {
                        chain: format!("{:?}", deposit_chain),
                    },
                    loc: location!(),
                })?;

        let market_maker_batch = self
            .db
            .batches()
            .get_batch(deposit_chain, mm_tx_hash)
            .await
            .context(DatabaseSnafu)?;
        let (market_maker_batch, _) =
            market_maker_batch.ok_or(MonitoringError::InvalidMarketMakerBatch {
                context: format!(
                    "Market maker batch not found in local db for tx hash {}",
                    mm_tx_hash
                ),
                tx_hash: mm_tx_hash.clone(),
                loc: location!(),
            })?;

        // Check confirmation status once for the entire batch
        let tx_status = chain_ops
            .verify_market_maker_batch_transaction(mm_tx_hash, &market_maker_batch)
            .await
            .context(ChainOperationSnafu)?;

        match tx_status {
            Some(confirmations) => {
                let swap_ids: Vec<Uuid> = swaps.iter().map(|s| s.id).collect();

                debug!(
                    "MM deposit batch {} has {} confirmations for {} swaps",
                    mm_tx_hash,
                    confirmations,
                    swaps.len()
                );

                // Update confirmations for all swaps in the batch atomically
                self.db
                    .swaps()
                    .batch_update_mm_confirmations(&swap_ids, confirmations as u32)
                    .await
                    .context(DatabaseSnafu)?;

                // Check if we have enough confirmations (use first swap's requirements)
                if confirmations >= chain_ops.minimum_block_confirmations().into() {
                    debug!(
                        "MM deposit batch {} has reached required confirmations, settling {} swaps",
                        mm_tx_hash,
                        swaps.len()
                    );

                    // Transition all swaps to settled state atomically
                    let swap_settlement_timestamp = self.db
                        .swaps()
                        .batch_mm_deposit_confirmed(&swap_ids)
                        .await
                        .context(DatabaseSnafu)?;

                    // Send private keys to MM for each swap and mark as sent
                    for swap in swaps {
                        // Get chain ops for the user's deposit chain (from currency)
                        let user_chain_ops = self
                            .chain_registry
                            .get(&swap.quote.from.currency.chain)
                            .ok_or(MonitoringError::ChainOperation {
                                source: otc_chains::Error::ChainNotSupported {
                                    chain: format!("{:?}", swap.quote.from.currency.chain),
                                },
                                loc: location!(),
                            })?;

                        let user_wallet = user_chain_ops
                            .derive_wallet(
                                &self.settings.master_key_bytes(),
                                &swap.user_deposit_salt,
                            )
                            .context(ChainOperationSnafu)?;

                        let mm_registry = self.mm_registry.clone();
                        let market_maker_id = swap.market_maker_id;
                        let swap_id = swap.id;
                        let private_key = user_wallet.private_key().to_string();
                        let user_deposit_tx_hash =
                            swap.user_deposit_status.as_ref().unwrap().tx_hash.clone();
                        let mut actual_deposit_lot = swap.quote.from.clone();
                        // Note: Instead of sending quote.to to the MM, we use swap.user_deposit_status.amount,
                        // which stores the actual on-chain balance of the user's deposit vault, so the MM doesn't
                        // have to query the on-chain balance for this private key.
                        actual_deposit_lot.amount = swap.user_deposit_status.as_ref().expect("user deposit status should be some").amount;

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

                        // Mark private key as sent
                        self.db
                            .swaps()
                            .mark_private_key_sent(swap.id)
                            .await
                            .context(DatabaseSnafu)?;
                    }
                }
            }
            None => {
                warn!(
                    "MM deposit batch tx {} not found on chain {:?} for {} swaps",
                    mm_tx_hash,
                    deposit_chain,
                    swaps.len()
                );
            }
        }

        Ok(())
    }
}
