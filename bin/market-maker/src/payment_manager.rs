// Middleware that prevents double-sending payments to the same swap request

use std::{collections::HashMap, sync::Arc, time::Duration};

use alloy::primitives::U256;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dashmap::{mapref::entry::Entry, DashMap};
use otc_chains::traits::{MarketMakerQueuedPayment, MarketMakerQueuedPaymentExt};
use otc_models::{can_be_refunded_soon, ChainType, Lot};
use otc_protocols::mm::{MMErrorCode, MMResponse, NetworkBatch};
use tokio::{
    sync::mpsc::{self, UnboundedSender},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    balance_strat::QuoteBalanceStrategy,
    db::{BatchStatus, BroadcastedTransactionRepository, PaymentRepository},
    wallet::{PreparedBatchPayment, TransactionState, WalletManager},
};
use otc_protocols::mm::ProtocolMessage;

// For tests
#[async_trait]
trait BatchPaymentRecorder: Send + Sync {
    async fn persist_prepared_batch(
        &self,
        swap_ids: Vec<Uuid>,
        chain: ChainType,
        batch_nonce_digest: [u8; 32],
        aggregated_fee_sats: u64,
        prepared_batch: &PreparedBatchPayment,
    ) -> crate::db::PaymentRepositoryResult<()>;

    async fn update_batch_status(
        &self,
        txid: &str,
        status: BatchStatus,
    ) -> crate::db::PaymentRepositoryResult<()>;
}

#[async_trait]
impl BatchPaymentRecorder for PaymentRepository {
    async fn persist_prepared_batch(
        &self,
        swap_ids: Vec<Uuid>,
        chain: ChainType,
        batch_nonce_digest: [u8; 32],
        aggregated_fee_sats: u64,
        prepared_batch: &PreparedBatchPayment,
    ) -> crate::db::PaymentRepositoryResult<()> {
        PaymentRepository::persist_prepared_batch(
            self,
            swap_ids,
            chain,
            batch_nonce_digest,
            aggregated_fee_sats,
            prepared_batch,
        )
        .await
    }

    async fn update_batch_status(
        &self,
        txid: &str,
        status: BatchStatus,
    ) -> crate::db::PaymentRepositoryResult<()> {
        PaymentRepository::update_batch_status(self, txid, status).await
    }
}

/// Configuration for batch payment processing
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Duration between batch executions
    pub interval_secs: u64,
    /// Maximum number of payments per batch
    pub batch_size: usize,
}

pub struct PaymentManager {
    wallet_manager: Arc<WalletManager>,
    payment_repository: Arc<PaymentRepository>,
    broadcasted_transaction_repository: Arc<BroadcastedTransactionRepository>,
    otc_response_tx: UnboundedSender<ProtocolMessage<MMResponse>>,
    /// Channels for queuing payments per chain
    bitcoin_tx: UnboundedSender<MarketMakerQueuedPayment>,
    ethereum_tx: UnboundedSender<MarketMakerQueuedPayment>,
    base_tx: UnboundedSender<MarketMakerQueuedPayment>,
    /// Tracks swap_ids that are currently queued but not yet broadcast
    /// Prevents duplicate queueing during websocket reconnect replays
    in_flight_payments: Arc<DashMap<Uuid, InFlightPaymentSnapshot>>,
}

#[derive(Debug, Clone)]
pub struct InFlightPaymentSnapshot {
    pub swap_id: Uuid,
    pub quote_id: Uuid,
    pub chain: ChainType,
    pub amount: U256,
    pub queued_at: DateTime<Utc>,
    pub user_deposit_confirmed_at: DateTime<Utc>,
}

impl PaymentManager {
    pub fn new(
        wallet_manager: Arc<WalletManager>,
        payment_repository: Arc<PaymentRepository>,
        broadcasted_transaction_repository: Arc<BroadcastedTransactionRepository>,
        batch_configs: HashMap<ChainType, BatchConfig>,
        balance_strategy: Arc<QuoteBalanceStrategy>,
        otc_response_tx: UnboundedSender<ProtocolMessage<MMResponse>>,
        cancellation_token: CancellationToken,
        join_set: &mut JoinSet<crate::Result<()>>,
    ) -> Self {
        // Create channels for each chain type
        let (bitcoin_tx, bitcoin_rx) = mpsc::unbounded_channel();
        let (ethereum_tx, ethereum_rx) = mpsc::unbounded_channel();
        let (base_tx, base_rx) = mpsc::unbounded_channel();
        // Create shared in-flight tracking map
        let in_flight_payments = Arc::new(DashMap::new());

        // Spawn batch processor for Bitcoin if configured
        if let Some(config) = batch_configs.get(&ChainType::Bitcoin) {
            if let Some(wallet) = wallet_manager.get(ChainType::Bitcoin) {
                spawn_batch_processor(
                    ChainType::Bitcoin,
                    wallet,
                    payment_repository.clone(),
                    bitcoin_rx,
                    config.clone(),
                    balance_strategy.clone(),
                    otc_response_tx.clone(),
                    in_flight_payments.clone(),
                    cancellation_token.clone(),
                    join_set,
                );
            }
        }

        // Spawn batch processor for Ethereum if configured
        if let Some(config) = batch_configs.get(&ChainType::Ethereum) {
            if let Some(wallet) = wallet_manager.get(ChainType::Ethereum) {
                spawn_batch_processor(
                    ChainType::Ethereum,
                    wallet,
                    payment_repository.clone(),
                    ethereum_rx,
                    config.clone(),
                    balance_strategy.clone(),
                    otc_response_tx.clone(),
                    in_flight_payments.clone(),
                    cancellation_token.clone(),
                    join_set,
                );
            }
        }

        // Spawn batch processor for Base if configured
        if let Some(config) = batch_configs.get(&ChainType::Base) {
            if let Some(wallet) = wallet_manager.get(ChainType::Base) {
                spawn_batch_processor(
                    ChainType::Base,
                    wallet,
                    payment_repository.clone(),
                    base_rx,
                    config.clone(),
                    balance_strategy.clone(),
                    otc_response_tx.clone(),
                    in_flight_payments.clone(),
                    cancellation_token.clone(),
                    join_set,
                );
            }
        }
        Self {
            wallet_manager,
            payment_repository,
            broadcasted_transaction_repository,
            otc_response_tx,
            bitcoin_tx,
            ethereum_tx,
            base_tx,
            in_flight_payments,
        }
    }

    /// Queue a payment for a swap, this will add to a local pool of payments that will be sent in a batch on a fixed interval
    pub async fn queue_payment(
        &self,
        request_id: &Uuid,
        swap_id: &Uuid,
        quote_id: &Uuid,
        user_destination_address: &str,
        user_deposit_confirmed_at: DateTime<Utc>,
        mm_nonce: &[u8; 16],
        expected_lot: &Lot,
        protocol_fee: U256,
    ) -> MMResponse {
        // Validate that we have a wallet for this chain
        let wallet = self.wallet_manager.get(expected_lot.currency.chain);
        if wallet.is_none() {
            return MMResponse::Error {
                request_id: *request_id,
                error_code: MMErrorCode::UnsupportedChain,
                message: "No wallet found for chain".to_string(),
                timestamp: utc::now(),
            };
        }

        // Check if payment has already been broadcast to blockchain
        match self
            .payment_repository
            .has_payment_been_made(*swap_id)
            .await
        {
            Ok(Some(txid)) => {
                return MMResponse::Error {
                    request_id: *request_id,
                    error_code: MMErrorCode::InternalError,
                    message: format!("Payment already made for swap {swap_id}, txid: {txid:?}"),
                    timestamp: utc::now(),
                };
            }
            Err(e) => {
                return MMResponse::Error {
                    request_id: *request_id,
                    error_code: MMErrorCode::InternalError,
                    message: format!("Failed to check if payment has been made for swap: {e}"),
                    timestamp: utc::now(),
                };
            }
            Ok(None) => {
                // Payment not yet broadcast, continue to in-flight check
            }
        }

        // Atomically check if payment is already queued and insert if not
        // This prevents duplicate queueing during websocket reconnect replays
        // Using entry API ensures atomic check-and-insert, preventing TOCTOU races
        match self.in_flight_payments.entry(*swap_id) {
            Entry::Occupied(_) => {
                // Payment is already queued (in-flight but not yet broadcast)
                warn!(
                    "Payment for swap {swap_id} is already queued, preventing duplicate. \
                    This typically happens during websocket reconnects."
                );
                // Return PaymentQueued for idempotent behavior
                return MMResponse::PaymentQueued {
                    request_id: *request_id,
                    swap_id: *swap_id,
                    timestamp: utc::now(),
                };
            }
            Entry::Vacant(entry) => {
                // Mark payment as in-flight by inserting into map
                entry.insert(InFlightPaymentSnapshot {
                    swap_id: *swap_id,
                    quote_id: *quote_id,
                    chain: expected_lot.currency.chain,
                    amount: expected_lot.amount,
                    queued_at: utc::now(),
                    user_deposit_confirmed_at,
                });
            }
        }

        info!("Queueing payment for swap {swap_id} and quote {quote_id}");

        // Create queued payment
        let queued_payment = MarketMakerQueuedPayment {
            swap_id: *swap_id,
            quote_id: *quote_id,
            lot: expected_lot.clone(),
            destination_address: user_destination_address.to_string(),
            mm_nonce: *mm_nonce,
            user_deposit_confirmed_at: Some(user_deposit_confirmed_at),
            protocol_fee,
        };

        // Send to appropriate chain's channel
        let send_result = match expected_lot.currency.chain {
            ChainType::Bitcoin => self.bitcoin_tx.send(queued_payment),
            ChainType::Ethereum => self.ethereum_tx.send(queued_payment),
            ChainType::Base => self.base_tx.send(queued_payment),
        };

        match send_result {
            Ok(()) => MMResponse::PaymentQueued {
                request_id: *request_id,
                swap_id: *swap_id,
                timestamp: utc::now(),
            },
            Err(e) => {
                error!("Failed to queue payment for swap {swap_id}: {e}");
                // Failed to queue, remove from in-flight tracking
                self.in_flight_payments.remove(swap_id);
                MMResponse::Error {
                    request_id: *request_id,
                    error_code: MMErrorCode::InternalError,
                    message: "Failed to queue payment - channel closed".to_string(),
                    timestamp: utc::now(),
                }
            }
        }
    }

    pub fn payment_repository(&self) -> Arc<PaymentRepository> {
        self.payment_repository.clone()
    }

    pub async fn recover_prepared_batches(&self) -> crate::Result<()> {
        let built_batches = self
            .payment_repository
            .get_batches_by_status(BatchStatus::Built)
            .await
            .map_err(|source| crate::Error::PaymentRepository { source })?;

        for batch in built_batches {
            let wallet =
                self.wallet_manager
                    .get(batch.chain)
                    .ok_or_else(|| crate::Error::GenericWallet {
                        source: Box::new(crate::WalletError::WalletNotRegistered {
                            chain_type: batch.chain,
                        }),
                    })?;
            let stored_tx = self
                .broadcasted_transaction_repository
                .get_broadcasted_transaction(&batch.txid)
                .await
                .map_err(|source| crate::Error::GenericWallet {
                    source: Box::new(crate::WalletError::TransactionCreationFailed {
                        reason: format!(
                            "failed to load prepared transaction {} for built batch recovery: {source}",
                            batch.txid
                        ),
                    }),
                })?
                .ok_or_else(|| crate::Error::GenericWallet {
                    source: Box::new(crate::WalletError::TransactionCreationFailed {
                        reason: format!(
                            "missing persisted transaction bytes for built batch {}",
                            batch.txid
                        ),
                    }),
                })?;

            let prepared_batch = match batch.chain {
                ChainType::Bitcoin => PreparedBatchPayment::Bitcoin {
                    txid: stored_tx.txid.clone(),
                    txdata: stored_tx.txdata.clone(),
                    foreign_utxos: stored_tx.bitcoin_tx_foreign_utxos.unwrap_or_default(),
                    absolute_fee: stored_tx.absolute_fee,
                    reserved_deposit_keys: Vec::new(),
                },
                ChainType::Ethereum | ChainType::Base => PreparedBatchPayment::Evm {
                    txid: stored_tx.txid.clone(),
                    txdata: stored_tx.txdata.clone(),
                    reserved_deposit_keys: Vec::new(),
                },
            };

            match wallet.lookup_transaction_state(&batch.txid).await? {
                TransactionState::Missing => {
                    wallet.broadcast_prepared_batch(&prepared_batch).await?;
                }
                TransactionState::Pending | TransactionState::Confirmed(_) => {}
            }

            self.payment_repository
                .update_batch_status(&batch.txid, BatchStatus::Created)
                .await
                .map_err(|source| crate::Error::PaymentRepository { source })?;
        }

        Ok(())
    }

    pub async fn has_durable_batch_for_swaps(&self, swap_ids: &[Uuid]) -> bool {
        for swap_id in swap_ids {
            match self.payment_repository.has_payment_been_made(*swap_id).await {
                Ok(Some(_)) => {}
                _ => return false,
            }
        }
        true
    }

    pub async fn execute_planned_batch(
        &self,
        queued_payments: Vec<MarketMakerQueuedPayment>,
    ) -> crate::Result<String> {
        let Some(first_payment) = queued_payments.first() else {
            return Err(crate::Error::GenericWallet {
                source: Box::new(crate::WalletError::TransactionCreationFailed {
                    reason: "planner attempted to execute an empty outbound batch".to_string(),
                }),
            });
        };

        let chain_type = first_payment.lot.currency.chain;
        let wallet =
            self.wallet_manager
                .get(chain_type)
                .ok_or_else(|| crate::Error::GenericWallet {
                    source: Box::new(crate::WalletError::WalletNotRegistered { chain_type }),
                })?;

        for payment in &queued_payments {
            self.in_flight_payments.insert(
                payment.swap_id,
                InFlightPaymentSnapshot {
                    swap_id: payment.swap_id,
                    quote_id: payment.quote_id,
                    chain: payment.lot.currency.chain,
                    amount: payment.lot.amount,
                    queued_at: utc::now(),
                    user_deposit_confirmed_at: payment
                        .user_deposit_confirmed_at
                        .unwrap_or_else(utc::now),
                },
            );
        }

        match process_batch(
            chain_type,
            &wallet,
            self.payment_repository.as_ref(),
            &self.otc_response_tx,
            &self.in_flight_payments,
            &queued_payments,
        )
        .await
        {
            Ok(Some(txid)) => Ok(txid),
            Ok(None) => Err(crate::Error::GenericWallet {
                source: Box::new(crate::WalletError::TransactionCreationFailed {
                    reason: "planner batch became ineligible before execution".to_string(),
                }),
            }),
            Err(error) => Err(error),
        }
    }

    pub fn in_flight_payments(&self) -> Vec<InFlightPaymentSnapshot> {
        let mut payments: Vec<_> = self
            .in_flight_payments
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        payments.sort_by_key(|payment| payment.queued_at);
        payments
    }

    #[cfg(test)]
    pub fn insert_in_flight_payment_for_test(&self, payment: InFlightPaymentSnapshot) {
        self.in_flight_payments.insert(payment.swap_id, payment);
    }
}

/// Spawns a background task that periodically processes batched payments for a specific chain
fn spawn_batch_processor(
    chain_type: ChainType,
    wallet: Arc<dyn crate::wallet::Wallet>,
    payment_recorder: Arc<dyn BatchPaymentRecorder>,
    mut rx: mpsc::UnboundedReceiver<MarketMakerQueuedPayment>,
    config: BatchConfig,
    balance_strategy: Arc<QuoteBalanceStrategy>,
    otc_response_tx: UnboundedSender<ProtocolMessage<MMResponse>>,
    in_flight_payments: Arc<DashMap<Uuid, InFlightPaymentSnapshot>>,
    cancellation_token: CancellationToken,
    join_set: &mut JoinSet<crate::Result<()>>,
) {
    join_set.spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(config.interval_secs));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        info!(
            "Starting batch processor for {:?} (interval: {}s, batch_size: {})",
            chain_type, config.interval_secs, config.batch_size
        );

        // Payments that couldn't fit in previous batches due to balance constraints
        let mut retry_payments: Vec<MarketMakerQueuedPayment> = Vec::new();

        loop {
            // Check for cancellation before processing
            if cancellation_token.is_cancelled() {
                info!(
                    "Batch processor for {:?} cancelled, shutting down",
                    chain_type
                );
                return Ok(());
            }

            let mut queued_payments: Vec<MarketMakerQueuedPayment> = Vec::new();
            let mut pending_payments: Vec<MarketMakerQueuedPayment> = Vec::new();

            // First, collect retry payments from previous iteration
            pending_payments.append(&mut retry_payments);

            // Then drain up to batch_size items from the channel
            while pending_payments.len() < config.batch_size {
                match rx.try_recv() {
                    Ok(payment) => pending_payments.push(payment),
                    Err(_) => break, // Channel is empty
                }
            }

            // If we don't have any pending payments, wait for the interval
            if pending_payments.is_empty() {
                interval.tick().await;
                continue;
            }

            // Get wallet balance for balance checking
            let total_wallet_balance = wallet
                .balance(&pending_payments[0].lot.currency.token)
                .await?
                .total_balance;

            // Process each payment, adding to batch if it fits within balance constraints
            let mut cumulative_amount = U256::ZERO;
            let mut processed_count = 0;

            for payment in &pending_payments {
                let new_amount = cumulative_amount + payment.lot.amount;

                // Check if adding this payment would exceed balance constraints
                if balance_strategy.can_fill_quote(new_amount, total_wallet_balance) {
                    cumulative_amount = new_amount;
                    queued_payments.push(payment.clone());
                    processed_count += 1;

                    // Stop if we've reached batch size
                    if queued_payments.len() >= config.batch_size {
                        break;
                    }
                } else {
                    // Can't fit this payment, save for retry
                    retry_payments.push(payment.clone());
                    processed_count += 1;
                }
            }

            // Move any unprocessed payments back to retry queue
            if processed_count < pending_payments.len() {
                retry_payments.extend(pending_payments.into_iter().skip(processed_count));
            }

            // If we don't have a full batch and haven't exhausted pending payments, wait for interval
            if queued_payments.len() < config.batch_size && retry_payments.is_empty() {
                if queued_payments.is_empty() {
                    // No items could be added, wait for interval
                    interval.tick().await;
                    continue;
                } else {
                    // Have some items but not a full batch, wait for interval then process
                    interval.tick().await;
                }
            }

            if queued_payments.is_empty() {
                // All payments failed balance check, log and wait
                warn!(
                    "No payments could be added to batch for {:?}: total_wallet_balance={}, retry_queue_size={}",
                    chain_type, total_wallet_balance, retry_payments.len()
                );
                interval.tick().await;
                continue;
            }

            let total_batch_amount = cumulative_amount;
            info!(
                "Processing batch for {:?}: {} payments, total_amount={}, wallet_balance={}",
                chain_type, queued_payments.len(), total_batch_amount, total_wallet_balance
            );
            if !retry_payments.is_empty() {
                info!(
                    "Retrying {} payments for {:?} on next iteration...",
                    retry_payments.len(),
                    chain_type
                );
            }

            // Process the batch (filtering and processing handled in helper)
            if let Err(e) = process_batch(
                chain_type,
                &wallet,
                payment_recorder.as_ref(),
                &otc_response_tx,
                &in_flight_payments,
                &queued_payments,
            )
            .await
            {
                error!(
                    "Critical error processing batch for {:?}: {}. Market maker will shut down.",
                    chain_type, e
                );
                return Err(e);
            }
        }
    });
}

/// Helper function to process a batch of payments
async fn process_batch(
    chain_type: ChainType,
    wallet: &Arc<dyn crate::wallet::Wallet>,
    payment_recorder: &dyn BatchPaymentRecorder,
    otc_response_tx: &UnboundedSender<ProtocolMessage<MMResponse>>,
    in_flight_payments: &Arc<DashMap<Uuid, InFlightPaymentSnapshot>>,
    queued_payments: &[MarketMakerQueuedPayment],
) -> crate::Result<Option<String>> {
    // Split queued payments into eligible and ineligible (near refund window)
    let (eligible_payments, ineligible_payments): (Vec<_>, Vec<_>) =
        queued_payments.into_iter().partition(|p| {
            !can_be_refunded_soon(
                otc_models::SwapStatus::WaitingMMDepositInitiated,
                p.user_deposit_confirmed_at,
                None,
            )
        });

    for swap_id in ineligible_payments.iter().map(|p| p.swap_id) {
        in_flight_payments.remove(&swap_id);
    }

    if !ineligible_payments.is_empty() {
        info!(
            skipped = ineligible_payments.len(),
            "Skipping ineligible payments that are near refund window"
        );
    }

    if eligible_payments.is_empty() {
        return Ok(None);
    }

    info!(
        "Processing batch of {} payments for {:?}",
        eligible_payments.len(),
        chain_type
    );

    let collected_payments: Vec<MarketMakerQueuedPayment> =
        eligible_payments.iter().map(|&p| p.clone()).collect();
    let payment_batch = match collected_payments.as_slice().to_market_maker_batch() {
        Some(payment_batch) => payment_batch,
        None => {
            error!(
                "No market maker batch could be created for {:?}: queued_payments={:?}",
                chain_type, eligible_payments
            );
            return Ok(None);
        }
    };

    let batch_nonce_digest = payment_batch.payment_verification.batch_nonce_digest;
    let aggregated_fee_sats = payment_batch
        .payment_verification
        .aggregated_fee
        .to::<u64>();

    // Execute the batch payment
    let prepared_batch = match wallet
        .prepare_batch_payment(
            payment_batch.ordered_payments,
            Some(payment_batch.payment_verification),
        )
        .await
    {
        Ok(prepared_batch) => prepared_batch,
        Err(error) => {
            let swap_ids: Vec<Uuid> = eligible_payments.iter().map(|qp| qp.swap_id).collect();
            error!(
                "Batch payment preparation failed for {:?}: swap_ids={:?}, error={}",
                chain_type, swap_ids, error
            );
            for swap_id in &swap_ids {
                in_flight_payments.remove(swap_id);
            }
            return Err(error.into());
        }
    };

    let tx_hash = prepared_batch.txid().to_string();
    let swap_ids: Vec<Uuid> = eligible_payments.iter().map(|qp| qp.swap_id).collect();

    if let Err(error) = payment_recorder
        .persist_prepared_batch(
            swap_ids.clone(),
            chain_type,
            batch_nonce_digest,
            aggregated_fee_sats,
            &prepared_batch,
        )
        .await
    {
        error!(
            "Failed to persist prepared batch for {:?}: tx_hash={}, swap_ids={:?}, error={}",
            chain_type, tx_hash, swap_ids, error
        );
        let _ = wallet.discard_prepared_batch(&prepared_batch).await;
        for swap_id in &swap_ids {
            in_flight_payments.remove(swap_id);
        }
        return Err(crate::Error::PaymentRepository { source: error });
    }

    match wallet.broadcast_prepared_batch(&prepared_batch).await {
        Ok(broadcast_tx_hash) => {
            if broadcast_tx_hash != tx_hash {
                return Err(crate::Error::GenericWallet {
                    source: Box::new(crate::WalletError::TransactionCreationFailed {
                        reason: format!(
                            "prepared txid {tx_hash} did not match broadcast txid {broadcast_tx_hash}"
                        ),
                    }),
                });
            }

            payment_recorder
                .update_batch_status(&tx_hash, BatchStatus::Created)
                .await
                .map_err(|source| crate::Error::PaymentRepository { source })?;

            info!(
                "Batch payment successful for {:?}: tx_hash={}, swap_ids={:?}",
                chain_type, tx_hash, swap_ids
            );

            for swap_id in &swap_ids {
                in_flight_payments.remove(swap_id);
            }

            let response = MMResponse::Batches {
                request_id: Uuid::now_v7(),
                batches: vec![NetworkBatch {
                    tx_hash: tx_hash.clone(),
                    swap_ids: swap_ids.clone(),
                    batch_nonce_digest,
                }],
            };

            let protocol_msg = ProtocolMessage {
                version: "1.0.0".to_string(),
                sequence: 0,
                payload: response,
            };

            if let Err(error) = otc_response_tx.send(protocol_msg) {
                error!(
                    "Failed to send batch payment notification to OTC server for {:?}: swap_ids={:?}, error={}",
                    chain_type, swap_ids, error
                );
            } else {
                info!(
                    "Sent batch payment notification to OTC server for {:?}: tx_hash={}, swap_ids={:?}",
                    chain_type, tx_hash, swap_ids
                );
            }

            Ok(Some(tx_hash))
        }
        Err(error) => {
            error!(
                "Batch payment broadcast failed after durable prepare for {:?}: tx_hash={}, swap_ids={:?}, error={}",
                chain_type, tx_hash, swap_ids, error
            );
            Err(error.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::U256;
    use chrono::Duration as ChronoDuration;
    use otc_chains::traits::{MarketMakerPaymentVerification, Payment};
    use otc_models::{
        Currency, Lot, TokenIdentifier, MM_DEPOSIT_RISK_WINDOW, MM_NEVER_DEPOSITS_TIMEOUT,
    };
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    };
    use tokio::sync::mpsc;
    use tokio::task::{yield_now, JoinSet};
    use tokio::time::sleep;

    #[derive(Default)]
    struct RecordingBatchStore {
        persisted: Mutex<Vec<Vec<Uuid>>>,
        status_updates: Mutex<Vec<(String, BatchStatus)>>,
        fail_persist: AtomicBool,
        persisted_flag: Arc<AtomicBool>,
    }

    #[async_trait]
    impl BatchPaymentRecorder for RecordingBatchStore {
        async fn persist_prepared_batch(
            &self,
            swap_ids: Vec<Uuid>,
            _chain: ChainType,
            _batch_nonce_digest: [u8; 32],
            _aggregated_fee_sats: u64,
            _prepared_batch: &PreparedBatchPayment,
        ) -> crate::db::PaymentRepositoryResult<()> {
            if self.fail_persist.load(Ordering::Relaxed) {
                return Err(crate::db::PaymentRepositoryError::UnknownBatchStatus {
                    value: "persist failed".to_string(),
                });
            }
            self.persisted.lock().unwrap().push(swap_ids);
            self.persisted_flag.store(true, Ordering::Relaxed);
            Ok(())
        }

        async fn update_batch_status(
            &self,
            txid: &str,
            status: BatchStatus,
        ) -> crate::db::PaymentRepositoryResult<()> {
            self.status_updates
                .lock()
                .unwrap()
                .push((txid.to_string(), status));
            Ok(())
        }
    }

    impl RecordingBatchStore {
        fn swap_ids(&self) -> Vec<Vec<Uuid>> {
            self.persisted.lock().unwrap().clone()
        }

        fn statuses(&self) -> Vec<(String, BatchStatus)> {
            self.status_updates.lock().unwrap().clone()
        }
    }

    struct RecordingWallet {
        chain: ChainType,
        prepared_calls: Mutex<Vec<Vec<Payment>>>,
        broadcasted_txids: Mutex<Vec<String>>,
        discarded_txids: Mutex<Vec<String>>,
        fail_broadcast: AtomicBool,
        require_persist_before_broadcast: Option<Arc<AtomicBool>>,
        transaction_state: Mutex<TransactionState>,
    }

    impl RecordingWallet {
        fn new(chain: ChainType) -> Self {
            Self {
                chain,
                prepared_calls: Mutex::new(Vec::new()),
                broadcasted_txids: Mutex::new(Vec::new()),
                discarded_txids: Mutex::new(Vec::new()),
                fail_broadcast: AtomicBool::new(false),
                require_persist_before_broadcast: None,
                transaction_state: Mutex::new(TransactionState::Missing),
            }
        }

        fn calls(&self) -> Vec<Vec<Payment>> {
            self.prepared_calls.lock().unwrap().clone()
        }

        fn with_persist_assert(mut self, persisted_flag: Arc<AtomicBool>) -> Self {
            self.require_persist_before_broadcast = Some(persisted_flag);
            self
        }

        fn set_fail_broadcast(&self, should_fail: bool) {
            self.fail_broadcast.store(should_fail, Ordering::Relaxed);
        }

        fn set_transaction_state(&self, state: TransactionState) {
            *self.transaction_state.lock().unwrap() = state;
        }

        fn broadcasted_txids(&self) -> Vec<String> {
            self.broadcasted_txids.lock().unwrap().clone()
        }

        fn discarded_txids(&self) -> Vec<String> {
            self.discarded_txids.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl crate::wallet::Wallet for RecordingWallet {
        async fn consolidate(
            &self,
            _lot: &otc_models::Lot,
            _max_deposits_per_iteration: usize,
        ) -> crate::wallet::WalletResult<crate::wallet::ConsolidationSummary> {
            Ok(crate::wallet::ConsolidationSummary {
                total_amount: U256::ZERO,
                iterations: 0,
                tx_hashes: Vec::new(),
            })
        }

        async fn cancel_tx(&self, _tx_hash: &str) -> crate::wallet::WalletResult<String> {
            Ok("mock_txid_123".to_string())
        }

        async fn check_tx_confirmations(&self, _tx_hash: &str) -> crate::wallet::WalletResult<u64> {
            Ok(6) // Mock: always 6 confirmations
        }

        async fn prepare_batch_payment(
            &self,
            payments: Vec<Payment>,
            _mm_payment_validation: Option<MarketMakerPaymentVerification>,
        ) -> crate::wallet::WalletResult<PreparedBatchPayment> {
            self.prepared_calls.lock().unwrap().push(payments);
            Ok(match self.chain {
                ChainType::Bitcoin => PreparedBatchPayment::Bitcoin {
                    txid: "mock_tx".to_string(),
                    txdata: vec![1, 2, 3],
                    foreign_utxos: Vec::new(),
                    absolute_fee: 42,
                    reserved_deposit_keys: vec!["dep_a".to_string()],
                },
                ChainType::Ethereum | ChainType::Base => PreparedBatchPayment::Evm {
                    txid: "mock_tx".to_string(),
                    txdata: vec![1, 2, 3],
                    reserved_deposit_keys: vec!["dep_a".to_string()],
                },
            })
        }

        async fn broadcast_prepared_batch(
            &self,
            prepared_batch: &PreparedBatchPayment,
        ) -> crate::wallet::WalletResult<String> {
            if let Some(persisted_flag) = &self.require_persist_before_broadcast {
                assert!(
                    persisted_flag.load(Ordering::Relaxed),
                    "broadcast was attempted before durable persistence"
                );
            }
            let txid = prepared_batch.txid().to_string();
            if self.fail_broadcast.load(Ordering::Relaxed) {
                return Err(crate::wallet::WalletError::TransactionCreationFailed {
                    reason: "broadcast failed".to_string(),
                });
            }
            self.broadcasted_txids.lock().unwrap().push(txid.clone());
            Ok(txid)
        }

        async fn discard_prepared_batch(
            &self,
            prepared_batch: &PreparedBatchPayment,
        ) -> crate::wallet::WalletResult<()> {
            self.discarded_txids
                .lock()
                .unwrap()
                .push(prepared_batch.txid().to_string());
            Ok(())
        }

        async fn guarantee_confirmations(
            &self,
            _tx_hash: &str,
            _confirmations: u64,
            _poll_interval: std::time::Duration,
        ) -> crate::wallet::WalletResult<()> {
            Ok(())
        }

        async fn balance(
            &self,
            _token: &TokenIdentifier,
        ) -> crate::wallet::WalletResult<crate::wallet::WalletBalance> {
            // Return a large balance so tests can process payments
            let large_balance = U256::from(1_000_000_000u64);
            Ok(crate::wallet::WalletBalance {
                total_balance: large_balance,
                native_balance: large_balance,
                deposit_key_balance: U256::ZERO,
            })
        }

        fn receive_address(&self, _token: &TokenIdentifier) -> String {
            "mock_address".to_string()
        }

        fn chain_type(&self) -> ChainType {
            self.chain
        }

        async fn lookup_transaction_state(
            &self,
            _tx_hash: &str,
        ) -> crate::wallet::WalletResult<TransactionState> {
            Ok(*self.transaction_state.lock().unwrap())
        }
    }

    #[tokio::test]
    async fn filters_payments_close_to_refund_window() {
        let chain_type = ChainType::Bitcoin;
        let wallet_inner = Arc::new(RecordingWallet::new(chain_type));
        let wallet: Arc<dyn crate::wallet::Wallet> = wallet_inner.clone();
        let storage_inner = Arc::new(RecordingBatchStore::default());
        let storage: Arc<dyn BatchPaymentRecorder> = storage_inner.clone();
        let (tx, rx) = mpsc::unbounded_channel();
        let (otc_tx, mut otc_rx) = mpsc::unbounded_channel();
        let in_flight = Arc::new(DashMap::new());
        let mut join_set = JoinSet::new();
        let cancellation_token = CancellationToken::new();

        spawn_batch_processor(
            chain_type,
            wallet,
            storage,
            rx,
            BatchConfig {
                interval_secs: 1,
                batch_size: 10,
            },
            Arc::new(QuoteBalanceStrategy::new(10000)), // 100% for test (no balance constraints)
            otc_tx,
            in_flight.clone(),
            cancellation_token,
            &mut join_set,
        );

        let payment_currency = Currency {
            chain: chain_type,
            token: TokenIdentifier::Native,
            decimals: 8,
        };

        let lot = Lot {
            currency: payment_currency.clone(),
            amount: U256::from(1u64),
        };

        let threshold = MM_NEVER_DEPOSITS_TIMEOUT - MM_DEPOSIT_RISK_WINDOW;
        let now = utc::now();

        let eligible_swap = Uuid::now_v7();
        let eligible_payment = MarketMakerQueuedPayment {
            swap_id: eligible_swap,
            quote_id: Uuid::now_v7(),
            lot: lot.clone(),
            destination_address: "dest_a".to_string(),
            mm_nonce: [1u8; 16],
            user_deposit_confirmed_at: Some(now - (threshold - ChronoDuration::seconds(5))),
            protocol_fee: U256::from(300),
        };

        let ineligible_swap = Uuid::now_v7();
        let ineligible_payment = MarketMakerQueuedPayment {
            swap_id: ineligible_swap,
            quote_id: Uuid::now_v7(),
            lot,
            destination_address: "dest_b".to_string(),
            mm_nonce: [2u8; 16],
            user_deposit_confirmed_at: Some(now - (threshold + ChronoDuration::seconds(5))),
            protocol_fee: U256::from(300),
        };

        in_flight.insert(
            eligible_swap,
            InFlightPaymentSnapshot {
                swap_id: eligible_swap,
                quote_id: eligible_payment.quote_id,
                chain: chain_type,
                amount: eligible_payment.lot.amount,
                queued_at: now,
                user_deposit_confirmed_at: eligible_payment.user_deposit_confirmed_at.unwrap(),
            },
        );
        in_flight.insert(
            ineligible_swap,
            InFlightPaymentSnapshot {
                swap_id: ineligible_swap,
                quote_id: ineligible_payment.quote_id,
                chain: chain_type,
                amount: ineligible_payment.lot.amount,
                queued_at: now,
                user_deposit_confirmed_at: ineligible_payment.user_deposit_confirmed_at.unwrap(),
            },
        );

        tx.send(eligible_payment).unwrap();
        tx.send(ineligible_payment).unwrap();

        sleep(std::time::Duration::from_millis(50)).await;
        yield_now().await;

        let wallet_calls = wallet_inner.calls();
        assert_eq!(wallet_calls.len(), 1);
        assert_eq!(wallet_calls[0].len(), 1);
        assert_eq!(wallet_calls[0][0].to_address, "dest_a");

        assert_eq!(storage_inner.swap_ids(), vec![vec![eligible_swap]]);
        assert_eq!(
            storage_inner.statuses(),
            vec![("mock_tx".to_string(), BatchStatus::Created)]
        );

        assert!(in_flight.get(&ineligible_swap).is_none());
        assert!(in_flight.get(&eligible_swap).is_none());

        let response = otc_rx.recv().await.expect("batch response sent");
        match response.payload {
            MMResponse::Batches { batches, .. } => {
                assert_eq!(batches.len(), 1);
                assert_eq!(batches[0].swap_ids, vec![eligible_swap]);
            }
            other => panic!("unexpected response: {:?}", other),
        }

        join_set.abort_all();
        while join_set.join_next().await.is_some() {}
    }

    #[tokio::test]
    async fn persists_built_batch_before_broadcast() {
        let chain_type = ChainType::Bitcoin;
        let persisted_flag = Arc::new(AtomicBool::new(false));
        let wallet_inner = Arc::new(RecordingWallet::new(chain_type).with_persist_assert(
            persisted_flag.clone(),
        ));
        let wallet: Arc<dyn crate::wallet::Wallet> = wallet_inner.clone();
        let storage_inner = Arc::new(RecordingBatchStore {
            persisted_flag,
            ..Default::default()
        });
        let storage: Arc<dyn BatchPaymentRecorder> = storage_inner.clone();
        let (otc_tx, mut otc_rx) = mpsc::unbounded_channel();
        let in_flight = Arc::new(DashMap::new());
        let swap_id = Uuid::now_v7();
        let quote_id = Uuid::now_v7();
        let payment = MarketMakerQueuedPayment {
            swap_id,
            quote_id,
            lot: Lot {
                currency: Currency {
                    chain: chain_type,
                    token: TokenIdentifier::Native,
                    decimals: 8,
                },
                amount: U256::from(1u64),
            },
            destination_address: "dest_a".to_string(),
            mm_nonce: [1u8; 16],
            user_deposit_confirmed_at: Some(utc::now()),
            protocol_fee: U256::from(300),
        };
        in_flight.insert(
            swap_id,
            InFlightPaymentSnapshot {
                swap_id,
                quote_id,
                chain: chain_type,
                amount: payment.lot.amount,
                queued_at: utc::now(),
                user_deposit_confirmed_at: payment.user_deposit_confirmed_at.unwrap(),
            },
        );

        let result = process_batch(
            chain_type,
            &wallet,
            storage.as_ref(),
            &otc_tx,
            &in_flight,
            &[payment],
        )
        .await
        .expect("batch should succeed");

        assert_eq!(result, Some("mock_tx".to_string()));
        assert_eq!(storage_inner.swap_ids(), vec![vec![swap_id]]);
        assert_eq!(
            storage_inner.statuses(),
            vec![("mock_tx".to_string(), BatchStatus::Created)]
        );
        assert_eq!(wallet_inner.broadcasted_txids(), vec!["mock_tx".to_string()]);
        assert!(in_flight.get(&swap_id).is_none());
        let response = otc_rx.recv().await.expect("batch response sent");
        match response.payload {
            MMResponse::Batches { batches, .. } => {
                assert_eq!(batches.len(), 1);
                assert_eq!(batches[0].tx_hash, "mock_tx");
            }
            other => panic!("unexpected response: {:?}", other),
        }
    }

    #[tokio::test]
    async fn persist_failure_discards_prepared_batch_and_skips_broadcast() {
        let chain_type = ChainType::Bitcoin;
        let wallet_inner = Arc::new(RecordingWallet::new(chain_type));
        let wallet: Arc<dyn crate::wallet::Wallet> = wallet_inner.clone();
        let storage_inner = Arc::new(RecordingBatchStore::default());
        storage_inner.fail_persist.store(true, Ordering::Relaxed);
        let storage: Arc<dyn BatchPaymentRecorder> = storage_inner.clone();
        let (otc_tx, otc_rx) = mpsc::unbounded_channel();
        let in_flight = Arc::new(DashMap::new());
        let swap_id = Uuid::now_v7();
        let quote_id = Uuid::now_v7();
        let payment = MarketMakerQueuedPayment {
            swap_id,
            quote_id,
            lot: Lot {
                currency: Currency {
                    chain: chain_type,
                    token: TokenIdentifier::Native,
                    decimals: 8,
                },
                amount: U256::from(1u64),
            },
            destination_address: "dest_a".to_string(),
            mm_nonce: [1u8; 16],
            user_deposit_confirmed_at: Some(utc::now()),
            protocol_fee: U256::from(300),
        };
        in_flight.insert(
            swap_id,
            InFlightPaymentSnapshot {
                swap_id,
                quote_id,
                chain: chain_type,
                amount: payment.lot.amount,
                queued_at: utc::now(),
                user_deposit_confirmed_at: payment.user_deposit_confirmed_at.unwrap(),
            },
        );

        let result = process_batch(
            chain_type,
            &wallet,
            storage.as_ref(),
            &otc_tx,
            &in_flight,
            &[payment],
        )
        .await;

        assert!(result.is_err());
        assert_eq!(wallet_inner.broadcasted_txids(), Vec::<String>::new());
        assert_eq!(wallet_inner.discarded_txids(), vec!["mock_tx".to_string()]);
        assert!(in_flight.get(&swap_id).is_none());
        assert!(otc_rx.is_empty());
    }

    #[tokio::test]
    async fn broadcast_failure_after_durable_prepare_keeps_batch_durable() {
        let chain_type = ChainType::Bitcoin;
        let wallet_inner = Arc::new(RecordingWallet::new(chain_type));
        wallet_inner.set_fail_broadcast(true);
        let wallet: Arc<dyn crate::wallet::Wallet> = wallet_inner.clone();
        let storage_inner = Arc::new(RecordingBatchStore::default());
        let storage: Arc<dyn BatchPaymentRecorder> = storage_inner.clone();
        let (otc_tx, otc_rx) = mpsc::unbounded_channel();
        let in_flight = Arc::new(DashMap::new());
        let swap_id = Uuid::now_v7();
        let quote_id = Uuid::now_v7();
        let payment = MarketMakerQueuedPayment {
            swap_id,
            quote_id,
            lot: Lot {
                currency: Currency {
                    chain: chain_type,
                    token: TokenIdentifier::Native,
                    decimals: 8,
                },
                amount: U256::from(1u64),
            },
            destination_address: "dest_a".to_string(),
            mm_nonce: [1u8; 16],
            user_deposit_confirmed_at: Some(utc::now()),
            protocol_fee: U256::from(300),
        };
        in_flight.insert(
            swap_id,
            InFlightPaymentSnapshot {
                swap_id,
                quote_id,
                chain: chain_type,
                amount: payment.lot.amount,
                queued_at: utc::now(),
                user_deposit_confirmed_at: payment.user_deposit_confirmed_at.unwrap(),
            },
        );

        let result = process_batch(
            chain_type,
            &wallet,
            storage.as_ref(),
            &otc_tx,
            &in_flight,
            &[payment],
        )
        .await;

        assert!(result.is_err());
        assert_eq!(storage_inner.swap_ids(), vec![vec![swap_id]]);
        assert!(storage_inner.statuses().is_empty());
        assert_eq!(wallet_inner.discarded_txids(), Vec::<String>::new());
        assert!(in_flight.get(&swap_id).is_some());
        assert!(otc_rx.is_empty());
    }

    #[sqlx::test]
    async fn recover_prepared_batches_rebroadcasts_missing_transactions(
        pool: sqlx::PgPool,
    ) -> sqlx::Result<()> {
        let db = crate::db::Database::from_pool(pool).await.unwrap();
        let payment_repository = Arc::new(db.payments());
        let broadcasted_transaction_repository = Arc::new(db.broadcasted_transactions());

        let wallet_inner = Arc::new(RecordingWallet::new(ChainType::Base));
        wallet_inner.set_transaction_state(TransactionState::Missing);
        let wallet: Arc<dyn crate::wallet::Wallet> = wallet_inner.clone();

        let mut wallet_manager = WalletManager::new();
        wallet_manager.register(ChainType::Base, wallet);
        let wallet_manager = Arc::new(wallet_manager);

        let (otc_tx, _otc_rx) = mpsc::unbounded_channel();
        let mut join_set = JoinSet::new();
        let payment_manager = PaymentManager::new(
            wallet_manager,
            payment_repository.clone(),
            broadcasted_transaction_repository,
            HashMap::new(),
            Arc::new(QuoteBalanceStrategy::new(10_000)),
            otc_tx,
            CancellationToken::new(),
            &mut join_set,
        );

        payment_repository
            .persist_prepared_batch(
                vec![Uuid::now_v7()],
                ChainType::Base,
                [7u8; 32],
                0,
                &PreparedBatchPayment::Evm {
                    txid: "built_tx".to_string(),
                    txdata: vec![1, 2, 3],
                    reserved_deposit_keys: Vec::new(),
                },
            )
            .await
            .unwrap();

        payment_manager.recover_prepared_batches().await.unwrap();

        let created = payment_repository
            .get_batches_by_status(BatchStatus::Created)
            .await
            .unwrap();
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].txid, "built_tx");
        assert_eq!(wallet_inner.broadcasted_txids(), vec!["built_tx".to_string()]);

        Ok(())
    }

    #[sqlx::test]
    async fn recover_prepared_bitcoin_batches_rebroadcasts_missing_transactions(
        pool: sqlx::PgPool,
    ) -> sqlx::Result<()> {
        let db = crate::db::Database::from_pool(pool).await.unwrap();
        let payment_repository = Arc::new(db.payments());
        let broadcasted_transaction_repository = Arc::new(db.broadcasted_transactions());

        let wallet_inner = Arc::new(RecordingWallet::new(ChainType::Bitcoin));
        wallet_inner.set_transaction_state(TransactionState::Missing);
        let wallet: Arc<dyn crate::wallet::Wallet> = wallet_inner.clone();

        let mut wallet_manager = WalletManager::new();
        wallet_manager.register(ChainType::Bitcoin, wallet);
        let wallet_manager = Arc::new(wallet_manager);

        let (otc_tx, _otc_rx) = mpsc::unbounded_channel();
        let mut join_set = JoinSet::new();
        let payment_manager = PaymentManager::new(
            wallet_manager,
            payment_repository.clone(),
            broadcasted_transaction_repository,
            HashMap::new(),
            Arc::new(QuoteBalanceStrategy::new(10_000)),
            otc_tx,
            CancellationToken::new(),
            &mut join_set,
        );

        payment_repository
            .persist_prepared_batch(
                vec![Uuid::now_v7()],
                ChainType::Bitcoin,
                [11u8; 32],
                21,
                &PreparedBatchPayment::Bitcoin {
                    txid: "built_btc_tx".to_string(),
                    txdata: vec![4, 5, 6],
                    foreign_utxos: Vec::new(),
                    absolute_fee: 21,
                    reserved_deposit_keys: Vec::new(),
                },
            )
            .await
            .unwrap();

        payment_manager.recover_prepared_batches().await.unwrap();

        let created = payment_repository
            .get_batches_by_status(BatchStatus::Created)
            .await
            .unwrap();
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].txid, "built_btc_tx");
        assert_eq!(wallet_inner.broadcasted_txids(), vec!["built_btc_tx".to_string()]);

        Ok(())
    }

    #[sqlx::test]
    async fn recover_prepared_batches_promotes_pending_transactions_without_rebroadcast(
        pool: sqlx::PgPool,
    ) -> sqlx::Result<()> {
        let db = crate::db::Database::from_pool(pool).await.unwrap();
        let payment_repository = Arc::new(db.payments());
        let broadcasted_transaction_repository = Arc::new(db.broadcasted_transactions());

        let wallet_inner = Arc::new(RecordingWallet::new(ChainType::Base));
        wallet_inner.set_transaction_state(TransactionState::Pending);
        let wallet: Arc<dyn crate::wallet::Wallet> = wallet_inner.clone();

        let mut wallet_manager = WalletManager::new();
        wallet_manager.register(ChainType::Base, wallet);
        let wallet_manager = Arc::new(wallet_manager);

        let (otc_tx, _otc_rx) = mpsc::unbounded_channel();
        let mut join_set = JoinSet::new();
        let payment_manager = PaymentManager::new(
            wallet_manager,
            payment_repository.clone(),
            broadcasted_transaction_repository,
            HashMap::new(),
            Arc::new(QuoteBalanceStrategy::new(10_000)),
            otc_tx,
            CancellationToken::new(),
            &mut join_set,
        );

        payment_repository
            .persist_prepared_batch(
                vec![Uuid::now_v7()],
                ChainType::Base,
                [9u8; 32],
                0,
                &PreparedBatchPayment::Evm {
                    txid: "pending_tx".to_string(),
                    txdata: vec![1, 2, 3],
                    reserved_deposit_keys: Vec::new(),
                },
            )
            .await
            .unwrap();

        payment_manager.recover_prepared_batches().await.unwrap();

        let created = payment_repository
            .get_batches_by_status(BatchStatus::Created)
            .await
            .unwrap();
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].txid, "pending_tx");
        assert!(wallet_inner.broadcasted_txids().is_empty());

        Ok(())
    }
}
