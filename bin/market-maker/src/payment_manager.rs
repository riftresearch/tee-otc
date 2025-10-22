// Middleware that prevents double-sending payments to the same swap request

use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dashmap::{mapref::entry::Entry, DashMap};
use otc_chains::traits::{MarketMakerQueuedPayment, MarketMakerQueuedPaymentExt};
use otc_models::{can_be_refunded_soon, ChainType, Lot};
use otc_protocols::mm::{MMErrorCode, MMResponse};
use tokio::{
    sync::mpsc::{self, UnboundedSender},
    task::JoinSet,
};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{db::PaymentRepository, wallet::WalletManager};
use otc_protocols::mm::ProtocolMessage;

// For tests
#[async_trait]
trait BatchPaymentRecorder: Send + Sync {
    async fn record_batch_payment(
        &self,
        swap_ids: Vec<Uuid>,
        txid: String,
        chain: ChainType,
        batch_nonce_digest: [u8; 32],
    ) -> crate::db::PaymentRepositoryResult<()>;
}

#[async_trait]
impl BatchPaymentRecorder for PaymentRepository {
    async fn record_batch_payment(
        &self,
        swap_ids: Vec<Uuid>,
        txid: String,
        chain: ChainType,
        batch_nonce_digest: [u8; 32],
    ) -> crate::db::PaymentRepositoryResult<()> {
        PaymentRepository::set_batch_payment(self, swap_ids, txid, chain, batch_nonce_digest).await
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
    /// Channels for queuing payments per chain
    bitcoin_tx: UnboundedSender<MarketMakerQueuedPayment>,
    ethereum_tx: UnboundedSender<MarketMakerQueuedPayment>,
    /// Tracks swap_ids that are currently queued but not yet broadcast
    /// Prevents duplicate queueing during websocket reconnect replays
    in_flight_payments: Arc<DashMap<Uuid, ()>>,
}

impl PaymentManager {
    pub fn new(
        wallet_manager: Arc<WalletManager>,
        payment_repository: Arc<PaymentRepository>,
        batch_configs: HashMap<ChainType, BatchConfig>,
        otc_response_tx: UnboundedSender<ProtocolMessage<MMResponse>>,
        join_set: &mut JoinSet<crate::Result<()>>,
    ) -> Self {
        // Create channels for each chain type
        let (bitcoin_tx, bitcoin_rx) = mpsc::unbounded_channel();
        let (ethereum_tx, ethereum_rx) = mpsc::unbounded_channel();

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
                    otc_response_tx.clone(),
                    in_flight_payments.clone(),
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
                    otc_response_tx.clone(),
                    in_flight_payments.clone(),
                    join_set,
                );
            }
        }

        Self {
            wallet_manager,
            payment_repository,
            bitcoin_tx,
            ethereum_tx,
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
                entry.insert(());
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
        };

        // Send to appropriate chain's channel
        let send_result = match expected_lot.currency.chain {
            ChainType::Bitcoin => self.bitcoin_tx.send(queued_payment),
            ChainType::Ethereum => self.ethereum_tx.send(queued_payment),
        };

        match send_result {
            Ok(()) => MMResponse::PaymentQueued {
                request_id: *request_id,
                swap_id: *swap_id,
                timestamp: utc::now(),
            },
            Err(_) => {
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
}

/// Spawns a background task that periodically processes batched payments for a specific chain
fn spawn_batch_processor(
    chain_type: ChainType,
    wallet: Arc<dyn crate::wallet::Wallet>,
    payment_recorder: Arc<dyn BatchPaymentRecorder>,
    mut rx: mpsc::UnboundedReceiver<MarketMakerQueuedPayment>,
    config: BatchConfig,
    otc_response_tx: UnboundedSender<ProtocolMessage<MMResponse>>,
    in_flight_payments: Arc<DashMap<Uuid, ()>>,
    join_set: &mut JoinSet<crate::Result<()>>,
) {
    join_set.spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(config.interval_secs));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        info!(
            "Starting batch processor for {:?} (interval: {}s, batch_size: {})",
            chain_type, config.interval_secs, config.batch_size
        );

        loop {
            // Drain up to batch_size items from the channel
            let mut queued_payments = Vec::new();
            while queued_payments.len() < config.batch_size {
                match rx.try_recv() {
                    Ok(payment) => queued_payments.push(payment),
                    Err(_) => break, // Channel is empty
                }
            }

            // If we don't have a full batch, wait for the interval before processing
            if queued_payments.len() < config.batch_size {
                if queued_payments.is_empty() {
                    // No items at all, wait for interval
                    interval.tick().await;
                    continue;
                } else {
                    // Have some items but not a full batch, wait for interval then process
                    interval.tick().await;
                }
            }

            // Split queued payments into eligible and ineligible (near refund window)
            let (eligible_payments, ineligible_payments): (Vec<_>, Vec<_>) = queued_payments
                .into_iter()
                .partition(|p| {
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
                interval.tick().await;
                continue;
            }


            info!(
                "Processing batch of {} payments for {:?}",
                eligible_payments.len(),
                chain_type
            );


            let payment_batch = match eligible_payments.to_market_maker_batch() {
                Some(payment_batch) => payment_batch,
                None => {
                    error!("No market maker batch could be created for {:?}: queued_payments={:?}", chain_type, eligible_payments);
                    continue;
                }
            };

            let batch_nonce_digest = payment_batch.payment_verification.batch_nonce_digest;

            // Execute the batch payment
            match wallet.create_batch_payment(payment_batch.ordered_payments, Some(payment_batch.payment_verification)).await {
                Ok(tx_hash) => {
                    let swap_ids: Vec<Uuid> = eligible_payments.iter().map(|qp| qp.swap_id).collect();
                    info!(
                        "Batch payment successful for {:?}: tx_hash={}, swap_ids={:?}",
                        chain_type, tx_hash, swap_ids
                    );

                    // Store all payments with the same tx_hash
                    if let Err(e) = payment_recorder
                        .record_batch_payment(
                            swap_ids.clone(),
                            tx_hash.clone(),
                            chain_type,
                            batch_nonce_digest,
                        )
                        .await
                    {
                        error!(
                            "Failed to store batch payment in database for {:?}: swap_ids={:?}, error={}",
                            chain_type, swap_ids, e
                        );
                    }

                    // Clear in-flight tracking now that payments are broadcast
                    for swap_id in &swap_ids {
                        in_flight_payments.remove(swap_id);
                    }

                    // Send batch payment notification to OTC server
                    let response = MMResponse::BatchPaymentSent {
                        request_id: Uuid::new_v4(),
                        tx_hash: tx_hash.clone(),
                        swap_ids: swap_ids.clone(),
                        batch_nonce_digest,
                        timestamp: utc::now(),
                    };

                    let protocol_msg = ProtocolMessage {
                        version: "1.0.0".to_string(),
                        sequence: 0, // Unsolicited message, sequence doesn't matter
                        payload: response,
                    };

                    if let Err(e) = otc_response_tx.send(protocol_msg) {
                        error!(
                            "Failed to send batch payment notification to OTC server for {:?}: swap_ids={:?}, error={}",
                            chain_type, swap_ids, e
                        );
                    } else {
                        info!(
                            "Sent batch payment notification to OTC server for {:?}: tx_hash={}, swap_ids={:?}",
                            chain_type, tx_hash, swap_ids
                        );
                    }
                }
                Err(e) => {
                    let swap_ids: Vec<Uuid> = eligible_payments.iter().map(|qp| qp.swap_id).collect();
                    error!(
                        "Batch payment failed for {:?}: swap_ids={:?}, error={}",
                        chain_type, swap_ids, e
                    );

                    // Clear in-flight tracking to allow retry on next reconnect replay
                    for swap_id in &swap_ids {
                        in_flight_payments.remove(swap_id);
                    }
                }
            }
        }
    });
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
    use std::sync::{Arc, Mutex};
    use tokio::sync::mpsc;
    use tokio::task::{yield_now, JoinSet};
    use tokio::time::sleep;

    #[derive(Default)]
    struct RecordingBatchStore {
        recorded: Mutex<Vec<Vec<Uuid>>>,
    }

    #[async_trait]
    impl BatchPaymentRecorder for RecordingBatchStore {
        async fn record_batch_payment(
            &self,
            swap_ids: Vec<Uuid>,
            _txid: String,
            _chain: ChainType,
            _batch_nonce_digest: [u8; 32],
        ) -> crate::db::PaymentRepositoryResult<()> {
            self.recorded.lock().unwrap().push(swap_ids);
            Ok(())
        }
    }

    impl RecordingBatchStore {
        fn swap_ids(&self) -> Vec<Vec<Uuid>> {
            self.recorded.lock().unwrap().clone()
        }
    }

    struct RecordingWallet {
        chain: ChainType,
        calls: Mutex<Vec<Vec<Payment>>>,
    }

    impl RecordingWallet {
        fn new(chain: ChainType) -> Self {
            Self {
                chain,
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<Vec<Payment>> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl crate::wallet::Wallet for RecordingWallet {
        async fn cancel_tx(&self, _tx_hash: &str) -> crate::wallet::WalletResult<String> {
            Ok("mock_txid_123".to_string())
        }

        async fn check_tx_confirmations(&self, _tx_hash: &str) -> crate::wallet::WalletResult<u64> {
            Ok(6) // Mock: always 6 confirmations
        }

        async fn create_batch_payment(
            &self,
            payments: Vec<Payment>,
            _mm_payment_validation: Option<MarketMakerPaymentVerification>,
        ) -> crate::wallet::WalletResult<String> {
            self.calls.lock().unwrap().push(payments);
            Ok("mock_tx".to_string())
        }

        async fn guarantee_confirmations(
            &self,
            _tx_hash: &str,
            _confirmations: u64,
        ) -> crate::wallet::WalletResult<()> {
            Ok(())
        }

        async fn balance(
            &self,
            _token: &TokenIdentifier,
        ) -> crate::wallet::WalletResult<crate::wallet::WalletBalance> {
            Ok(crate::wallet::WalletBalance {
                total_balance: U256::ZERO,
                native_balance: U256::ZERO,
                deposit_key_balance: U256::ZERO,
            })
        }

        fn receive_address(&self, _token: &TokenIdentifier) -> String {
            "mock_address".to_string()
        }

        fn chain_type(&self) -> ChainType {
            self.chain
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

        spawn_batch_processor(
            chain_type,
            wallet,
            storage,
            rx,
            BatchConfig {
                interval_secs: 1,
                batch_size: 10,
            },
            otc_tx,
            in_flight.clone(),
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

        let eligible_swap = Uuid::new_v4();
        let eligible_payment = MarketMakerQueuedPayment {
            swap_id: eligible_swap,
            quote_id: Uuid::new_v4(),
            lot: lot.clone(),
            destination_address: "dest_a".to_string(),
            mm_nonce: [1u8; 16],
            user_deposit_confirmed_at: Some(now - (threshold - ChronoDuration::seconds(5))),
        };

        let ineligible_swap = Uuid::new_v4();
        let ineligible_payment = MarketMakerQueuedPayment {
            swap_id: ineligible_swap,
            quote_id: Uuid::new_v4(),
            lot,
            destination_address: "dest_b".to_string(),
            mm_nonce: [2u8; 16],
            user_deposit_confirmed_at: Some(now - (threshold + ChronoDuration::seconds(5))),
        };

        in_flight.insert(eligible_swap, ());
        in_flight.insert(ineligible_swap, ());

        tx.send(eligible_payment).unwrap();
        tx.send(ineligible_payment).unwrap();

        sleep(std::time::Duration::from_millis(50)).await;
        yield_now().await;

        let wallet_calls = wallet_inner.calls();
        assert_eq!(wallet_calls.len(), 1);
        assert_eq!(wallet_calls[0].len(), 1);
        assert_eq!(wallet_calls[0][0].to_address, "dest_a");

        assert_eq!(storage_inner.swap_ids(), vec![vec![eligible_swap]]);

        assert!(in_flight.get(&ineligible_swap).is_none());
        assert!(in_flight.get(&eligible_swap).is_none());

        let response = otc_rx.recv().await.expect("batch response sent");
        match response.payload {
            MMResponse::BatchPaymentSent { swap_ids, .. } => {
                assert_eq!(swap_ids, vec![eligible_swap]);
            }
            other => panic!("unexpected response: {:?}", other),
        }

        join_set.abort_all();
        while join_set.join_next().await.is_some() {}
    }
}
