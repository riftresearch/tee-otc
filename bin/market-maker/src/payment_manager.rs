// Middleware that prevents double-sending payments to the same swap request

use std::{collections::HashMap, sync::Arc, time::Duration};

use dashmap::{mapref::entry::Entry, DashMap};
use otc_chains::traits::{MarketMakerQueuedPayment, MarketMakerQueuedPaymentExt};
use otc_models::{ChainType, Lot};
use otc_protocols::mm::{MMErrorCode, MMResponse};
use tokio::{sync::mpsc::{self, UnboundedSender}, task::JoinSet};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{payment_storage::PaymentStorage, wallet::WalletManager};
use otc_protocols::mm::ProtocolMessage;

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
    payment_storage: Arc<PaymentStorage>,
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
        payment_storage: Arc<PaymentStorage>,
        batch_configs: HashMap<ChainType, BatchConfig>,
        otc_response_tx: Option<UnboundedSender<ProtocolMessage<MMResponse>>>,
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
                    payment_storage.clone(),
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
                    payment_storage.clone(),
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
            payment_storage,
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
        user_destination_address: &String,
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
        match self.payment_storage.has_payment_been_made(*swap_id).await {
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
            destination_address: user_destination_address.clone(),
            mm_nonce: *mm_nonce,
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
            },
        }
    }
}

/// Spawns a background task that periodically processes batched payments for a specific chain
fn spawn_batch_processor(
    chain_type: ChainType,
    wallet: Arc<dyn crate::wallet::Wallet>,
    payment_storage: Arc<PaymentStorage>,
    mut rx: mpsc::UnboundedReceiver<MarketMakerQueuedPayment>,
    config: BatchConfig,
    otc_response_tx: Option<UnboundedSender<ProtocolMessage<MMResponse>>>,
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
            // If we have a full batch, process immediately without waiting

            info!(
                "Processing batch of {} payments for {:?}",
                queued_payments.len(),
                chain_type
            );

            let payment_batch = match queued_payments.to_market_maker_batch() {
                Some(payment_batch) => payment_batch,
                None => {
                    error!("No market maker batch cloud be created for {:?}: queued_payments={:?}", chain_type, queued_payments);
                    continue;
                }
            };

            let batch_nonce_digest = payment_batch.payment_verification.batch_nonce_digest;

            // Execute the batch payment
            match wallet.create_batch_payment(payment_batch.ordered_payments, Some(payment_batch.payment_verification)).await {
                Ok(tx_hash) => {
                    let swap_ids: Vec<Uuid> = queued_payments.iter().map(|qp| qp.swap_id).collect();
                    
                    info!(
                        "Batch payment successful for {:?}: tx_hash={}, swap_ids={:?}",
                        chain_type, tx_hash, swap_ids
                    );

                    // Store all payments with the same tx_hash
                    if let Err(e) = payment_storage.set_batch_payment(swap_ids.clone(), tx_hash.clone()).await {
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
                    if let Some(ref tx) = otc_response_tx {
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

                        if let Err(e) = tx.send(protocol_msg) {
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
                }
                Err(e) => {
                    let swap_ids: Vec<Uuid> = queued_payments.iter().map(|qp| qp.swap_id).collect();
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
