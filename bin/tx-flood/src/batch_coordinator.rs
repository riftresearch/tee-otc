use std::time::Duration;

use anyhow::Result;
use otc_models::{ChainType, Lot};
use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    time::{interval, MissedTickBehavior},
};
use tracing::{debug, info, warn};

use crate::wallets::PaymentWallets;

/// Request to make a payment, either individually or as part of a batch
#[derive(Debug)]
pub struct PaymentRequest {
    pub swap_index: usize,
    pub lot: Lot,
    pub recipient: String,
    pub response_tx: tokio::sync::oneshot::Sender<Result<(String, String)>>,
}

/// Coordinates batching of payments across multiple swap tasks
#[derive(Clone)]
pub struct BatchCoordinator {
    tx: UnboundedSender<PaymentRequest>,
}

impl BatchCoordinator {
    /// Creates a new batch coordinator and spawns background tasks
    pub fn new(
        wallets: PaymentWallets,
        ethereum_interval: Duration,
        bitcoin_interval: Duration,
    ) -> Self {
        let (tx, rx) = unbounded_channel();

        // Spawn the batch processor
        tokio::spawn(run_batch_processor(
            wallets,
            rx,
            ethereum_interval,
            bitcoin_interval,
        ));

        Self { tx }
    }

    /// Submits a payment request to be batched
    pub async fn submit_payment(
        &self,
        swap_index: usize,
        lot: &Lot,
        recipient: &str,
    ) -> Result<(String, String)> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        let request = PaymentRequest {
            swap_index,
            lot: lot.clone(),
            recipient: recipient.to_string(),
            response_tx,
        };

        self.tx
            .send(request)
            .map_err(|_| anyhow::anyhow!("batch coordinator has shut down"))?;

        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("batch coordinator dropped response channel"))?
    }
}

/// Main batch processing loop
async fn run_batch_processor(
    wallets: PaymentWallets,
    mut rx: UnboundedReceiver<PaymentRequest>,
    ethereum_interval: Duration,
    bitcoin_interval: Duration,
) {
    // Separate queues for each chain
    let mut ethereum_queue: Vec<PaymentRequest> = Vec::new();
    let mut bitcoin_queue: Vec<PaymentRequest> = Vec::new();

    // Timers for each chain
    let mut ethereum_timer = interval(ethereum_interval);
    ethereum_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    
    let mut bitcoin_timer = interval(bitcoin_interval);
    bitcoin_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

    // Skip the first tick (which fires immediately)
    ethereum_timer.tick().await;
    bitcoin_timer.tick().await;

    loop {
        tokio::select! {
            // Receive new payment requests
            Some(request) = rx.recv() => {
                match request.lot.currency.chain {
                    ChainType::Ethereum => {
                        debug!(
                            swap_index = request.swap_index,
                            "queued Ethereum payment for batching"
                        );
                        ethereum_queue.push(request);
                    }
                    ChainType::Bitcoin => {
                        debug!(
                            swap_index = request.swap_index,
                            "queued Bitcoin payment for batching"
                        );
                        bitcoin_queue.push(request);
                    }
                }
            }

            // Ethereum batch interval elapsed
            _ = ethereum_timer.tick() => {
                if !ethereum_queue.is_empty() {
                    let batch = std::mem::take(&mut ethereum_queue);
                    let wallets = wallets.clone();
                    tokio::spawn(async move {
                        process_batch(wallets, batch, ChainType::Ethereum).await;
                    });
                }
            }

            // Bitcoin batch interval elapsed
            _ = bitcoin_timer.tick() => {
                if !bitcoin_queue.is_empty() {
                    let batch = std::mem::take(&mut bitcoin_queue);
                    let wallets = wallets.clone();
                    tokio::spawn(async move {
                        process_batch(wallets, batch, ChainType::Bitcoin).await;
                    });
                }
            }

            // Channel closed, flush remaining batches and exit
            else => {
                if !ethereum_queue.is_empty() {
                    process_batch(wallets.clone(), ethereum_queue, ChainType::Ethereum).await;
                }
                if !bitcoin_queue.is_empty() {
                    process_batch(wallets.clone(), bitcoin_queue, ChainType::Bitcoin).await;
                }
                break;
            }
        }
    }

    info!("batch coordinator shut down");
}

/// Processes a batch of payments for a specific chain
async fn process_batch(
    wallets: PaymentWallets,
    batch: Vec<PaymentRequest>,
    chain: ChainType,
) {
    let batch_size = batch.len();
    info!(chain = ?chain, size = batch_size, "processing payment batch");

    // Extract swap indices for logging
    let swap_indices: Vec<usize> = batch.iter().map(|r| r.swap_index).collect();

    // Execute the batch payment
    let result = wallets
        .create_batch_payment(
            &batch.iter().map(|r| &r.lot).collect::<Vec<_>>(),
            &batch.iter().map(|r| r.recipient.as_str()).collect::<Vec<_>>(),
        )
        .await;

    match result {
        Ok((tx_hash, sender_address)) => {
            info!(
                chain = ?chain,
                tx_hash = %tx_hash,
                batch_size = batch_size,
                swap_indices = ?swap_indices,
                "batch payment successful"
            );

            // Send success to all waiting tasks
            for request in batch {
                let _ = request
                    .response_tx
                    .send(Ok((tx_hash.clone(), sender_address.clone())));
            }
        }
        Err(err) => {
            warn!(
                chain = ?chain,
                error = %err,
                batch_size = batch_size,
                swap_indices = ?swap_indices,
                "batch payment failed"
            );

            // Send error to all waiting tasks
            let error_msg = err.to_string();
            for request in batch {
                let _ = request
                    .response_tx
                    .send(Err(anyhow::anyhow!("batch payment failed: {}", error_msg)));
            }
        }
    }
}

