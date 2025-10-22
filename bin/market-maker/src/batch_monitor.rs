use std::sync::Arc;
use tokio::task::JoinSet;
use tokio::time::{interval, Duration};
use tracing::{error, info, warn};

use crate::db::{BatchStatus, PaymentRepository, StoredBatch};
use crate::wallet::WalletManager;
use otc_models::{can_be_refunded_soon_bc_mm_not_confirmed, ChainType};

const BITCOIN_MIN_CONFIRMATIONS: u64 = 2;

/// Spawns a background task that monitors batches and cancels those approaching refund window
pub fn spawn_batch_monitor(
    wallet_manager: Arc<WalletManager>,
    payment_repository: Arc<PaymentRepository>,
    polling_interval_secs: u64,
    join_set: &mut JoinSet<crate::Result<()>>,
) {
    join_set.spawn(async move {
        let mut tick = interval(Duration::from_secs(polling_interval_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        info!(
            "Starting batch monitor service (interval: {}s)",
            polling_interval_secs
        );

        loop {
            tick.tick().await;

            if let Err(e) = monitor_batches(&wallet_manager, &payment_repository).await {
                error!("Error monitoring batches: {}", e);
            }
        }
    });
}

async fn monitor_batches(
    wallet_manager: &WalletManager,
    payment_repository: &PaymentRepository,
) -> crate::Result<()> {
    // Query all batches with status='created'
    let batches = payment_repository
        .get_batches_by_status(BatchStatus::Created)
        .await
        .map_err(|e| {
            error!("Failed to query batches: {}", e);
            crate::Error::PaymentRepository { source: e }
        })?;

    if batches.is_empty() {
        return Ok(());
    }

    info!(
        "Monitoring {} batch(es) with status 'created'",
        batches.len()
    );

    for batch in batches {
        if let Err(e) = process_batch(wallet_manager, payment_repository, &batch).await {
            error!(
                "Error processing batch {} (chain: {:?}): {}",
                batch.txid, batch.chain, e
            );
        }
    }

    Ok(())
}

async fn process_batch(
    wallet_manager: &WalletManager,
    payment_repository: &PaymentRepository,
    batch: &StoredBatch,
) -> crate::Result<()> {
    let wallet = wallet_manager
        .get(batch.chain)
        .ok_or_else(|| crate::Error::GenericWallet {
            source: Box::new(crate::WalletError::WalletNotRegistered {
                chain_type: batch.chain,
            }),
        })?;

    // Check if the batch transaction is confirmed
    let is_confirmed = check_confirmation_status(wallet.as_ref(), &batch.txid, batch.chain).await?;

    if is_confirmed {
        info!(
            "Batch {} (chain: {:?}) is confirmed, updating status",
            batch.txid, batch.chain
        );
        payment_repository
            .update_batch_status(&batch.txid, BatchStatus::Confirmed)
            .await
            .map_err(|e| crate::Error::PaymentRepository { source: e })?;
        return Ok(());
    }

    // Check if the batch is approaching the refund window
    if can_be_refunded_soon_bc_mm_not_confirmed(batch.created_at) {
        warn!(
            "Batch {} (chain: {:?}) is approaching refund window and not confirmed, cancelling transaction",
            batch.txid, batch.chain
        );

        // Attempt to cancel the transaction
        match wallet.cancel_tx(&batch.txid).await {
            Ok(replacement_txid) => {
                info!(
                    "Successfully cancelled batch {} (chain: {:?}), replacement tx: {}",
                    batch.txid, batch.chain, replacement_txid
                );
                payment_repository
                    .update_batch_status(&batch.txid, BatchStatus::Cancelled)
                    .await
                    .map_err(|e| crate::Error::PaymentRepository { source: e })?;
            }
            Err(e) => {
                error!(
                    "Failed to cancel batch {} (chain: {:?}): {}",
                    batch.txid, batch.chain, e
                );
                // Don't fail the entire monitor loop, just log and continue
            }
        }
    }

    Ok(())
}

/// Check if a transaction has sufficient confirmations
async fn check_confirmation_status(
    wallet: &dyn crate::wallet::Wallet,
    txid: &str,
    chain: ChainType,
) -> crate::Result<bool> {
    match chain {
        ChainType::Bitcoin => check_bitcoin_confirmation(wallet, txid).await,
        ChainType::Ethereum => {
            // Mock implementation for Ethereum - always return true (confirmed)
            // since Ethereum batches aren't relevant for this feature
            info!("Ethereum batch {} marked as confirmed (mocked)", txid);
            Ok(true)
        }
    }
}

/// Check if a Bitcoin transaction has sufficient confirmations using the wallet's esplora client
async fn check_bitcoin_confirmation(
    wallet: &dyn crate::wallet::Wallet,
    txid: &str,
) -> crate::Result<bool> {
    // Use the wallet trait's check_tx_confirmations method
    match wallet.check_tx_confirmations(txid).await {
        Ok(confirmations) => {
            info!(
                "Bitcoin transaction {} has {} confirmations (required: {})",
                txid, confirmations, BITCOIN_MIN_CONFIRMATIONS
            );
            Ok(confirmations >= BITCOIN_MIN_CONFIRMATIONS)
        }
        Err(e) => {
            warn!(
                "Failed to check Bitcoin transaction {} confirmations: {}",
                txid, e
            );
            // If we can't check, assume not confirmed to be safe
            Ok(false)
        }
    }
}
