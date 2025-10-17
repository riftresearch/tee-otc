use std::{str::FromStr, sync::Arc};

use bdk_esplora::{esplora_client, EsploraAsyncExt};
use bdk_wallet::chain::spk_client::{FullScanRequestBuilder, FullScanResponse};
use bdk_wallet::KeychainKind;
use bdk_wallet::{
    bitcoin::{self, Address, Amount, ScriptBuf},
    signer::SignOptions,
    CreateParams, PersistedWallet, Wallet,
};
use otc_chains::traits::{MarketMakerPaymentVerification, Payment};
use otc_models::ChainType;
use snafu::ResultExt;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{error, info};

use crate::bitcoin_wallet::{
    AddForeignUtxoSnafu, BdkWalletCannotConnectSnafu, BroadcasterFailedSnafu,
    BuildTransactionSnafu, ExtractTransactionSnafu, PersistWalletSnafu, PsbtNotFinalizedSnafu,
    ReceiverFailedSnafu, SignTransactionSnafu, SyncWalletSnafu,
};

use super::{BitcoinWalletError, PARALLEL_REQUESTS};

pub struct TransactionRequest {
    pub payments: Vec<Payment>,
    pub foreign_utxos: Vec<ForeignUtxo>,
    pub mm_payment_validation: Option<MarketMakerPaymentVerification>,
    pub response_tx: oneshot::Sender<Result<String, BitcoinWalletError>>,
}

pub struct BitcoinTransactionBroadcaster {
    request_tx: mpsc::UnboundedSender<TransactionRequest>,
}

impl BitcoinTransactionBroadcaster {
    pub fn new(
        wallet: Arc<Mutex<PersistedWallet<bdk_wallet::rusqlite::Connection>>>,
        connection: Arc<Mutex<bdk_wallet::rusqlite::Connection>>,
        esplora_client: Arc<esplora_client::AsyncClient>,
        network: bitcoin::Network,
        join_set: &mut JoinSet<crate::Result<()>>,
    ) -> Self {
        let (request_tx, mut request_rx) = mpsc::unbounded_channel::<TransactionRequest>();
        join_set.spawn(async move {
            info!("Bitcoin transaction broadcaster started");
            full_scan_wallet(&wallet, &connection, &esplora_client)
                .await
                .expect("Initial full scan should succeed");

            while let Some(request) = request_rx.recv().await {
                let result = process_transaction(
                    &wallet,
                    &connection,
                    &esplora_client,
                    network,
                    request.payments,
                    request.foreign_utxos,
                    request.mm_payment_validation,
                )
                .await;

                if let Err(e) = request.response_tx.send(result) {
                    error!("Failed to send transaction response: {:?}", e);
                }
            }

            info!("Bitcoin transaction broadcaster stopped");
            Ok(())
        });

        Self { request_tx }
    }

    pub async fn broadcast_transaction(
        &self,
        payments: Vec<Payment>,
        foreign_utxos: Vec<ForeignUtxo>,
        mm_payment_validation: Option<MarketMakerPaymentVerification>,
    ) -> Result<String, BitcoinWalletError> {
        let (response_tx, response_rx) = oneshot::channel();

        let request = TransactionRequest {
            payments,
            foreign_utxos,
            mm_payment_validation,
            response_tx,
        };

        self.request_tx
            .send(request)
            .context(BroadcasterFailedSnafu)?;

        response_rx.await.context(ReceiverFailedSnafu)?
    }
}

#[derive(Debug, Clone)]
pub struct ForeignUtxo {
    pub outpoint: bdk_esplora::esplora_client::OutPoint,
    pub psbt_input: bdk_wallet::bitcoin::psbt::Input,
    pub satisfaction_weight: bdk_wallet::bitcoin::Weight,
    pub foreign_descriptor: String,
}

async fn process_transaction(
    wallet: &Arc<Mutex<PersistedWallet<bdk_wallet::rusqlite::Connection>>>,
    connection: &Arc<Mutex<bdk_wallet::rusqlite::Connection>>,
    esplora_client: &Arc<esplora_client::AsyncClient>,
    network: bitcoin::Network,
    payments: Vec<Payment>,
    foreign_utxos: Vec<ForeignUtxo>,
    mm_payment_validation: Option<MarketMakerPaymentVerification>,
) -> Result<String, BitcoinWalletError> {
    let start_time = Instant::now();

    if payments.len() > 1 {
        info!("Processing batch bitcoin payments to {:?}", payments);
    } else {
        info!("Processing bitcoin payment to {:?}", payments[0]);
    }

    // Parse the recipient address
    let mut payment_tuple: Vec<(Address, Amount)> = vec![];

    for payment in payments {
        let address = Address::from_str(&payment.to_address)
            .map_err(|e| BitcoinWalletError::InvalidAddress {
                reason: e.to_string(),
            })?
            .require_network(network)
            .map_err(|_| BitcoinWalletError::InvalidAddress {
                reason: format!(
                    "Address {} is not valid for network {:?}",
                    payment.to_address, network
                ),
            })?;
        payment_tuple.push((address, Amount::from_sat(payment.lot.amount.to::<u64>())));
    }

    // Sync wallet before building transaction
    light_sync_wallet(wallet, connection, esplora_client).await?;

    // Lock wallet for transaction creation and persist immediately after building
    let mut wallet_guard = wallet.lock().await;

    // Check balance
    let balance = wallet_guard.balance();
    info!("Wallet Balance before payment: {:?}", balance);

    // Build transaction
    let mut tx_builder = wallet_guard.build_tx();

    tx_builder.ordering(bdk_wallet::TxOrdering::Untouched);

    // Add foreign UTXOs first
    for foreign_utxo in &foreign_utxos {
        tx_builder
            .add_foreign_utxo(
                foreign_utxo.outpoint,
                foreign_utxo.psbt_input.clone(),
                foreign_utxo.satisfaction_weight,
            )
            .context(AddForeignUtxoSnafu)?;
    }

    // Then actual recipients
    for (address, amount) in payment_tuple {
        tx_builder.add_recipient(address.script_pubkey(), amount);
    }

    // Then fee DIRECTLY after the last recipient && OP_RETURN w/ nonce if this is a market maker payment
    if let Some(mm_payment_validation) = mm_payment_validation {
        // Now handle fees
        let fee_amount = mm_payment_validation.aggregated_fee;
        let fee_address =
            Address::from_str(&otc_models::FEE_ADDRESSES_BY_CHAIN[&ChainType::Bitcoin])
                .unwrap()
                .assume_checked();
        tx_builder.add_recipient(
            fee_address.script_pubkey(),
            Amount::from_sat(fee_amount.to::<u64>()),
        );

        // Then OP_RETURN w/ nonce
        let nonce = mm_payment_validation.batch_nonce_digest;
        let op_return_script = create_op_return_script(&nonce);
        tx_builder.add_recipient(op_return_script, Amount::ZERO);
    }



    tx_builder.nlocktime(crate::bitcoin::absolute::LockTime::ZERO);

    // Create and sign the transaction
    let build_start = Instant::now();
    let mut psbt = tx_builder.finish().context(BuildTransactionSnafu)?;
    info!("Transaction built in {:?}", build_start.elapsed());

    // CRITICAL: Persist immediately after building to lock UTXOs
    let mut conn = connection.lock().await;
    wallet_guard
        .persist(&mut conn)
        .context(PersistWalletSnafu)?;
    drop(conn);

    // Sign with wallet
    let finalized = wallet_guard
        .sign(&mut psbt, SignOptions::default())
        .context(SignTransactionSnafu)?;

    // We no longer need the wallet lock
    drop(wallet_guard);

    // Now loop through all the private keys we have and sign the psbt with each
    let mut fully_finalized = finalized;
    for foreign_utxo in foreign_utxos {
        info!(
            "Signing transaction with foreign descriptor: {:?}",
            foreign_utxo.foreign_descriptor
        );
        let temp_wallet =
            Wallet::create_with_params(CreateParams::new_single(foreign_utxo.foreign_descriptor))
                .expect("valid wallet");
        fully_finalized = temp_wallet
            .sign(&mut psbt, SignOptions::default())
            .context(SignTransactionSnafu)?;
    }

    if !fully_finalized {
        // If signing failed, we need to cancel the transaction to free UTXOs
        let mut wallet_guard = wallet.lock().await;
        if let Ok(tx) = psbt.extract_tx() {
            wallet_guard.cancel_tx(&tx);
            let mut conn = connection.lock().await;
            let _ = wallet_guard.persist(&mut conn);
        }
        PsbtNotFinalizedSnafu.fail()?
    } else {
        // Extract transaction
        let tx = psbt.extract_tx().context(ExtractTransactionSnafu)?;
        let txid = tx.compute_txid().to_string();

        // Broadcast the transaction with retry logic for mempool chain errors
        let broadcast_start = Instant::now();
        const MAX_MEMPOOL_RETRIES: u32 = 10;
        let mut retry_count = 0;

        let broadcast_result = loop {
            match esplora_client.broadcast(&tx).await {
                Ok(_) => break Ok(()),
                Err(e) => {
                    if is_mempool_chain_error(&e) && retry_count < MAX_MEMPOOL_RETRIES {
                        retry_count += 1;
                        info!(
                            "Mempool chain error detected (attempt {}/{MAX_MEMPOOL_RETRIES}): {:?}. Waiting for new block...",
                            retry_count, e
                        );
                        wait_for_new_block(esplora_client).await?;
                        continue;
                    }
                    break Err(e);
                }
            }
        };

        if let Err(e) = broadcast_result {
            let mut wallet_guard = wallet.lock().await;
            wallet_guard.cancel_tx(&tx);
            let mut conn = connection.lock().await;
            wallet_guard
                .persist(&mut conn)
                .context(PersistWalletSnafu)?;
            return Err(BitcoinWalletError::BroadcastTransaction { source: e });
        }

        info!("Transaction broadcast in {:?}", broadcast_start.elapsed());

        // Sync after broadcast to update wallet state
        light_sync_wallet(wallet, connection, esplora_client).await?;

        let total_duration = start_time.elapsed();
        info!(
            "Bitcoin transaction created and broadcast successfully: {} (total time: {:?})",
            txid, total_duration
        );

        Ok(txid)
    }
}

async fn full_scan_wallet(
    wallet: &Arc<Mutex<PersistedWallet<bdk_wallet::rusqlite::Connection>>>,
    connection: &Arc<Mutex<bdk_wallet::rusqlite::Connection>>,
    esplora_client: &Arc<esplora_client::AsyncClient>,
) -> Result<(), BitcoinWalletError> {
    let mut wallet_guard = wallet.lock().await;
    let full_scan_request: FullScanRequestBuilder<KeychainKind> = wallet_guard.start_full_scan();
    let update: FullScanResponse<KeychainKind> = esplora_client
        // stop gap isnt relevant as long as we do simple single address wallets
        .full_scan(full_scan_request, 5, PARALLEL_REQUESTS)
        .await
        .context(SyncWalletSnafu)?;

    wallet_guard
        .apply_update(update)
        .context(BdkWalletCannotConnectSnafu)?;
    let mut conn = connection.lock().await;

    wallet_guard
        .persist(&mut conn)
        .context(PersistWalletSnafu)?;

    Ok(())
}

async fn light_sync_wallet(
    wallet: &Arc<Mutex<PersistedWallet<bdk_wallet::rusqlite::Connection>>>,
    connection: &Arc<Mutex<bdk_wallet::rusqlite::Connection>>,
    esplora_client: &Arc<esplora_client::AsyncClient>,
) -> Result<(), BitcoinWalletError> {
    let sync_start = Instant::now();
    let mut wallet_guard = wallet.lock().await;

    let request = wallet_guard.start_sync_with_revealed_spks().build();
    let update = esplora_client
        .sync(request, PARALLEL_REQUESTS)
        .await
        .context(SyncWalletSnafu)?;

    wallet_guard
        .apply_update(update)
        .context(BdkWalletCannotConnectSnafu)?;

    let mut conn = connection.lock().await;
    wallet_guard
        .persist(&mut conn)
        .context(PersistWalletSnafu)?;

    info!("Wallet sync completed in {:?}", sync_start.elapsed());
    Ok(())
}

fn create_op_return_script(nonce: &[u8; 32]) -> ScriptBuf {
    bitcoin::blockdata::script::Builder::new()
        .push_opcode(bitcoin::opcodes::all::OP_RETURN)
        .push_slice(nonce)
        .into_script()
}

/// Check if an esplora error is a mempool chain error (too many unconfirmed ancestors)
fn is_mempool_chain_error(error: &bdk_esplora::esplora_client::Error) -> bool {
    let error_str = format!("{:?}", error);
    error_str.contains("too-long-mempool-chain")
        || error_str.contains("too many unconfirmed ancestors")
}

/// Wait for a new Bitcoin block to be mined
async fn wait_for_new_block(
    esplora_client: &Arc<esplora_client::AsyncClient>,
) -> Result<(), BitcoinWalletError> {
    const POLL_INTERVAL_SECS: u64 = 5;
    const MAX_WAIT_MINUTES: u64 = 30;

    let initial_height = esplora_client
        .get_height()
        .await
        .map_err(Box::new)
        .context(SyncWalletSnafu)?;

    info!(
        "Waiting for new block (current height: {})...",
        initial_height
    );

    let start = Instant::now();
    let max_wait = std::time::Duration::from_secs(MAX_WAIT_MINUTES * 60);

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(POLL_INTERVAL_SECS)).await;

        let current_height = esplora_client
            .get_height()
            .await
            .map_err(Box::new)
            .context(SyncWalletSnafu)?;

        if current_height > initial_height {
            info!(
                "New block mined! Height increased from {} to {} (waited {:?})",
                initial_height,
                current_height,
                start.elapsed()
            );
            return Ok(());
        }

        if start.elapsed() > max_wait {
            error!(
                "Timeout waiting for new block after {:?} (height still {})",
                start.elapsed(),
                current_height
            );
            return Ok(()); // Continue anyway after timeout
        }
    }
}
