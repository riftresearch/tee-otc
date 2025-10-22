use std::collections::HashSet;
use std::{str::FromStr, sync::Arc};

use bdk_esplora::{esplora_client, EsploraAsyncExt};
use bdk_wallet::bitcoin::{FeeRate, Sequence};
use bdk_wallet::chain::spk_client::{FullScanRequestBuilder, FullScanResponse};
use bdk_wallet::KeychainKind;
use bdk_wallet::{
    bitcoin::{self, Address, Amount, ScriptBuf},
    signer::SignOptions,
    CreateParams, PersistedWallet, Wallet,
};
use esplora_client::OutPoint;
use otc_chains::traits::{MarketMakerPaymentVerification, Payment};
use otc_models::ChainType;
use serde::{Deserialize, Serialize};
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
use crate::db::BroadcastedTransactionRepository;

use super::{BitcoinWalletError, PARALLEL_REQUESTS};

pub struct TransactionRequest {
    pub payments: Vec<Payment>,
    pub foreign_utxos: Vec<ForeignUtxo>,
    pub mm_payment_validation: Option<MarketMakerPaymentVerification>,
    pub response_tx: oneshot::Sender<Result<String, BitcoinWalletError>>,
    pub explicit_absolute_fee: Option<u64>,
}

pub struct BitcoinTransactionBroadcaster {
    request_tx: mpsc::UnboundedSender<TransactionRequest>,
}

impl BitcoinTransactionBroadcaster {
    pub fn new(
        wallet: Arc<Mutex<PersistedWallet<bdk_wallet::rusqlite::Connection>>>,
        wallet_descriptor: &str,
        connection: Arc<Mutex<bdk_wallet::rusqlite::Connection>>,
        esplora_client: Arc<esplora_client::AsyncClient>,
        network: bitcoin::Network,
        broadcasted_transaction_repository: Option<Arc<BroadcastedTransactionRepository>>,
        join_set: &mut JoinSet<crate::Result<()>>,
    ) -> Self {
        let (request_tx, mut request_rx) = mpsc::unbounded_channel::<TransactionRequest>();
        let main_wallet_descriptor = wallet_descriptor.to_string();
        join_set.spawn(async move {
            info!("Bitcoin transaction broadcaster started");
            full_scan_wallet(&wallet, &connection, &esplora_client)
                .await
                .expect("Initial full scan should succeed");

            while let Some(request) = request_rx.recv().await {
                let result = process_transaction(
                    &wallet,
                    &main_wallet_descriptor,
                    &connection,
                    &esplora_client,
                    network,
                    &broadcasted_transaction_repository,
                    request.payments,
                    request.explicit_absolute_fee,
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
        explicit_absolute_fee: Option<u64>,
    ) -> Result<String, BitcoinWalletError> {
        let (response_tx, response_rx) = oneshot::channel();

        let request = TransactionRequest {
            payments,
            foreign_utxos,
            mm_payment_validation,
            response_tx,
            explicit_absolute_fee,
        };

        self.request_tx
            .send(request)
            .context(BroadcasterFailedSnafu)?;

        response_rx.await.context(ReceiverFailedSnafu)?
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignUtxo {
    pub outpoint: bdk_esplora::esplora_client::OutPoint,
    pub psbt_input: bdk_wallet::bitcoin::psbt::Input,
    pub satisfaction_weight: bdk_wallet::bitcoin::Weight,
    pub foreign_descriptor: String,
}

#[allow(clippy::too_many_arguments)]
async fn process_transaction(
    wallet: &Arc<Mutex<PersistedWallet<bdk_wallet::rusqlite::Connection>>>,
    main_wallet_descriptor: &str,
    connection: &Arc<Mutex<bdk_wallet::rusqlite::Connection>>,
    esplora_client: &Arc<esplora_client::AsyncClient>,
    network: bitcoin::Network,
    broadcasted_transaction_repository: &Option<Arc<BroadcastedTransactionRepository>>,
    payments: Vec<Payment>,
    explicit_absolute_fee: Option<u64>,
    foreign_utxos: Vec<ForeignUtxo>,
    mm_payment_validation: Option<MarketMakerPaymentVerification>,
) -> Result<String, BitcoinWalletError> {
    let start_time = Instant::now();

    let is_consolidation_tx = if payments.len() > 1 {
        info!("Processing batch bitcoin payments to {:?}", payments);
        false
    } else if payments.len() == 1 {
        info!("Processing bitcoin payment to {:?}", payments[0]);
        false
    } else {
        info!("Processing a consolidation transaction");
        true
    };

    // Parse the recipient address
    let mut payment_tuple: Vec<(Address, Amount)> = vec![];

    for payment in &payments {
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
    let script_pubkey = wallet_guard
        .next_unused_address(KeychainKind::External)
        .script_pubkey();

    // Check balance
    let balance = wallet_guard.balance();
    info!("Wallet Balance before payment: {:?}", balance);

    // Build transaction
    let mut tx_builder = wallet_guard.build_tx();

    if is_consolidation_tx {
        tx_builder.drain_to(script_pubkey);
    }

    if let Some(explicit_absolute_fee) = explicit_absolute_fee {
        tx_builder.fee_absolute(Amount::from_sat(explicit_absolute_fee));
    } else {
        let fee_estimates = esplora_client
            .get_fee_estimates()
            .await
            .map_err(Box::new)
            .context(SyncWalletSnafu)?;
        let explicit_fee_rate = *fee_estimates.get(&1).unwrap_or(&1.1) as f64;
        info!("Using fee rate: {:?} sats/vbyte", explicit_fee_rate);
        let fee_rate = from_sat_per_vb_f64(explicit_fee_rate).unwrap();
        tx_builder.fee_rate(fee_rate);
    }

    tx_builder.ordering(bdk_wallet::TxOrdering::Untouched);
    tx_builder.nlocktime(crate::bitcoin::absolute::LockTime::ZERO);
    tx_builder.set_exact_sequence(Sequence::ENABLE_RBF_NO_LOCKTIME);

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
    for foreign_utxo in &foreign_utxos {
        info!(
            "Signing transaction with foreign descriptor: {:?}",
            foreign_utxo.foreign_descriptor
        );
        let temp_wallet = Wallet::create_with_params(CreateParams::new_single(
            foreign_utxo.foreign_descriptor.clone(),
        ))
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
        let absolute_fee = psbt.fee().unwrap().to_sat();

        let (tx, fully_qualified_foreign_utxos) = build_tx_and_get_fully_qualified_foreign_utxos(
            &psbt,
            &foreign_utxos,
            main_wallet_descriptor,
        )
        .map_err(|e| *e)?;

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

        if let Some(broadcasted_transaction_repository) = broadcasted_transaction_repository {
            broadcasted_transaction_repository
                .set_broadcasted_transaction(
                    &txid,
                    ChainType::Bitcoin,
                    bitcoin::consensus::serialize(&tx),
                    Some(fully_qualified_foreign_utxos),
                    absolute_fee,
                )
                .await
                .context(crate::bitcoin_wallet::BroadcastedTransactionRepositorySnafu)?;
        }

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

fn build_tx_and_get_fully_qualified_foreign_utxos(
    psbt: &bdk_wallet::bitcoin::psbt::Psbt,
    objectively_foreign_utxos: &[ForeignUtxo],
    main_wallet_descriptor: &str,
) -> Result<(bitcoin::Transaction, Vec<ForeignUtxo>), Box<BitcoinWalletError>> {
    // Extract transaction
    let tx = psbt
        .clone()
        .extract_tx()
        .context(ExtractTransactionSnafu)
        .map_err(Box::new)?;

    let objectively_foreign_utxos_set: HashSet<OutPoint> = objectively_foreign_utxos
        .iter()
        .map(|utxo| utxo.outpoint)
        .collect();
    let main_wallet_utxos = psbt
        .inputs
        .iter()
        .zip(tx.input.iter())
        .filter(|(_, txin)| !objectively_foreign_utxos_set.contains(&txin.previous_output))
        .map(|(input, txin)| {
            let satisfaction_weight = bdk_wallet::bitcoin::Weight::from_wu(108);
            let psbt_input = bdk_wallet::bitcoin::psbt::Input {
                witness_utxo: input.witness_utxo.clone(),
                non_witness_utxo: input.non_witness_utxo.clone(),
                ..Default::default()
            };
            ForeignUtxo {
                outpoint: txin.previous_output,
                psbt_input,
                satisfaction_weight,
                foreign_descriptor: main_wallet_descriptor.to_string(),
            }
        })
        .collect::<Vec<ForeignUtxo>>();

    let mut total_utxos = main_wallet_utxos;
    total_utxos.extend(objectively_foreign_utxos.iter().cloned());

    Ok((tx, total_utxos))
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
fn from_sat_per_vb_f64(sat_vb: f64) -> Option<FeeRate> {
    if sat_vb.is_finite() && sat_vb >= 0.0 {
        let kwu = (sat_vb * 250.0).round();
        if kwu <= u64::MAX as f64 {
            return Some(FeeRate::from_sat_per_kwu(kwu as u64));
        }
    }
    None
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
