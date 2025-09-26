use std::{str::FromStr, sync::Arc};
use std::time::Duration;

use bdk_esplora::{esplora_client, EsploraAsyncExt};
use bdk_wallet::KeychainKind;
use bdk_wallet::chain::spk_client::{FullScanRequestBuilder, FullScanResponse};
use bdk_wallet::{
    bitcoin::{self, Address, Amount, ScriptBuf},
    signer::SignOptions,
    CreateParams, PersistedWallet, Wallet,
};
use otc_chains::traits::MarketMakerPaymentValidation;
use otc_models::{ChainType, Lot};
use snafu::ResultExt;
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
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
    pub lot: Lot,
    pub to_address: String,
    pub foreign_utxos: Vec<ForeignUtxo>,
    pub mm_payment_validation: Option<MarketMakerPaymentValidation>,
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
            full_scan_wallet(&wallet, &connection, &esplora_client).await.expect("Initial full scan should succeed");

            while let Some(request) = request_rx.recv().await {
                let result = process_transaction(
                    &wallet,
                    &connection,
                    &esplora_client,
                    network,
                    request.lot,
                    request.to_address,
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
        lot: Lot,
        to_address: String,
        foreign_utxos: Vec<ForeignUtxo>,
        mm_payment_validation: Option<MarketMakerPaymentValidation>,
    ) -> Result<String, BitcoinWalletError> {
        let (response_tx, response_rx) = oneshot::channel();

        let request = TransactionRequest {
            lot,
            to_address,
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
    lot: Lot,
    to_address: String,
    foreign_utxos: Vec<ForeignUtxo>,
    mm_payment_validation: Option<MarketMakerPaymentValidation>,
) -> Result<String, BitcoinWalletError> {
    let start_time = Instant::now();

    info!(
        "Processing Bitcoin transaction to {} for {:?}",
        to_address, lot
    );

    // Parse the recipient address
    let address = Address::from_str(&to_address)
        .map_err(|e| BitcoinWalletError::InvalidAddress {
            reason: e.to_string(),
        })?
        .require_network(network)
        .map_err(|_| BitcoinWalletError::InvalidAddress {
            reason: format!(
                "Address {} is not valid for network {:?}",
                to_address, network
            ),
        })?;

    // Sync wallet before building transaction
    light_sync_wallet(wallet, connection, esplora_client).await?;

    // Lock wallet for transaction creation and persist immediately after building
    let mut wallet_guard = wallet.lock().await;

    // Check balance
    let balance = wallet_guard.balance();
    let amount_sats = lot.amount.to::<u64>();
    let amount = Amount::from_sat(amount_sats);
    info!("balance: {:?}", balance);

    // Build transaction
    let mut tx_builder = wallet_guard.build_tx();
    tx_builder.add_recipient(address.script_pubkey(), amount);

    // Add OP_RETURN output with nonce if provided
    if let Some(mm_payment_validation) = mm_payment_validation {
        let nonce = mm_payment_validation.embedded_nonce;
        let op_return_script = create_op_return_script(&nonce);
        tx_builder.add_recipient(op_return_script, Amount::ZERO);
        // Now handle fees
        let fee_amount = mm_payment_validation.fee_amount;
        let fee_address =
            Address::from_str(&otc_models::FEE_ADDRESSES_BY_CHAIN[&ChainType::Bitcoin])
                .unwrap()
                .assume_checked();
        tx_builder.add_recipient(
            fee_address.script_pubkey(),
            Amount::from_sat(fee_amount.to::<u64>()),
        );
    }

    for foreign_utxo in &foreign_utxos {
        tx_builder
            .add_foreign_utxo(
                foreign_utxo.outpoint,
                foreign_utxo.psbt_input.clone(),
                foreign_utxo.satisfaction_weight,
            )
            .context(AddForeignUtxoSnafu)?;
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

        // Broadcast the transaction
        let broadcast_start = Instant::now();
        if let Err(e) = esplora_client.broadcast(&tx).await {
            // If broadcast fails, cancel the transaction to free UTXOs
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
    
    wallet_guard.apply_update(update).context(BdkWalletCannotConnectSnafu)?;
    let mut conn = connection.lock().await;

    wallet_guard.persist(&mut conn).context(PersistWalletSnafu)?;

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

fn create_op_return_script(nonce: &[u8; 16]) -> ScriptBuf {
    bitcoin::blockdata::script::Builder::new()
        .push_opcode(bitcoin::opcodes::all::OP_RETURN)
        .push_slice(nonce)
        .into_script()
}