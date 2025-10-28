pub mod transaction_broadcaster;

use std::{str::FromStr, sync::Arc, time::Duration};

use ::esplora_client::{OutPoint, Txid};
use alloy::primitives::U256;
use async_trait::async_trait;
use bdk_esplora::{esplora_client, EsploraAsyncExt};
use bdk_wallet::bitcoin::secp256k1::Secp256k1;
use bdk_wallet::keys::DescriptorPublicKey;
use bdk_wallet::rusqlite::Connection;
use bdk_wallet::{bitcoin, descriptor};
use bdk_wallet::{
    bitcoin::Network, error::CreateTxError, signer::SignerError, AddForeignUtxoError, CreateParams,
    KeychainKind, LoadParams, LoadWithPersistError, PersistedWallet,
};
use otc_chains::traits::{MarketMakerPaymentVerification, Payment};
use otc_models::{ChainType, Currency, Lot, TokenIdentifier};
use rand::Rng;
use snafu::{location, Location, ResultExt, Snafu};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::bitcoin_wallet::transaction_broadcaster::{ForeignUtxo, TransactionRequest};
use crate::db::{BroadcastedTransactionRepository, Deposit, DepositRepository, DepositStore, FillStatus};
use crate::wallet::{self, Wallet as WalletTrait, WalletBalance, WalletError};
use crate::WalletResult;

const PARALLEL_REQUESTS: usize = 5;

#[derive(Debug, Snafu)]
pub enum BitcoinWalletError {
    #[snafu(display(
        "Failed to interact with broadcasted transaction repository: {source} at {loc:#?}"
    ))]
    BroadcastedTransactionRepositoryError {
        source: crate::db::broadcasted_transaction_repo::BroadcastedTransactionRepositoryError,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Transaction broadcaster stopped, {source} at {loc:#?}"))]
    BroadcasterFailed {
        source: tokio::sync::mpsc::error::SendError<TransactionRequest>,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Failed to send transaction request: {source} at {loc:#?}"))]
    ReceiverFailed {
        source: tokio::sync::oneshot::error::RecvError,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Failed to open database: {}", source))]
    OpenDatabase { source: bdk_wallet::rusqlite::Error },

    #[snafu(display("Failed to load wallet: {}", source))]
    LoadWallet {
        source: Box<LoadWithPersistError<bdk_wallet::rusqlite::Error>>,
    },

    #[snafu(display("Failed to create wallet: {}", source))]
    CreateWallet {
        source: Box<bdk_wallet::CreateWithPersistError<bdk_wallet::rusqlite::Error>>,
    },

    #[snafu(display("Failed to persist wallet: {}", source))]
    PersistWallet { source: bdk_wallet::rusqlite::Error },

    #[snafu(display("Failed to build Esplora client: {}", source))]
    BuildEsploraClient {
        source: bdk_esplora::esplora_client::Error,
    },

    #[snafu(display("Failed to sync wallet: {source} at {loc:#?}"))]
    SyncWallet {
        source: Box<bdk_esplora::esplora_client::Error>,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Failed to apply update: {source} at {loc:#?}"))]
    BdkWalletCannotConnect {
        source: bdk_wallet::chain::local_chain::CannotConnectError,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Failed to apply update"))]
    ApplyUpdate,

    #[snafu(display("Failed to build transaction: {}", source))]
    BuildTransaction {
        source: CreateTxError,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Failed to sign transaction: {source} at {loc:#?}"))]
    SignTransaction {
        source: SignerError,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Failed to add foreign UTXO: {source} at {loc:#?}"))]
    AddForeignUtxo {
        source: AddForeignUtxoError,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Failed to add UTXO: {source} at {loc:#?}"))]
    AddUtxo {
        source: bdk_wallet::AddUtxoError,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Failed to extract transaction: {}", source))]
    ExtractTransaction {
        source: bdk_wallet::bitcoin::psbt::ExtractTxError,
    },

    #[snafu(display("Failed to broadcast transaction: {}", source))]
    BroadcastTransaction {
        source: bdk_esplora::esplora_client::Error,
    },

    #[snafu(display("Invalid Bitcoin address: {reason}"))]
    InvalidAddress { reason: String },

    #[snafu(display("Failed to parse txid: {reason} at {loc:#?}"))]
    ParseTxid {
        reason: String,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Failed to sign transaction: at {loc:#?}"))]
    PsbtNotFinalized {
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Insufficient balance"))]
    InsufficientBalance,
}

pub struct BitcoinWallet {
    pub tx_broadcaster: transaction_broadcaster::BitcoinTransactionBroadcaster,
    wallet: Arc<Mutex<PersistedWallet<Connection>>>,
    connection: Arc<Mutex<Connection>>,
    esplora_client: Arc<esplora_client::AsyncClient>,
    receive_address: String,
    deposit_repository: Option<Arc<DepositRepository>>,
    broadcasted_transaction_repository: Option<Arc<BroadcastedTransactionRepository>>,
    max_deposits_per_lot: usize,
}

impl BitcoinWallet {
    pub async fn new(
        db_file: &str,
        external_descriptor: &str,
        network: Network,
        esplora_url: &str,
        deposit_repository: Option<Arc<DepositRepository>>,
        broadcasted_transaction_repository: Option<Arc<BroadcastedTransactionRepository>>,
        max_deposits_per_lot: usize,
        join_set: &mut JoinSet<crate::Result<()>>,
    ) -> Result<Self, BitcoinWalletError> {
        let mut conn = Connection::open(db_file).context(OpenDatabaseSnafu)?;

        // Try to load existing wallet
        let load_params = LoadParams::new()
            .descriptor(
                KeychainKind::External,
                Some(external_descriptor.to_string()),
            )
            .extract_keys()
            .check_network(network);

        let wallet_opt = PersistedWallet::load(&mut conn, load_params).map_err(|e| {
            BitcoinWalletError::LoadWallet {
                source: Box::new(e),
            }
        })?;

        let mut wallet = match wallet_opt {
            Some(wallet) => wallet,
            None => {
                // Create new wallet
                let create_params =
                    CreateParams::new_single(external_descriptor.to_string()).network(network);

                PersistedWallet::create(&mut conn, create_params).map_err(|e| {
                    BitcoinWalletError::CreateWallet {
                        source: Box::new(e),
                    }
                })?
            }
        };

        let receive_address = wallet
            .next_unused_address(KeychainKind::External)
            .address
            .to_string();

        let esplora_client = esplora_client::Builder::new(esplora_url)
            .build_async()
            .context(BuildEsploraClientSnafu)?;

        let wallet = Arc::new(Mutex::new(wallet));
        let connection = Arc::new(Mutex::new(conn));
        let esplora_client = Arc::new(esplora_client);

        // Log the wallet's address for debugging
        {
            let mut wallet_guard = wallet.lock().await;
            let address = wallet_guard.next_unused_address(bdk_wallet::KeychainKind::External);
            info!("Bitcoin wallet initialized with address: {}", address);
        }

        let tx_broadcaster = transaction_broadcaster::BitcoinTransactionBroadcaster::new(
            wallet.clone(),
            external_descriptor,
            connection.clone(),
            esplora_client.clone(),
            network,
            broadcasted_transaction_repository.clone(),
            join_set,
        );

        Ok(Self {
            tx_broadcaster,
            wallet,
            connection,
            esplora_client,
            receive_address,
            deposit_repository,
            broadcasted_transaction_repository,
            max_deposits_per_lot,
        })
    }

    /// Converts a single deposit to ForeignUtxos by fetching the funding transaction
    /// and parsing the deposit's descriptor to identify spendable outputs.
    async fn deposit_to_foreign_utxos(
        &self,
        deposit: Deposit,
    ) -> Result<Vec<ForeignUtxo>, WalletError> {
        let tx = self
            .esplora_client
            .get_tx(&Txid::from_str(&deposit.funding_tx_hash).unwrap())
            .await
            .map_err(|e| WalletError::EsploraClientError {
                source: e,
                loc: location!(),
            })?;
        
        let tx = match tx {
            Some(tx) => tx,
            None => {
                warn!("Transaction not found for deposit: {:?}", deposit);
                return Ok(Vec::new());
            }
        };

        // Parse the descriptor string to get the address
        let mut descriptor_str = deposit.private_key.clone();
        if !descriptor_str.starts_with("wpkh(") {
            descriptor_str = format!("wpkh({})", descriptor_str);
        }
        let network = self.wallet.lock().await.network();
        let secp = Secp256k1::new();

        // Parse the descriptor with secret keys using BDK's parse_descriptor
        let (public_desc, _key_map) =
            descriptor::Descriptor::<DescriptorPublicKey>::parse_descriptor(&secp, &descriptor_str)
                .map_err(|e| WalletError::InvalidDescriptor {
                    reason: format!("Failed to parse descriptor with secret keys: {e}"),
                    loc: location!(),
                })?;

        // Derive at index 0 to get the concrete address
        let derived_desc = public_desc.at_derivation_index(0).map_err(|e| {
            WalletError::InvalidDescriptor {
                reason: format!("Failed to derive descriptor at index 0: {e}"),
                loc: location!(),
            }
        })?;

        let deposit_address = derived_desc.address(network).map_err(|e| {
            WalletError::InvalidDescriptor {
                reason: format!("Failed to get address from descriptor: {e}"),
                loc: location!(),
            }
        })?;

        // find all UTXOs in `tx` that pay to `deposit_address`
        let target_spk = deposit_address.script_pubkey();
        let mut foreign_utxos = Vec::new();

        for (vout, txo) in tx
            .output
            .iter()
            .enumerate()
            .filter(|(_, o)| o.script_pubkey == target_spk)
        {
            // Build PSBT input for each spendable output
            let psbt_input = bdk_wallet::bitcoin::psbt::Input {
                witness_utxo: Some(txo.clone()),
                non_witness_utxo: Some(tx.clone()),
                ..Default::default()
            };

            let satisfaction_weight = bdk_wallet::bitcoin::Weight::from_wu(108);

            foreign_utxos.push(ForeignUtxo {
                outpoint: OutPoint::new(tx.compute_txid(), vout as u32),
                psbt_input,
                satisfaction_weight,
                foreign_descriptor: descriptor_str.clone(),
            });
        }

        Ok(foreign_utxos)
    }

    async fn get_dedicated_wallet_balance(&self) -> Result<u64, BitcoinWalletError> {
        // First sync the wallet to get the latest balance
        let mut wallet = self.wallet.lock().await;

        // Do a full scan to get the latest balance from the blockchain
        let request = wallet.start_full_scan().build();
        let update = self
            .esplora_client
            .full_scan(request, 10, 5)
            .await
            .context(SyncWalletSnafu)?;

        wallet
            .apply_update(update)
            .context(BdkWalletCannotConnectSnafu)?;

        // Persist the updated wallet state
        let mut conn = self.connection.lock().await;
        wallet.persist(&mut conn).context(PersistWalletSnafu)?;
        drop(conn);

        let balance = wallet.balance();

        Ok(balance.total().to_sat())
    }
}

impl BitcoinWallet {
    async fn check_tx_confirmations_internal(
        &self,
        tx_hash: &str,
    ) -> Result<u64, BitcoinWalletError> {
        let txid = Txid::from_str(tx_hash).map_err(|e| BitcoinWalletError::ParseTxid {
            reason: e.to_string(),
            loc: location!(),
        })?;

        let status = self
            .esplora_client
            .get_tx_status(&txid)
            .await
            .map_err(|e| BitcoinWalletError::SyncWallet {
                source: Box::new(e),
                loc: location!(),
            })?;

        if status.confirmed {
            if let Some(block_height) = status.block_height {
                let current_height = self.esplora_client.get_height().await.map_err(|e| {
                    BitcoinWalletError::SyncWallet {
                        source: Box::new(e),
                        loc: location!(),
                    }
                })?;

                let confirmations = (current_height as u64)
                    .saturating_sub(block_height as u64)
                    .saturating_add(1);
                Ok(confirmations)
            } else {
                // Confirmed but no block height? Should not happen
                Ok(0)
            }
        } else {
            // Not confirmed yet
            Ok(0)
        }
    }
}

#[async_trait]
impl WalletTrait for BitcoinWallet {
    fn chain_type(&self) -> ChainType {
        ChainType::Bitcoin
    }

    async fn cancel_tx(&self, tx_hash: &str) -> WalletResult<String> {
        let broadcasted_transaction = self
            .broadcasted_transaction_repository
            .as_ref()
            .unwrap()
            .get_broadcasted_transaction(tx_hash)
            .await
            .map_err(|e| WalletError::BitcoinWalletClient {
                source: BitcoinWalletError::BroadcastedTransactionRepositoryError {
                    source: e,
                    loc: location!(),
                },
                loc: location!(),
            })?;
        if broadcasted_transaction.is_none() {
            Err(WalletError::CancelError {
                message: "Transaction not found in broadcasted transaction repository".to_string(),
                loc: location!(),
            })?;
        }
        let broadcasted_transaction = broadcasted_transaction.unwrap();
        let tx: bitcoin::Transaction =
            bitcoin::consensus::deserialize(&broadcasted_transaction.txdata).unwrap();

        // Calculate proper RBF fee: original fee + (tx_vsize * min_relay_fee_rate)
        // Use 2 sat/vB to ensure it passes (covers 1 sat/vB min relay + margin)
        let vsize = tx.vsize() as u64;
        let min_additional_fee = vsize * 2;
        let replacement_fee = broadcasted_transaction.absolute_fee + min_additional_fee;

        if broadcasted_transaction.bitcoin_tx_foreign_utxos.is_none() {
            return Err(WalletError::CancelError {
                message: "Transaction has no foreign UTXOs to attempt to replace".to_string(),
                loc: location!(),
            });
        }

        let replacement_txid = self
            .tx_broadcaster
            .broadcast_transaction(
                vec![],
                broadcasted_transaction
                    .bitcoin_tx_foreign_utxos
                    .unwrap_or(vec![]),
                None,
                Some(replacement_fee),
            )
            .await
            .map_err(|e| WalletError::BitcoinWalletClient {
                source: e,
                loc: location!(),
            })?;

        Ok(replacement_txid)
    }

    async fn check_tx_confirmations(&self, tx_hash: &str) -> WalletResult<u64> {
        self.check_tx_confirmations_internal(tx_hash)
            .await
            .map_err(|e| WalletError::BitcoinWalletClient {
                source: e,
                loc: location!(),
            })
    }

    async fn guarantee_confirmations(
        &self,
        tx_hash: &str,
        confirmations: u64,
    ) -> Result<(), WalletError> {
        let txid = Txid::from_str(tx_hash).map_err(|e| WalletError::ParseAddressFailed {
            context: e.to_string(),
        })?;

        loop {
            let status = self
                .esplora_client
                .get_tx_status(&txid)
                .await
                .map_err(|e| WalletError::EsploraClientError {
                    source: e,
                    loc: location!(),
                })?;

            if status.confirmed {
                if let Some(block_height) = status.block_height {
                    let current_height = self.esplora_client.get_height().await.map_err(|e| {
                        WalletError::EsploraClientError {
                            source: e,
                            loc: location!(),
                        }
                    })?;

                    if (block_height as u64) + confirmations <= current_height as u64 {
                        break;
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(12)).await;
        }

        Ok(())
    }

    async fn create_batch_payment(
        &self,
        payments: Vec<Payment>,
        mm_payment_validation: Option<MarketMakerPaymentVerification>,
    ) -> WalletResult<String> {
        for payment in &payments {
            ensure_valid_lot(&payment.lot)?;
        }

        let batch_id = alloy::hex::encode(rand::thread_rng().gen::<[u8; 8]>());
        for payment in &payments {
            println!("[{}] queuing payment: {:?}", batch_id, payment);
        }

        let mut foreign_utxos = Vec::new();
        let mut reserved_deposit_keys = Vec::new();
        
        if let Some(deposit_repository) = self.deposit_repository.clone() {
            for payment in &payments {
                if foreign_utxos.len() >= self.max_deposits_per_lot {
                    // roughly limit number of foreign UTXOs for this batch 
                    break;
                }
                let lot = payment.lot.clone();
                match deposit_repository
                    .take_deposits_that_fill_lot(&lot, Some(self.max_deposits_per_lot))
                    .await
                    .map_err(|e| WalletError::DepositRepositoryError {
                        source: e,
                        loc: location!(),
                    })? {
                    FillStatus::Full(deposits) | FillStatus::Partial(deposits) => {
                        for deposit in deposits {
                            reserved_deposit_keys.push(deposit.private_key.clone());
                            let utxos = self.deposit_to_foreign_utxos(deposit).await?;
                            foreign_utxos.extend(utxos);
                        }
                    }
                    FillStatus::Empty => {
                        println!("FillStatus::Empty");
                        // No foreign utxos to add
                    }
                }
            }
        }

        info!(
            "Broadcasting bitcoin tx w/ foreign_utxos: {:?}",
            foreign_utxos
        );
        
        // Send transaction request to the broadcaster
        match self.tx_broadcaster
            .broadcast_transaction(payments, foreign_utxos, mm_payment_validation, None)
            .await
        {
            Ok(txid) => Ok(txid),
            Err(e) => {
                // Broadcast failed - unreserve the deposits so they can be used again
                if !reserved_deposit_keys.is_empty() {
                    if let Some(deposit_repository) = &self.deposit_repository {
                        match deposit_repository.unreserve_deposits(&reserved_deposit_keys).await {
                            Ok(count) => {
                                tracing::warn!(
                                    "Unreserved {} deposits after broadcast failure: {:?}",
                                    count,
                                    e
                                );
                            }
                            Err(unreserve_err) => {
                                tracing::error!(
                                    "Failed to unreserve {} deposits after broadcast failure. \
                                    Original error: {:?}, Unreserve error: {:?}",
                                    reserved_deposit_keys.len(),
                                    e,
                                    unreserve_err
                                );
                            }
                        }
                    }
                }
                Err(WalletError::BitcoinWalletClient {
                    source: e,
                    loc: location!(),
                })
            }
        }
    }

    async fn balance(&self, token: &TokenIdentifier) -> WalletResult<WalletBalance> {
        if token != &TokenIdentifier::Native {
            return Err(WalletError::UnsupportedToken {
                token: token.clone(),
                loc: location!(),
            });
        }

        let native_balance =
            U256::from(self.get_dedicated_wallet_balance().await.map_err(|e| {
                wallet::WalletError::BalanceCheckFailed {
                    source: Box::new(e),
                }
            })?);

        let mut net_deposit_key_balance = U256::from(0);

        if let Some(deposit_repository) = &self.deposit_repository {
            let deposit_key_bal = deposit_repository
                .balance(&Currency {
                    chain: ChainType::Bitcoin,
                    token: token.clone(),
                    decimals: 8, //TODO(med): this should not be hardcoded
                })
                .await
                .map_err(|e| WalletError::BalanceCheckFailed {
                    source: Box::new(e),
                })?;
            net_deposit_key_balance += deposit_key_bal;
        }

        let total_balance = native_balance.saturating_add(net_deposit_key_balance);

        Ok(WalletBalance {
            total_balance,
            native_balance,
            deposit_key_balance: net_deposit_key_balance,
        })
    }

    async fn consolidate(
        &self,
        lot: &otc_models::Lot,
        max_deposits_per_iteration: usize,
    ) -> WalletResult<wallet::ConsolidationSummary> {
        let deposit_repository = self.deposit_repository.as_ref().ok_or_else(|| {
            WalletError::TransactionCreationFailed {
                reason: "No deposit repository available for consolidation".to_string(),
            }
        })?;

        let mut total_amount = U256::from(0);
        let mut iterations = 0;
        let mut tx_hashes = Vec::new();

        loop {
            let fill_status = deposit_repository
                .take_deposits_that_fill_lot(lot, Some(max_deposits_per_iteration))
                .await
                .map_err(|e| WalletError::DepositRepositoryError {
                    source: e,
                    loc: location!(),
                })?;

            match fill_status {
                FillStatus::Empty => break,
                FillStatus::Full(deposits) | FillStatus::Partial(deposits) => {
                    // Track deposit keys for potential unreserving on failure
                    let reserved_deposit_keys: Vec<String> = deposits
                        .iter()
                        .map(|d| d.private_key.clone())
                        .collect();
                    
                    // Sum the amount in this batch
                    let mut batch_amount = U256::from(0);
                    for deposit in &deposits {
                        batch_amount = batch_amount.saturating_add(deposit.holdings.amount);
                    }
                    total_amount = total_amount.saturating_add(batch_amount);

                    // Convert deposits to foreign UTXOs
                    let mut foreign_utxos = Vec::new();
                    for deposit in deposits {
                        let utxos = self.deposit_to_foreign_utxos(deposit).await?;
                        foreign_utxos.extend(utxos);
                    }

                    if foreign_utxos.is_empty() {
                        warn!("No foreign UTXOs found for this batch, skipping");
                        continue;
                    }

                    // Create a consolidation payment to our own address
                    let receive_address = self.receive_address(&lot.currency.token);
                    let payment = Payment {
                        lot: Lot {
                            currency: lot.currency.clone(),
                            amount: batch_amount,
                        },
                        to_address: receive_address.clone(),
                    };

                    info!(
                        "Consolidating {} deposits ({} UTXOs) with total amount {} to {}",
                        foreign_utxos.len(),
                        foreign_utxos.len(),
                        batch_amount,
                        receive_address
                    );

                    // Broadcast the consolidation transaction
                    match self
                        .tx_broadcaster
                        .broadcast_transaction(
                            vec![payment],
                            foreign_utxos,
                            None, // No MM validation for consolidation
                            None, // No explicit fee
                        )
                        .await
                    {
                        Ok(tx_hash) => {
                            info!("Consolidation transaction successful: {}", tx_hash);
                            tx_hashes.push(tx_hash);
                            iterations += 1;
                        }
                        Err(e) => {
                            // Broadcast failed - unreserve the deposits so they can be used again
                            match deposit_repository.unreserve_deposits(&reserved_deposit_keys).await {
                                Ok(count) => {
                                    tracing::warn!(
                                        "Unreserved {} deposits after consolidation broadcast failure: {:?}",
                                        count,
                                        e
                                    );
                                }
                                Err(unreserve_err) => {
                                    tracing::error!(
                                        "Failed to unreserve {} deposits after consolidation broadcast failure. \
                                        Original error: {:?}, Unreserve error: {:?}",
                                        reserved_deposit_keys.len(),
                                        e,
                                        unreserve_err
                                    );
                                }
                            }
                            return Err(WalletError::BitcoinWalletClient {
                                source: e,
                                loc: location!(),
                            });
                        }
                    }
                }
            }
        }

        Ok(wallet::ConsolidationSummary {
            total_amount,
            iterations,
            tx_hashes,
        })
    }

    fn receive_address(&self, _token: &TokenIdentifier) -> String {
        self.receive_address.clone()
    }
}

fn ensure_valid_lot(lot: &Lot) -> Result<(), WalletError> {
    if !matches!(lot.currency.chain, ChainType::Bitcoin)
        || !matches!(lot.currency.token, TokenIdentifier::Native)
    {
        return Err(WalletError::UnsupportedToken {
            token: lot.currency.token.clone(),
            loc: location!(),
        });
    }

    // Bitcoin has 8 decimals
    if lot.currency.decimals != 8 {
        return Err(WalletError::UnsupportedToken {
            token: lot.currency.token.clone(),
            loc: location!(),
        });
    }

    info!("Bitcoin lot is valid: {:?}", lot);
    Ok(())
}
