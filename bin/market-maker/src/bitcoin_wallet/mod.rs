pub mod transaction_broadcaster;

use std::str::FromStr;
use std::sync::Arc;

use ::esplora_client::{OutPoint, Txid};
use alloy::primitives::U256;
use async_trait::async_trait;
use bdk_esplora::{esplora_client, EsploraAsyncExt};
use bdk_wallet::bitcoin::secp256k1::Secp256k1;
use bdk_wallet::descriptor;
use bdk_wallet::keys::DescriptorPublicKey;
use bdk_wallet::rusqlite::Connection;
use bdk_wallet::{
    bitcoin::Network, error::CreateTxError, signer::SignerError, CreateParams, KeychainKind,
    LoadParams, LoadWithPersistError, PersistedWallet,
};
use otc_chains::traits::MarketMakerPaymentValidation;
use otc_models::{ChainType, Lot, TokenIdentifier};
use snafu::{location, Location, ResultExt, Snafu};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::info;

use crate::bitcoin_wallet::transaction_broadcaster::{ForeignUtxo, TransactionRequest};
use crate::deposit_key_storage::{DepositKeyStorage, DepositKeyStorageTrait, FillStatus};
use crate::wallet::{self, Wallet as WalletTrait, WalletError};
use crate::WalletResult;

const STOP_GAP: usize = 50;
const PARALLEL_REQUESTS: usize = 5;
const BALANCE_BUFFER_PERCENT: u64 = 25; // 25% buffer

#[derive(Debug, Snafu)]
pub enum BitcoinWalletError {
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
    deposit_key_storage: Option<Arc<DepositKeyStorage>>,
}

impl BitcoinWallet {
    pub async fn new(
        db_file: &str,
        external_descriptor: &str,
        network: Network,
        esplora_url: &str,
        deposit_key_storage: Option<Arc<DepositKeyStorage>>,
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
            connection.clone(),
            esplora_client.clone(),
            network,
            join_set,
        );

        Ok(Self {
            tx_broadcaster,
            wallet,
            connection,
            esplora_client,
            receive_address,
            deposit_key_storage,
        })
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

#[async_trait]
impl WalletTrait for BitcoinWallet {
    fn chain_type(&self) -> ChainType {
        ChainType::Bitcoin
    }

    async fn guarantee_confirmations(
        &self,
        tx_hash: &str,
        confirmations: u64,
    ) -> Result<(), WalletError> {
        // TODO(high): implement this
        Ok(())
    }

    async fn create_payment(
        &self,
        lot: &Lot,
        to_address: &str,
        mm_payment_validation: Option<MarketMakerPaymentValidation>,
    ) -> WalletResult<String> {
        ensure_valid_lot(lot)?;

        info!(
            "Queueing Bitcoin transaction to {} for {:?}",
            to_address, lot
        );
        let mut foreign_utxos = Vec::new();
        if let Some(deposit_key_storage) = self.deposit_key_storage.clone() {
            match deposit_key_storage
                .take_deposits_that_fill_lot(lot)
                .await
                .map_err(|e| WalletError::DepositKeyStorageError {
                    source: e,
                    loc: location!(),
                })? {
                FillStatus::Full(deposits) | FillStatus::Partial(deposits) => {
                    for deposit in deposits {
                        let tx = self
                            .esplora_client
                            .get_tx(&Txid::from_str(&deposit.funding_tx_hash).unwrap())
                            .await
                            .map_err(|e| WalletError::EsploraClientError {
                                source: e,
                                loc: location!(),
                            })?;
                        // Parse the descriptor string to get the address
                        let descriptor_str = &deposit.private_key;
                        let network = self.wallet.lock().await.network();
                        let secp = Secp256k1::new();

                        // Parse the descriptor with secret keys using BDK's parse_descriptor
                        let (public_desc, _key_map) =
                            descriptor::Descriptor::<DescriptorPublicKey>::parse_descriptor(
                                &secp,
                                descriptor_str,
                            )
                            .map_err(|e| {
                                WalletError::InvalidDescriptor {
                                    reason: format!(
                                        "Failed to parse descriptor with secret keys: {e}"
                                    ),
                                    loc: location!(),
                                }
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
                        info!("deposit_address from descriptor: {:?}", deposit_address);

                        if let Some(tx) = tx {
                            let out = tx
                                .output
                                .iter()
                                .enumerate()
                                .filter(|(i, out)| {
                                    out.value.to_sat() == lot.amount.to::<u64>()
                                        && out.script_pubkey == deposit_address.script_pubkey()
                                })
                                .take(1)
                                .next();
                            if let Some(out) = out {
                                // Craft a complete PSBT input for a foreign UTXO.
                                // For comprehensive BIP143 compliance, we include both witness_utxo
                                // and non_witness_utxo fields.
                                let psbt_input = bdk_wallet::bitcoin::psbt::Input {
                                    witness_utxo: Some(out.1.clone()),
                                    non_witness_utxo: Some(tx.clone()),
                                    ..Default::default()
                                };

                                // Typical satisfaction weight for P2WPKH inputs is ~108 wu.
                                let satisfaction_weight = bdk_wallet::bitcoin::Weight::from_wu(108);

                                info!("Adding foreign UTXO: {:?}", out.0);

                                foreign_utxos.push(ForeignUtxo {
                                    outpoint: OutPoint::new(tx.compute_txid(), out.0 as u32),
                                    psbt_input,
                                    satisfaction_weight,
                                    foreign_descriptor: deposit.private_key.clone(),
                                });
                            }
                        }
                    }
                }
                FillStatus::Empty => {
                    println!("FillStatus::Empty");
                    // No foreign utxos to add
                }
            }
        }

        // Send transaction request to the broadcaster
        self.tx_broadcaster
            .broadcast_transaction(
                lot.clone(),
                to_address.to_string(),
                foreign_utxos,
                mm_payment_validation,
            )
            .await
            .map_err(|e| WalletError::BitcoinWalletClient { source: e })
    }

    async fn balance(&self, token: &TokenIdentifier) -> WalletResult<U256> {
        if token != &TokenIdentifier::Native {
            return Err(WalletError::UnsupportedToken {
                token: token.clone(),
                loc: location!(),
            });
        }

        Ok(U256::from(
            self.get_dedicated_wallet_balance().await.map_err(|_| {
                wallet::WalletError::BalanceCheckFailed {
                    reason: "Failed to get balance".to_string(),
                }
            })?,
        ))
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

fn balance_with_buffer(balance_sats: u64) -> u64 {
    balance_sats + (balance_sats * BALANCE_BUFFER_PERCENT) / 100
}
