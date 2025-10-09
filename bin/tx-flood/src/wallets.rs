use std::{sync::Arc, time::Duration};

use alloy::{primitives::Address, providers::{DynProvider, Provider}, signers::local::PrivateKeySigner};
use anyhow::{Context, Result};
use eip3009_erc20_contract::GenericEIP3009ERC20::GenericEIP3009ERC20Instance;
use market_maker::{
    bitcoin_wallet::{transaction_broadcaster::BitcoinTransactionBroadcaster, BitcoinWallet}, evm_wallet::{build_transaction_with_validation, create_payment_executions, transaction_broadcaster::{EVMTransactionBroadcaster, PreflightCheck, TransactionExecutionResult}, EVMWallet}, wallet::{Payment, Wallet},
    Result as MarketMakerResult, WalletError,
};
use otc_models::{ChainType, Lot, TokenIdentifier};
use snafu::location;
use tokio::{task::JoinSet, time::sleep};
use tracing::{debug, warn};

use crate::args::{BitcoinWalletConfig, Config, EvmWalletConfig};
use blockchain_utils::create_websocket_wallet_provider;

#[derive(Clone)]
pub enum SinglePaymentWallet {
    Bitcoin(Arc<BitcoinTransactionBroadcaster>),
    Ethereum(Arc<EVMTransactionBroadcaster>, DynProvider),
}

impl SinglePaymentWallet {
    pub fn chain_type(&self) -> ChainType {
        match self {
            SinglePaymentWallet::Bitcoin(_) => ChainType::Bitcoin,
            SinglePaymentWallet::Ethereum(_, _) => ChainType::Ethereum,
        }
    }

    pub async fn create_payment(&self, lot: &Lot, recipient: &str) -> Result<String> {
        match self {
            SinglePaymentWallet::Bitcoin(wallet) => {
                create_bitcoin_payment_with_retry(wallet, lot, recipient).await
            }
            SinglePaymentWallet::Ethereum(wallet, provider) => { 

                let sender = wallet.sender;
                let token_address = match &lot.currency.token {
                    TokenIdentifier::Address(address) => address.parse::<Address>().unwrap(),
                    TokenIdentifier::Native => return Err(map_wallet_error(WalletError::UnsupportedToken { token: lot.currency.token.clone(), loc: location!() })),
                };
                let token_contract = GenericEIP3009ERC20Instance::new(token_address, provider.clone());
                let payment_executions = create_payment_executions(&token_contract, &[recipient.parse::<Address>().unwrap()], &[lot.amount]);
                let transaction_request = build_transaction_with_validation(&sender, provider.clone(), payment_executions, None)?;
                let wallet_result = wallet
                    .broadcast_transaction(transaction_request, PreflightCheck::Simulate)
                    .await
                    .map_err(map_wallet_error)?;
                match wallet_result {
                    TransactionExecutionResult::Success(tx_receipt) => {
                        Ok(tx_receipt.transaction_hash.to_string())
                    }
                    _ => Err(map_wallet_error(WalletError::TransactionCreationFailed { reason: format!("{wallet_result:?}") })),
                }
            }
        }
    }
}

fn map_wallet_error(error: WalletError) -> anyhow::Error {
    anyhow::Error::new(error)
}

const BTC_PAYMENT_RETRY_ATTEMPTS: usize = 10;
const BTC_PAYMENT_RETRY_BACKOFF: Duration = Duration::from_millis(500);

async fn create_bitcoin_payment_with_retry(
    wallet: &Arc<BitcoinTransactionBroadcaster>,
    lot: &Lot,
    recipient: &str,
) -> Result<String> {
    let mut attempt = 0usize;
    loop {
        attempt += 1;
        let payments = vec![Payment {
            to_address: recipient.to_string(),
            lot: lot.clone(),
        }];
        match wallet.broadcast_transaction(payments, vec![], None).await {
            Ok(txid) => {
                if attempt > 1 {
                    debug!(attempt = attempt, "bitcoin payment succeeded after retry");
                }
                return Ok(txid);
            }
            Err(err) => {
                let err_str = err.to_string();
                if should_retry_missing_or_spent(&err_str) && attempt < BTC_PAYMENT_RETRY_ATTEMPTS {
                    warn!(
                        attempt = attempt,
                        error = %err,
                        "bitcoin payment failed due to missing/spent inputs, retrying"
                    );
                    sleep(BTC_PAYMENT_RETRY_BACKOFF).await;
                    continue;
                }
                return Err(map_wallet_error(WalletError::BitcoinWalletClient { source: err, loc: location!() }));
            }
        }
    }
}

fn should_retry_missing_or_spent(error_message: &str) -> bool {
    let lower = error_message.to_ascii_lowercase();
    lower.contains("bad-txns-inputs-missingorspent")
        || lower.contains("bax-txns-inputs-missingorspent")
}

pub struct WalletResources {
    pub payment_wallet: SinglePaymentWallet,
    pub join_set: JoinSet<MarketMakerResult<()>>,
}

pub async fn setup_wallets(config: &Config) -> Result<WalletResources> {
    let mut join_set = JoinSet::<MarketMakerResult<()>>::new();
    let payment_wallet = match config.deposit_chain() {
        ChainType::Bitcoin => {
            let btc_cfg = config
                .bitcoin
                .as_ref()
                .context("missing Bitcoin wallet configuration")?;
            let wallet = init_bitcoin_wallet(btc_cfg, &mut join_set).await?;
            SinglePaymentWallet::Bitcoin(Arc::new(wallet.tx_broadcaster))
        }
        ChainType::Ethereum => {
            let evm_cfg = config
                .evm
                .as_ref()
                .context("missing Ethereum wallet configuration")?;
            let wallet = init_evm_wallet(evm_cfg, &mut join_set).await?;
            let provider = wallet.provider.clone().erased();
            SinglePaymentWallet::Ethereum(Arc::new(wallet.tx_broadcaster), provider)
        }
    };

    Ok(WalletResources {
        payment_wallet,
        join_set,
    })
}

async fn init_bitcoin_wallet(
    cfg: &BitcoinWalletConfig,
    join_set: &mut JoinSet<MarketMakerResult<()>>,
) -> Result<BitcoinWallet> {
    BitcoinWallet::new(
        &cfg.db_path,
        &cfg.descriptor,
        cfg.network,
        &cfg.esplora_url,
        None,
        join_set,
    )
    .await
    .context("failed to initialise Bitcoin wallet")
}

async fn init_evm_wallet(
    cfg: &EvmWalletConfig,
    join_set: &mut JoinSet<MarketMakerResult<()>>,
) -> Result<EVMWallet> {
    let provider = Arc::new(
        create_websocket_wallet_provider(&cfg.rpc_ws_url, cfg.private_key)
            .await
            .context("failed to create websocket wallet provider")?,
    );

    let wallet = EVMWallet::new(
        provider,
        cfg.debug_rpc_url.clone(),
        cfg.confirmations,
        None,
        join_set,
    );

    wallet
        .ensure_eip7702_delegation(
            PrivateKeySigner::from_slice(&cfg.private_key)
                .context("failed to create private key signer")?,
        )
        .await
        .context("failed to ensure EIP-7702 delegation")?;

    Ok(wallet)
}
