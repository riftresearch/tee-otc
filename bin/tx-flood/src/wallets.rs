use std::sync::Arc;

use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result};
use market_maker::{
    bitcoin_wallet::BitcoinWallet, evm_wallet::EVMWallet, wallet::Wallet,
    Result as MarketMakerResult, WalletError,
};
use otc_models::{ChainType, Lot};
use tokio::task::JoinSet;

use crate::args::{BitcoinWalletConfig, Config, EvmWalletConfig};
use blockchain_utils::create_websocket_wallet_provider;

#[derive(Clone)]
pub enum PaymentWallet {
    Bitcoin(Arc<BitcoinWallet>),
    Ethereum(Arc<EVMWallet>),
}

impl PaymentWallet {
    pub fn chain_type(&self) -> ChainType {
        match self {
            PaymentWallet::Bitcoin(_) => ChainType::Bitcoin,
            PaymentWallet::Ethereum(_) => ChainType::Ethereum,
        }
    }

    pub async fn create_payment(&self, lot: &Lot, recipient: &str) -> Result<String> {
        match self {
            PaymentWallet::Bitcoin(wallet) => wallet
                .create_payment(lot, recipient, None)
                .await
                .map_err(map_wallet_error),
            PaymentWallet::Ethereum(wallet) => wallet
                .create_payment(lot, recipient, None)
                .await
                .map_err(map_wallet_error),
        }
    }
}

fn map_wallet_error(error: WalletError) -> anyhow::Error {
    anyhow::Error::new(error)
}

pub struct WalletResources {
    pub payment_wallet: PaymentWallet,
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
            PaymentWallet::Bitcoin(Arc::new(wallet))
        }
        ChainType::Ethereum => {
            let evm_cfg = config
                .evm
                .as_ref()
                .context("missing Ethereum wallet configuration")?;
            let wallet = init_evm_wallet(evm_cfg, &mut join_set).await?;
            PaymentWallet::Ethereum(Arc::new(wallet))
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
