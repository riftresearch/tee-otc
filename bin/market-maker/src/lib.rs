mod balance_strat;
pub mod bitcoin_wallet;
pub mod cb_bitcoin_converter;
mod config;
pub mod deposit_key_storage;
pub mod evm_wallet;
mod otc_client;
mod otc_handler;
pub mod price_oracle;
pub mod quote_storage;
mod rfq_client;
mod rfq_handler;
mod strategy;
pub mod wallet;
mod wrapped_bitcoin_quoter;

pub use wallet::WalletError;
pub use wallet::WalletResult;

use std::{str::FromStr, sync::Arc, time::Duration};

use alloy::{primitives::Address, providers::Provider, signers::local::PrivateKeySigner};
use bdk_wallet::bitcoin;
use blockchain_utils::{create_websocket_wallet_provider, handle_background_thread_result};
use clap::Parser;
use config::Config;
use otc_models::ChainType;
use reqwest::Url;
use snafu::{prelude::*, ResultExt};
use tokio::task::JoinSet;
use tracing::info;
use uuid::Uuid;

use crate::{
    bitcoin_wallet::BitcoinWallet,
    cb_bitcoin_converter::{coinbase_client::CoinbaseClient, run_rebalancer, BandsParams},
    deposit_key_storage::DepositKeyStorage,
    evm_wallet::EVMWallet,
    quote_storage::QuoteStorage,
    wallet::WalletManager,
    wrapped_bitcoin_quoter::WrappedBitcoinQuoter,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Configuration error: {}", source))]
    Config { source: config::ConfigError },

    #[snafu(display("Client error: {}", source))]
    Client { source: otc_client::ClientError },

    #[snafu(display("Bitcoin wallet error: {}", source))]
    BitcoinWallet {
        source: bitcoin_wallet::BitcoinWalletError,
    },

    #[snafu(display("EVM wallet error: {}", source))]
    GenericWallet { source: wallet::WalletError },

    #[snafu(display("Provider error: {}", source))]
    Provider {
        source: blockchain_utils::ProviderError,
    },

    #[snafu(display("Esplora client error: {}", source))]
    EsploraInitialization { source: esplora_client::Error },

    #[snafu(display("Deposit key storage error: {}", source))]
    DepositKeyStorage { source: deposit_key_storage::Error },

    #[snafu(display("Background thread error: {}", source))]
    BackgroundThread {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Quote storage error: {}", source))]
    QuoteStorage {
        source: quote_storage::QuoteStorageError,
    },

    #[snafu(display("Coinbase client error: {}", source))]
    CoinbaseClientError {
        source: cb_bitcoin_converter::coinbase_client::ConversionError,
    },

    #[snafu(display("Conversion actor error: {}", source))]
    ConversionActor {
        source: cb_bitcoin_converter::ConversionActorError,
    },
}

impl From<blockchain_utils::ProviderError> for Error {
    fn from(error: blockchain_utils::ProviderError) -> Self {
        Error::Provider { source: error }
    }
}

impl From<wallet::WalletError> for Error {
    fn from(error: wallet::WalletError) -> Self {
        Error::GenericWallet { source: error }
    }
}

impl From<otc_client::ClientError> for Error {
    fn from(error: otc_client::ClientError) -> Self {
        Error::Client { source: error }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Parser, Debug)]
#[command(name = "market-maker")]
#[command(about = "Market Maker client for TEE-OTC")]
pub struct MarketMakerArgs {
    /// Log level
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    pub log_level: String,

    /// Market maker identifier
    #[arg(long, env = "MM_TAG")]
    pub market_maker_tag: String,

    /// Market maker ID (UUID) for authentication
    #[arg(long, env = "MM_ID")]
    pub market_maker_id: String,

    /// API secret for authentication
    #[arg(long, env = "MM_API_SECRET")]
    pub api_secret: String,

    /// OTC server WebSocket URL
    #[arg(long, env = "OTC_WS_URL", default_value = "ws://localhost:3000/ws/mm")]
    pub otc_ws_url: String,

    /// RFQ server WebSocket URL
    #[arg(long, env = "RFQ_WS_URL", default_value = "ws://localhost:3001/ws/mm")]
    pub rfq_ws_url: String,

    /// Bitcoin wallet database file
    #[arg(long, env = "BITCOIN_WALLET_DB_PATH")]
    pub bitcoin_wallet_db_file: String,

    /// Bitcoin wallet descriptor (aka private key in descriptor format)
    #[arg(long, env = "BITCOIN_WALLET_DESCRIPTOR")]
    pub bitcoin_wallet_descriptor: String,

    /// Bitcoin wallet network
    #[arg(long, env = "BITCOIN_WALLET_NETWORK", default_value = "bitcoin")]
    pub bitcoin_wallet_network: bitcoin::Network,

    /// Bitcoin Esplora URL
    #[arg(long, env = "BITCOIN_WALLET_ESPLORA_URL")]
    pub bitcoin_wallet_esplora_url: String,

    /// Ethereum wallet private key
    #[arg(long, env = "ETHEREUM_WALLET_PRIVATE_KEY", value_parser = parse_hex_string)]
    pub ethereum_wallet_private_key: [u8; 32],

    /// Ethereum confirmations necessary for a transaction to be considered confirmed (for the wallet to be allowed to send a new transaction)
    #[arg(long, env = "ETHEREUM_CONFIRMATIONS", default_value = "1")]
    pub ethereum_confirmations: u64,

    /// Ethereum RPC URL
    #[arg(long, env = "ETHEREUM_RPC_WS_URL")]
    pub ethereum_rpc_ws_url: String,

    /// Trade spread in basis points
    #[arg(long, env = "TRADE_SPREAD_BPS", default_value = "0")]
    pub trade_spread_bps: u64,

    /// Fee safety multiplier, by default 1.5x
    #[arg(long, env = "FEE_SAFETY_MULTIPLIER", default_value = "1.5")]
    pub fee_safety_multiplier: f64,

    /// Max balance utilization for quote fills
    #[arg(
        long,
        env = "BALANCE_UTILIZATION_THRESHOLD_BPS",
        default_value = "7500"
    )]
    pub balance_utilization_threshold_bps: u16,

    /// Database URL for quote storage
    #[arg(long, env = "MM_DATABASE_URL")]
    pub database_url: String,

    /// Coinbase API key
    #[arg(long, env = "COINBASE_EXCHANGE_API_KEY")]
    pub coinbase_exchange_api_key: String,

    /// Coinbase API passphrase
    #[arg(long, env = "COINBASE_EXCHANGE_API_PASSPHRASE")]
    pub coinbase_exchange_api_passphrase: String,

    /// Coinbase API secret
    #[arg(long, env = "COINBASE_EXCHANGE_API_SECRET")]
    pub coinbase_exchange_api_secret: String,

    /// Coinbase API base URL
    #[arg(
        long,
        env = "COINBASE_EXCHANGE_API_BASE_URL",
        default_value = "https://api.exchange.coinbase.com",
        value_parser = parse_url
    )]
    pub coinbase_exchange_api_base_url: Url,

    /// Target BTC allocation as percentage of total inventory (in basis points)
    /// Default 5000 = 50% BTC, 50% cbBTC
    #[arg(long, env = "INVENTORY_TARGET_RATIO_BPS", default_value = "5000")]
    pub inventory_target_ratio_bps: u64,

    /// Rebalancing tolerance - triggers rebalance when allocation drifts beyond this threshold
    /// Default 2500 = ±25% deviation from target triggers rebalancing
    #[arg(long, env = "REBALANCE_TOLERANCE_BPS", default_value = "2500")]
    pub rebalance_tolerance_bps: u64,

    /// Auto manage inventory, based on inventory target ratio and rebalance tolerance and using coinbase exchange to facilitate the conversion between BTC and cbBTC
    #[arg(long, env = "AUTO_MANAGE_INVENTORY", default_value = "false")]
    pub auto_manage_inventory: bool,
}

fn parse_hex_string(s: &str) -> std::result::Result<[u8; 32], String> {
    let bytes = alloy::hex::decode(s).map_err(|e| e.to_string())?;
    if bytes.len() != 32 {
        return Err(format!("Expected 32 bytes, got {bytes:#?}"));
    }
    Ok(bytes.try_into().unwrap())
}

fn parse_url(s: &str) -> std::result::Result<Url, String> {
    Url::parse(s).map_err(|e| e.to_string())
}

pub async fn run_market_maker(args: MarketMakerArgs) -> Result<()> {
    let mut join_set: JoinSet<Result<()>> = JoinSet::new();
    let market_maker_id = Uuid::parse_str(&args.market_maker_id).map_err(|e| Error::Config {
        source: config::ConfigError::InvalidUuid {
            uuid: args.market_maker_id,
            error: e,
        },
    })?;

    info!("Starting market maker with ID: {}", market_maker_id);

    // Initialize quote storage
    let quote_storage = Arc::new(
        QuoteStorage::new(&args.database_url, &mut join_set)
            .await
            .context(QuoteStorageSnafu)?,
    );

    let deposit_key_storage = Arc::new(
        DepositKeyStorage::new(&args.database_url)
            .await
            .context(DepositKeyStorageSnafu)?,
    );

    let esplora_client = esplora_client::Builder::new(&args.bitcoin_wallet_esplora_url)
        .build_async()
        .context(EsploraInitializationSnafu)?;

    let bitcoin_wallet = Arc::new(
        BitcoinWallet::new(
            &args.bitcoin_wallet_db_file,
            &args.bitcoin_wallet_descriptor,
            args.bitcoin_wallet_network,
            &args.bitcoin_wallet_esplora_url,
            Some(deposit_key_storage.clone()),
            &mut join_set,
        )
        .await
        .context(BitcoinWalletSnafu)?,
    );

    let provider = Arc::new(
        create_websocket_wallet_provider(
            &args.ethereum_rpc_ws_url,
            args.ethereum_wallet_private_key,
        )
        .await?,
    );
    let evm_wallet = Arc::new(EVMWallet::new(
        provider.clone(),
        args.ethereum_rpc_ws_url,
        args.ethereum_confirmations,
        Some(deposit_key_storage.clone()),
        &mut join_set,
    ));

    evm_wallet
        .ensure_eip7702_delegation(
            PrivateKeySigner::from_slice(&args.ethereum_wallet_private_key).unwrap(),
        )
        .await
        .expect("Should have been able to ensure EIP-7702 delegation");

    let mut wallet_manager = WalletManager::new();

    wallet_manager.register(ChainType::Bitcoin, bitcoin_wallet.clone());
    wallet_manager.register(ChainType::Ethereum, evm_wallet.clone());

    let btc_eth_price_oracle = price_oracle::BitcoinEtherPriceOracle::new(&mut join_set);

    let wrapped_bitcoin_quoter = WrappedBitcoinQuoter::new(
        btc_eth_price_oracle,
        esplora_client,
        provider.clone().erased(),
        args.trade_spread_bps,
        args.fee_safety_multiplier,
    );

    let otc_fill_client = otc_client::OtcFillClient::new(
        Config {
            market_maker_id,
            market_maker_tag: args.market_maker_tag.clone(),
            api_secret: args.api_secret.clone(),
            otc_ws_url: args.otc_ws_url.clone(),
            reconnect_interval_secs: 5,
            max_reconnect_attempts: 5,
        },
        wallet_manager.clone(),
        quote_storage.clone(),
        deposit_key_storage.clone(),
    );
    join_set.spawn(async move { otc_fill_client.run().await.map_err(Error::from) });

    let balance_strategy =
        balance_strat::QuoteBalanceStrategy::new(args.balance_utilization_threshold_bps);

    let rfq_handler = rfq_handler::RFQMessageHandler::new(
        market_maker_id,
        wrapped_bitcoin_quoter,
        quote_storage,
        wallet_manager,
        balance_strategy,
    );

    // Add RFQ client for handling quote requests
    let rfq_client = rfq_client::RfqClient::new(
        Config {
            market_maker_id,
            market_maker_tag: args.market_maker_tag.clone(),
            api_secret: args.api_secret.clone(),
            otc_ws_url: args.otc_ws_url,
            reconnect_interval_secs: 5,
            max_reconnect_attempts: 5,
        },
        rfq_handler,
        args.rfq_ws_url,
    );
    join_set.spawn(async move {
        rfq_client.run().await.map_err(|e| Error::Client {
            source: otc_client::ClientError::BackgroundThreadExited {
                source: Box::new(e),
            },
        })
    });

    if args.auto_manage_inventory {
        let coinbase_client = CoinbaseClient::new(
            args.coinbase_exchange_api_base_url,
            args.coinbase_exchange_api_key,
            args.coinbase_exchange_api_passphrase,
            args.coinbase_exchange_api_secret,
        )
        .context(CoinbaseClientSnafu)?;

        let conversion_actor = run_rebalancer(
            coinbase_client,
            bitcoin_wallet.clone(),
            evm_wallet.clone(),
            BandsParams {
                target_bps: args.inventory_target_ratio_bps,
                band_width_bps: args.rebalance_tolerance_bps,
                poll_interval: Duration::from_secs(60),
            },
        );

        join_set.spawn(async move { conversion_actor.await.context(ConversionActorSnafu) });
    }

    handle_background_thread_result(join_set.join_next().await).context(BackgroundThreadSnafu)?;

    Ok(())
}
