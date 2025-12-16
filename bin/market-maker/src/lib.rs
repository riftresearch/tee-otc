mod admin_api;
mod balance_strat;
mod batch_monitor;
pub mod bitcoin_wallet;
pub mod db;
pub mod evm_wallet;
mod liquidity_cache;
mod liquidity_lock;
mod fee_settlement;
mod otc_handler;
pub mod payment_manager;
pub mod price_oracle;
mod rfq_handler;
mod rebalancer;
pub mod wallet;
mod websocket_client;
mod wrapped_bitcoin_quoter;

use blockchain_utils::shutdown_signal;
pub use wallet::WalletError;
pub use wallet::WalletResult;

use std::{net::SocketAddr, sync::Arc, sync::OnceLock, time::Duration};

use alloy::{providers::Provider, signers::local::PrivateKeySigner};
use bdk_wallet::bitcoin;
use blockchain_utils::{create_websocket_wallet_provider, handle_background_thread_result};
use clap::Parser;
use clap::ValueEnum;
use otc_models::ChainType;
use reqwest::Url;
use snafu::{prelude::*, ResultExt};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, Instrument};

use axum::{
    extract::State, http::header, http::StatusCode, response::IntoResponse, routing::get, Router,
};
use metrics_exporter_prometheus::{BuildError, PrometheusBuilder, PrometheusHandle};
use tokio::net::TcpListener;
use uuid::Uuid;
use coinbase_exchange_client::CoinbaseClient;
use crate::payment_manager::PaymentManager;
use crate::rebalancer::BandsParams;
use crate::rebalancer::run_inventory_metrics_reporter;
use crate::rebalancer::run_rebalancer;
use crate::{
    bitcoin_wallet::BitcoinWallet,
    db::Database,
    evm_wallet::EVMWallet,
    wallet::WalletManager,
    wrapped_bitcoin_quoter::WrappedBitcoinQuoter,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Configuration error: {}", context))]
    Config { context: String },

    #[snafu(display("WebSocket client error: {}", source))]
    WebSocketClient { source: websocket_client::WebSocketError },

    #[snafu(display("Bitcoin wallet error: {}", source))]
    BitcoinWallet {
        #[snafu(source(from(bitcoin_wallet::BitcoinWalletError, Box::new)))]
        source: Box<bitcoin_wallet::BitcoinWalletError>,
    },

    #[snafu(display("EVM wallet error: {}", source))]
    GenericWallet {
        #[snafu(source(from(wallet::WalletError, Box::new)))]
        source: Box<wallet::WalletError>,
    },

    #[snafu(display("Provider error: {}", source))]
    Provider {
        source: blockchain_utils::ProviderError,
    },

    #[snafu(display("Esplora client error: {}", source))]
    EsploraInitialization { source: esplora_client::Error },

    #[snafu(display("Wrapped bitcoin quoter error: {}", source))]
    WrappedBitcoinQuoter {
        source: wrapped_bitcoin_quoter::WrappedBitcoinQuoterError,
    },

    #[snafu(display("Database error: {}", source))]
    Database { source: db::DatabaseError },

    #[snafu(display("Deposit repository error: {}", source))]
    DepositRepository { source: db::DepositRepositoryError },

    #[snafu(display("Background thread error: {}", source))]
    BackgroundThread {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Quote repository error: {}", source))]
    QuoteRepository { source: db::QuoteRepositoryError },

    #[snafu(display("Payment repository error: {}", source))]
    PaymentRepository { source: db::PaymentRepositoryError },

    #[snafu(display("Coinbase exchange client error: {source} at {loc:#?}"))]
    CoinbaseExchangeClientError {
        source: coinbase_exchange_client::CoinbaseExchangeClientError,
        #[snafu(implicit)]
        loc: snafu::Location,
    },

    #[snafu(display("Rebalancer error: {source} at {loc:#?}"))]
    RebalancerError {
        #[snafu(source(from(rebalancer::RebalancerError, Box::new)))]
        source: Box<rebalancer::RebalancerError>,
        #[snafu(implicit)]
        loc: snafu::Location,
    },

    #[snafu(display("Failed to install metrics recorder: {}", source))]
    MetricsRecorder { source: BuildError },

    #[snafu(display("Failed to bind metrics listener on {}: {}", addr, source))]
    MetricsServerBind {
        source: std::io::Error,
        addr: SocketAddr,
    },

    #[snafu(display("Metrics server error: {}", source))]
    MetricsServer { source: std::io::Error },

    #[snafu(display("Failed to bind admin API listener on {}: {}", addr, source))]
    AdminApiServerBind {
        source: std::io::Error,
        addr: SocketAddr,
    },

    #[snafu(display("Admin API server error: {}", source))]
    AdminApiServer { source: std::io::Error },

    #[snafu(display("Chain ID mismatch: expected {} for {:?}, but RPC returned {}", expected, chain, actual))]
    ChainIdMismatch {
        expected: u64,
        actual: u64,
        chain: ChainType,
    },
}

impl From<blockchain_utils::ProviderError> for Error {
    fn from(error: blockchain_utils::ProviderError) -> Self {
        Error::Provider { source: error }
    }
}

impl From<wallet::WalletError> for Error {
    fn from(error: wallet::WalletError) -> Self {
        Error::GenericWallet {
            source: Box::new(error),
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[clap(rename_all = "kebab_case")]
pub enum FeeSettlementRail {
    Evm,
    Bitcoin,
}

#[derive(Parser, Debug, Clone)]
#[command(name = "market-maker")]
#[command(about = "Market Maker client for TEE-OTC")]
pub struct MarketMakerArgs {
    /// Log level
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    pub log_level: String,

    /// Address for the Prometheus metrics exporter (will only be exposed if address is provided)
    #[arg(long, env = "METRICS_LISTEN_ADDR")]
    pub metrics_listen_addr: Option<SocketAddr>,

    /// Address for the admin API server (will only be exposed if address is provided)
    #[arg(long, env = "ADMIN_API_LISTEN_ADDR")]
    pub admin_api_listen_addr: Option<SocketAddr>,

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

    /// EVM chain to operate on (ethereum or base)
    #[arg(long, env = "EVM_CHAIN", value_parser = parse_evm_chain, default_value = "ethereum")]
    pub evm_chain: ChainType,

    /// EVM wallet private key
    #[arg(long, env = "EVM_WALLET_PRIVATE_KEY", value_parser = parse_hex_string)]
    pub evm_wallet_private_key: [u8; 32],

    /// Number of confirmations required before broadcasting the next transaction. Defaults to 2 as empirically this
    /// seems to be enough to ensure EIP-7702 delegated accounts never exceed their in-flight transaction limit (of 1 in most cases),
    /// which would cause mempool rejections. Annoyingly, setting to this 1 will lead to transient EIP-7702 broadcast errors.
    #[arg(long, env = "EVM_CONFIRMATIONS", default_value = "2")]
    pub evm_confirmations: u64,

    /// EVM RPC WebSocket URL
    #[arg(long, env = "EVM_RPC_WS_URL")]
    pub evm_rpc_ws_url: String,

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

    /// Database max connections
    #[arg(long, env = "DB_MAX_CONNECTIONS", default_value = "128")]
    pub db_max_connections: u32,

    /// Database min connections
    #[arg(long, env = "DB_MIN_CONNECTIONS", default_value = "16")]
    pub db_min_connections: u32,

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

    /// Rebalancing poll interval in seconds - how often to check inventory and trigger rebalancing if needed
    #[arg(long, env = "REBALANCE_POLL_INTERVAL_SECS", default_value = "60")]
    pub rebalance_poll_interval_secs: u64,

    /// Bitcoin batch payment interval in seconds
    #[arg(long, env = "BITCOIN_BATCH_INTERVAL_SECS", default_value = "24")]
    pub bitcoin_batch_interval_secs: u64,

    /// Bitcoin batch payment size (max payments per batch)
    #[arg(long, env = "BITCOIN_BATCH_SIZE", default_value = "100")]
    pub bitcoin_batch_size: usize,

    /// Ethereum batch payment interval in seconds
    #[arg(long, env = "ETHEREUM_BATCH_INTERVAL_SECS", default_value = "5")]
    pub ethereum_batch_interval_secs: u64,

    /// Ethereum batch payment size (max payments per batch)
    #[arg(long, env = "ETHEREUM_BATCH_SIZE", default_value = "392")]
    pub ethereum_batch_size: usize,

    /// Batch monitor polling interval in seconds
    #[arg(long, env = "BATCH_MONITOR_INTERVAL_SECS", default_value = "600")]
    pub batch_monitor_interval_secs: u64,

    /// Maximum number of deposits to collect per lot for Ethereum
    #[arg(long, env = "ETHEREUM_MAX_DEPOSITS_PER_LOT", default_value = "350")]
    pub ethereum_max_deposits_per_lot: usize,

    /// Maximum number of deposits to collect per lot for Bitcoin
    #[arg(long, env = "BITCOIN_MAX_DEPOSITS_PER_LOT", default_value = "100")]
    pub bitcoin_max_deposits_per_lot: usize,

    /// Confirmation polling interval in seconds - how often to poll for transaction confirmations
    #[arg(long, env = "CONFIRMATION_POLL_INTERVAL_SECS", default_value = "12")]
    pub confirmation_poll_interval_secs: u64,

    /// Number of Bitcoin confirmations required before crediting deposit to Coinbase (default: 3 for production safety)
    #[arg(long, env = "BTC_COINBASE_CONFIRMATIONS", default_value = "3")]
    pub btc_coinbase_confirmations: u32,

    /// Number of Ethereum (cbBTC) confirmations required before crediting deposit to Coinbase (default: 36 for production safety)
    #[arg(long, env = "CBBTC_COINBASE_CONFIRMATIONS", default_value = "36")]
    pub cbbtc_coinbase_confirmations: u32,

    /// Loki logging URL (if provided, logs will be shipped to Loki)
    #[arg(long, env = "LOKI_URL")]
    pub loki_url: Option<String>,

    /// Enable tokio console subscriber
    #[arg(long, env = "ENABLE_TOKIO_CONSOLE_SUBSCRIBER", default_value = "false")]
    pub enable_tokio_console_subscriber: bool,

    /// Preferred fee settlement rail: either EVM (configured `--evm-chain`) or Bitcoin.
    #[arg(long, env = "FEE_SETTLEMENT_RAIL", value_enum, default_value = "evm")]
    pub fee_settlement_rail: FeeSettlementRail,

    /// Fee settlement polling interval in seconds.
    #[arg(long, env = "FEE_SETTLEMENT_INTERVAL_SECS", default_value = "300")]
    pub fee_settlement_interval_secs: u64,

    /// Number of confirmations required before notifying otc-server about an EVM fee settlement tx.
    /// Defaults to `--evm-confirmations` if not provided.
    #[arg(long, env = "FEE_SETTLEMENT_EVM_CONFIRMATIONS")]
    pub fee_settlement_evm_confirmations: Option<u64>,
}

fn parse_hex_string(s: &str) -> std::result::Result<[u8; 32], String> {
    let bytes = alloy::hex::decode(s).map_err(|e| e.to_string())?;
    if bytes.len() != 32 {
        return Err(format!("Expected 32 bytes, got {bytes:#?}"));
    }
    Ok(bytes.try_into().unwrap())
}

fn parse_evm_chain(s: &str) -> std::result::Result<ChainType, String> {
    match s.to_lowercase().as_str() {
        "ethereum" => Ok(ChainType::Ethereum),
        "base" => Ok(ChainType::Base),
        _ => Err(format!("Invalid EVM chain: '{}'. Must be 'ethereum' or 'base'", s)),
    }
}

fn parse_url(s: &str) -> std::result::Result<Url, String> {
    Url::parse(s).map_err(|e| e.to_string())
}

async fn validate_chain_id<P>(provider: &P, expected_chain: ChainType) -> Result<()>
where
    P: alloy::providers::Provider,
{
    use otc_models::constants::EXPECTED_CHAIN_IDS;

    let expected_chain_id = EXPECTED_CHAIN_IDS
        .get(&expected_chain)
        .ok_or_else(|| Error::Config {
            context: format!("No expected chain ID configured for {:?}", expected_chain),
        })?;

    let actual_chain_id = provider.get_chain_id().await.map_err(|e| Error::Config {
        context: format!("Failed to query chain ID from RPC: {}", e),
    })?;

    if actual_chain_id != *expected_chain_id {
        return Err(Error::ChainIdMismatch {
            expected: *expected_chain_id,
            actual: actual_chain_id,
            chain: expected_chain,
        });
    }

    info!(
        "Chain ID validation successful: {:?} chain ID is {}",
        expected_chain, actual_chain_id
    );

    Ok(())
}

pub async fn run_market_maker(
    args: MarketMakerArgs,
) -> Result<()> {
    let cancellation_token = CancellationToken::new();
    let mut join_set: JoinSet<Result<()>> = JoinSet::new();
    let market_maker_id = Uuid::parse_str(&args.market_maker_id).map_err(|_| Error::Config {
        context: format!("Invalid UUID: {}", args.market_maker_id),
    })?;

    info!("Starting market maker with ID: {}", market_maker_id);

    if let Some(addr) = args.metrics_listen_addr {
        info!("Setting up metrics listener on {}", addr);
        setup_metrics(&mut join_set, addr)?;
    }


    let database = Arc::new(
        Database::connect(
            &args.database_url,
            args.db_max_connections,
            args.db_min_connections,
        )
        .await
        .context(DatabaseSnafu)?,
    );

    let quote_repository = Arc::new(database.quotes());
    quote_repository.start_cleanup_task(&mut join_set);

    let deposit_repository = Arc::new(database.deposits());
    let broadcasted_transaction_repository = Arc::new(database.broadcasted_transactions());

    let esplora_client = esplora_client::Builder::new(&args.bitcoin_wallet_esplora_url)
        .build_async()
        .context(EsploraInitializationSnafu)?;

    let bitcoin_wallet = Arc::new(
        BitcoinWallet::new(
            &args.bitcoin_wallet_db_file,
            &args.bitcoin_wallet_descriptor,
            args.bitcoin_wallet_network,
            &args.bitcoin_wallet_esplora_url,
            Some(deposit_repository.clone()),
            Some(broadcasted_transaction_repository.clone()),
            args.bitcoin_max_deposits_per_lot,
            &mut join_set,
        )
        .await
        .context(BitcoinWalletSnafu)?,
    );

    info!("Configuring market maker for EVM chain: {:?}", args.evm_chain);

    let provider = Arc::new(
        create_websocket_wallet_provider(
            &args.evm_rpc_ws_url,
            args.evm_wallet_private_key,
        )
        .await?,
    );

    // Validate that the RPC URL is for the correct chain
    validate_chain_id(&provider, args.evm_chain).await?;

    let evm_wallet = Arc::new(EVMWallet::new(
        provider.clone(),
        args.evm_rpc_ws_url.clone(),
        args.evm_confirmations,
        args.evm_chain,
        Some(deposit_repository.clone()),
        args.ethereum_max_deposits_per_lot,
        &mut join_set,
    ));

    evm_wallet
        .ensure_eip7702_delegation(
            PrivateKeySigner::from_slice(&args.evm_wallet_private_key).unwrap(),
        )
        .await
        .expect("Should have been able to ensure EIP-7702 delegation");

    // Setup admin API server if address is provided
    if let Some(addr) = args.admin_api_listen_addr {
        info!("Setting up admin API listener on {}", addr);
        setup_admin_api(
            &mut join_set,
            addr,
            deposit_repository.clone(),
            bitcoin_wallet.clone(),
            evm_wallet.clone(),
        )?;
    }

    let mut wallet_manager = WalletManager::new();

    wallet_manager.register(ChainType::Bitcoin, bitcoin_wallet.clone());
    wallet_manager.register(args.evm_chain, evm_wallet.clone());

    let wallet_manager = Arc::new(wallet_manager);
    let balance_strategy = Arc::new(
        balance_strat::QuoteBalanceStrategy::new(args.balance_utilization_threshold_bps),
    );

    // Create liquidity lock manager for tracking locked liquidity
    let liquidity_lock_manager = Arc::new(liquidity_lock::LiquidityLockManager::new(&mut join_set));

    // Create liquidity cache BEFORE WrappedBitcoinQuoter (it needs it)
    let liquidity_cache = Arc::new(liquidity_cache::LiquidityCache::new(
        wallet_manager.clone(),
        balance_strategy.clone(),
        liquidity_lock_manager.clone(),
        args.evm_chain,
        &mut join_set,
    ));

    let btc_eth_price_oracle = price_oracle::BitcoinEtherPriceOracle::new(&mut join_set);

    let wrapped_bitcoin_quoter = Arc::new(WrappedBitcoinQuoter::new(
        wallet_manager.clone(),
        liquidity_cache.clone(),
        btc_eth_price_oracle,
        esplora_client,
        provider.clone().erased(),
        args.trade_spread_bps,
        args.fee_safety_multiplier,
        args.evm_chain,
        wrapped_bitcoin_quoter::AffiliateFeeConfig::default(),
        &mut join_set,
    ));

    let payment_repository = Arc::new(database.payments());

    // Spawn batch monitor to track and cancel at-risk batches
    batch_monitor::spawn_batch_monitor(
        wallet_manager.clone(),
        payment_repository.clone(),
        args.batch_monitor_interval_secs,
        &mut join_set,
    );

    // Configure batch payment processing for each chain
    let mut batch_configs = std::collections::HashMap::new();
    batch_configs.insert(
        ChainType::Bitcoin,
        payment_manager::BatchConfig {
            interval_secs: args.bitcoin_batch_interval_secs,
            batch_size: args.bitcoin_batch_size,
        },
    );
    batch_configs.insert(
        args.evm_chain,
        payment_manager::BatchConfig {
            interval_secs: args.ethereum_batch_interval_secs,
            batch_size: args.ethereum_batch_size,
        },
    );

    // Create channel for MMResponse messages from PaymentManager to OTC server
    let (otc_response_tx, otc_response_rx) = tokio::sync::mpsc::unbounded_channel();

    // dedicated join set for payment manager tasks b/c if we shutdown
    // we want to wait for the payment manager tasks to finish before exiting
    let mut payment_manager_join_set: JoinSet<Result<()>> = JoinSet::new();
    let payment_manager = Arc::new(PaymentManager::new(
        wallet_manager.clone(),
        payment_repository.clone(),
        batch_configs,
        balance_strategy.clone(),
        otc_response_tx,
        cancellation_token.clone(),
        &mut payment_manager_join_set,
    ));

    // Set up OTC WebSocket client
    let standing_requests = fee_settlement::StandingRequestRegistry::new();
    let otc_handler = otc_handler::OTCMessageHandler::new(
        quote_repository.clone(),
        deposit_repository.clone(),
        payment_manager.clone(),
        payment_repository.clone(),
        liquidity_lock_manager.clone(),
        standing_requests.clone(),
    );
    let otc_ws_client = websocket_client::WebSocketClient::new(
        args.otc_ws_url.clone(),
        market_maker_id.to_string(),
        args.api_secret.clone(),
        otc_handler,
    );
    let (otc_handle, otc_future) = otc_ws_client.connect().instrument(tracing::info_span!("ws", client = "otc")).await.context(WebSocketClientSnafu)?;
    
    // Spawn OTC WebSocket connection (ezsockets handles reconnection automatically)
    join_set.spawn(
        async move {
            otc_future.await.context(WebSocketClientSnafu)
        }
    );
    
    // Spawn OTC response forwarder task for unsolicited MMResponse messages
    let otc_handle_for_forwarder = otc_handle.clone();
    join_set.spawn(async move {
        let mut otc_response_rx = otc_response_rx;
        while let Some(response_msg) = otc_response_rx.recv().await {
            if let Err(e) = otc_handle_for_forwarder.send(&response_msg) {
                tracing::error!("Failed to send unsolicited MMResponse: {}", e);
            }
        }
        tracing::info!("OTC response forwarder task ended");
        Ok(())
    });

    // Spawn fee settlement engine (deadline-aware; confirmation-gated notify)
    fee_settlement::spawn_fee_settlement_engine(
        args.clone(),
        market_maker_id,
        wallet_manager.clone(),
        payment_repository.clone(),
        otc_handle.clone(),
        standing_requests,
        &mut join_set,
    );

    // LiquidityCache was already created above (needed by WrappedBitcoinQuoter)

    // Make sure fees + balances are initialized before quotes can be computed
    wrapped_bitcoin_quoter
        .ensure_cache_ready()
        .await
        .context(WrappedBitcoinQuoterSnafu)?;

    // Set up RFQ WebSocket client
    let rfq_handler = rfq_handler::RFQMessageHandler::new(
        market_maker_id,
        wrapped_bitcoin_quoter.clone(),
        quote_repository,
        liquidity_cache,
    );
    let rfq_ws_client = websocket_client::WebSocketClient::new(
        args.rfq_ws_url,
        market_maker_id.to_string(),
        args.api_secret.clone(),
        rfq_handler,
    );
    let (_rfq_handle, rfq_future) = rfq_ws_client.connect().instrument(tracing::info_span!("ws", client = "rfq")).await.context(WebSocketClientSnafu)?;
    
    // Spawn RFQ WebSocket connection (ezsockets handles reconnection automatically)
    join_set.spawn(
        async move {
            rfq_future.await.context(WebSocketClientSnafu)
        }
    );

    let coinbase_client = CoinbaseClient::new(
        args.coinbase_exchange_api_base_url,
        args.coinbase_exchange_api_key,
        args.coinbase_exchange_api_passphrase,
        args.coinbase_exchange_api_secret,
    )
    .context(CoinbaseExchangeClientSnafu)?;

    // Run rebalancer, even if auto manage inventory is disabled
    // b/c it's still useful to track the balance of the wallets in metrics
    let conversion_actor = run_rebalancer(
        args.evm_chain,
        coinbase_client,
        bitcoin_wallet.clone(),
        evm_wallet.clone(),
        BandsParams {
            target_bps: args.inventory_target_ratio_bps,
            band_width_bps: args.rebalance_tolerance_bps,
            poll_interval: Duration::from_secs(args.rebalance_poll_interval_secs),
        },
        args.auto_manage_inventory,
        Duration::from_secs(args.confirmation_poll_interval_secs),
        args.btc_coinbase_confirmations,
        args.cbbtc_coinbase_confirmations,
    );

    join_set.spawn(async move { conversion_actor.await.context(RebalancerSnafu) });

    // run inventory metrics reporter
    let inventory_metrics_reporter = run_inventory_metrics_reporter(
        bitcoin_wallet.clone(),
        evm_wallet.clone(),
        Duration::from_secs(args.rebalance_poll_interval_secs),
    );
    join_set.spawn(async move { inventory_metrics_reporter.await.context(RebalancerSnafu) });


    tokio::select! { 
        _ = shutdown_signal() => {
            info!("Shutdown signal received");
        }
        task_end = join_set.join_next() => {
            info!("Main Task exited: {:?}", handle_background_thread_result(task_end));
        }
        payment_manager_task_end = payment_manager_join_set.join_next() => {
            info!("PaymentManager task exited: {:?}", handle_background_thread_result(payment_manager_task_end));
        }
    }

    info!("Triggering graceful shutdown...");
    cancellation_token.cancel();

    while let Some(_) = payment_manager_join_set.join_next().await {
        info!("PaymentManager task exited gracefully...");
    }
    join_set.abort_all();

    info!("All background tasks have been shut down");

    Ok(())
}

pub const QUOTE_LATENCY_METRIC: &str = "mm_quote_response_seconds";
static PROMETHEUS_HANDLE: OnceLock<Arc<PrometheusHandle>> = OnceLock::new();

pub fn install_metrics_recorder() -> Result<Arc<PrometheusHandle>> {
    if let Some(handle) = PROMETHEUS_HANDLE.get() {
        return Ok(handle.clone());
    }

    let handle = PrometheusBuilder::new()
        .install_recorder()
        .context(MetricsRecorderSnafu)?;
    let shared_handle = Arc::new(handle);

    metrics::describe_gauge!(
        "mm_metrics_exporter_up",
        "Set to 1 when the market-maker metrics recorder is installed."
    );
    metrics::gauge!("mm_metrics_exporter_up").set(1.0);

    metrics::describe_histogram!(
        QUOTE_LATENCY_METRIC,
        "Latency in seconds for responding to RFQ quote requests."
    );

    metrics::describe_gauge!(
        "mm_quote_eth_priority_fee_gwei",
        "Ethereum max priority fee in gwei (after safety multiplier)"
    );

    metrics::describe_gauge!(
        "mm_quote_btc_sats_per_vbyte",
        "Bitcoin fee rate in sats per vByte (after safety multiplier)"
    );

    if PROMETHEUS_HANDLE.set(shared_handle.clone()).is_err() {
        if let Some(existing) = PROMETHEUS_HANDLE.get() {
            return Ok(existing.clone());
        }
    }

    Ok(shared_handle)
}

fn setup_metrics(join_set: &mut JoinSet<Result<()>>, addr: SocketAddr) -> Result<()> {
    let shared_handle = install_metrics_recorder()?;

    let upkeep_handle = shared_handle.clone();
    join_set.spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(10));
        loop {
            ticker.tick().await;
            upkeep_handle.run_upkeep();
        }
    });

    let metrics_state = shared_handle.clone();

    join_set.spawn(async move {
        let listener = TcpListener::bind(addr)
            .await
            .context(MetricsServerBindSnafu { addr })?;

        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .with_state(metrics_state);

        axum::serve(listener, app)
            .await
            .context(MetricsServerSnafu)?;

        Ok(())
    });

    Ok(())
}

fn setup_admin_api(
    join_set: &mut JoinSet<Result<()>>,
    addr: SocketAddr,
    deposit_repository: Arc<crate::db::DepositRepository>,
    bitcoin_wallet: Arc<BitcoinWallet>,
    evm_wallet: Arc<EVMWallet>,
) -> Result<()> {
    let state = admin_api::AdminApiState {
        deposit_repository,
        bitcoin_wallet,
        evm_wallet,
    };

    let app = admin_api::create_admin_router(state);

    join_set.spawn(async move {
        let listener = TcpListener::bind(addr)
            .await
            .context(AdminApiServerBindSnafu { addr })?;

        info!("Admin API server listening on {}", addr);

        axum::serve(listener, app)
            .await
            .context(AdminApiServerSnafu)?;

        Ok(())
    });

    Ok(())
}

async fn metrics_handler(State(handle): State<Arc<PrometheusHandle>>) -> impl IntoResponse {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        handle.render(),
    )
}
