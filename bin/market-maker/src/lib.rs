mod admin_api;
mod balance_strat;
mod batch_monitor;
pub mod bitcoin_wallet;
pub mod cb_bitcoin_converter;
mod config;
pub mod db;
pub mod evm_wallet;
mod liquidity_cache;
mod otc_client;
mod otc_handler;
pub mod payment_manager;
pub mod price_oracle;
mod rfq_client;
mod rfq_handler;
pub mod wallet;
mod wrapped_bitcoin_quoter;

use blockchain_utils::shutdown_signal;
pub use wallet::WalletError;
pub use wallet::WalletResult;

use std::{net::SocketAddr, sync::Arc, sync::OnceLock, time::Duration};

use alloy::{providers::Provider, signers::local::PrivateKeySigner};
use bdk_wallet::bitcoin;
use blockchain_utils::{create_websocket_wallet_provider, handle_background_thread_result};
use clap::Parser;
use config::Config;
use otc_models::ChainType;
use reqwest::Url;
use snafu::{prelude::*, ResultExt};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::info;

use axum::{
    extract::State, http::header, http::StatusCode, response::IntoResponse, routing::get, Router,
};
use metrics_exporter_prometheus::{BuildError, PrometheusBuilder, PrometheusHandle};
use tokio::net::TcpListener;
use uuid::Uuid;

use crate::payment_manager::PaymentManager;
use crate::{
    bitcoin_wallet::BitcoinWallet,
    cb_bitcoin_converter::{coinbase_client::CoinbaseClient, run_rebalancer, BandsParams},
    db::Database,
    evm_wallet::EVMWallet,
    wallet::WalletManager,
    wrapped_bitcoin_quoter::WrappedBitcoinQuoter,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Configuration error: {}", source))]
    Config { source: config::ConfigError },

    #[snafu(display("Client error: {}", source))]
    Client {
        #[snafu(source(from(otc_client::ClientError, Box::new)))]
        source: Box<otc_client::ClientError>,
    },

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

    #[snafu(display("Coinbase client error: {}", source))]
    CoinbaseClientError {
        #[snafu(source(from(cb_bitcoin_converter::coinbase_client::ConversionError, Box::new)))]
        source: Box<cb_bitcoin_converter::coinbase_client::ConversionError>,
    },

    #[snafu(display("Conversion actor error: {}", source))]
    ConversionActor {
        #[snafu(source(from(cb_bitcoin_converter::ConversionActorError, Box::new)))]
        source: Box<cb_bitcoin_converter::ConversionActorError>,
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

impl From<otc_client::ClientError> for Error {
    fn from(error: otc_client::ClientError) -> Self {
        Error::Client {
            source: Box::new(error),
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

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

    /// Ethereum wallet private key
    #[arg(long, env = "ETHEREUM_WALLET_PRIVATE_KEY", value_parser = parse_hex_string)]
    pub ethereum_wallet_private_key: [u8; 32],

    /// Number of confirmations required before broadcasting the next transaction. Defaults to 2 as empirically this
    /// seems to be enough to ensure EIP-7702 delegated accounts never exceed their in-flight transaction limit (of 1 in most cases),
    /// which would cause mempool rejections. Annoyingly, setting to this 1 will lead to transient EIP-7702 broadcast errors.
    #[arg(long, env = "ETHEREUM_CONFIRMATIONS", default_value = "2")]
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

pub async fn run_market_maker(
    args: MarketMakerArgs,
) -> Result<()> {
    let cancellation_token = CancellationToken::new();
    let mut join_set: JoinSet<Result<()>> = JoinSet::new();
    let market_maker_id = Uuid::parse_str(&args.market_maker_id).map_err(|e| Error::Config {
        source: config::ConfigError::InvalidUuid {
            uuid: args.market_maker_id,
            error: e,
        },
    })?;

    info!("Starting market maker with ID: {}", market_maker_id);

    if let Some(addr) = args.metrics_listen_addr {
        info!("Setting up metrics listener on {}", addr);
        setup_metrics(&mut join_set, addr)?;
    }

    let admin_api_addr = args.admin_api_listen_addr;

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
        Some(deposit_repository.clone()),
        args.ethereum_max_deposits_per_lot,
        &mut join_set,
    ));

    evm_wallet
        .ensure_eip7702_delegation(
            PrivateKeySigner::from_slice(&args.ethereum_wallet_private_key).unwrap(),
        )
        .await
        .expect("Should have been able to ensure EIP-7702 delegation");

    // Setup admin API server if address is provided
    if let Some(addr) = admin_api_addr {
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
    wallet_manager.register(ChainType::Ethereum, evm_wallet.clone());

    let wallet_manager = Arc::new(wallet_manager);
    let balance_strategy = Arc::new(
        balance_strat::QuoteBalanceStrategy::new(args.balance_utilization_threshold_bps),
    );

    let btc_eth_price_oracle = price_oracle::BitcoinEtherPriceOracle::new(&mut join_set);

    let wrapped_bitcoin_quoter = Arc::new(WrappedBitcoinQuoter::new(
        wallet_manager.clone(),
        balance_strategy.clone(),
        btc_eth_price_oracle,
        esplora_client,
        provider.clone().erased(),
        args.trade_spread_bps,
        args.fee_safety_multiplier,
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
        ChainType::Ethereum,
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
        otc_response_tx,
        cancellation_token.clone(),
        &mut payment_manager_join_set,
    ));

    let otc_fill_client = otc_client::OtcFillClient::new(
        Config {
            market_maker_id,
            api_secret: args.api_secret.clone(),
            otc_ws_url: args.otc_ws_url.clone(),
            reconnect_interval_secs: 5,
            max_reconnect_attempts: 250,
        },
        quote_repository.clone(),
        deposit_repository.clone(),
        payment_manager.clone(),
        payment_repository.clone(),
        otc_response_rx,
    );
    join_set.spawn(async move { otc_fill_client.run().await.map_err(Error::from) });

    // Create liquidity cache for RFQ liquidity requests
    let liquidity_cache = Arc::new(liquidity_cache::LiquidityCache::new(wallet_manager.clone(), balance_strategy.clone()));

    let rfq_handler = rfq_handler::RFQMessageHandler::new(
        market_maker_id,
        wrapped_bitcoin_quoter.clone(),
        quote_repository,
        liquidity_cache,
    );

    // Add RFQ client for handling quote requests
    let mut rfq_client = rfq_client::RfqClient::new(
        Config {
            market_maker_id,
            api_secret: args.api_secret.clone(),
            otc_ws_url: args.otc_ws_url,
            reconnect_interval_secs: 5,
            max_reconnect_attempts: 5,
        },
        rfq_handler,
        args.rfq_ws_url,
    );
    // make sure fees + balances are initiailized before quotes can be computed
    wrapped_bitcoin_quoter
        .ensure_cache_ready()
        .await
        .context(WrappedBitcoinQuoterSnafu)?;
    join_set.spawn(async move {
        rfq_client.run().await.map_err(|e| Error::Client {
            source: Box::new(otc_client::ClientError::BackgroundThreadExited {
                source: Box::new(e),
            }),
        })
    });

    let coinbase_client = CoinbaseClient::new(
        args.coinbase_exchange_api_base_url,
        args.coinbase_exchange_api_key,
        args.coinbase_exchange_api_passphrase,
        args.coinbase_exchange_api_secret,
    )
    .context(CoinbaseClientSnafu)?;

    // Run rebalancer, even if auto manage inventory is disabled
    // b/c it's still useful to track the balance of the wallets in metrics
    let conversion_actor = run_rebalancer(
        coinbase_client,
        bitcoin_wallet.clone(),
        evm_wallet.clone(),
        BandsParams {
            target_bps: args.inventory_target_ratio_bps,
            band_width_bps: args.rebalance_tolerance_bps,
            poll_interval: Duration::from_secs(60),
        },
        args.auto_manage_inventory,
    );

    join_set.spawn(async move { conversion_actor.await.context(ConversionActorSnafu) });


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
