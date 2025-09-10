use std::{
    env::current_dir,
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
    time::Duration,
};

use bitcoincore_rpc_async::Auth;
use blockchain_utils::create_websocket_wallet_provider;
use ctor::ctor;
use devnet::MultichainAccount;
use market_maker::{evm_wallet::EVMWallet, MarketMakerArgs};
use otc_server::{api::SwapResponse, OtcServerArgs};
use rfq_server::RfqServerArgs;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Connection, PgConnection, Pool, Postgres,
};
use tokio::{net::TcpListener, task::JoinSet};
use tracing::info;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

pub trait PgConnectOptionsExt {
    fn to_database_url(&self) -> String;
}

impl PgConnectOptionsExt for PgConnectOptions {
    fn to_database_url(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.get_username(),
            "password",
            self.get_host(),
            self.get_port(),
            self.get_database().expect("database should be set")
        )
    }
}

pub async fn get_free_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("Should be able to bind to port");

    listener
        .local_addr()
        .expect("Should have a local address")
        .port()
}

pub const TEST_MARKET_MAKER_ID: &str = "550e8400-e29b-41d4-a716-446655440000";
pub const TEST_API_KEY_ID: &str = "d2e0a695-e3b1-494e-b645-1b41a72d7e75";
pub const TEST_API_KEY: &str = "7KNJu1t1j9DtVqS0d8FB6pfX0nkqr4TX";
pub const TEST_MM_WHITELIST_FILE: &str =
    "integration-tests/src/utils/test_whitelisted_market_makers.json";
pub const INTEGRATION_TEST_TIMEOUT_SECS: u64 = 60;

pub fn get_whitelist_file_path() -> String {
    // Convert relative path to absolute path from workspace root
    let mut current_dir = current_dir().expect("Should be able to get current directory");

    // If we're already in integration-tests, go up to workspace root
    if current_dir.file_name().and_then(|n| n.to_str()) == Some("integration-tests") {
        current_dir = current_dir.parent().unwrap().to_path_buf();
    }

    let whitelist_file_path = current_dir.join(TEST_MM_WHITELIST_FILE);
    whitelist_file_path.to_string_lossy().to_string()
}

pub async fn wait_for_otc_server_to_be_ready(otc_port: u16) {
    // Hit the otc server status endpoint every 100ms until it returns 200
    let client = reqwest::Client::new();
    let status_url = format!("http://127.0.0.1:{otc_port}/status");

    let start_time = std::time::Instant::now();
    let timeout = Duration::from_secs(INTEGRATION_TEST_TIMEOUT_SECS);

    loop {
        assert!(
            (start_time.elapsed() <= timeout),
            "Timeout waiting for OTC server to become ready"
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        if let Ok(response) = client.get(&status_url).send().await {
            if response.status() == 200 {
                println!("OTC server is ready!");
                break;
            }
        }
    }
}

pub async fn wait_for_rfq_server_to_be_ready(rfq_port: u16) {
    // Hit the rfq server status endpoint every 100ms until it returns 200
    let client = reqwest::Client::new();
    let status_url = format!("http://127.0.0.1:{rfq_port}/status");

    let start_time = std::time::Instant::now();
    let timeout = Duration::from_secs(INTEGRATION_TEST_TIMEOUT_SECS);

    loop {
        assert!(
            (start_time.elapsed() <= timeout),
            "Timeout waiting for RFQ server to become ready"
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        if let Ok(response) = client.get(&status_url).send().await {
            if response.status() == 200 {
                println!("RFQ server is ready!");
                break;
            }
        }
    }
}

pub async fn wait_for_swap_to_be_settled(otc_port: u16, swap_id: Uuid) {
    let client = reqwest::Client::new();

    let start_time = std::time::Instant::now();
    let mut last_log_time = std::time::Instant::now();
    let log_interval = Duration::from_secs(5);
    let timeout = Duration::from_secs(INTEGRATION_TEST_TIMEOUT_SECS);
    // now call the otc-server swap status endpoint until it's detected as complete
    loop {
        let response = client
            .get(format!(
                "http://localhost:{otc_port}/api/v1/swaps/{swap_id}"
            ))
            .send()
            .await
            .unwrap();
        let response_json: SwapResponse = response.json().await.unwrap();
        if last_log_time.elapsed() > log_interval {
            info!("Response from swap status endpoint: {:#?}", response_json);
            last_log_time = std::time::Instant::now();
        }
        if start_time.elapsed() > timeout {
            info!(
                "Final response from swap status endpoint: {:#?}",
                response_json
            );
            panic!("Timeout waiting for swap to be settled");
        }
        if response_json.status == "Settled" {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

pub async fn wait_for_market_maker_to_connect_to_rfq_server(rfq_port: u16) {
    let client = reqwest::Client::new();
    let connected_url = format!("http://127.0.0.1:{rfq_port}/api/v1/market-makers/connected");

    let start_time = std::time::Instant::now();
    let timeout = Duration::from_secs(INTEGRATION_TEST_TIMEOUT_SECS);

    loop {
        assert!(
            (start_time.elapsed() <= timeout),
            "Timeout waiting for market maker to connect to RFQ server"
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        if let Ok(response) = client.get(&connected_url).send().await {
            if response.status() == 200 {
                if let Ok(body) = response.json::<serde_json::Value>().await {
                    if let Some(market_makers) = body["market_makers"].as_array() {
                        if market_makers.len() == 1
                            && market_makers[0].as_str() == Some(TEST_MARKET_MAKER_ID)
                        {
                            println!("Market maker is connected to RFQ server!");
                            break;
                        }
                    }
                }
            }
        }
    }
}

pub fn build_bitcoin_wallet_descriptor(private_key: &bitcoin::PrivateKey) -> String {
    format!("wpkh({private_key})")
}

pub fn build_tmp_bitcoin_wallet_db_file() -> String {
    format!("/tmp/bitcoin_wallet_{}.db", uuid::Uuid::new_v4())
}
// TODO:

pub async fn build_mm_test_args(
    otc_port: u16,
    rfq_port: u16,
    multichain_account: &MultichainAccount,
    devnet: &devnet::RiftDevnet,
    connect_options: &PgConnectOptions,
) -> MarketMakerArgs {
    let db_url = create_test_database(connect_options).await.unwrap();
    MarketMakerArgs {
        market_maker_id: TEST_MARKET_MAKER_ID.to_string(),
        api_key_id: TEST_API_KEY_ID.to_string(),
        api_key: TEST_API_KEY.to_string(),
        otc_ws_url: format!("ws://127.0.0.1:{otc_port}/ws/mm"),
        rfq_ws_url: format!("ws://127.0.0.1:{rfq_port}/ws/mm"),
        log_level: "info".to_string(),
        bitcoin_wallet_db_file: build_tmp_bitcoin_wallet_db_file(),
        bitcoin_wallet_descriptor: build_bitcoin_wallet_descriptor(
            &multichain_account.bitcoin_wallet.private_key,
        ),
        bitcoin_wallet_network: bitcoin::Network::Regtest,
        bitcoin_wallet_esplora_url: devnet.bitcoin.esplora_url.as_ref().unwrap().to_string(),
        ethereum_wallet_private_key: multichain_account.secret_bytes,
        ethereum_confirmations: 1,
        ethereum_rpc_ws_url: devnet.ethereum.anvil.ws_endpoint(),
        trade_spread_bps: 0,
        fee_safety_multiplier: 1.5,
        database_url: db_url,
        inventory_target_ratio_bps: 5000,
        rebalance_tolerance_bps: 2500,
        balance_utilization_threshold_bps: 9500, // 95%, require headroom so 100% utilization is rejected in tests
        coinbase_exchange_api_base_url: "https://api.coinbase.com".parse().unwrap(), // TODO: This needs to be a test server url
        coinbase_exchange_api_key: "".to_string(),
        coinbase_exchange_api_passphrase: "".to_string(),
        coinbase_exchange_api_secret: "".to_string(),
        auto_manage_inventory: false,
    }
}

pub async fn create_test_database(connect_options: &PgConnectOptions) -> sqlx::Result<String> {
    let mut admin =
        PgConnection::connect_with(&connect_options.clone().database("postgres")).await?;

    let db = format!("test_db_{}", Uuid::new_v4().simple());

    sqlx::query(&format!("CREATE DATABASE {db}"))
        .execute(&mut admin)
        .await?;

    let db_url = connect_options.clone().database(&db).to_database_url();

    Ok(db_url)
}

pub fn build_rfq_server_test_args(rfq_port: u16) -> RfqServerArgs {
    RfqServerArgs {
        port: rfq_port,
        host: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        log_level: "info".to_string(),
        whitelist_file: get_whitelist_file_path(),
        quote_timeout_milliseconds: 5000,
        cors_domain: None,
        chainalysis_host: None,
        chainalysis_token: None,
    }
}

pub async fn build_otc_server_test_args(
    otc_port: u16,
    devnet: &devnet::RiftDevnet,
    connect_options: &PgConnectOptions,
) -> OtcServerArgs {
    let db_url = create_test_database(connect_options).await.unwrap();
    OtcServerArgs {
        port: otc_port,
        database_url: db_url,
        whitelist_file: get_whitelist_file_path(),
        host: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        log_level: "debug".to_string(),
        ethereum_mainnet_rpc_url: devnet.ethereum.anvil.endpoint(),
        ethereum_mainnet_token_indexer_url: devnet
            .ethereum
            .token_indexer
            .as_ref()
            .unwrap()
            .api_server_url
            .clone(),
        ethereum_mainnet_chain_id: devnet.ethereum.anvil.chain_id(),
        bitcoin_rpc_url: devnet.bitcoin.rpc_url_with_cookie.clone(),
        bitcoin_rpc_auth: Auth::CookieFile(devnet.bitcoin.cookie.clone()),
        esplora_http_server_url: devnet.bitcoin.esplora_url.as_ref().unwrap().to_string(),
        bitcoin_network: bitcoin::network::Network::Regtest,
        chain_monitor_interval_seconds: 2,
        cors_domain: None,
        chainalysis_host: None,
        chainalysis_token: None,
    }
}

pub async fn build_test_user_ethereum_wallet(
    devnet: &devnet::RiftDevnet,
    account: &MultichainAccount,
) -> (JoinSet<market_maker::Result<()>>, EVMWallet) {
    let private_key = account.secret_bytes;
    let provider =
        create_websocket_wallet_provider(&devnet.ethereum.anvil.ws_endpoint(), private_key)
            .await
            .unwrap();
    let mut join_set = JoinSet::new();
    let wallet = EVMWallet::new(
        Arc::new(provider),
        devnet.ethereum.anvil.ws_endpoint(),
        1,
        None,
        &mut join_set,
    );
    (join_set, wallet)
}

#[ctor]
fn init_test_tracing() {
    let has_nocapture = std::env::args().any(|arg| arg == "--nocapture" || arg == "--show-output");
    if has_nocapture {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                EnvFilter::new("info,otc_server=debug,otc_chains=debug,market-maker=debug")
            }))
            .try_init()
            .ok();
    }
}
