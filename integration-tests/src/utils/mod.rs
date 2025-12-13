mod test_proxy;

use otc_models::ChainType;
pub use test_proxy::TestProxy;

use std::{
    env::current_dir,
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
    time::Duration,
    time::Instant,
};

use bitcoincore_rpc_async::Auth;
use blockchain_utils::{create_websocket_wallet_provider, init_logger};
use ctor::ctor;
use devnet::MultichainAccount;
use market_maker::{evm_wallet::EVMWallet, MarketMakerArgs};
use otc_server::{
    api::{
        SwapResponse,
    },
    OtcServerArgs,
};
use rfq_server::RfqServerArgs;
use sqlx::{postgres::PgConnectOptions, Connection, PgConnection};
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

// Ethereum market maker credentials
pub const TEST_MARKET_MAKER_TAG: &str = "test-mm-eth";
pub const TEST_MARKET_MAKER_API_ID: &str = "96c0bedb-bfda-4680-a8df-1317d1e09c8d";
pub const TEST_MARKET_MAKER_API_SECRET: &str = "Bt7nDfOLlstMLLMvj3dlY3kFozxHk6An";

// Base market maker credentials
pub const TEST_BASE_MARKET_MAKER_TAG: &str = "test-mm-base";
pub const TEST_BASE_MARKET_MAKER_API_ID: &str = "f901369b-84d7-4c03-8799-f504c22125f9";
pub const TEST_BASE_MARKET_MAKER_API_SECRET: &str = "5iAlXNoDVsGvjwiEjqhbLBVOpAN0wFZ8";

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

pub async fn wait_for_swap_status(
    client: &reqwest::Client,
    otc_port: u16,
    swap_id: Uuid,
    expected_status: &str,
) -> SwapResponse {
    let timeout = Duration::from_secs(crate::utils::INTEGRATION_TEST_TIMEOUT_SECS);
    let start = Instant::now();
    let poll_interval = Duration::from_millis(500);
    let url = format!("http://localhost:{otc_port}/api/v2/swap/{swap_id}");

    loop {
        let response = client.get(&url).send().await.unwrap();
        let response_json: SwapResponse = response.json().await.unwrap();

        if response_json.status == expected_status {
            return response_json;
        }

        if start.elapsed() > timeout {
            panic!(
                "Timeout waiting for swap {swap_id} status to become {expected_status}, last response: {response_json:#?}"
            );
        }

        tokio::time::sleep(poll_interval).await;
    }
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
                "http://localhost:{otc_port}/api/v2/swap/{swap_id}"
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
    let status_url = format!("http://127.0.0.1:{rfq_port}/status");

    let start_time = std::time::Instant::now();
    let timeout = Duration::from_secs(INTEGRATION_TEST_TIMEOUT_SECS);

    loop {
        assert!(
            (start_time.elapsed() <= timeout),
            "Timeout waiting for market maker to connect to RFQ server"
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        if let Ok(response) = client.get(&status_url).send().await {
            if response.status() == 200 {
                if let Ok(body) = response.json::<serde_json::Value>().await {
                    if let Some(connected_mms) = body["connected_market_makers"].as_array() {
                        if connected_mms.len() == 1
                            && connected_mms[0].as_str() == Some(TEST_MARKET_MAKER_API_ID)
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

/// Wait for multiple market makers to connect to the RFQ server.
pub async fn wait_for_market_makers_to_connect_to_rfq_server(rfq_port: u16, expected_mm_ids: &[&str]) {
    let client = reqwest::Client::new();
    let status_url = format!("http://127.0.0.1:{rfq_port}/status");

    let start_time = std::time::Instant::now();
    let timeout = Duration::from_secs(INTEGRATION_TEST_TIMEOUT_SECS);

    loop {
        assert!(
            start_time.elapsed() <= timeout,
            "Timeout waiting for market makers to connect to RFQ server"
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        if let Ok(response) = client.get(&status_url).send().await {
            if response.status() == 200 {
                if let Ok(body) = response.json::<serde_json::Value>().await {
                    if let Some(connected_mms) = body["connected_market_makers"].as_array() {
                        let connected_ids: Vec<&str> = connected_mms
                            .iter()
                            .filter_map(|v| v.as_str())
                            .collect();
                        
                        let all_connected = expected_mm_ids
                            .iter()
                            .all(|id| connected_ids.contains(id));
                        
                        if all_connected {
                            println!(
                                "All {} market makers connected to RFQ server!",
                                expected_mm_ids.len()
                            );
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

pub async fn build_mm_test_args(
    otc_port: u16,
    rfq_port: u16,
    multichain_account: &MultichainAccount,
    devnet: &devnet::RiftDevnet,
    connect_options: &PgConnectOptions,
) -> MarketMakerArgs {
    let coinbase_exchange_api_base_url = format!("http://127.0.0.1:{}", devnet.coinbase_mock_server_port.unwrap_or(8080));
    let auto_manage_inventory = devnet.coinbase_mock_server_port.is_some();
    let db_url = create_test_database(connect_options).await.unwrap();
    MarketMakerArgs {
        enable_tokio_console_subscriber: false,
        admin_api_listen_addr: None,
        bitcoin_batch_interval_secs: 1,
        bitcoin_batch_size: 100,
        ethereum_batch_interval_secs: 1,
        ethereum_batch_size: 392,
        market_maker_tag: TEST_MARKET_MAKER_TAG.to_string(),
        market_maker_id: TEST_MARKET_MAKER_API_ID.to_string(),
        api_secret: TEST_MARKET_MAKER_API_SECRET.to_string(),
        otc_ws_url: format!("ws://127.0.0.1:{otc_port}/ws/mm"),
        rfq_ws_url: format!("ws://127.0.0.1:{rfq_port}/ws/mm"),
        log_level: "info".to_string(),
        bitcoin_wallet_db_file: build_tmp_bitcoin_wallet_db_file(),
        bitcoin_wallet_descriptor: build_bitcoin_wallet_descriptor(
            &multichain_account.bitcoin_wallet.private_key,
        ),
        bitcoin_wallet_network: bitcoin::Network::Regtest,
        bitcoin_wallet_esplora_url: devnet.bitcoin.esplora_url.as_ref().unwrap().to_string(),
        evm_chain: otc_models::ChainType::Ethereum, // Default to Ethereum for tests
        evm_wallet_private_key: multichain_account.secret_bytes,
        evm_confirmations: 1,
        evm_rpc_ws_url: devnet.ethereum.anvil.ws_endpoint(),
        trade_spread_bps: 0,
        fee_safety_multiplier: 1.5,
        database_url: db_url,
        db_max_connections: 10,
        db_min_connections: 2,
        inventory_target_ratio_bps: 5000,
        rebalance_tolerance_bps: 2500,
        rebalance_poll_interval_secs: 5,
        balance_utilization_threshold_bps: 9500, // 95%, require headroom so 100% utilization is rejected in tests
        confirmation_poll_interval_secs: 1, // Fast polling for tests since blocks are mined every 1s
        btc_coinbase_confirmations: 2, // Reduced for tests (production: 3)
        cbbtc_coinbase_confirmations: 3, // Greatly reduced for tests (production: 36)
        coinbase_exchange_api_base_url: coinbase_exchange_api_base_url.parse().unwrap(),
        coinbase_exchange_api_key: "".to_string(),
        coinbase_exchange_api_passphrase: "".to_string(),
        coinbase_exchange_api_secret: "".to_string(),
        auto_manage_inventory: auto_manage_inventory,
        metrics_listen_addr: None,
        batch_monitor_interval_secs: 5,
        ethereum_max_deposits_per_lot: 350,
        bitcoin_max_deposits_per_lot: 100,
        loki_url: None,
        fee_settlement_rail: market_maker::FeeSettlementRail::Evm,
        fee_settlement_interval_secs: 300,
        fee_settlement_evm_confirmations: None,
    }
}

/// Build market maker args configured for Base chain.
pub async fn build_mm_test_args_for_base(
    otc_port: u16,
    rfq_port: u16,
    multichain_account: &MultichainAccount,
    devnet: &devnet::RiftDevnet,
    connect_options: &PgConnectOptions,
) -> MarketMakerArgs {
    let coinbase_exchange_api_base_url =
        format!("http://127.0.0.1:{}", devnet.coinbase_mock_server_port.unwrap_or(8080));
    let auto_manage_inventory = devnet.coinbase_mock_server_port.is_some();
    let db_url = create_test_database(connect_options).await.unwrap();
    MarketMakerArgs {
        enable_tokio_console_subscriber: false,
        admin_api_listen_addr: None,
        bitcoin_batch_interval_secs: 1,
        bitcoin_batch_size: 100,
        ethereum_batch_interval_secs: 1,
        ethereum_batch_size: 392,
        market_maker_tag: TEST_BASE_MARKET_MAKER_TAG.to_string(),
        market_maker_id: TEST_BASE_MARKET_MAKER_API_ID.to_string(),
        api_secret: TEST_BASE_MARKET_MAKER_API_SECRET.to_string(),
        otc_ws_url: format!("ws://127.0.0.1:{otc_port}/ws/mm"),
        rfq_ws_url: format!("ws://127.0.0.1:{rfq_port}/ws/mm"),
        log_level: "info".to_string(),
        bitcoin_wallet_db_file: build_tmp_bitcoin_wallet_db_file(),
        bitcoin_wallet_descriptor: build_bitcoin_wallet_descriptor(
            &multichain_account.bitcoin_wallet.private_key,
        ),
        bitcoin_wallet_network: bitcoin::Network::Regtest,
        bitcoin_wallet_esplora_url: devnet.bitcoin.esplora_url.as_ref().unwrap().to_string(),
        evm_chain: otc_models::ChainType::Base, // Configured for Base
        evm_wallet_private_key: multichain_account.secret_bytes,
        evm_confirmations: 1,
        evm_rpc_ws_url: devnet.base.anvil.ws_endpoint(),
        trade_spread_bps: 0,
        fee_safety_multiplier: 1.5,
        database_url: db_url,
        db_max_connections: 10,
        db_min_connections: 2,
        inventory_target_ratio_bps: 5000,
        rebalance_tolerance_bps: 2500,
        rebalance_poll_interval_secs: 5,
        balance_utilization_threshold_bps: 9500,
        confirmation_poll_interval_secs: 1,
        btc_coinbase_confirmations: 2,
        cbbtc_coinbase_confirmations: 3,
        coinbase_exchange_api_base_url: coinbase_exchange_api_base_url.parse().unwrap(),
        coinbase_exchange_api_key: "".to_string(),
        coinbase_exchange_api_passphrase: "".to_string(),
        coinbase_exchange_api_secret: "".to_string(),
        auto_manage_inventory,
        metrics_listen_addr: None,
        batch_monitor_interval_secs: 5,
        ethereum_max_deposits_per_lot: 350,
        bitcoin_max_deposits_per_lot: 100,
        loki_url: None,
        fee_settlement_rail: market_maker::FeeSettlementRail::Evm,
        fee_settlement_interval_secs: 5, // Fast settlement for tests
        fee_settlement_evm_confirmations: None,
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
        otc_server_url: None,
        port: rfq_port,
        host: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        log_level: "info".to_string(),
        quote_timeout_milliseconds: 5000,
        cors_domain: None,
        chainalysis_host: None,
        chainalysis_token: None,
        metrics_listen_addr: None,
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
        host: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        log_level: "debug".to_string(),
        ethereum_mainnet_rpc_url: devnet.ethereum.anvil.endpoint(),
        untrusted_ethereum_mainnet_token_indexer_url: devnet
            .ethereum
            .token_indexer
            .as_ref()
            .unwrap()
            .api_server_url
            .clone(),
        ethereum_allowed_token: "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf".to_string(),
        base_rpc_url: devnet.base.anvil.endpoint(),
        untrusted_base_token_indexer_url: devnet
            .base
            .token_indexer
            .as_ref()
            .map(|indexer| indexer.api_server_url.clone())
            .unwrap_or_else(|| "http://localhost:42069".to_string()), 
        base_allowed_token: "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf".to_string(),
        bitcoin_rpc_url: devnet.bitcoin.rpc_url_with_cookie.clone(),
        bitcoin_rpc_auth: Auth::CookieFile(devnet.bitcoin.cookie.clone()),
        untrusted_esplora_http_server_url: devnet.bitcoin.esplora_url.as_ref().unwrap().to_string(),
        bitcoin_network: bitcoin::network::Network::Regtest,
        chain_monitor_interval_seconds: 2,
        cors_domain: None,
        chainalysis_host: None,
        chainalysis_token: None,
        config_dir: devnet
            .otc_server_config_dir
            .path()
            .to_string_lossy()
            .to_string(),
        dstack_sock_path: "/var/run/dstack.sock".to_string(),
        metrics_listen_addr: None,
        db_max_connections: 10,
        db_min_connections: 2,
        max_concurrent_swaps: 10,
        loki_url: None,
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
        ChainType::Ethereum,
        None,
        350,
        &mut join_set,
    );
    (join_set, wallet)
}

pub async fn build_test_user_base_wallet(
    devnet: &devnet::RiftDevnet,
    account: &MultichainAccount,
) -> (JoinSet<market_maker::Result<()>>, EVMWallet) {
    let private_key = account.secret_bytes;
    let provider =
        create_websocket_wallet_provider(&devnet.base.anvil.ws_endpoint(), private_key)
            .await
            .unwrap();
    let mut join_set = JoinSet::new();
    let wallet = EVMWallet::new(
        Arc::new(provider),
        devnet.base.anvil.ws_endpoint(),
        1,
        ChainType::Base,
        None,
        350,
        &mut join_set,
    );
    (join_set, wallet)
}

#[ctor]
fn init_test_tracing() {
    let has_nocapture = std::env::args().any(|arg| arg == "--nocapture" || arg == "--show-output");
    if has_nocapture {
        init_logger("info,otc_server=debug,otc_chains=debug,market-maker=debug", None::<console_subscriber::ConsoleLayer>).expect("Logger should initialize");
    }
}
