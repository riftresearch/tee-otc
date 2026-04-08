mod test_proxy;

use otc_models::ChainType;
pub use test_proxy::TestProxy;

use std::{
    collections::HashMap,
    env::current_dir,
    fs::OpenOptions,
    io::{Read, Seek, SeekFrom, Write},
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
    time::Instant,
};

use bitcoincore_rpc_async::Auth;
use blockchain_utils::{create_websocket_wallet_provider, init_logger};
use ctor::ctor;
use devnet::MultichainAccount;
use fs2::FileExt;
use market_maker::{evm_wallet::EVMWallet, MarketMakerArgs};
use otc_models::Swap;
use otc_server::OtcServerArgs;
use rfq_server::RfqServerArgs;
use sauron::{run as run_sauron, SauronArgs};
use sqlx::{postgres::PgConnectOptions, Connection, PgConnection};
use tokio::{net::TcpListener, task::JoinSet};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const PORT_ALLOCATOR_FILE: &str = "tee-otc-test-port-allocator";
const PORT_ALLOCATOR_START: u16 = 20000;
const PORT_ALLOCATOR_END: u16 = 30000;

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
    loop {
        let candidate_port = next_allocated_port();
        if let Ok(listener) = TcpListener::bind(("127.0.0.1", candidate_port)).await {
            drop(listener);
            return candidate_port;
        }
    }
}

fn next_allocated_port() -> u16 {
    let allocator_file_path = std::env::temp_dir().join(PORT_ALLOCATOR_FILE);
    let mut allocator_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&allocator_file_path)
        .unwrap_or_else(|error| {
            panic!(
                "Should be able to open integration-test port allocator at {}: {}",
                allocator_file_path.display(),
                error
            )
        });

    allocator_file
        .lock_exclusive()
        .expect("Should be able to acquire integration-test port allocator lock");

    let mut buffer = String::new();
    allocator_file
        .read_to_string(&mut buffer)
        .expect("Should be able to read integration-test port allocator");

    let candidate_port = buffer
        .trim()
        .parse::<u16>()
        .ok()
        .filter(|port| *port >= PORT_ALLOCATOR_START && *port < PORT_ALLOCATOR_END)
        .unwrap_or(PORT_ALLOCATOR_START);

    let next_port = if candidate_port + 1 >= PORT_ALLOCATOR_END {
        PORT_ALLOCATOR_START
    } else {
        candidate_port + 1
    };

    allocator_file
        .set_len(0)
        .expect("Should be able to truncate integration-test port allocator");
    allocator_file
        .seek(SeekFrom::Start(0))
        .expect("Should be able to rewind integration-test port allocator");
    write!(allocator_file, "{next_port}")
        .expect("Should be able to update integration-test port allocator");
    allocator_file
        .unlock()
        .expect("Should be able to unlock integration-test port allocator");

    candidate_port
}

// Ethereum market maker credentials
pub const TEST_MARKET_MAKER_TAG: &str = "test-mm-eth";
pub const TEST_MARKET_MAKER_API_ID: &str = "96c0bedb-bfda-4680-a8df-1317d1e09c8d";
pub const TEST_MARKET_MAKER_API_SECRET: &str = "Bt7nDfOLlstMLLMvj3dlY3kFozxHk6An";

// Base market maker credentials
pub const TEST_BASE_MARKET_MAKER_TAG: &str = "test-mm-base";
pub const TEST_BASE_MARKET_MAKER_API_ID: &str = "f901369b-84d7-4c03-8799-f504c22125f9";
pub const TEST_BASE_MARKET_MAKER_API_SECRET: &str = "5iAlXNoDVsGvjwiEjqhbLBVOpAN0wFZ8";
pub const TEST_FEE_SET_API_SECRET: &str = "Bt7nDfOLlstMLLMvj3dlY3kFozxHk6An";
pub const TEST_DETECTOR_API_ID: &str = "019d01bc-4352-75f1-bbe0-128040d0e781";
pub const TEST_DETECTOR_API_SECRET: &str = "5SSXJSnQOAxycnzLOlr3BBVVAQnwYiza";

pub const TEST_MM_WHITELIST_FILE: &str =
    "integration-tests/src/utils/test_whitelisted_market_makers.json";
pub const INTEGRATION_TEST_TIMEOUT_SECS: u64 = 60;

static SAURON_AUTOSTART_BY_OTC_PORT: OnceLock<Mutex<HashMap<u16, SauronArgs>>> = OnceLock::new();

fn sauron_autostart_registry() -> &'static Mutex<HashMap<u16, SauronArgs>> {
    SAURON_AUTOSTART_BY_OTC_PORT.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn disable_auto_sauron_for_port(otc_port: u16) {
    sauron_autostart_registry()
        .lock()
        .expect("sauron autostart registry lock poisoned")
        .remove(&otc_port);
}

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
) -> Swap {
    let timeout = Duration::from_secs(crate::utils::INTEGRATION_TEST_TIMEOUT_SECS);
    let start = Instant::now();
    let poll_interval = Duration::from_millis(500);
    let url = format!("http://localhost:{otc_port}/api/v2/swap/{swap_id}");

    loop {
        let response = client.get(&url).send().await.unwrap();
        let swap: Swap = response.json().await.unwrap();
        let status_str = format!("{:?}", swap.status);

        if status_str == expected_status {
            return swap;
        }

        if start.elapsed() > timeout {
            panic!(
                "Timeout waiting for swap {swap_id} status to become {expected_status}, last response: {swap:#?}"
            );
        }

        tokio::time::sleep(poll_interval).await;
    }
}

pub async fn wait_for_swap_statuses(
    client: &reqwest::Client,
    otc_port: u16,
    swap_id: Uuid,
    expected_statuses: &[&str],
) -> Swap {
    let timeout = Duration::from_secs(crate::utils::INTEGRATION_TEST_TIMEOUT_SECS);
    let start = Instant::now();
    let poll_interval = Duration::from_millis(500);
    let url = format!("http://localhost:{otc_port}/api/v2/swap/{swap_id}");

    loop {
        let response = client.get(&url).send().await.unwrap();
        let swap: Swap = response.json().await.unwrap();
        let status_str = format!("{:?}", swap.status);

        if expected_statuses
            .iter()
            .any(|expected| *expected == status_str)
        {
            return swap;
        }

        assert!(
            start.elapsed() <= timeout,
            "Timeout waiting for swap {} to reach one of statuses {:?}, last seen {}",
            swap_id,
            expected_statuses,
            status_str
        );

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
                maybe_spawn_sauron_for_otc_port(otc_port).await;
                break;
            }
        }
    }
}

async fn maybe_spawn_sauron_for_otc_port(otc_port: u16) {
    let Some(args) = sauron_autostart_registry()
        .lock()
        .expect("sauron autostart registry lock poisoned")
        .remove(&otc_port)
    else {
        return;
    };

    tokio::spawn(async move {
        if let Err(error) = run_sauron(args).await {
            warn!(%error, "Auto-started Sauron worker exited");
        }
    });
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
            .get(format!("http://localhost:{otc_port}/api/v2/swap/{swap_id}"))
            .send()
            .await
            .unwrap();
        let swap: Swap = response.json().await.unwrap();
        if last_log_time.elapsed() > log_interval {
            info!("Response from swap status endpoint: {:#?}", swap);
            last_log_time = std::time::Instant::now();
        }
        if start_time.elapsed() > timeout {
            info!("Final response from swap status endpoint: {:#?}", swap);
            panic!("Timeout waiting for swap to be settled");
        }
        if swap.status == otc_models::SwapStatus::Settled {
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
                            && matches!(
                                connected_mms[0].as_str(),
                                Some(TEST_MARKET_MAKER_API_ID | TEST_BASE_MARKET_MAKER_API_ID)
                            )
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
pub async fn wait_for_market_makers_to_connect_to_rfq_server(
    rfq_port: u16,
    expected_mm_ids: &[&str],
) {
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
                        let connected_ids: Vec<&str> =
                            connected_mms.iter().filter_map(|v| v.as_str()).collect();

                        let all_connected =
                            expected_mm_ids.iter().all(|id| connected_ids.contains(id));

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
    format!("/tmp/bitcoin_wallet_{}.db", uuid::Uuid::now_v7())
}

pub async fn build_mm_test_args(
    otc_port: u16,
    rfq_port: u16,
    multichain_account: &MultichainAccount,
    devnet: &devnet::RiftDevnet,
    connect_options: &PgConnectOptions,
) -> MarketMakerArgs {
    let coinbase_exchange_api_base_url = format!(
        "http://127.0.0.1:{}",
        devnet.coinbase_mock_server_port.unwrap_or(8080)
    );
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
        btc_coinbase_confirmations: 2,      // Reduced for tests (production: 3)
        cbbtc_coinbase_confirmations: 3,    // Greatly reduced for tests (production: 36)
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
    let coinbase_exchange_api_base_url = format!(
        "http://127.0.0.1:{}",
        devnet.coinbase_mock_server_port.unwrap_or(8080)
    );
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

    let db = format!("test_db_{}", Uuid::now_v7().simple());

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

    let args = OtcServerArgs {
        port: otc_port,
        database_url: db_url.clone(),
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
        participant_signed_detection_enabled: true,
        participant_detection_min_interval_seconds: 60,
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
        loki_url: None,
    };

    let sauron_args = build_sauron_test_args(otc_port, &db_url, devnet);
    sauron_autostart_registry()
        .lock()
        .expect("sauron autostart registry lock poisoned")
        .insert(otc_port, sauron_args);

    args
}

fn build_sauron_test_args(
    otc_port: u16,
    otc_database_url: &str,
    devnet: &devnet::RiftDevnet,
) -> SauronArgs {
    SauronArgs {
        log_level: "info".to_string(),
        otc_replica_database_url: otc_database_url.to_string(),
        otc_replica_database_name: "otc_test_db".to_string(),
        otc_replica_notification_channel: "sauron_watch_set_changed".to_string(),
        otc_internal_base_url: format!("http://127.0.0.1:{otc_port}"),
        otc_detector_api_id: TEST_DETECTOR_API_ID.to_string(),
        otc_detector_api_secret: TEST_DETECTOR_API_SECRET.to_string(),
        electrum_http_server_url: devnet.bitcoin.esplora_url.as_ref().unwrap().to_string(),
        bitcoin_rpc_url: devnet.bitcoin.rpc_url_with_cookie.clone(),
        bitcoin_rpc_auth: Auth::CookieFile(devnet.bitcoin.cookie.clone()),
        bitcoin_zmq_rawtx_endpoint: devnet.bitcoin.zmq_rawtx_endpoint.clone(),
        bitcoin_zmq_sequence_endpoint: devnet.bitcoin.zmq_sequence_endpoint.clone(),
        ethereum_mainnet_rpc_url: devnet.ethereum.anvil.endpoint(),
        ethereum_token_indexer_url: devnet
            .ethereum
            .token_indexer
            .as_ref()
            .unwrap()
            .api_server_url
            .clone(),
        ethereum_allowed_token: devnet.ethereum.cbbtc_contract.address().to_string(),
        base_rpc_url: devnet.base.anvil.endpoint(),
        base_token_indexer_url: devnet
            .base
            .token_indexer
            .as_ref()
            .map(|indexer| indexer.api_server_url.clone())
            .unwrap_or_else(|| {
                devnet
                    .ethereum
                    .token_indexer
                    .as_ref()
                    .unwrap()
                    .api_server_url
                    .clone()
            }),
        base_allowed_token: devnet.base.cbbtc_contract.address().to_string(),
        sauron_reconcile_interval_seconds: 60,
        sauron_bitcoin_scan_interval_seconds: 1,
        sauron_bitcoin_indexed_lookup_concurrency: 8,
        sauron_evm_indexed_lookup_concurrency: 4,
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
    let provider = create_websocket_wallet_provider(&devnet.base.anvil.ws_endpoint(), private_key)
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
        init_logger(
            "info,otc_server=debug,otc_chains=debug,market-maker=debug",
            None::<console_subscriber::ConsoleLayer>,
        )
        .expect("Logger should initialize");
    }
}
