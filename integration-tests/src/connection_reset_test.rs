use alloy::primitives::U256;
use market_maker::run_market_maker;
use otc_server::server::run_server as run_otc_server;
use rfq_server::server::run_server as run_rfq_server;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::time::Duration;
use tokio::task::JoinSet;

use crate::utils::{
    build_mm_test_args, build_otc_server_test_args, build_rfq_server_test_args, get_free_port,
    wait_for_otc_server_to_be_ready, wait_for_rfq_server_to_be_ready, PgConnectOptionsExt,
    TestProxy, INTEGRATION_TEST_TIMEOUT_SECS, TEST_MARKET_MAKER_API_ID,
};

#[sqlx::test]
async fn test_connection_reset_and_reconnection(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    // Setup market maker account
    let market_maker_account = devnet::MultichainAccount::new(0);
    let devnet = devnet::RiftDevnet::builder()
        .using_esplora(true)
        .using_token_indexer(connect_options.to_database_url())
        .build()
        .await
        .unwrap()
        .0;

    // Fund the market maker with ETH
    devnet
        .ethereum
        .fund_eth_address(
            market_maker_account.ethereum_address,
            U256::from(100_000_000_000_000_000_000i128),
        )
        .await
        .unwrap();

    let mut join_set = JoinSet::new();

    // Get free ports for OTC and RFQ servers
    let otc_port = get_free_port().await;
    let rfq_port = get_free_port().await;

    tracing::info!("OTC port: {}", otc_port);
    tracing::info!("RFQ port: {}", rfq_port);

    // Start OTC server
    let otc_args = build_otc_server_test_args(otc_port, &devnet, &connect_options).await;
    join_set.spawn(async move {
        run_otc_server(otc_args)
            .await
            .expect("OTC server should not crash");
    });

    // Wait for OTC server to be ready
    wait_for_otc_server_to_be_ready(otc_port).await;

    // Start RFQ server
    let rfq_args = build_rfq_server_test_args(rfq_port);
    join_set.spawn(async move {
        run_rfq_server(rfq_args)
            .await
            .expect("RFQ server should not crash");
    });

    // Wait for RFQ server to be ready
    wait_for_rfq_server_to_be_ready(rfq_port).await;

    // Create test proxies for both servers
    let otc_target_addr = format!("127.0.0.1:{}", otc_port)
        .parse()
        .expect("Should parse OTC address");
    let rfq_target_addr = format!("127.0.0.1:{}", rfq_port)
        .parse()
        .expect("Should parse RFQ address");

    let otc_proxy = TestProxy::spawn(otc_target_addr)
        .await
        .expect("Should create OTC proxy");
    let rfq_proxy = TestProxy::spawn(rfq_target_addr)
        .await
        .expect("Should create RFQ proxy");

    let otc_proxy_port = otc_proxy.listen_addr.port();
    let rfq_proxy_port = rfq_proxy.listen_addr.port();

    tracing::info!("OTC proxy port: {}", otc_proxy_port);
    tracing::info!("RFQ proxy port: {}", rfq_proxy_port);

    // Start market maker connected through proxies
    let mm_args = build_mm_test_args(
        otc_proxy_port, // Connect through proxy instead of directly
        rfq_proxy_port, // Connect through proxy instead of directly
        &market_maker_account,
        &devnet,
        &connect_options,
    )
    .await;

    join_set.spawn(async move {
        run_market_maker(mm_args)
            .await
            .expect("Market maker should not crash");
    });

    // Wait for market maker to connect to both OTC and RFQ servers
    println!("Waiting for market maker to connect to OTC and RFQ servers...");
    wait_for_mm_connection_to_otc(otc_port).await;
    wait_for_mm_connection_to_rfq(rfq_port).await;
    println!("✓ Market maker successfully connected to both servers");

    // Trigger server-side reset
    println!("\n=== Testing server-side connection reset ===");
    println!("Triggering connection reset from server side...");
    otc_proxy.reset_all_from_server().await;
    rfq_proxy.reset_all_from_server().await;

    // Poll until both connections are re-established
    println!("Waiting for market maker to reconnect after server reset...");
    wait_for_mm_connection_to_otc(otc_port).await;
    wait_for_mm_connection_to_rfq(rfq_port).await;
    println!("✓ Market maker successfully reconnected to both servers after server reset");

    println!("\n=== Server-side connection reset test completed successfully! ===");
}

/// Wait for market maker to connect to OTC server
async fn wait_for_mm_connection_to_otc(otc_port: u16) {
    let client = reqwest::Client::new();
    let status_url = format!("http://127.0.0.1:{otc_port}/status");

    let start_time = std::time::Instant::now();
    let timeout = Duration::from_secs(INTEGRATION_TEST_TIMEOUT_SECS);

    loop {
        assert!(
            start_time.elapsed() <= timeout,
            "Timeout waiting for market maker to connect to OTC server"
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        if let Ok(response) = client.get(&status_url).send().await {
            if response.status() == 200 {
                if let Ok(body) = response.json::<serde_json::Value>().await {
                    if let Some(connected_mms) = body["connected_market_makers"].as_array() {
                        if connected_mms.len() == 1
                            && connected_mms[0].as_str() == Some(TEST_MARKET_MAKER_API_ID)
                        {
                            return;
                        }
                    }
                }
            }
        }
    }
}

/// Wait for market maker to connect to RFQ server
async fn wait_for_mm_connection_to_rfq(rfq_port: u16) {
    let client = reqwest::Client::new();
    let status_url = format!("http://127.0.0.1:{rfq_port}/status");

    let start_time = std::time::Instant::now();
    let timeout = Duration::from_secs(INTEGRATION_TEST_TIMEOUT_SECS);

    loop {
        assert!(
            start_time.elapsed() <= timeout,
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
                            return;
                        }
                    }
                }
            }
        }
    }
}

#[sqlx::test]
async fn test_connection_reset_from_client(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    // Setup market maker account
    let market_maker_account = devnet::MultichainAccount::new(0);
    let devnet = devnet::RiftDevnet::builder()
        .using_esplora(true)
        .using_token_indexer(connect_options.to_database_url())
        .build()
        .await
        .unwrap()
        .0;

    // Fund the market maker with ETH
    devnet
        .ethereum
        .fund_eth_address(
            market_maker_account.ethereum_address,
            U256::from(100_000_000_000_000_000_000i128),
        )
        .await
        .unwrap();

    let mut join_set = JoinSet::new();

    // Get free ports for OTC and RFQ servers
    let otc_port = get_free_port().await;
    let rfq_port = get_free_port().await;

    tracing::info!("OTC port: {}", otc_port);
    tracing::info!("RFQ port: {}", rfq_port);

    // Start OTC server
    let otc_args = build_otc_server_test_args(otc_port, &devnet, &connect_options).await;
    join_set.spawn(async move {
        run_otc_server(otc_args)
            .await
            .expect("OTC server should not crash");
    });

    // Wait for OTC server to be ready
    wait_for_otc_server_to_be_ready(otc_port).await;

    // Start RFQ server
    let rfq_args = build_rfq_server_test_args(rfq_port);
    join_set.spawn(async move {
        run_rfq_server(rfq_args)
            .await
            .expect("RFQ server should not crash");
    });

    // Wait for RFQ server to be ready
    wait_for_rfq_server_to_be_ready(rfq_port).await;

    // Create test proxies for both servers
    let otc_target_addr = format!("127.0.0.1:{}", otc_port)
        .parse()
        .expect("Should parse OTC address");
    let rfq_target_addr = format!("127.0.0.1:{}", rfq_port)
        .parse()
        .expect("Should parse RFQ address");

    let otc_proxy = TestProxy::spawn(otc_target_addr)
        .await
        .expect("Should create OTC proxy");
    let rfq_proxy = TestProxy::spawn(rfq_target_addr)
        .await
        .expect("Should create RFQ proxy");

    let otc_proxy_port = otc_proxy.listen_addr.port();
    let rfq_proxy_port = rfq_proxy.listen_addr.port();

    tracing::info!("OTC proxy port: {}", otc_proxy_port);
    tracing::info!("RFQ proxy port: {}", rfq_proxy_port);

    // Start market maker connected through proxies
    let mm_args = build_mm_test_args(
        otc_proxy_port, // Connect through proxy instead of directly
        rfq_proxy_port, // Connect through proxy instead of directly
        &market_maker_account,
        &devnet,
        &connect_options,
    )
    .await;

    join_set.spawn(async move {
        run_market_maker(mm_args)
            .await
            .expect("Market maker should not crash");
    });

    // Wait for market maker to connect to both OTC and RFQ servers
    println!("Waiting for market maker to connect to OTC and RFQ servers...");
    wait_for_mm_connection_to_otc(otc_port).await;
    wait_for_mm_connection_to_rfq(rfq_port).await;
    println!("✓ Market maker successfully connected to both servers");

    // Trigger client-side reset
    println!("\n=== Testing client-side connection reset ===");
    println!("Triggering connection reset from client side...");
    otc_proxy.reset_all_from_client().await;
    rfq_proxy.reset_all_from_client().await;

    // Poll until both connections are re-established
    println!("Waiting for market maker to reconnect after client reset...");
    wait_for_mm_connection_to_otc(otc_port).await;
    wait_for_mm_connection_to_rfq(rfq_port).await;
    println!("✓ Market maker successfully reconnected to both servers after client reset");

    println!("\n=== Client-side connection reset test completed successfully! ===");
}

#[sqlx::test]
async fn test_connection_reset_hammering(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    // Setup market maker account
    let market_maker_account = devnet::MultichainAccount::new(0);
    let devnet = devnet::RiftDevnet::builder()
        .using_esplora(true)
        .using_token_indexer(connect_options.to_database_url())
        .build()
        .await
        .unwrap()
        .0;

    // Fund the market maker with ETH
    devnet
        .ethereum
        .fund_eth_address(
            market_maker_account.ethereum_address,
            U256::from(100_000_000_000_000_000_000i128),
        )
        .await
        .unwrap();

    let mut join_set = JoinSet::new();

    // Get free ports for OTC and RFQ servers
    let otc_port = get_free_port().await;
    let rfq_port = get_free_port().await;

    tracing::info!("OTC port: {}", otc_port);
    tracing::info!("RFQ port: {}", rfq_port);

    // Start OTC server
    let otc_args = build_otc_server_test_args(otc_port, &devnet, &connect_options).await;
    join_set.spawn(async move {
        run_otc_server(otc_args)
            .await
            .expect("OTC server should not crash");
    });

    // Wait for OTC server to be ready
    wait_for_otc_server_to_be_ready(otc_port).await;

    // Start RFQ server
    let rfq_args = build_rfq_server_test_args(rfq_port);
    join_set.spawn(async move {
        run_rfq_server(rfq_args)
            .await
            .expect("RFQ server should not crash");
    });

    // Wait for RFQ server to be ready
    wait_for_rfq_server_to_be_ready(rfq_port).await;

    // Create test proxies for both servers
    let otc_target_addr = format!("127.0.0.1:{}", otc_port)
        .parse()
        .expect("Should parse OTC address");
    let rfq_target_addr = format!("127.0.0.1:{}", rfq_port)
        .parse()
        .expect("Should parse RFQ address");

    let otc_proxy = TestProxy::spawn(otc_target_addr)
        .await
        .expect("Should create OTC proxy");
    let rfq_proxy = TestProxy::spawn(rfq_target_addr)
        .await
        .expect("Should create RFQ proxy");

    let otc_proxy_port = otc_proxy.listen_addr.port();
    let rfq_proxy_port = rfq_proxy.listen_addr.port();

    tracing::info!("OTC proxy port: {}", otc_proxy_port);
    tracing::info!("RFQ proxy port: {}", rfq_proxy_port);

    // Start market maker connected through proxies
    let mm_args = build_mm_test_args(
        otc_proxy_port, // Connect through proxy instead of directly
        rfq_proxy_port, // Connect through proxy instead of directly
        &market_maker_account,
        &devnet,
        &connect_options,
    )
    .await;

    join_set.spawn(async move {
        run_market_maker(mm_args)
            .await
            .expect("Market maker should not crash");
    });

    // Wait for market maker to connect to both OTC and RFQ servers
    println!("Waiting for market maker to connect to OTC and RFQ servers...");
    wait_for_mm_connection_to_otc(otc_port).await;
    wait_for_mm_connection_to_rfq(rfq_port).await;
    println!("✓ Market maker successfully connected to both servers");

    // Hammer with server resets for 5 seconds
    println!("\n=== Hammering with server-side resets for 5 seconds ===");
    let hammer_duration = Duration::from_secs(5);
    let reset_interval = Duration::from_secs(1);
    let start_time = std::time::Instant::now();
    let mut reset_count = 0;

    while start_time.elapsed() < hammer_duration {
        println!("Triggering reset #{} from server side...", reset_count + 1);
        otc_proxy.reset_all_from_server().await;
        rfq_proxy.reset_all_from_server().await;
        reset_count += 1;

        tokio::time::sleep(reset_interval).await;
    }

    println!(
        "✓ Completed {} server-side resets over 5 seconds",
        reset_count
    );

    // After hammering, verify that the market maker can still reconnect
    println!("\nWaiting for market maker to reconnect after hammering...");
    wait_for_mm_connection_to_otc(otc_port).await;
    wait_for_mm_connection_to_rfq(rfq_port).await;
    println!("✓ Market maker successfully reconnected to both servers after reset hammering");

    println!("\n=== Connection reset hammering test completed successfully! ===");
}
