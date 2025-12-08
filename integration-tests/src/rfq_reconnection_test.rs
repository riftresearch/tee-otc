use alloy::primitives::U256;
use market_maker::run_market_maker;
use otc_models::{ChainType, Currency, QuoteRequest, TokenIdentifier};
use otc_protocols::rfq::RFQResult;
use rfq_server::server::run_server as run_rfq_server;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tracing::info;

use crate::utils::{
    build_mm_test_args, build_rfq_server_test_args, get_free_port,
    wait_for_market_maker_to_connect_to_rfq_server, wait_for_rfq_server_to_be_ready,
};

/// Test that market maker detects disconnection and reconnects within expected timeframe
/// 
/// This test verifies the fix for the edge case where MM believes it's connected
/// but the RFQ server has unregistered it. With the fixes:
/// - PING_INTERVAL_SECS reduced from 30s to 10s
/// - PING_TIMEOUT_SECS reduced from 10s to 5s
/// - Server now properly tracks and shuts down background tasks
///
/// Expected behavior:
/// - MM detects disconnection within 15 seconds (10s ping + 5s timeout)
/// - MM reconnects automatically
/// - Quote requests work after reconnection
#[sqlx::test]
async fn test_rfq_market_maker_reconnection_and_detection(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    // Setup market maker account
    let market_maker_account = devnet::MultichainAccount::new(0);
    let devnet = devnet::RiftDevnet::builder()
        .using_esplora(true)
        .build()
        .await
        .unwrap()
        .0;

    let mut join_set = JoinSet::new();

    // Get free ports for RFQ server and dummy OTC server port
    let rfq_port = get_free_port().await;
    let otc_port = get_free_port().await; // Not used but needed for MM args

    info!("RFQ port: {}", rfq_port);
    info!("OTC port: {}", otc_port);

    // Start RFQ server
    let rfq_args = build_rfq_server_test_args(rfq_port);
    let rfq_handle = join_set.spawn(async move {
        run_rfq_server(rfq_args)
            .await
            .expect("RFQ server should not crash");
    });

    // Wait for RFQ server to be ready
    wait_for_rfq_server_to_be_ready(rfq_port).await;

    // Start market maker
    let mm_args = build_mm_test_args(
        otc_port,
        rfq_port,
        &market_maker_account,
        &devnet,
        &connect_options,
    )
    .await;

    // Fund market maker with enough balance to provide quotes
    devnet
        .ethereum
        .fund_eth_address(
            market_maker_account.ethereum_address,
            U256::from(100_000_000_000_000_000_000i128),
        )
        .await
        .unwrap();

    devnet
        .ethereum
        .mint_cbbtc(
            market_maker_account.ethereum_address,
            U256::from(100_000_000), // 1 bitcoin in sats (cbbtc has 8 decimals)
        )
        .await
        .unwrap();

    join_set.spawn(async move {
        run_market_maker(mm_args)
            .await
            .expect("Market maker should not crash");
    });

    // Wait for market maker to connect to RFQ server
    info!("Waiting for MM to connect to RFQ server...");
    wait_for_market_maker_to_connect_to_rfq_server(rfq_port).await;
    info!("MM connected successfully");

    // Verify quote request works while connected
    let quote_request = QuoteRequest {
        input_hint: Some(U256::from(50_000_000)), // 0.5 BTC in sats
        to: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::Address(devnet.ethereum.cbbtc_contract.address().to_string()),
            decimals: 8,
        },
        from: Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        },
    };

    let quote_request_url = format!("http://127.0.0.1:{rfq_port}/api/v1/quotes/request");
    let client = reqwest::Client::new();

    let response = client
        .post(&quote_request_url)
        .json(&quote_request)
        .send()
        .await
        .expect("Should be able to send quote request");

    assert_eq!(
        response.status(),
        200,
        "Quote request should succeed at HTTP level"
    );

    let quote_response: rfq_server::server::QuoteResponse = response
        .json()
        .await
        .expect("Should be able to parse quote response");

    info!("Initial quote response: {:?}", quote_response);
    assert_eq!(
        quote_response.total_quotes_received, 1,
        "Should receive 1 response from market maker"
    );
    assert_eq!(
        quote_response.market_makers_contacted, 1,
        "Should contact 1 market maker"
    );

    // Verify we got a successful quote
    match quote_response.quote.as_ref().unwrap() {
        RFQResult::Success(_) => {
            info!("✓ Successfully received quote while connected");
        }
        other => {
            panic!("Expected successful quote, got: {:?}", other);
        }
    }

    // Abort the RFQ server to simulate connection loss
    info!("Simulating connection loss by stopping RFQ server...");
    rfq_handle.abort();
    
    // Wait for the server to actually stop
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Restart RFQ server
    info!("Restarting RFQ server...");
    let rfq_args = build_rfq_server_test_args(rfq_port);
    join_set.spawn(async move {
        run_rfq_server(rfq_args)
            .await
            .expect("RFQ server should not crash");
    });

    // Wait for RFQ server to be ready again
    wait_for_rfq_server_to_be_ready(rfq_port).await;
    info!("RFQ server restarted");

    // Market maker should detect the disconnection and reconnect within 15 seconds
    // (10s PING_INTERVAL_SECS + 5s PING_TIMEOUT_SECS)
    let reconnect_start = Instant::now();
    let max_detection_time = Duration::from_secs(20); // 15s + 5s buffer
    
    info!(
        "Waiting for MM to detect disconnection and reconnect (max {}s)...",
        max_detection_time.as_secs()
    );
    
    // Poll for reconnection
    let mut reconnected = false;
    while reconnect_start.elapsed() < max_detection_time {
        tokio::time::sleep(Duration::from_millis(500)).await;
        
        // Check if MM is connected
        if let Ok(response) = client
            .get(&format!(
                "http://127.0.0.1:{rfq_port}/api/v1/market-makers/connected"
            ))
            .send()
            .await
        {
            if response.status() == 200 {
                if let Ok(body) = response.json::<serde_json::Value>().await {
                    if let Some(market_makers) = body["market_makers"].as_array() {
                        if !market_makers.is_empty() {
                            reconnected = true;
                            let detection_time = reconnect_start.elapsed();
                            info!(
                                "✓ MM reconnected in {:.2}s (within {}s limit)",
                                detection_time.as_secs_f64(),
                                max_detection_time.as_secs()
                            );
                            break;
                        }
                    }
                }
            }
        }
    }

    assert!(
        reconnected,
        "Market maker should have detected disconnection and reconnected within {}s",
        max_detection_time.as_secs()
    );

    // Verify quote requests work after reconnection
    info!("Verifying quote requests work after reconnection...");
    let response = client
        .post(&quote_request_url)
        .json(&quote_request)
        .send()
        .await
        .expect("Should be able to send quote request after reconnection");

    assert_eq!(
        response.status(),
        200,
        "Quote request should succeed after reconnection"
    );

    let quote_response: rfq_server::server::QuoteResponse = response
        .json()
        .await
        .expect("Should be able to parse quote response");

    info!("Post-reconnection quote response: {:?}", quote_response);
    assert_eq!(
        quote_response.total_quotes_received, 1,
        "Should receive 1 response from market maker after reconnection"
    );

    match quote_response.quote.as_ref().unwrap() {
        RFQResult::Success(_) => {
            info!("✓ Successfully received quote after reconnection");
        }
        other => {
            panic!(
                "Expected successful quote after reconnection, got: {:?}",
                other
            );
        }
    }

    info!("✓ RFQ reconnection test completed successfully!");
    
    // Cleanup
    join_set.abort_all();
}

