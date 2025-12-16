use alloy::primitives::U256;
use market_maker::run_market_maker;
use otc_models::{SwapMode, ChainType, Currency, QuoteRequest, TokenIdentifier};
use otc_protocols::rfq::RFQResult;
use rfq_server::server::run_server as run_rfq_server;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::time::Instant;
use tokio::task::JoinSet;

use crate::utils::{
    build_mm_test_args, build_rfq_server_test_args, get_free_port,
    wait_for_market_maker_to_connect_to_rfq_server, wait_for_rfq_server_to_be_ready,
};

#[sqlx::test]
async fn test_rfq_flow(_: PoolOptions<sqlx::Postgres>, connect_options: PgConnectOptions) {
    // Setup market maker account
    let market_maker_account = devnet::MultichainAccount::new(0);
    let devnet = devnet::RiftDevnet::builder()
        .using_esplora(true)
        .build()
        .await
        .unwrap()
        .0;

    // Don't fund the market maker - we want to test that it correctly rejects quotes
    // when it has insufficient balance
    println!("Market maker starting with 0 BTC balance");

    let mut join_set = JoinSet::new();

    // Get free ports for RFQ server and dummy OTC server port
    let rfq_port = get_free_port().await;
    let otc_port = get_free_port().await; // Not used but needed for MM args

    tracing::info!("RFQ port: {}", rfq_port);
    tracing::info!("OTC port: {}", otc_port);

    // Start RFQ server
    let rfq_args = build_rfq_server_test_args(rfq_port);
    join_set.spawn(async move {
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
    wait_for_market_maker_to_connect_to_rfq_server(rfq_port).await;

    // Test: Request for any amount when MM has no balance - should fail
    let test_amount = U256::from(1_000_000_000); // 10 BTC in sats

    // Send a quote request (that will fail)
    let quote_request = QuoteRequest {
        mode: SwapMode::ExactInput(test_amount.to::<u64>()),
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
        affiliate: None,
    };

    let quote_request_url = format!("http://127.0.0.1:{rfq_port}/api/v2/quote");
    let client = reqwest::Client::new();

    // Start timing the quote request
    let start_time = Instant::now();

    let response = client
        .post(&quote_request_url)
        .json(&quote_request)
        .send()
        .await
        .expect("Should be able to send quote request");

    // Calculate and record the latency
    let latency = start_time.elapsed();

    assert_eq!(
        response.status(),
        200,
        "Quote request should succeed at HTTP level"
    );

    let quote_response: rfq_server::server::QuoteResponse = response
        .json()
        .await
        .expect("Should be able to parse quote response");

    println!("quote_response: {quote_response:?}");

    // Verify the response
    assert_eq!(
        quote_response.total_quotes_received, 1,
        "Should receive 1 response from market maker"
    );
    assert_eq!(
        quote_response.market_makers_contacted, 1,
        "Should contact 1 market maker"
    );

    // Verify the quote is MakerUnavailable due to insufficient balance
    let quote = &quote_response.quote;
    println!("Quote response for 0.1 BTC with 0 balance: {quote:?}");

    assert!(quote.is_some(), "Quote response should be present");
    match quote.as_ref().unwrap() {
        RFQResult::MakerUnavailable(reason) => {
            assert!(
                reason.contains("Insufficient") || reason.contains("liquidity"),
                "Should indicate insufficient balance/liquidity, got: {reason}"
            );
            println!("✓ Correctly rejected quote due to insufficient balance/liquidity");
        }
        RFQResult::Success(_) => {
            panic!("Quote should not succeed when market maker has insufficient balance");
        }
        RFQResult::InvalidRequest(reason) => {
            panic!("Quote should not be invalid request, got: {reason}");
        }
        RFQResult::Unsupported(reason) => {
            panic!("Quote should not be unsupported, got: {reason}");
        }
    }

    // Output the latency to get the response
    tracing::info!("Quote request latency: {latency:?}");

    // Now try for a quote that is barely too much (exceeds max_input by a small margin)
    // MM has ~100M sats of cbBTC, which translates to max_input of ~100.15M sats
    // So we request slightly more than that
    let test_amount = U256::from(100_200_000); // Slightly above max_input
    let quote_request = QuoteRequest {
        mode: SwapMode::ExactInput(test_amount.to::<u64>()),
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
        affiliate: None,
    };

    let quote_request_url = format!("http://127.0.0.1:{rfq_port}/api/v2/quote");
    let client = reqwest::Client::new();

    let response = client
        .post(&quote_request_url)
        .json(&quote_request)
        .send()
        .await
        .expect("Should be able to send quote request");

    let quote_response: rfq_server::server::QuoteResponse = response
        .json()
        .await
        .expect("Should be able to parse quote response");

    // Verify the quote fails because it exceeds max_input
    let quote = &quote_response.quote;
    println!("Quote response for {test_amount} sats with 1 BTC balance: {quote:?}");

    assert!(quote.is_some(), "Quote response should be present");
    match quote.as_ref().unwrap() {
        RFQResult::Success(quote) => {
            panic!("Quote should not succeed for amount exceeding max_input: {quote:?}");
        }
        RFQResult::MakerUnavailable(reason) => {
            assert!(
                reason.contains("Insufficient") || reason.contains("liquidity"),
                "Should indicate insufficient balance/liquidity, got: {reason}"
            );
            println!("✓ Correctly rejected quote that exceeds max_input");
        }
        RFQResult::InvalidRequest(reason) => {
            panic!("Quote should not be invalid request, got: {reason}");
        }
        RFQResult::Unsupported(reason) => {
            panic!("Quote should not be unsupported, got: {reason}");
        }
    }

    // finally try for an amount that is valid
    let test_amount = U256::from(50_000_000); // 1 BTC in sats
    let quote_request = QuoteRequest {
        mode: SwapMode::ExactInput(test_amount.to::<u64>()),
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
        affiliate: None,
    };

    let quote_request_url = format!("http://127.0.0.1:{rfq_port}/api/v2/quote");
    let client = reqwest::Client::new();

    let response = client
        .post(&quote_request_url)
        .json(&quote_request)
        .send()
        .await
        .expect("Should be able to send quote request");

    let quote_response: rfq_server::server::QuoteResponse = response
        .json()
        .await
        .expect("Should be able to parse quote response");

    let quote = &quote_response.quote;
    println!("Quote response for {test_amount} BTC with 1 BTC balance: {quote:?}");

    assert!(quote.is_some(), "Quote response should be present");
    match quote.as_ref().unwrap() {
        RFQResult::Success(quote) => {
            println!("Correctly received success quote: {quote:?}");
        }
        RFQResult::MakerUnavailable(reason) => {
            panic!("Quote should not be maker unavailable, got: {reason}");
        }
        RFQResult::InvalidRequest(reason) => {
            panic!("Quote should not be invalid request, got: {reason}");
        }
        RFQResult::Unsupported(reason) => {
            panic!("Quote should not be unsupported, got: {reason}");
        }
    }

    println!("RFQ flow test with balance check completed successfully!");
}
