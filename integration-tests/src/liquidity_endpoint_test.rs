use alloy::primitives::U256;
use devnet::{MultichainAccount, RiftDevnet};
use market_maker::run_market_maker;
use otc_models::{ChainType, Currency, QuoteRequest, TokenIdentifier};
use otc_protocols::rfq::RFQResult;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::info;

use crate::utils::{
    build_mm_test_args, build_rfq_server_test_args, get_free_port,
    wait_for_market_maker_to_connect_to_rfq_server, wait_for_rfq_server_to_be_ready,
    PgConnectOptionsExt,
};

#[sqlx::test]
async fn test_liquidity_endpoint_returns_data(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let market_maker_account = MultichainAccount::new(1);

    let devnet = RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .using_esplora(true)
        .build()
        .await
        .unwrap()
        .0;

    // Fund the market maker with both BTC and CBBTC
    devnet
        .bitcoin
        .deal_bitcoin(
            &market_maker_account.bitcoin_wallet.address,
            &bitcoin::Amount::from_sat(500_000_000), // 5 BTC
        )
        .await
        .unwrap();

    devnet
        .ethereum
        .fund_eth_address(
            market_maker_account.ethereum_address,
            U256::from(100_000_000_000_000_000_000i128), // 100 ETH
        )
        .await
        .unwrap();

    devnet
        .ethereum
        .mint_cbbtc(
            market_maker_account.ethereum_address,
            U256::from(9_000_000_000i128), // 90 CBBTC
        )
        .await
        .unwrap();

    let mut service_join_set = JoinSet::new();

    // Start RFQ server
    let rfq_port = get_free_port().await;
    let rfq_args = build_rfq_server_test_args(rfq_port);
    service_join_set.spawn(async move {
        rfq_server::server::run_server(rfq_args)
            .await
            .expect("RFQ server should not crash");
    });

    wait_for_rfq_server_to_be_ready(rfq_port).await;

    // Wait for esplora to sync before starting market maker
    devnet
        .bitcoin
        .wait_for_esplora_sync(Duration::from_secs(30))
        .await
        .unwrap();

    // Start market maker
    let mm_args = build_mm_test_args(
        0, // We don't need OTC server for this test
        rfq_port,
        &market_maker_account,
        &devnet,
        &connect_options,
    )
    .await;

    service_join_set.spawn(async move {
        run_market_maker(mm_args)
            .await
            .expect("Market maker should not crash");
    });

    wait_for_market_maker_to_connect_to_rfq_server(rfq_port).await;

    // Give the market maker a moment to report its initial liquidity
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Request liquidity from the endpoint
    let client = reqwest::Client::new();
    let liquidity_response = client
        .get(format!("http://localhost:{rfq_port}/api/v1/liquidity"))
        .send()
        .await
        .unwrap();

    assert_eq!(
        liquidity_response.status(),
        200,
        "Liquidity request should succeed"
    );

    let liquidity_result: rfq_server::liquidity_aggregator::LiquidityAggregatorResult =
        liquidity_response
            .json()
            .await
            .expect("Should be able to parse liquidity response");

    info!("Received liquidity: {:?}", liquidity_result);

    // Verify we got liquidity data
    assert!(
        !liquidity_result.market_makers.is_empty(),
        "Should have at least one market maker"
    );

    let market_maker = &liquidity_result.market_makers[0];
    assert!(
        !market_maker.trading_pairs.is_empty(),
        "Market maker should report trading pairs"
    );

    // Verify that trading pairs have non-zero max amounts
    for trading_pair in &market_maker.trading_pairs {
        info!("Trading pair: {:?}", trading_pair);
        assert!(
            trading_pair.max_amount > U256::ZERO,
            "Trading pair should have non-zero max amount"
        );
    }

    // Find BTC->CBBTC and CBBTC->BTC trading pairs to test quotes
    let btc_to_cbbtc = market_maker
        .trading_pairs
        .iter()
        .find(|tp| {
            tp.from.chain == ChainType::Bitcoin
                && tp.from.token == TokenIdentifier::Native
                && tp.to.chain == ChainType::Ethereum
                && matches!(&tp.to.token, TokenIdentifier::Address(addr) if addr.to_lowercase() == devnet.ethereum.cbbtc_contract.address().to_string().to_lowercase())
        })
        .expect("Should have BTC->CBBTC trading pair");

    let cbbtc_to_btc = market_maker
        .trading_pairs
        .iter()
        .find(|tp| {
            tp.from.chain == ChainType::Ethereum
                && matches!(&tp.from.token, TokenIdentifier::Address(addr) if addr.to_lowercase() == devnet.ethereum.cbbtc_contract.address().to_string().to_lowercase())
                && tp.to.chain == ChainType::Bitcoin
                && tp.to.token == TokenIdentifier::Native
        })
        .expect("Should have CBBTC->BTC trading pair");

    info!(
        "BTC->CBBTC max_amount: {}, CBBTC->BTC max_amount: {}",
        btc_to_cbbtc.max_amount, cbbtc_to_btc.max_amount
    );

    // Test 1: Request quote for max_amount + 1% on BTC->CBBTC (should fail or return error)
    // Using ExactOutput to directly specify the OUTPUT amount the MM must provide
    // Note: max_amount is conservative, so we add a significant margin to ensure it exceeds capacity
    let over_limit_amount = btc_to_cbbtc.max_amount + btc_to_cbbtc.max_amount / U256::from(100); // +1%
    let quote_request = QuoteRequest {
        input_hint: Some(over_limit_amount),
        from: btc_to_cbbtc.from.clone(),
        to: btc_to_cbbtc.to.clone(),
    };

    info!(
        "Testing BTC->CBBTC with over-limit OUTPUT amount: {}",
        over_limit_amount
    );
    let quote_response = client
        .post(format!("http://localhost:{rfq_port}/api/v1/quotes/request"))
        .json(&quote_request)
        .send()
        .await
        .unwrap();

    assert_eq!(
        quote_response.status(),
        200,
        "Quote request should return 200"
    );

    let quote_response: rfq_server::server::QuoteResponse =
        quote_response.json().await.unwrap();
    info!("Over-limit BTC->CBBTC quote response: {:?}", quote_response);

    // Should either have no quote or an error result
    if let Some(result) = quote_response.quote {
        match result {
            RFQResult::Success(_) => {
                panic!("Should not get successful quote for amount exceeding liquidity limit");
            }
            RFQResult::MakerUnavailable(err) | RFQResult::InvalidRequest(err) | RFQResult::Unsupported(err) => {
                info!("Got expected error for over-limit amount: {}", err);
            }
        }
    } else {
        info!("Got no quote for over-limit amount (expected)");
    }

    // Test 2: Request quote for max_amount + 1% on CBBTC->BTC (should fail or return error)
    // Using ExactOutput to directly specify the OUTPUT amount the MM must provide
    // Note: max_amount is conservative, so we add a significant margin to ensure it exceeds capacity
    let over_limit_amount = cbbtc_to_btc.max_amount + cbbtc_to_btc.max_amount / U256::from(100); // +1%
    let quote_request = QuoteRequest {
        input_hint: Some(over_limit_amount),
        from: cbbtc_to_btc.from.clone(),
        to: cbbtc_to_btc.to.clone(),
    };

    info!(
        "Testing CBBTC->BTC with over-limit OUTPUT amount: {}",
        over_limit_amount
    );
    let quote_response = client
        .post(format!("http://localhost:{rfq_port}/api/v1/quotes/request"))
        .json(&quote_request)
        .send()
        .await
        .unwrap();

    assert_eq!(
        quote_response.status(),
        200,
        "Quote request should return 200"
    );

    let quote_response: rfq_server::server::QuoteResponse =
        quote_response.json().await.unwrap();
    info!("Over-limit CBBTC->BTC quote response: {:?}", quote_response);

    // Should either have no quote or an error result
    if let Some(result) = quote_response.quote {
        match result {
            RFQResult::Success(_) => {
                panic!("Should not get successful quote for amount exceeding liquidity limit");
            }
            RFQResult::MakerUnavailable(err) | RFQResult::InvalidRequest(err) | RFQResult::Unsupported(err) => {
                info!("Got expected error for over-limit amount: {}", err);
            }
        }
    } else {
        info!("Got no quote for over-limit amount (expected)");
    }

    // Test 3: Request quote for exactly max_amount on BTC->CBBTC (should succeed)
    // Using ExactOutput to directly specify the OUTPUT amount the MM must provide
    let exact_amount = btc_to_cbbtc.max_amount;
    let quote_request = QuoteRequest {
        input_hint: Some(exact_amount),
        from: btc_to_cbbtc.from.clone(),
        to: btc_to_cbbtc.to.clone(),
    };

    info!("Testing BTC->CBBTC with exact max OUTPUT amount: {}", exact_amount);
    let quote_response = client
        .post(format!("http://localhost:{rfq_port}/api/v1/quotes/request"))
        .json(&quote_request)
        .send()
        .await
        .unwrap();

    assert_eq!(
        quote_response.status(),
        200,
        "Quote request should succeed"
    );

    let quote_response: rfq_server::server::QuoteResponse =
        quote_response.json().await.unwrap();
    info!("Exact amount BTC->CBBTC quote response: {:?}", quote_response);

    // Should get a successful quote
    match quote_response.quote.expect("Should have a quote") {
        RFQResult::Success(quote) => {
            info!("Got successful quote for exact max amount: {:?}", quote);
            // In rate-based model, verify the input_hint is within bounds
            assert!(
                exact_amount >= quote.min_input && exact_amount <= quote.max_input,
                "Requested amount should be within quote bounds"
            );
        }
        RFQResult::MakerUnavailable(err) | RFQResult::InvalidRequest(err) | RFQResult::Unsupported(err) => {
            panic!("Should get successful quote for exact max amount, got error: {}", err);
        }
    }

    // Test 4: Request quote for exactly max_amount on CBBTC->BTC (should succeed)
    // Using ExactOutput to directly specify the OUTPUT amount the MM must provide
    let exact_amount = cbbtc_to_btc.max_amount;
    let quote_request = QuoteRequest {
        input_hint: Some(exact_amount),
        from: cbbtc_to_btc.from.clone(),
        to: cbbtc_to_btc.to.clone(),
    };

    info!("Testing CBBTC->BTC with exact max OUTPUT amount: {}", exact_amount);
    let quote_response = client
        .post(format!("http://localhost:{rfq_port}/api/v1/quotes/request"))
        .json(&quote_request)
        .send()
        .await
        .unwrap();

    assert_eq!(
        quote_response.status(),
        200,
        "Quote request should succeed"
    );

    let quote_response: rfq_server::server::QuoteResponse =
        quote_response.json().await.unwrap();
    info!("Exact amount CBBTC->BTC quote response: {:?}", quote_response);

    // Should get a successful quote
    match quote_response.quote.expect("Should have a quote") {
        RFQResult::Success(quote) => {
            info!("Got successful quote for exact max amount: {:?}", quote);
            // In rate-based model, verify the input_hint is within bounds
            assert!(
                exact_amount >= quote.min_input && exact_amount <= quote.max_input,
                "Requested amount should be within quote bounds"
            );
        }
        RFQResult::MakerUnavailable(err) | RFQResult::InvalidRequest(err) | RFQResult::Unsupported(err) => {
            panic!("Should get successful quote for exact max amount, got error: {}", err);
        }
    }

    drop(devnet);
    tokio::join!(service_join_set.shutdown());
}

