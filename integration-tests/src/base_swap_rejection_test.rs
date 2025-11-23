use alloy::primitives::U256;
use bitcoincore_rpc_async::RpcApi;
use devnet::{bitcoin_devnet::MiningMode, MultichainAccount, RiftDevnet};
use market_maker::run_market_maker;
use otc_models::{ChainType, Currency, QuoteMode, QuoteRequest, TokenIdentifier};
use otc_protocols::rfq::RFQResult;
use otc_server::server::run_server;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::info;

use crate::utils::{
    build_mm_test_args, build_otc_server_test_args, build_rfq_server_test_args, get_free_port,
    wait_for_market_maker_to_connect_to_rfq_server, wait_for_otc_server_to_be_ready,
    wait_for_rfq_server_to_be_ready, PgConnectOptionsExt,
};

/// This test verifies that when a market maker is configured for Ethereum,
/// it correctly rejects quote requests for Base <> Bitcoin swaps.
/// 
/// The market maker should only provide quotes for its configured EVM chain,
/// which in this case is Ethereum. Requests for Base swaps should fail.
#[sqlx::test]
async fn test_base_btc_swap_rejected_when_mm_configured_for_ethereum(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let market_maker_account = MultichainAccount::new(1);

    // Build devnet with both Ethereum and Base chains
    let devnet = RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .using_esplora(true)
        .bitcoin_mining_mode(MiningMode::Interval(2))
        .build()
        .await
        .unwrap()
        .0;

    // Fund the market maker on Ethereum (but not Base)
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
            U256::from(9_000_000_000i128), // 90 cbbtc
        )
        .await
        .unwrap();

    // Also fund some Bitcoin for the market maker
    devnet
        .bitcoin
        .deal_bitcoin(
            &market_maker_account.bitcoin_wallet.address,
            &bitcoin::Amount::from_sat(500_000_000), // 5 BTC
        )
        .await
        .unwrap();

    let mut service_join_set = JoinSet::new();

    // Start OTC server
    let otc_port = get_free_port().await;
    let otc_args = build_otc_server_test_args(otc_port, &devnet, &connect_options).await;

    service_join_set.spawn(async move {
        run_server(otc_args)
            .await
            .expect("OTC server should not crash");
    });

    wait_for_otc_server_to_be_ready(otc_port).await;
    info!("OTC server is ready");

    // Start RFQ server
    let rfq_port = get_free_port().await;
    let rfq_args = build_rfq_server_test_args(rfq_port);
    service_join_set.spawn(async move {
        rfq_server::server::run_server(rfq_args)
            .await
            .expect("RFQ server should not crash");
    });

    wait_for_rfq_server_to_be_ready(rfq_port).await;
    info!("RFQ server is ready");

    // Start market maker configured for ETHEREUM (not Base)
    let mm_args = build_mm_test_args(
        otc_port,
        rfq_port,
        &market_maker_account,
        &devnet,
        &connect_options,
    )
    .await;

    // Verify the market maker is configured for Ethereum
    assert_eq!(
        mm_args.evm_chain,
        ChainType::Ethereum,
        "Market maker should be configured for Ethereum"
    );

    service_join_set.spawn(async move {
        run_market_maker(mm_args)
            .await
            .expect("Market maker should not crash");
    });

    wait_for_market_maker_to_connect_to_rfq_server(rfq_port).await;
    info!("Market maker connected to RFQ server");

    // Wait for esplora to sync
    devnet
        .bitcoin
        .wait_for_esplora_sync(Duration::from_secs(30))
        .await
        .unwrap();

    let client = reqwest::Client::new();

    // Test 1: Request a quote for Base -> Bitcoin swap (should fail)
    info!("Test 1: Requesting Base -> Bitcoin quote (should be rejected)");
    let base_to_btc_quote_request = QuoteRequest {
        mode: QuoteMode::ExactInput,
        amount: U256::from(10_000_000), // 0.1 cbBTC on Base
        from: Currency {
            chain: ChainType::Base,
            token: TokenIdentifier::Address(devnet.base.cbbtc_contract.address().to_string()),
            decimals: 8,
        },
        to: Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        },
    };

    let quote_response = client
        .post(format!("http://localhost:{rfq_port}/api/v1/quotes/request"))
        .json(&base_to_btc_quote_request)
        .send()
        .await
        .unwrap();

    assert_eq!(
        quote_response.status(),
        200,
        "Quote request should return 200 (but quote may be rejected)"
    );

    let quote_response: rfq_server::server::QuoteResponse =
        quote_response.json().await.unwrap();

    info!("Base -> Bitcoin quote response: {:?}", quote_response.quote);

    // The quote should either be None or a rejection (MakerUnavailable/InvalidRequest), not a Success
    match quote_response.quote {
        None => {
            info!("✓ Quote correctly rejected (None returned)");
        }
        Some(RFQResult::MakerUnavailable(reason)) => {
            info!("✓ Quote correctly rejected (MakerUnavailable): {:?}", reason);
        }
        Some(RFQResult::InvalidRequest(reason)) => {
            info!("✓ Quote correctly rejected (InvalidRequest): {:?}", reason);
        }
        Some(RFQResult::Unsupported(reason)) => {
            info!("✓ Quote correctly rejected (Unsupported): {:?}", reason);
        }
        Some(RFQResult::Success(_)) => {
            panic!("Quote should NOT succeed for Base -> Bitcoin when MM is configured for Ethereum");
        }
    }

    // Test 2: Request a quote for Bitcoin -> Base swap (should also fail)
    info!("Test 2: Requesting Bitcoin -> Base quote (should be rejected)");
    let btc_to_base_quote_request = QuoteRequest {
        mode: QuoteMode::ExactInput,
        amount: U256::from(10_000_000), // 0.1 BTC
        from: Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        },
        to: Currency {
            chain: ChainType::Base,
            token: TokenIdentifier::Address(devnet.base.cbbtc_contract.address().to_string()),
            decimals: 8,
        },
    };

    let quote_response = client
        .post(format!("http://localhost:{rfq_port}/api/v1/quotes/request"))
        .json(&btc_to_base_quote_request)
        .send()
        .await
        .unwrap();

    assert_eq!(quote_response.status(), 200);

    let quote_response: rfq_server::server::QuoteResponse =
        quote_response.json().await.unwrap();

    info!("Bitcoin -> Base quote response: {:?}", quote_response.quote);

    // The quote should either be None or a rejection (MakerUnavailable/InvalidRequest), not a Success
    match quote_response.quote {
        None => {
            info!("✓ Quote correctly rejected (None returned)");
        }
        Some(RFQResult::MakerUnavailable(reason)) => {
            info!("✓ Quote correctly rejected (MakerUnavailable): {:?}", reason);
        }
        Some(RFQResult::InvalidRequest(reason)) => {
            info!("✓ Quote correctly rejected (InvalidRequest): {:?}", reason);
        }
        Some(RFQResult::Unsupported(reason)) => {
            info!("✓ Quote correctly rejected (Unsupported): {:?}", reason);
        }
        Some(RFQResult::Success(_)) => {
            panic!("Quote should NOT succeed for Bitcoin -> Base when MM is configured for Ethereum");
        }
    }

    // Test 3: Verify that Ethereum <> Bitcoin swaps still work
    info!("Test 3: Requesting Bitcoin -> Ethereum quote (should succeed)");
    let btc_to_eth_quote_request = QuoteRequest {
        mode: QuoteMode::ExactInput,
        amount: U256::from(10_000_000), // 0.1 BTC
        from: Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        },
        to: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::Address(devnet.ethereum.cbbtc_contract.address().to_string()),
            decimals: 8,
        },
    };

    let quote_response = client
        .post(format!("http://localhost:{rfq_port}/api/v1/quotes/request"))
        .json(&btc_to_eth_quote_request)
        .send()
        .await
        .unwrap();

    assert_eq!(quote_response.status(), 200);

    let quote_response: rfq_server::server::QuoteResponse =
        quote_response.json().await.unwrap();

    info!("Bitcoin -> Ethereum quote response: {:?}", quote_response.quote);

    // This quote SHOULD succeed since the MM is configured for Ethereum
    match quote_response.quote {
        Some(RFQResult::Success(_)) => {
            info!("✓ Quote correctly succeeded for Bitcoin -> Ethereum");
        }
        Some(RFQResult::MakerUnavailable(reason)) => {
            panic!(
                "Quote should succeed for Bitcoin -> Ethereum, but got MakerUnavailable: {:?}",
                reason
            );
        }
        Some(RFQResult::InvalidRequest(reason)) => {
            panic!(
                "Quote should succeed for Bitcoin -> Ethereum, but got InvalidRequest: {:?}",
                reason
            );
        }
        Some(RFQResult::Unsupported(reason)) => {
            panic!(
                "Quote should succeed for Bitcoin -> Ethereum, but got Unsupported: {:?}",
                reason
            );
        }
        None => {
            panic!("Quote should succeed for Bitcoin -> Ethereum, but got None");
        }
    }

    info!("All tests passed! Market maker correctly rejects Base swaps when configured for Ethereum");

    drop(devnet);
    service_join_set.shutdown().await;
}

