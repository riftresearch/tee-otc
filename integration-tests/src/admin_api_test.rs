use alloy::primitives::U256;
use devnet::{MultichainAccount, RiftDevnet};
use market_maker::run_market_maker;
use otc_models::{ChainType, Currency, Quote, QuoteRequest, SwapMode, TokenIdentifier};
use otc_protocols::rfq::RFQResult;
use otc_server::{api::CreateSwapRequest, server::run_server as run_otc_server};
use reqwest::StatusCode;
use rfq_server::server::run_server as run_rfq_server;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};
use tokio::task::JoinSet;

use crate::utils::{
    build_mm_test_args, build_otc_server_test_args, build_rfq_server_test_args, get_free_port,
    wait_for_market_maker_to_connect_to_rfq_server, wait_for_otc_server_to_be_ready,
    wait_for_rfq_server_to_be_ready, PgConnectOptionsExt, TEST_MARKET_MAKER_API_ID,
};

async fn wait_for_market_maker_to_connect_to_otc_server(otc_port: u16) {
    let client = reqwest::Client::new();
    let status_url = format!("http://127.0.0.1:{otc_port}/status");

    let start_time = std::time::Instant::now();
    let timeout = Duration::from_secs(60);

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

async fn wait_for_admin_api(admin_port: u16) {
    let client = reqwest::Client::new();
    let quoting_url = format!("http://127.0.0.1:{admin_port}/quoting");

    let start_time = std::time::Instant::now();
    let timeout = Duration::from_secs(60);

    loop {
        assert!(
            start_time.elapsed() <= timeout,
            "Timeout waiting for admin API to become ready"
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        if let Ok(response) = client.get(&quoting_url).send().await {
            if response.status() == 200 {
                return;
            }
        }
    }
}

fn btc_to_cbbtc_quote_request(devnet: &RiftDevnet) -> QuoteRequest {
    QuoteRequest {
        mode: SwapMode::ExactInput(10_000_000), // 0.1 BTC
        from: Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        },
        to: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::address(devnet.ethereum.cbbtc_contract.address().to_string()),
            decimals: 8,
        },
        affiliate: None,
    }
}

async fn request_quote(
    client: &reqwest::Client,
    rfq_port: u16,
    quote_request: &QuoteRequest,
) -> rfq_server::server::QuoteResponse {
    client
        .post(format!("http://127.0.0.1:{rfq_port}/api/v2/quote"))
        .json(quote_request)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

#[sqlx::test]
async fn test_admin_disable_quoting_blocks_rfq_and_otc_paths(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let market_maker_account = MultichainAccount::new(1);
    let user_account = MultichainAccount::new(2);

    let devnet = RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .using_esplora(true)
        .build()
        .await
        .unwrap()
        .0;

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
            U256::from(100_000_000), // 1 BTC worth of cbbtc
        )
        .await
        .unwrap();

    let mut service_join_set = JoinSet::new();

    let otc_port = get_free_port().await;
    let otc_args = build_otc_server_test_args(otc_port, &devnet, &connect_options).await;
    service_join_set.spawn(async move {
        run_otc_server(otc_args)
            .await
            .expect("OTC server should not crash");
    });
    wait_for_otc_server_to_be_ready(otc_port).await;

    let rfq_port = get_free_port().await;
    let rfq_args = build_rfq_server_test_args(rfq_port);
    service_join_set.spawn(async move {
        run_rfq_server(rfq_args)
            .await
            .expect("RFQ server should not crash");
    });
    wait_for_rfq_server_to_be_ready(rfq_port).await;

    let admin_port = get_free_port().await;
    let mut mm_args = build_mm_test_args(
        otc_port,
        rfq_port,
        &market_maker_account,
        &devnet,
        &connect_options,
    )
    .await;
    mm_args.admin_api_listen_addr = Some(SocketAddr::from((Ipv4Addr::LOCALHOST, admin_port)));

    service_join_set.spawn(async move {
        run_market_maker(mm_args)
            .await
            .expect("Market maker should not crash");
    });

    wait_for_market_maker_to_connect_to_rfq_server(rfq_port).await;
    wait_for_market_maker_to_connect_to_otc_server(otc_port).await;
    wait_for_admin_api(admin_port).await;

    let client = reqwest::Client::new();
    let quote_request = btc_to_cbbtc_quote_request(&devnet);

    let initial_quote_response = request_quote(&client, rfq_port, &quote_request).await;
    let initial_quote = match initial_quote_response.quote {
        Some(RFQResult::Success(quote)) => quote,
        other => panic!("Expected an initial successful quote, got {other:?}"),
    };

    let quoting_response = client
        .post(format!("http://127.0.0.1:{admin_port}/quoting"))
        .json(&serde_json::json!({ "enabled": false }))
        .send()
        .await
        .unwrap();
    assert_eq!(quoting_response.status(), StatusCode::OK);

    let quoting_body: serde_json::Value = quoting_response.json().await.unwrap();
    assert_eq!(quoting_body["enabled"], false);

    let disabled_quote_response = request_quote(&client, rfq_port, &quote_request).await;
    match disabled_quote_response.quote {
        Some(RFQResult::MakerUnavailable(reason)) => {
            assert!(
                reason.contains("quoting disabled"),
                "expected quoting-disabled reason, got: {reason}"
            );
        }
        other => panic!("Expected MakerUnavailable after disabling quoting, got {other:?}"),
    }

    let swap_request = CreateSwapRequest {
        quote: Quote {
            expires_at: initial_quote.expires_at,
            ..initial_quote
        },
        user_destination_address: user_account.ethereum_address.to_string(),
        refund_address: user_account.bitcoin_wallet.address.to_string(),
        metadata: None,
    };

    let swap_response = client
        .post(format!("http://127.0.0.1:{otc_port}/api/v2/swap"))
        .json(&swap_request)
        .send()
        .await
        .unwrap();

    assert_eq!(swap_response.status(), StatusCode::CONFLICT);

    let error_body: serde_json::Value = swap_response.json().await.unwrap();
    let details = error_body["error"]["details"]
        .as_str()
        .expect("error details should be present");
    assert!(
        details.contains("Market maker rejected the quote"),
        "unexpected OTC rejection details: {details}"
    );
}
