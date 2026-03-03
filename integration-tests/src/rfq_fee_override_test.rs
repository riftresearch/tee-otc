use alloy::primitives::U256;
use bitcoin::Network;
use devnet::bitcoin_devnet::MiningMode;
use market_maker::bitcoin_wallet::BitcoinWallet;
use market_maker::run_market_maker;
use market_maker::wallet::Wallet;
use otc_chains::traits::Payment;
use otc_models::{ChainType, Currency, Lot, QuoteRequest, Swap, SwapMode, TokenIdentifier};
use otc_protocols::rfq::RFQResult;
use otc_server::{api::CreateSwapRequest, server::run_server};
use reqwest::StatusCode;
use rfq_server::server::run_server as run_rfq_server;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::time::Duration;
use tokio::task::JoinSet;

use crate::utils::{
    TEST_FEE_SET_API_SECRET, PgConnectOptionsExt, build_bitcoin_wallet_descriptor,
    build_mm_test_args, build_otc_server_test_args, build_rfq_server_test_args,
    build_tmp_bitcoin_wallet_db_file, get_free_port,
    wait_for_market_maker_to_connect_to_rfq_server, wait_for_otc_server_to_be_ready,
    wait_for_rfq_server_to_be_ready, wait_for_swap_to_be_settled,
};

#[sqlx::test]
async fn test_privileged_quote_protocol_fee_override(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let market_maker_account = devnet::MultichainAccount::new(0);
    let devnet = devnet::RiftDevnet::builder()
        .using_esplora(true)
        .build()
        .await
        .unwrap()
        .0;

    let mut join_set = JoinSet::new();

    let rfq_port = get_free_port().await;
    let otc_port = get_free_port().await; // Not used by this test, but required for MM args

    let rfq_args = build_rfq_server_test_args(rfq_port);
    join_set.spawn(async move {
        run_rfq_server(rfq_args)
            .await
            .expect("RFQ server should not crash");
    });
    wait_for_rfq_server_to_be_ready(rfq_port).await;

    let mm_args = build_mm_test_args(
        otc_port,
        rfq_port,
        &market_maker_account,
        &devnet,
        &connect_options,
    )
    .await;

    // MM needs gas + cbBTC inventory to quote BTC -> cbBTC swaps.
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
        .mint_cbbtc(market_maker_account.ethereum_address, U256::from(100_000_000))
        .await
        .unwrap();

    join_set.spawn(async move {
        run_market_maker(mm_args)
            .await
            .expect("Market maker should not crash");
    });
    wait_for_market_maker_to_connect_to_rfq_server(rfq_port).await;

    let input_sats = 50_000_000u64;
    let protocol_fee_bps = 25u64;
    let quote_request = QuoteRequest {
        mode: SwapMode::ExactInput(input_sats),
        to: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::address(devnet.ethereum.cbbtc_contract.address().to_string()),
            decimals: 8,
        },
        from: Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        },
        affiliate: Some("partner-a".to_string()),
    };

    let quote_request_url = format!("http://127.0.0.1:{rfq_port}/api/v2/quote");
    let client = reqwest::Client::new();
    let response = client
        .post(&quote_request_url)
        .header("x-api-secret", TEST_FEE_SET_API_SECRET)
        .header("x-protocol-fee-bps", protocol_fee_bps.to_string())
        .json(&quote_request)
        .send()
        .await
        .expect("Should be able to send quote request");

    assert_eq!(response.status(), 200, "Quote request should succeed");

    let quote_response: rfq_server::server::QuoteResponse = response
        .json()
        .await
        .expect("Should parse quote response");

    let quote = match quote_response.quote.expect("Quote response should be present") {
        RFQResult::Success(quote) => quote,
        other => panic!("Expected successful quote, got: {other:?}"),
    };

    // End-to-end assertion that the fee charged to the user matches the privileged header bps exactly.
    let expected_protocol_fee = input_sats.saturating_mul(protocol_fee_bps).div_ceil(10_000);
    assert_eq!(
        quote.rates.protocol_fee_bps, protocol_fee_bps,
        "Quote should use overridden protocol fee bps"
    );
    assert_eq!(
        quote.fees.protocol_fee,
        U256::from(expected_protocol_fee),
        "Protocol fee amount should match requested bps exactly"
    );
}

#[sqlx::test]
async fn test_privileged_fee_override_end_to_end_btc_to_cbbtc(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let market_maker_account = devnet::MultichainAccount::new(11);
    let user_account = devnet::MultichainAccount::new(12);

    let devnet = devnet::RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .using_esplora(true)
        .bitcoin_mining_mode(MiningMode::Interval(2))
        .build()
        .await
        .unwrap()
        .0;

    let mut wallet_join_set = JoinSet::new();
    let user_bitcoin_wallet = BitcoinWallet::new(
        &build_tmp_bitcoin_wallet_db_file(),
        &build_bitcoin_wallet_descriptor(&user_account.bitcoin_wallet.private_key),
        Network::Regtest,
        &devnet.bitcoin.esplora_url.as_ref().unwrap().to_string(),
        None,
        None,
        100,
        &mut wallet_join_set,
    )
    .await
    .unwrap();

    // Fund accounts for a BTC -> cbBTC swap.
    devnet
        .bitcoin
        .deal_bitcoin(
            &user_account.bitcoin_wallet.address,
            &bitcoin::Amount::from_sat(500_000_000), // 5 BTC
        )
        .await
        .unwrap();
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
            U256::from(9_000_000_000i128), // 90 cbBTC
        )
        .await
        .unwrap();

    let mut service_join_set = JoinSet::new();

    let otc_port = get_free_port().await;
    let otc_args = build_otc_server_test_args(otc_port, &devnet, &connect_options).await;
    service_join_set.spawn(async move {
        run_server(otc_args).await.expect("OTC server should not crash");
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

    let mm_args = build_mm_test_args(
        otc_port,
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

    devnet
        .bitcoin
        .wait_for_esplora_sync(Duration::from_secs(30))
        .await
        .unwrap();

    let input_sats = 10_000_000u64; // 0.1 BTC
    let protocol_fee_bps = 25u64;
    let user_cbbtc_balance_before = devnet
        .ethereum
        .cbbtc_contract
        .balanceOf(user_account.ethereum_address)
        .call()
        .await
        .unwrap();

    let quote_request = QuoteRequest {
        mode: SwapMode::ExactInput(input_sats),
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
        affiliate: Some("partner-b".to_string()),
    };

    let quote_request_url = format!("http://127.0.0.1:{rfq_port}/api/v2/quote");
    let client = reqwest::Client::new();
    let quote_response = client
        .post(&quote_request_url)
        .header("x-api-secret", TEST_FEE_SET_API_SECRET)
        .header("x-protocol-fee-bps", protocol_fee_bps.to_string())
        .json(&quote_request)
        .send()
        .await
        .expect("Should send quote request");

    assert_eq!(quote_response.status(), 200, "Quote request should succeed");
    let quote_response: rfq_server::server::QuoteResponse = quote_response
        .json()
        .await
        .expect("Should parse quote response");
    let quote = match quote_response.quote.expect("Quote should be present") {
        RFQResult::Success(quote) => quote,
        other => panic!("Expected successful quote, got: {other:?}"),
    };

    // Verify quote protocol fee math uses the privileged override.
    let expected_protocol_fee = input_sats.saturating_mul(protocol_fee_bps).div_ceil(10_000);
    assert_eq!(quote.rates.protocol_fee_bps, protocol_fee_bps);
    assert_eq!(quote.fees.protocol_fee, U256::from(expected_protocol_fee));

    let swap_request = CreateSwapRequest {
        quote: quote.clone(),
        user_destination_address: user_account.ethereum_address.to_string(),
        refund_address: user_account.bitcoin_wallet.address.to_string(),
        metadata: None,
    };

    let swap_response = client
        .post(format!("http://localhost:{otc_port}/api/v2/swap"))
        .json(&swap_request)
        .send()
        .await
        .unwrap();
    let swap: Swap = match swap_response.status() {
        StatusCode::OK => swap_response.json().await.unwrap(),
        s => panic!("Swap request should succeed, got status {s}"),
    };

    // For ExactInput alignment assertions, deposit exactly the quoted input amount.
    user_bitcoin_wallet
        .create_batch_payment(
            vec![Payment {
                lot: Lot {
                    currency: swap.quote.from.currency.clone(),
                    amount: swap.quote.from.amount,
                },
                to_address: swap.deposit_vault_address.clone(),
            }],
            None,
        )
        .await
        .unwrap();

    wait_for_swap_to_be_settled(otc_port, swap.id).await;

    let user_cbbtc_balance_after = devnet
        .ethereum
        .cbbtc_contract
        .balanceOf(user_account.ethereum_address)
        .call()
        .await
        .unwrap();
    let received_cbbtc = user_cbbtc_balance_after - user_cbbtc_balance_before;
    assert_eq!(
        received_cbbtc, swap.quote.to.amount,
        "Actual received cbBTC should match quoted output for the custom protocol fee"
    );

    drop(devnet);
    tokio::join!(wallet_join_set.shutdown(), service_join_set.shutdown());
}
