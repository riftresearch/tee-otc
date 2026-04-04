use alloy::primitives::U256;
use devnet::bitcoin_devnet::MiningMode;
use devnet::{MultichainAccount, RiftDevnet, WithdrawalProcessingMode};
use market_maker::run_market_maker;
use market_maker::{
    bitcoin_wallet::BitcoinWallet, planner::DeterministicDrainPlan, wallet::Wallet,
};
use otc_chains::traits::Payment;
use otc_models::{ChainType, Currency, Lot, Quote, QuoteRequest, Swap, SwapMode, TokenIdentifier};
use otc_protocols::rfq::RFQResult;
use otc_server::{api::CreateSwapRequest, server::run_server as run_otc_server};
use reqwest::StatusCode;
use rfq_server::server::run_server as run_rfq_server;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use tokio::task::JoinSet;

use crate::utils::{
    build_bitcoin_wallet_descriptor, build_mm_test_args, build_otc_server_test_args,
    build_rfq_server_test_args, build_tmp_bitcoin_wallet_db_file, get_free_port,
    wait_for_market_maker_to_connect_to_rfq_server, wait_for_otc_server_to_be_ready,
    wait_for_rfq_server_to_be_ready, wait_for_swap_to_be_settled, PgConnectOptionsExt,
    TEST_MARKET_MAKER_API_ID,
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

fn btc_to_cbbtc_quote_request(devnet: &RiftDevnet, mode: SwapMode) -> QuoteRequest {
    QuoteRequest {
        mode,
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

async fn request_successful_quote(
    client: &reqwest::Client,
    rfq_port: u16,
    quote_request: &QuoteRequest,
) -> Quote {
    let response = client
        .post(format!("http://127.0.0.1:{rfq_port}/api/v2/quote"))
        .json(quote_request)
        .send()
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Quote request should succeed"
    );

    let quote_response: rfq_server::server::QuoteResponse = response.json().await.unwrap();
    match quote_response.quote.expect("Quote should be present") {
        RFQResult::Success(quote) => quote,
        other => panic!("Expected successful quote, got: {other:?}"),
    }
}

async fn create_swap(
    client: &reqwest::Client,
    otc_port: u16,
    quote: Quote,
    user_account: &MultichainAccount,
) -> Swap {
    let response = client
        .post(format!("http://127.0.0.1:{otc_port}/api/v2/swap"))
        .json(&CreateSwapRequest {
            quote,
            user_destination_address: user_account.ethereum_address.to_string(),
            refund_address: user_account.bitcoin_wallet.address.to_string(),
            metadata: None,
        })
        .send()
        .await
        .unwrap();

    let status = response.status();
    match status {
        StatusCode::OK => response.json().await.unwrap(),
        _ => panic!(
            "Swap creation failed with {}: {:?}",
            status,
            response.text().await.unwrap()
        ),
    }
}

async fn deposit_into_swap(wallet: &BitcoinWallet, swap: &Swap, amount: U256) {
    wallet
        .create_batch_payment(
            vec![Payment {
                lot: Lot {
                    currency: swap.quote.from.currency.clone(),
                    amount,
                },
                to_address: swap.deposit_vault_address.clone(),
            }],
            None,
        )
        .await
        .unwrap();
}

async fn fetch_drain_plan(
    client: &reqwest::Client,
    admin_port: u16,
    max_rounds: usize,
) -> DeterministicDrainPlan {
    client
        .get(format!(
            "http://127.0.0.1:{admin_port}/planner/drain-plan?max_rounds={max_rounds}"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn wait_for_nonempty_drain_plan(
    client: &reqwest::Client,
    admin_port: u16,
    max_rounds: usize,
) -> DeterministicDrainPlan {
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(60);
    let mut last_plan = None;

    loop {
        if start.elapsed() > timeout {
            panic!(
                "Timeout waiting for deterministic drain plan to become non-empty, last plan: {:#?}",
                last_plan
            );
        }

        let plan = fetch_drain_plan(client, admin_port, max_rounds).await;
        let active_obligations = plan.current_state.open_obligations
            + plan.current_state.outbound_obligations
            + plan.current_state.pending_settlement_obligations;
        if active_obligations >= 2 && !plan.rounds.is_empty() {
            return plan;
        }

        last_plan = Some(plan);
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[sqlx::test]
async fn test_planner_drain_plan_handles_oversubscribed_backlog_and_eventual_settlement(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let market_maker_account = MultichainAccount::new(1);
    let user_account = MultichainAccount::new(2);

    let devnet = Arc::new(
        RiftDevnet::builder()
            .using_token_indexer(connect_options.to_database_url())
            .using_esplora(true)
            .with_coinbase_mock_server(WithdrawalProcessingMode::Instant)
            .bitcoin_mining_mode(MiningMode::Manual)
            .build()
            .await
            .unwrap()
            .0,
    );

    devnet
        .bitcoin
        .deal_bitcoin(
            &market_maker_account.bitcoin_wallet.address,
            &bitcoin::Amount::from_sat(90_000_000),
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
            U256::from(10_000_000u64),
        )
        .await
        .unwrap();
    devnet
        .bitcoin
        .deal_bitcoin(
            &user_account.bitcoin_wallet.address,
            &bitcoin::Amount::from_sat(500_000_000),
        )
        .await
        .unwrap();
    devnet
        .bitcoin
        .wait_for_esplora_sync(Duration::from_secs(30))
        .await
        .unwrap();

    let mut service_join_set = JoinSet::new();

    let otc_port = get_free_port().await;
    let otc_args = build_otc_server_test_args(otc_port, devnet.as_ref(), &connect_options).await;
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
        devnet.as_ref(),
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

    let mut wallet_join_set = JoinSet::new();
    let user_bitcoin_wallet = BitcoinWallet::new(
        &build_tmp_bitcoin_wallet_db_file(),
        &build_bitcoin_wallet_descriptor(&user_account.bitcoin_wallet.private_key),
        bitcoin::Network::Regtest,
        &devnet.bitcoin.esplora_url.as_ref().unwrap().to_string(),
        None,
        None,
        100,
        &mut wallet_join_set,
    )
    .await
    .unwrap();

    let client = reqwest::Client::new();
    let mut swaps = Vec::new();
    for _ in 0..3 {
        let quote_request =
            btc_to_cbbtc_quote_request(devnet.as_ref(), SwapMode::ExactOutput(8_000_000));
        let quote = request_successful_quote(&client, rfq_port, &quote_request).await;
        let deposit_amount = quote.from.amount;
        let swap = create_swap(&client, otc_port, quote, &user_account).await;
        deposit_into_swap(&user_bitcoin_wallet, &swap, deposit_amount).await;
        swaps.push(swap);
    }

    devnet.bitcoin.mine_blocks(2).await.unwrap();

    let plan = wait_for_nonempty_drain_plan(&client, admin_port, 16).await;
    assert!(!plan.rounds.is_empty(), "{plan:#?}");
    assert!(
        plan.current_state.open_obligations >= 1,
        "expected blocked backlog in the planner state: {plan:#?}"
    );
    assert!(
        plan.current_state.open_obligations
            + plan.current_state.outbound_obligations
            + plan.current_state.pending_settlement_obligations
            >= 2,
        "expected multiple active obligations in the planner state: {plan:#?}"
    );
    assert!(
        !plan.current_in_flight.outbound_batches.is_empty(),
        "expected current outbound work in the deterministic plan: {plan:#?}"
    );
    assert!(
        plan.current_in_flight.rebalance.is_some(),
        "expected current rebalance work in the deterministic plan: {plan:#?}"
    );
    assert!(
        plan.fully_drained,
        "expected the deterministic projection to fully drain currently known obligations: {plan:#?}"
    );

    let miner_devnet = devnet.clone();
    let miner = tokio::spawn(async move {
        for _ in 0..30 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            miner_devnet.bitcoin.mine_blocks(1).await.unwrap();
        }
    });

    for swap in &swaps {
        wait_for_swap_to_be_settled(otc_port, swap.id).await;
    }

    miner.abort();

    let final_plan = fetch_drain_plan(&client, admin_port, 16).await;
    assert!(final_plan.fully_drained, "{final_plan:#?}");
    assert_eq!(final_plan.current_state.open_obligations, 0);
    assert_eq!(final_plan.current_state.outbound_obligations, 0);
    assert_eq!(final_plan.current_state.pending_settlement_obligations, 0);

    drop(devnet);
    tokio::join!(wallet_join_set.shutdown(), service_join_set.shutdown());
}
