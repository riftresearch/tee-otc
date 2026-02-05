use alloy::primitives::TxHash;
use alloy::primitives::U256;
use alloy::providers::ext::AnvilApi;
use alloy::providers::Provider;
use alloy::signers::local::PrivateKeySigner;

use devnet::bitcoin_devnet::MiningMode;
use devnet::{MultichainAccount, RiftDevnet};
use market_maker::run_market_maker;
use otc_chains::traits::Payment;
use otc_models::{Swap, SwapStatus, ChainType, Currency, Lot, Quote, QuoteRequest, SwapMode, TokenIdentifier};
use otc_protocols::rfq::RFQResult;
use otc_server::api::CreateSwapRequest;
use reqwest::StatusCode;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::info;
use uuid::Uuid;

use crate::utils::{
    build_bitcoin_wallet_descriptor, build_mm_test_args, build_otc_server_test_args,
    build_rfq_server_test_args, build_tmp_bitcoin_wallet_db_file, get_free_port,
    wait_for_market_maker_to_connect_to_rfq_server, wait_for_otc_server_to_be_ready,
    wait_for_rfq_server_to_be_ready, wait_for_swap_to_be_settled, PgConnectOptionsExt,
};
use market_maker::bitcoin_wallet::BitcoinWallet;
use market_maker::wallet::Wallet;

/// Test that validates quote modes work correctly when actually filled by market maker:
/// 1. ExactOutput quote: deposit exact input → receive exact quoted output
/// 2. Min input boundary: deposit min_input → MM fills successfully
/// 3. Max input boundary: deposit max_input → MM fills successfully
#[sqlx::test]
async fn test_quote_modes_exact_output_min_max_deposits(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let market_maker_account = MultichainAccount::new(1);
    let user_account = MultichainAccount::new(2);

    let mut devnet = RiftDevnet::builder()
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
        bitcoin::Network::Regtest,
        &devnet.bitcoin.esplora_url.as_ref().unwrap().to_string(),
        None,
        None,
        100,
        &mut wallet_join_set,
    )
    .await
    .unwrap();

    // Fund user with plenty of BTC for multiple swaps (including max_input test)
    devnet
        .bitcoin
        .deal_bitcoin(
            &user_account.bitcoin_wallet.address,
            &bitcoin::Amount::from_sat(50_000_000_000), // 500 BTC
        )
        .await
        .unwrap();

    // Fund market maker with ETH for gas and cbBTC for payouts
    devnet
        .ethereum
        .fund_eth_address(
            market_maker_account.ethereum_address,
            U256::from(100_000_000_000_000_000_000u128), // 100 ETH
        )
        .await
        .unwrap();

    devnet
        .ethereum
        .mint_cbbtc(
            market_maker_account.ethereum_address,
            U256::from(50_000_000_000u128), // 500 cbBTC (plenty for all swaps)
        )
        .await
        .unwrap();

    let mut service_join_set = JoinSet::new();

    let otc_port = get_free_port().await;
    let otc_args = build_otc_server_test_args(otc_port, &devnet, &connect_options).await;

    service_join_set.spawn(async move {
        otc_server::server::run_server(otc_args)
            .await
            .expect("OTC server should not crash");
    });

    tokio::select! {
        _ = wait_for_otc_server_to_be_ready(otc_port) => {
            info!("OTC server is ready");
        }
        _ = service_join_set.join_next() => {
            panic!("OTC server crashed");
        }
        _ = wallet_join_set.join_next() => {
            panic!("Bitcoin wallet crashed");
        }
    }

    let rfq_port = get_free_port().await;
    let rfq_args = build_rfq_server_test_args(rfq_port);
    service_join_set.spawn(async move {
        rfq_server::server::run_server(rfq_args)
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

    let client = reqwest::Client::new();
    let cbbtc_address = devnet.ethereum.cbbtc_contract.address().to_string();

    // =========================================================================
    // TEST 1: ExactOutput quote - deposit exact input, receive exact output
    // =========================================================================
    info!("=== TEST 1: ExactOutput quote - exact input deposit ===");

    let desired_output = 50_000_000u64; // 0.5 cbBTC output
    let exact_output_quote = request_quote(
        &client,
        rfq_port,
        SwapMode::ExactOutput(desired_output),
        &cbbtc_address,
    )
    .await;

    info!(
        "ExactOutput quote: input={}, output={}, min_input={}, max_input={}",
        exact_output_quote.from.amount,
        exact_output_quote.to.amount,
        exact_output_quote.min_input,
        exact_output_quote.max_input
    );

    // The quote's from.amount is the exact input needed for exact output
    let exact_input = exact_output_quote.from.amount;
    assert_eq!(
        exact_output_quote.to.amount,
        U256::from(desired_output),
        "ExactOutput quote should have exact output amount"
    );

    // Verify the exact input falls within min/max bounds
    assert!(
        exact_input >= exact_output_quote.min_input && exact_input <= exact_output_quote.max_input,
        "Exact input {} should be within bounds [{}, {}]",
        exact_input,
        exact_output_quote.min_input,
        exact_output_quote.max_input
    );

    // Execute swap with exact input from quote
    let swap1 = create_and_execute_swap(
        &client,
        &user_bitcoin_wallet,
        otc_port,
        exact_output_quote.clone(),
        exact_input, // deposit exactly what the quote says
        &user_account,
    )
    .await;

    wait_for_swap_to_be_settled(otc_port, swap1.id).await;

    // Verify the swap settled with expected output
    let swap1_status = get_swap_status(&client, otc_port, swap1.id).await;
    info!("Swap 1 (ExactOutput) settled: {:?}", swap1_status);
    assert_eq!(swap1_status.status, SwapStatus::Settled);

    // Verify the realized amounts from the API response
    // User's actual deposit amount
    let user_deposit_amount = swap1_status
        .user_deposit_status
        .as_ref()
        .expect("User deposit status should be present for settled swap")
        .amount;
    assert_eq!(
        user_deposit_amount, exact_input,
        "User input should match deposited amount"
    );

    // MM's expected output (computed from realized swap)
    let mm_expected_output = swap1_status
        .realized
        .as_ref()
        .expect("Realized swap should be present for settled swap")
        .mm_output;
    // With ExactOutput quote and exact input deposit, the output should be EXACTLY the quoted amount.
    // RealizedSwap::from_quote uses the quote's pre-computed fees when deposit matches quoted input.
    assert_eq!(
        mm_expected_output, exact_output_quote.to.amount,
        "MM output should be exactly the quoted output for ExactOutput quote with exact input deposit"
    );

    // =========================================================================
    // TEST 2: ExactInput quote - deposit min_input, MM fills successfully
    // =========================================================================
    info!("=== TEST 2: ExactInput quote - min_input deposit ===");

    let test_input = 100_000_000u64; // 1 BTC
    let exact_input_quote = request_quote(
        &client,
        rfq_port,
        SwapMode::ExactInput(test_input),
        &cbbtc_address,
    )
    .await;

    info!(
        "ExactInput quote: input={}, output={}, min_input={}, max_input={}",
        exact_input_quote.from.amount,
        exact_input_quote.to.amount,
        exact_input_quote.min_input,
        exact_input_quote.max_input
    );

    let min_input = exact_input_quote.min_input;
    assert!(
        min_input < exact_input_quote.from.amount,
        "min_input should be less than quoted amount for reasonable quotes"
    );

    // Execute swap with min_input
    let swap2 = create_and_execute_swap(
        &client,
        &user_bitcoin_wallet,
        otc_port,
        exact_input_quote.clone(),
        min_input, // deposit minimum
        &user_account,
    )
    .await;

    wait_for_swap_to_be_settled(otc_port, swap2.id).await;

    let swap2_status = get_swap_status(&client, otc_port, swap2.id).await;
    info!("Swap 2 (min_input) settled: {:?}", swap2_status);
    assert_eq!(swap2_status.status, SwapStatus::Settled);

    // Verify realized amounts use min_input
    let user_deposit_amount = swap2_status
        .user_deposit_status
        .as_ref()
        .expect("User deposit status should be present for settled swap")
        .amount;
    assert_eq!(
        user_deposit_amount, min_input,
        "User input should match min_input deposit"
    );

    let mm_expected_output = swap2_status
        .realized
        .as_ref()
        .expect("Realized swap should be present for settled swap")
        .mm_output;
    // Output should be proportionally less than quoted (since input is less)
    assert!(
        mm_expected_output < exact_input_quote.to.amount,
        "Output {} should be less than quoted {} when depositing min_input",
        mm_expected_output,
        exact_input_quote.to.amount
    );

    // =========================================================================
    // TEST 3: ExactInput quote - deposit max_input, MM fills successfully
    // =========================================================================
    info!("=== TEST 3: ExactInput quote - max_input deposit ===");

    // Request a new quote for max_input test (fresh quote with updated liquidity)
    let test_input = 100_000_000u64; // 1 BTC
    let max_input_quote = request_quote(
        &client,
        rfq_port,
        SwapMode::ExactInput(test_input),
        &cbbtc_address,
    )
    .await;

    info!(
        "ExactInput quote for max test: input={}, output={}, min_input={}, max_input={}",
        max_input_quote.from.amount,
        max_input_quote.to.amount,
        max_input_quote.min_input,
        max_input_quote.max_input
    );

    let max_input = max_input_quote.max_input;
    assert!(
        max_input > max_input_quote.from.amount,
        "max_input {} should be greater than quoted amount {} for reasonable quotes",
        max_input,
        max_input_quote.from.amount
    );

    // Execute swap with max_input
    let swap3 = create_and_execute_swap(
        &client,
        &user_bitcoin_wallet,
        otc_port,
        max_input_quote.clone(),
        max_input, // deposit maximum
        &user_account,
    )
    .await;

    wait_for_swap_to_be_settled(otc_port, swap3.id).await;

    let swap3_status = get_swap_status(&client, otc_port, swap3.id).await;
    info!("Swap 3 (max_input) settled: {:?}", swap3_status);
    assert_eq!(swap3_status.status, SwapStatus::Settled);

    // Verify realized amounts use max_input
    let user_deposit_amount = swap3_status
        .user_deposit_status
        .as_ref()
        .expect("User deposit status should be present for settled swap")
        .amount;
    assert_eq!(
        user_deposit_amount, max_input,
        "User input should match max_input deposit"
    );

    let mm_expected_output = swap3_status
        .realized
        .as_ref()
        .expect("Realized swap should be present for settled swap")
        .mm_output;
    // Output should be proportionally more than quoted (since input is more)
    assert!(
        mm_expected_output > max_input_quote.to.amount,
        "Output {} should be greater than quoted {} when depositing max_input",
        mm_expected_output,
        max_input_quote.to.amount
    );

    info!("=== All 3 quote mode tests passed! ===");

    // Abort the devnet's internal mining task before cleanup
    devnet.join_set.abort_all();
    
    // Drop devnet in a blocking task with a timeout to avoid blocking the async runtime.
    // The corepc-node bitcoind wrapper's Drop can hang waiting for the process to exit
    // when using a persistent datadir, so we use a timeout to avoid test hangs.
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::task::spawn_blocking(move || drop(devnet)),
    )
    .await;

    tokio::join!(wallet_join_set.shutdown(), service_join_set.shutdown());
}

/// Helper: Request a quote from the RFQ server
async fn request_quote(
    client: &reqwest::Client,
    rfq_port: u16,
    mode: SwapMode,
    cbbtc_address: &str,
) -> Quote {
    let quote_request = QuoteRequest {
        mode,
        from: Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        },
        to: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::address(cbbtc_address.to_string()),
            decimals: 8,
        },
        affiliate: None,
    };

    let response = client
        .post(format!("http://localhost:{rfq_port}/api/v2/quote"))
        .json(&quote_request)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200, "Quote request should succeed");

    let quote_response: rfq_server::server::QuoteResponse = response.json().await.unwrap();

    match quote_response.quote.expect("Quote should be present") {
        RFQResult::Success(quote) => quote,
        other => panic!("Expected successful quote, got: {:?}", other),
    }
}

/// Helper: Create swap and execute deposit
async fn create_and_execute_swap(
    client: &reqwest::Client,
    wallet: &BitcoinWallet,
    otc_port: u16,
    quote: Quote,
    deposit_amount: U256,
    user_account: &MultichainAccount,
) -> Swap {
    let swap_request = CreateSwapRequest {
        quote,
        user_destination_address: user_account.ethereum_address.to_string(),
        refund_address: user_account.bitcoin_wallet.address.to_string(),
        metadata: None,
    };

    let response = client
        .post(format!("http://localhost:{otc_port}/api/v2/swap"))
        .json(&swap_request)
        .send()
        .await
        .unwrap();

    let response_status = response.status();
    let swap: Swap = match response_status {
        StatusCode::OK => response.json().await.unwrap(),
        _ => {
            let text = response.text().await;
            panic!("Swap creation failed with {}: {:?}", response_status, text);
        }
    };

    // Execute deposit
    let _tx_hash = wallet
        .create_batch_payment(
            vec![Payment {
                lot: Lot {
                    currency: swap.quote.from.currency.clone(),
                    amount: deposit_amount,
                },
                to_address: swap.deposit_vault_address.clone(),
            }],
            None,
        )
        .await
        .unwrap();

    info!(
        "Deposited {} sats to {} for swap {}",
        deposit_amount, swap.deposit_vault_address, swap.id
    );

    swap
}

/// Helper: Get swap status
async fn get_swap_status(client: &reqwest::Client, otc_port: u16, swap_id: Uuid) -> Swap {
    let response = client
        .get(format!("http://localhost:{otc_port}/api/v2/swap/{swap_id}"))
        .send()
        .await
        .unwrap();

    response.json().await.unwrap()
}
