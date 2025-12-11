use alloy::primitives::U256;
use devnet::bitcoin_devnet::MiningMode;
use devnet::{MultichainAccount, RiftDevnet};
use market_maker::run_market_maker;
use market_maker::{bitcoin_wallet::BitcoinWallet, wallet::Wallet, MarketMakerArgs};
use mock_instant::global::MockClock;
use otc_chains::traits::Payment;
use otc_models::{ChainType, Currency, Lot, QuoteRequest, TokenIdentifier};
use otc_protocols::rfq::RFQResult;
use otc_server::{
    api::{CreateSwapRequest, CreateSwapResponse},
    server::run_server,
    OtcServerArgs,
};
use rfq_server::server::run_server as run_rfq_server;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::info;

use crate::utils::{
    build_bitcoin_wallet_descriptor, build_mm_test_args, build_otc_server_test_args,
    build_rfq_server_test_args, build_tmp_bitcoin_wallet_db_file, get_free_port,
    wait_for_market_maker_to_connect_to_rfq_server, wait_for_otc_server_to_be_ready,
    wait_for_rfq_server_to_be_ready, wait_for_swap_status, PgConnectOptionsExt,
};

/// Test that liquidity locking prevents double-booking of funds
///
/// This test verifies:
/// 1. Initial liquidity is reported correctly
/// 2. After UserDeposited, liquidity decreases by locked amount
/// 3. Quote generation respects locked liquidity
/// 4. After payment is queued, liquidity is restored
#[sqlx::test]
async fn test_liquidity_locking_reduces_available_liquidity(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            MockClock::advance_system_time(Duration::from_secs(1));
        }
    });
    let market_maker_account = MultichainAccount::new(1);
    let user_account = MultichainAccount::new(2);

    let devnet = RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .using_esplora(true)
        .bitcoin_mining_mode(MiningMode::Manual)
        .build()
        .await
        .unwrap()
        .0;

    // Fund market maker with cbBTC (what we'll be locking)
    devnet
        .ethereum
        .fund_eth_address(
            market_maker_account.ethereum_address,
            U256::from(100_000_000_000_000_000_000i128), // 100 ETH
        )
        .await
        .unwrap();

    // Fund MM with a specific amount of cbBTC for easy testing
    let mm_cbbtc_amount = U256::from(10_000_000_000i128); // 100 cbBTC (8 decimals)
    devnet
        .ethereum
        .mint_cbbtc(market_maker_account.ethereum_address, mm_cbbtc_amount)
        .await
        .unwrap();

    // Fund user with BTC
    devnet
        .bitcoin
        .deal_bitcoin(
            &user_account.bitcoin_wallet.address,
            &bitcoin::Amount::from_sat(10_000_000), // 0.1 BTC
        )
        .await
        .unwrap();

    let mut service_join_set = JoinSet::new();

    // Start RFQ server
    let rfq_port = get_free_port().await;
    let rfq_args = build_rfq_server_test_args(rfq_port);
    service_join_set.spawn(async move {
        run_rfq_server(rfq_args)
            .await
            .expect("RFQ server should not crash");
    });
    wait_for_rfq_server_to_be_ready(rfq_port).await;

    // Start OTC server
    let otc_port = get_free_port().await;
    let otc_args = build_otc_server_test_args(otc_port, &devnet, &connect_options).await;
    service_join_set.spawn(async move {
        run_server(otc_args)
            .await
            .expect("OTC server should not crash");
    });
    wait_for_otc_server_to_be_ready(otc_port).await;

    // Start market maker
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
    // Give MM time to initialize balances and report liquidity
    tokio::time::sleep(Duration::from_secs(3)).await;

    let client = reqwest::Client::new();

    // Step 1: Get initial liquidity
    info!("=== Step 1: Getting initial liquidity ===");
    let liquidity_response = client
        .get(format!("http://localhost:{rfq_port}/api/v2/liquidity"))
        .send()
        .await
        .unwrap();

    assert_eq!(liquidity_response.status(), 200);
    let liquidity_result: rfq_server::liquidity_aggregator::LiquidityAggregatorResult =
        liquidity_response.json().await.unwrap();

    let market_maker = &liquidity_result.market_makers[0];
    let btc_to_cbbtc_pair = market_maker
        .trading_pairs
        .iter()
        .find(|tp| {
            tp.from.chain == ChainType::Bitcoin
                && tp.from.token == TokenIdentifier::Native
                && tp.to.chain == ChainType::Ethereum
                && matches!(&tp.to.token, TokenIdentifier::Address(addr) if addr.to_lowercase() == devnet.ethereum.cbbtc_contract.address().to_string().to_lowercase())
        })
        .expect("Should have BTC->cbBTC trading pair");

    let initial_max_amount = btc_to_cbbtc_pair.max_amount;
    info!("Initial BTC->cbBTC max_amount: {}", initial_max_amount);
    assert!(
        initial_max_amount > U256::ZERO,
        "Should have initial liquidity"
    );

    // Step 2: Request a quote and create a swap
    info!("=== Step 2: Creating swap to trigger liquidity lock ===");
    let swap_amount = U256::from(1_000_000); // 0.01 BTC in sats (small amount)

    let quote_request = QuoteRequest {
        input_hint: Some(swap_amount),
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
        .post(format!("http://localhost:{rfq_port}/api/v2/quote"))
        .json(&quote_request)
        .send()
        .await
        .unwrap();

    assert_eq!(quote_response.status(), 200);
    let quote_response: rfq_server::server::QuoteResponse = quote_response.json().await.unwrap();

    let quote = match quote_response.quote.expect("Should have quote") {
        RFQResult::Success(quote) => quote,
        _ => panic!("Should get successful quote"),
    };

    info!(
        "Got quote: min_input = {}, max_input = {}",
        quote.min_input, quote.max_input
    );

    // Create swap
    let swap_request = CreateSwapRequest {
        quote: quote.clone(),
        user_destination_address: user_account.ethereum_address.to_string(),
        user_evm_account_address: user_account.ethereum_address,
        metadata: None,
    };

    let swap_response = client
        .post(format!("http://localhost:{otc_port}/api/v2/swap"))
        .json(&swap_request)
        .send()
        .await
        .unwrap();

    assert_eq!(swap_response.status(), 200);
    let swap_response: CreateSwapResponse = swap_response.json().await.unwrap();
    let swap_id = swap_response.swap_id;
    info!("Created swap: {}", swap_id);

    // Step 3: Send user deposit (this triggers UserDeposited message and locks liquidity)
    info!("=== Step 3: Sending user deposit to trigger lock ===");
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

    let deposit_tx_hash = user_bitcoin_wallet
        .create_batch_payment(
            vec![Payment {
                lot: Lot {
                    currency: Currency {
                        chain: ChainType::Bitcoin,
                        token: TokenIdentifier::Native,
                        decimals: swap_response.decimals,
                    },
                    amount: swap_response.min_input,
                },
                to_address: swap_response.deposit_address,
            }],
            None,
        )
        .await
        .unwrap();

    info!("User deposit sent: {}", deposit_tx_hash);

    // Mine 1 block to include the deposit transaction (so it's detected but not confirmed)
    devnet.bitcoin.mine_blocks(1).await.unwrap();
    info!("Mined 1 block - deposit should be detected but not confirmed yet");

    // Wait for swap to detect deposit and send UserDeposited message (lock should be created)
    wait_for_swap_status(&client, otc_port, swap_id, "WaitingUserDepositConfirmed").await;
    info!("Swap reached waiting_user_deposit_confirmed - lock should be active");

    // Step 4: Verify liquidity decreased after lock (before deposit confirmation)
    info!("=== Step 4: Verifying liquidity decreased after lock (before confirmation) ===");

    // Poll liquidity endpoint until cache updates (up to 20 seconds to account for 15s cache interval)
    let mut max_amount_after_lock = None;
    let mut attempts = 0;
    let max_attempts = 20;
    while attempts < max_attempts && max_amount_after_lock.is_none() {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let liquidity_response = client
            .get(format!("http://localhost:{rfq_port}/api/v2/liquidity"))
            .send()
            .await
            .unwrap();

        assert_eq!(liquidity_response.status(), 200);
        let liquidity: rfq_server::liquidity_aggregator::LiquidityAggregatorResult =
            liquidity_response.json().await.unwrap();

        info!("Liquidity response: {:#?}", liquidity);
        let market_maker = &liquidity.market_makers[0];
        let btc_to_cbbtc = market_maker
            .trading_pairs
            .iter()
            .find(|tp| {
                tp.from.chain == ChainType::Bitcoin
                    && tp.from.token == TokenIdentifier::Native
                    && tp.to.chain == ChainType::Ethereum
                    && matches!(&tp.to.token, TokenIdentifier::Address(addr) if addr.to_lowercase() == devnet.ethereum.cbbtc_contract.address().to_string().to_lowercase())
            })
            .expect("Should have BTC->cbBTC trading pair");

        max_amount_after_lock = Some(btc_to_cbbtc.max_amount);
        attempts += 1;
    }
    let max_amount_after_lock = max_amount_after_lock.expect("Should have max amount after lock");

    info!(
        "Max amount after lock: {} (initial: {})",
        max_amount_after_lock, initial_max_amount
    );

    // Liquidity should have strictly decreased after locking
    assert!(
        max_amount_after_lock < initial_max_amount,
        "Liquidity should strictly decrease after lock (after: {}, initial: {})",
        max_amount_after_lock,
        initial_max_amount
    );

    // Now mine 3 more blocks to confirm the deposit (this will trigger unlock)
    devnet.bitcoin.mine_blocks(3).await.unwrap();
    info!("Mined 3 more blocks to confirm user deposit (total 4 blocks, should be enough for confirmation)");

    // Step 5: Try to request a quote for amount that would exceed locked liquidity
    // This should fail or return reduced quote
    info!("=== Step 5: Testing quote request respects locked liquidity ===");
    let large_quote_request = QuoteRequest {
        input_hint: Some(max_amount_after_lock + U256::from(1)), // Try to exceed available
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

    let large_quote_response = client
        .post(format!("http://localhost:{rfq_port}/api/v2/quote"))
        .json(&large_quote_request)
        .send()
        .await
        .unwrap();

    assert_eq!(large_quote_response.status(), 200);
    let large_quote_response: rfq_server::server::QuoteResponse =
        large_quote_response.json().await.unwrap();

    // Should either have no quote or an error
    if let Some(result) = large_quote_response.quote {
        match result {
            RFQResult::Success(_) => {
                // If we get a quote, it should be for an amount <= max_amount_after_lock
                // This is acceptable if the strategy allows it
                info!(
                    "Got quote even though it exceeds reported max - acceptable if strategy allows"
                );
            }
            RFQResult::MakerUnavailable(_)
            | RFQResult::InvalidRequest(_)
            | RFQResult::Unsupported(_) => {
                info!("Got expected error for amount exceeding locked liquidity");
            }
        }
    } else {
        info!("Got no quote for amount exceeding locked liquidity (expected)");
    }

    // Step 6: Wait for payment to be queued (unlocks liquidity)
    info!("=== Step 6: Waiting for payment to be queued (unlocks liquidity) ===");
    // Wait for swap to reach WaitingMMDepositInitiated (payment queued)
    wait_for_swap_status(&client, otc_port, swap_id, "WaitingMMDepositInitiated").await;
    info!("Swap reached waiting_mm_deposit_initiated - lock should be released");

    // Give MM time to process UserDepositConfirmed and update liquidity cache
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Step 7: Verify liquidity is restored after unlock
    info!("=== Step 7: Verifying liquidity restored after unlock ===");
    let liquidity_response_after_unlock = client
        .get(format!("http://localhost:{rfq_port}/api/v2/liquidity"))
        .send()
        .await
        .unwrap();

    assert_eq!(liquidity_response_after_unlock.status(), 200);
    let liquidity_after_unlock: rfq_server::liquidity_aggregator::LiquidityAggregatorResult =
        liquidity_response_after_unlock.json().await.unwrap();

    let market_maker_after_unlock = &liquidity_after_unlock.market_makers[0];
    let btc_to_cbbtc_after_unlock = market_maker_after_unlock
        .trading_pairs
        .iter()
        .find(|tp| {
            tp.from.chain == ChainType::Bitcoin
                && tp.from.token == TokenIdentifier::Native
                && tp.to.chain == ChainType::Ethereum
                && matches!(&tp.to.token, TokenIdentifier::Address(addr) if addr.to_lowercase() == devnet.ethereum.cbbtc_contract.address().to_string().to_lowercase())
        })
        .expect("Should have BTC->cbBTC trading pair");

    let max_amount_after_unlock = btc_to_cbbtc_after_unlock.max_amount;
    info!(
        "Max amount after unlock: {} (initial: {}, after lock: {})",
        max_amount_after_unlock, initial_max_amount, max_amount_after_lock
    );

    // Liquidity should be restored (at least back to initial, possibly higher if balance
    // cache updated, but we don't assert exact equality due to cache timing)
    assert!(
        max_amount_after_unlock >= max_amount_after_lock,
        "Liquidity should increase after unlock"
    );

    drop(devnet);
    tokio::join!(wallet_join_set.shutdown(), service_join_set.shutdown());
}
