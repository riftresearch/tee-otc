use alloy::primitives::U256;
use devnet::bitcoin_devnet::MiningMode;
use devnet::{MultichainAccount, RiftDevnet};
use market_maker::run_market_maker;
use market_maker::{bitcoin_wallet::BitcoinWallet, wallet::Wallet, MarketMakerArgs};
use mock_instant::global::MockClock;
use otc_chains::traits::Payment;
use otc_models::{ChainType, Currency, Lot, QuoteRequest, Swap, SwapMode, TokenIdentifier};
use otc_protocols::rfq::RFQResult;
use otc_server::{api::CreateSwapRequest, server::run_server, OtcServerArgs};
use rfq_server::server::run_server as run_rfq_server;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::info;

use crate::utils::{
    build_bitcoin_wallet_descriptor, build_mm_test_args, build_otc_server_test_args,
    build_rfq_server_test_args, build_tmp_bitcoin_wallet_db_file, get_free_port,
    wait_for_market_maker_to_connect_to_rfq_server, wait_for_otc_server_to_be_ready,
    wait_for_rfq_server_to_be_ready, wait_for_swap_status, wait_for_swap_statuses,
    PgConnectOptionsExt,
};

/// Test that advertised route capacity stays fixed as swaps progress through the pipeline.
///
/// This test verifies:
/// 1. Initial route capacity is reported correctly
/// 2. After `UserDeposited`, advertised capacity does not shrink
/// 3. Quotes above the advertised capacity are still rejected
/// 4. After payment is queued, advertised capacity remains stable
#[sqlx::test]
async fn test_boot_time_capacity_does_not_shrink_as_swaps_progress(
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
        mode: SwapMode::ExactInput(swap_amount.to::<u64>()),
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
        refund_address: user_account.bitcoin_wallet.address.to_string(),
        metadata: None,
    };

    let swap_response = client
        .post(format!("http://localhost:{otc_port}/api/v2/swap"))
        .json(&swap_request)
        .send()
        .await
        .unwrap();

    assert_eq!(swap_response.status(), 200);
    let swap: Swap = swap_response.json().await.unwrap();
    let swap_id = swap.id;
    info!("Created swap: {}", swap_id);

    // Step 3: Send user deposit (this advances the swap, but should not shrink route capacity)
    info!("=== Step 3: Sending user deposit ===");
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
                    currency: swap.quote.from.currency.clone(),
                    amount: swap.quote.min_input,
                },
                to_address: swap.deposit_vault_address.clone(),
            }],
            None,
        )
        .await
        .unwrap();

    info!("User deposit sent: {}", deposit_tx_hash);

    // Mine 1 block to include the deposit transaction (so it's detected but not confirmed)
    devnet.bitcoin.mine_blocks(1).await.unwrap();
    info!("Mined 1 block - deposit should be detected but not confirmed yet");

    // Wait for swap to detect deposit and send UserDeposited message
    wait_for_swap_status(&client, otc_port, swap_id, "WaitingUserDepositConfirmed").await;
    info!("Swap reached waiting_user_deposit_confirmed");

    // Step 4: Verify advertised capacity did not shrink after deposit detection.
    info!("=== Step 4: Verifying advertised capacity remains stable ===");

    tokio::time::sleep(Duration::from_secs(2)).await;
    let liquidity_response = client
        .get(format!("http://localhost:{rfq_port}/api/v2/liquidity"))
        .send()
        .await
        .unwrap();

    assert_eq!(liquidity_response.status(), 200);
    let liquidity: rfq_server::liquidity_aggregator::LiquidityAggregatorResult =
        liquidity_response.json().await.unwrap();

    info!(
        "Liquidity response after deposit detection: {:#?}",
        liquidity
    );
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

    let max_amount_after_progress = btc_to_cbbtc.max_amount;

    info!(
        "Max amount after progress: {} (initial: {})",
        max_amount_after_progress, initial_max_amount
    );

    // Advertised capacity should not change as swaps move through detection/confirmation.
    assert!(
        max_amount_after_progress == initial_max_amount,
        "Advertised capacity should remain fixed (after progress: {}, initial: {})",
        max_amount_after_progress,
        initial_max_amount
    );

    // Now mine 3 more blocks to confirm the deposit (this will trigger unlock)
    devnet.bitcoin.mine_blocks(3).await.unwrap();
    info!("Mined 3 more blocks to confirm user deposit (total 4 blocks, should be enough for confirmation)");

    // Step 5: Try to request a quote above the advertised payout capacity. This should fail.
    info!("=== Step 5: Testing quote request respects boot-time capacity ===");
    let large_quote_request = QuoteRequest {
        mode: SwapMode::ExactOutput((initial_max_amount + U256::from(1)).to::<u64>()),
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
                panic!("Should not get a quote above the advertised boot-time capacity");
            }
            RFQResult::MakerUnavailable(_)
            | RFQResult::InvalidRequest(_)
            | RFQResult::Unsupported(_) => {
                info!("Got expected error for amount exceeding advertised capacity");
            }
        }
    } else {
        info!("Got no quote for amount exceeding advertised capacity (expected)");
    }

    // Step 6: Wait for payment to be queued.
    info!("=== Step 6: Waiting for payment to be queued ===");
    // Planner-driven execution can move the swap to MMDepositConfirmed or even Settled before
    // this polling loop sees the intermediate WaitingMMDepositInitiated state.
    wait_for_swap_statuses(
        &client,
        otc_port,
        swap_id,
        &[
            "WaitingMMDepositInitiated",
            "WaitingMMDepositConfirmed",
            "Settled",
        ],
    )
    .await;
    info!("Swap advanced beyond user-deposit-confirmed");

    // Give MM time to process UserDepositConfirmed
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Step 7: Verify advertised capacity is still unchanged.
    info!("=== Step 7: Verifying advertised capacity remains unchanged ===");
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
        "Max amount after settlement progress: {} (initial: {})",
        max_amount_after_unlock, initial_max_amount
    );

    // Advertised capacity should remain fixed for the lifetime of the process.
    assert!(
        max_amount_after_unlock == initial_max_amount,
        "Advertised capacity should remain unchanged after settlement progress"
    );

    drop(devnet);
    tokio::join!(wallet_join_set.shutdown(), service_join_set.shutdown());
}
