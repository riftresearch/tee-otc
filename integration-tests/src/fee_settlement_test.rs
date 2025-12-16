//! Fee settlement end-to-end integration test.
//!
//! This test validates that protocol fees are correctly settled by market makers:
//! 1. Execute swaps in both directions (BTC<->ETH)
//! 2. Wait for all swaps to settle
//! 3. Wait for fee settlement to trigger
//! 4. Verify fee wallet received the expected cbBTC amounts
//!
//! Note: Base swaps are excluded because the Base token indexer is only available
//! in interactive mode, not in automated tests.

use alloy::primitives::{Address, U256};
use alloy::providers::ext::AnvilApi;
use alloy::providers::Provider;
use alloy::signers::local::PrivateKeySigner;
use devnet::bitcoin_devnet::MiningMode;
use devnet::{MultichainAccount, RiftDevnet};
use eip3009_erc20_contract::GenericEIP3009ERC20::GenericEIP3009ERC20Instance;
use market_maker::bitcoin_wallet::BitcoinWallet;
use market_maker::run_market_maker;
use market_maker::wallet::Wallet;
use otc_chains::traits::Payment;
use otc_models::{SwapMode, ChainType, Currency, Lot, QuoteRequest, TokenIdentifier};
use otc_protocols::rfq::RFQResult;
use otc_server::api::{CreateSwapRequest, CreateSwapResponse};
use otc_server::server::run_server;
use reqwest::StatusCode;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::str::FromStr;
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::info;
use uuid::Uuid;

use crate::utils::{
    build_bitcoin_wallet_descriptor, build_mm_test_args, build_otc_server_test_args,
    build_rfq_server_test_args, build_test_user_ethereum_wallet, build_tmp_bitcoin_wallet_db_file,
    get_free_port, wait_for_market_maker_to_connect_to_rfq_server, wait_for_otc_server_to_be_ready,
    wait_for_rfq_server_to_be_ready, wait_for_swap_to_be_settled, PgConnectOptionsExt,
};

/// Helper struct to track swap info for fee verification
struct SwapInfo {
    swap_id: Uuid,
    direction: String,
    /// The actual deposit amount used (equals quote.from.amount for exact match)
    deposited_amount: U256,
}

/// Execute a swap from Bitcoin to Ethereum and return swap info.
async fn execute_btc_to_eth_swap(
    client: &reqwest::Client,
    rfq_port: u16,
    otc_port: u16,
    user_bitcoin_wallet: &BitcoinWallet,
    user_eth_address: Address,
    user_btc_refund_address: &str,
    cbbtc_address: &str,
    input_sats: u64,
) -> SwapInfo {
    let quote_request = QuoteRequest {
        mode: SwapMode::ExactInput(input_sats),
        from: Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        },
        to: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::Address(cbbtc_address.to_string()),
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

    assert_eq!(quote_response.status(), 200, "Quote request should succeed");

    let quote_response: rfq_server::server::QuoteResponse = quote_response.json().await.unwrap();
    let quote = match quote_response.quote.expect("Quote should be present") {
        RFQResult::Success(q) => q,
        other => panic!("Expected successful quote, got: {other:?}"),
    };

    let swap_request = CreateSwapRequest {
        quote: quote.clone(),
        user_destination_address: user_eth_address.to_string(),
        refund_address: user_btc_refund_address.to_string(),
        metadata: None,
    };

    let response = client
        .post(format!("http://localhost:{otc_port}/api/v2/swap"))
        .json(&swap_request)
        .send()
        .await
        .unwrap();

    let response_json: CreateSwapResponse = match response.status() {
        StatusCode::OK => response.json().await.unwrap(),
        status => panic!("Swap request failed with status {status}"),
    };

    // Deposit the exact quoted input to ensure from_quote uses quote's fees
    let deposit_amount = response_json.quoted_input;
    assert!(
        deposit_amount >= response_json.min_input,
        "Quoted input {} must be >= min_input {}",
        deposit_amount,
        response_json.min_input
    );

    user_bitcoin_wallet
        .create_batch_payment(
            vec![Payment {
                lot: Lot {
                    currency: Currency {
                        chain: ChainType::Bitcoin,
                        token: TokenIdentifier::Native,
                        decimals: response_json.decimals,
                    },
                    amount: deposit_amount,
                },
                to_address: response_json.deposit_address,
            }],
            None,
        )
        .await
        .unwrap();

    info!(
        "Executed BTC -> ETH swap {} with quoted_input {} sats",
        response_json.swap_id, deposit_amount
    );

    SwapInfo {
        swap_id: response_json.swap_id,
        direction: "BTC -> ETH".to_string(),
        deposited_amount: deposit_amount,
    }
}

/// Execute a swap from Ethereum to Bitcoin and return swap info.
async fn execute_eth_to_btc_swap(
    client: &reqwest::Client,
    rfq_port: u16,
    otc_port: u16,
    user_eth_wallet: &market_maker::evm_wallet::EVMWallet,
    user_btc_destination: &str,
    user_eth_refund_address: Address,
    cbbtc_address: &str,
    input_sats: u64,
) -> SwapInfo {
    let quote_request = QuoteRequest {
        mode: SwapMode::ExactInput(input_sats),
        from: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::Address(cbbtc_address.to_string()),
            decimals: 8,
        },
        to: Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
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

    assert_eq!(quote_response.status(), 200, "Quote request should succeed");

    let quote_response: rfq_server::server::QuoteResponse = quote_response.json().await.unwrap();
    let quote = match quote_response.quote.expect("Quote should be present") {
        RFQResult::Success(q) => q,
        other => panic!("Expected successful quote for ETH -> BTC, got: {other:?}"),
    };

    let swap_request = CreateSwapRequest {
        quote: quote.clone(),
        user_destination_address: user_btc_destination.to_string(),
        refund_address: user_eth_refund_address.to_string(),
        metadata: None,
    };

    let response = client
        .post(format!("http://localhost:{otc_port}/api/v2/swap"))
        .json(&swap_request)
        .send()
        .await
        .unwrap();

    let response_json: CreateSwapResponse = match response.status() {
        StatusCode::OK => response.json().await.unwrap(),
        status => panic!("Swap request failed with status {status}"),
    };

    // Deposit the exact quoted input to ensure from_quote uses quote's fees
    let deposit_amount = response_json.quoted_input;
    assert!(
        deposit_amount >= response_json.min_input,
        "Quoted input {} must be >= min_input {}",
        deposit_amount,
        response_json.min_input
    );

    user_eth_wallet
        .create_batch_payment(
            vec![Payment {
                lot: Lot {
                    currency: Currency {
                        chain: ChainType::Ethereum,
                        token: TokenIdentifier::Address(cbbtc_address.to_string()),
                        decimals: response_json.decimals,
                    },
                    amount: deposit_amount,
                },
                to_address: response_json.deposit_address,
            }],
            None,
        )
        .await
        .unwrap();

    info!(
        "Executed ETH -> BTC swap {} with quoted_input {} sats",
        response_json.swap_id, deposit_amount
    );

    SwapInfo {
        swap_id: response_json.swap_id,
        direction: "ETH -> BTC".to_string(),
        deposited_amount: deposit_amount,
    }
}

/// Query cbBTC balance at an address.
async fn get_cbbtc_balance(
    provider: &impl Provider,
    cbbtc_address: Address,
    wallet_address: Address,
) -> U256 {
    let contract = GenericEIP3009ERC20Instance::new(cbbtc_address, provider);
    contract
        .balanceOf(wallet_address)
        .call()
        .await
        .expect("Failed to query cbBTC balance")
}

#[sqlx::test]
async fn test_fee_settlement_with_ethereum_swaps(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    // Create accounts for market maker and user
    let mm_account = MultichainAccount::new(1);
    let user_account = MultichainAccount::new(2);

    // Build devnet with Ethereum chain + token indexer
    let devnet = RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .using_esplora(true)
        .bitcoin_mining_mode(MiningMode::Interval(2))
        .build()
        .await
        .unwrap()
        .0;

    let mut wallet_join_set = JoinSet::new();

    // Create user Bitcoin wallet
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

    // Fund user with Bitcoin
    devnet
        .bitcoin
        .deal_bitcoin(
            &user_account.bitcoin_wallet.address,
            &bitcoin::Amount::from_sat(1_000_000_000), // 10 BTC
        )
        .await
        .unwrap();

    // Fund market maker
    devnet
        .ethereum
        .fund_eth_address(
            mm_account.ethereum_address,
            U256::from(100_000_000_000_000_000_000u128),
        )
        .await
        .unwrap();
    devnet
        .ethereum
        .mint_cbbtc(
            mm_account.ethereum_address,
            U256::from(10_000_000_000u64), // 100 cbBTC
        )
        .await
        .unwrap();
    devnet
        .bitcoin
        .deal_bitcoin(
            &mm_account.bitcoin_wallet.address,
            &bitcoin::Amount::from_sat(1_000_000_000), // 10 BTC
        )
        .await
        .unwrap();

    // Fund user on Ethereum for ETH -> BTC swaps
    devnet
        .ethereum
        .fund_eth_address(
            user_account.ethereum_address,
            U256::from(100_000_000_000_000_000_000u128),
        )
        .await
        .unwrap();
    devnet
        .ethereum
        .mint_cbbtc(
            user_account.ethereum_address,
            U256::from(5_000_000_000u64), // 50 cbBTC
        )
        .await
        .unwrap();

    // Create user Ethereum wallet
    let (mut eth_wallet_join_set, user_eth_wallet) =
        build_test_user_ethereum_wallet(&devnet, &user_account).await;

    // Ensure EIP-7702 delegation for user wallet
    user_eth_wallet
        .ensure_eip7702_delegation(
            PrivateKeySigner::from_slice(&user_account.secret_bytes).unwrap(),
        )
        .await
        .unwrap();

    let mut service_join_set = JoinSet::new();

    // Start OTC server
    let otc_port = get_free_port().await;
    let otc_args = build_otc_server_test_args(otc_port, &devnet, &connect_options).await;
    service_join_set.spawn(async move {
        run_server(otc_args).await.expect("OTC server should not crash");
    });

    tokio::select! {
        _ = wait_for_otc_server_to_be_ready(otc_port) => {
            info!("OTC server is ready");
        }
        _ = service_join_set.join_next() => {
            panic!("OTC server crashed");
        }
    }

    // Start RFQ server
    let rfq_port = get_free_port().await;
    let rfq_args = build_rfq_server_test_args(rfq_port);
    service_join_set.spawn(async move {
        rfq_server::server::run_server(rfq_args)
            .await
            .expect("RFQ server should not crash");
    });

    wait_for_rfq_server_to_be_ready(rfq_port).await;

    // Wait for esplora sync BEFORE starting market maker so it sees funded balances
    devnet
        .bitcoin
        .wait_for_esplora_sync(Duration::from_secs(30))
        .await
        .unwrap();

    // Start market maker with fast fee settlement
    let mut mm_args =
        build_mm_test_args(otc_port, rfq_port, &mm_account, &devnet, &connect_options).await;
    mm_args.fee_settlement_interval_secs = 5; // Fast settlement polling

    service_join_set.spawn(async move {
        run_market_maker(mm_args)
            .await
            .expect("MM should not crash");
    });

    // Wait for market maker to connect
    wait_for_market_maker_to_connect_to_rfq_server(rfq_port).await;

    let client = reqwest::Client::new();
    let cbbtc_address = devnet.ethereum.cbbtc_contract.address().to_string();

    // Record initial fee wallet balance
    let fee_address = Address::from_str(&otc_models::FEE_ADDRESSES_BY_CHAIN[&ChainType::Ethereum])
        .expect("Valid fee address");

    let initial_fee_balance = get_cbbtc_balance(
        &devnet.ethereum.funded_provider,
        *devnet.ethereum.cbbtc_contract.address(),
        fee_address,
    )
    .await;

    info!("Initial fee wallet balance: {} sats", initial_fee_balance);

    // Execute swaps in both directions
    let mut swaps = Vec::new();

    // 1. BTC -> Ethereum (large swap to generate significant fees)
    let swap1 = execute_btc_to_eth_swap(
        &client,
        rfq_port,
        otc_port,
        &user_bitcoin_wallet,
        user_account.ethereum_address,
        &user_account.bitcoin_wallet.address.to_string(),
        &cbbtc_address,
        50_000_000, // 0.5 BTC
    )
    .await;
    swaps.push(swap1);

    // 2. Another BTC -> Ethereum swap
    let swap2 = execute_btc_to_eth_swap(
        &client,
        rfq_port,
        otc_port,
        &user_bitcoin_wallet,
        user_account.ethereum_address,
        &user_account.bitcoin_wallet.address.to_string(),
        &cbbtc_address,
        40_000_000, // 0.4 BTC
    )
    .await;
    swaps.push(swap2);

    // 3. Ethereum -> BTC
    devnet
        .ethereum
        .funded_provider
        .anvil_mine(Some(2), None)
        .await
        .unwrap();

    let swap3 = execute_eth_to_btc_swap(
        &client,
        rfq_port,
        otc_port,
        &user_eth_wallet,
        &user_account.bitcoin_wallet.address.to_string(),
        user_account.ethereum_address,
        &cbbtc_address,
        30_000_000, // 0.3 cbBTC
    )
    .await;
    swaps.push(swap3);

    // Mine blocks to confirm deposits
    devnet.bitcoin.mine_blocks(6).await.unwrap();
    devnet
        .ethereum
        .funded_provider
        .anvil_mine(Some(5), None)
        .await
        .unwrap();

    // Wait for all swaps to settle
    info!("Waiting for all {} swaps to settle...", swaps.len());
    for swap in &swaps {
        info!("Waiting for {} swap {} to settle", swap.direction, swap.swap_id);
        wait_for_swap_to_be_settled(otc_port, swap.swap_id).await;
        info!("Swap {} settled!", swap.swap_id);
    }

    info!("All swaps settled! Now waiting for fee settlement...");

    // Mine more blocks to ensure MM batches are confirmed
    devnet.bitcoin.mine_blocks(6).await.unwrap();
    devnet
        .ethereum
        .funded_provider
        .anvil_mine(Some(10), None)
        .await
        .unwrap();

    // Give the batch monitor time to mark batches as confirmed
    // and the fee settlement engine time to poll and process
    info!("Waiting for batch confirmations to propagate...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Wait for fee settlement (poll for up to 120 seconds)
    // The fee settlement engine runs every 5 seconds in tests
    // Add buffer for standing status polling and settlement tx confirmation
    let settlement_timeout = Duration::from_secs(120);
    let poll_interval = Duration::from_secs(3);
    let start = std::time::Instant::now();

    loop {
        // Mine more blocks to help confirmations
        devnet
            .ethereum
            .funded_provider
            .anvil_mine(Some(2), None)
            .await
            .unwrap();

        let fee_balance = get_cbbtc_balance(
            &devnet.ethereum.funded_provider,
            *devnet.ethereum.cbbtc_contract.address(),
            fee_address,
        )
        .await;

        let received = fee_balance.saturating_sub(initial_fee_balance);

        info!(
            "Fee wallet balance check - received: {} sats (current: {}, initial: {})",
            received, fee_balance, initial_fee_balance
        );

        // Fees should be exactly computable from actual deposited amounts
        // Protocol fee = ceil(deposited * 10 / 10000) for each swap
        if received > U256::ZERO {
            info!("Fee settlement detected! Total fees received: {} sats", received);

            // Calculate exact expected fees from actual deposited amounts
            // Protocol fee uses ceiling division: ceil(amount * bps / 10000)
            let protocol_fee_bps: u64 = 10;
            let mut expected_fees: u64 = 0;
            for swap in &swaps {
                let amount = swap.deposited_amount.to::<u64>();
                let fee = amount.saturating_mul(protocol_fee_bps).div_ceil(10_000);
                info!("Swap {}: deposited {} sats, protocol fee {} sats", swap.direction, amount, fee);
                expected_fees += fee;
            }

            let received_u64 = received.to::<u64>();
            assert_eq!(
                received_u64, expected_fees,
                "Expected exactly {} sats in protocol fees, got {} sats",
                expected_fees, received_u64
            );

            let total_volume: u64 = swaps.iter().map(|s| s.deposited_amount.to::<u64>()).sum();
            info!(
                "Fee settlement verification passed! Received {} sats ({:.2}% of {} sats volume)",
                received_u64,
                (received_u64 as f64 / total_volume as f64) * 100.0,
                total_volume
            );
            break;
        }

        if start.elapsed() > settlement_timeout {
            panic!(
                "Timeout waiting for fee settlement after {}s. Balance: {} (initial: {})",
                settlement_timeout.as_secs(), fee_balance, initial_fee_balance
            );
        }

        tokio::time::sleep(poll_interval).await;
    }

    info!("Fee settlement test completed successfully!");

    // Cleanup
    drop(devnet);
    tokio::join!(
        wallet_join_set.shutdown(),
        eth_wallet_join_set.shutdown(),
        service_join_set.shutdown()
    );
}
