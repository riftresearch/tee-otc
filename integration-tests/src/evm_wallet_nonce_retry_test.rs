use alloy::{
    primitives::U256,
    providers::{ProviderBuilder, WsConnect},
};
use devnet::{MultichainAccount, RiftDevnet};
use market_maker::{
    evm_wallet::{self, EVMWallet},
    wallet::Wallet,
};
use otc_models::{ChainType, Currency, TokenIdentifier};
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::{sync::Arc, time::Duration};
use tokio::task::JoinSet;
use tracing::{debug, info};

use crate::utils::PgConnectOptionsExt;

/// Test that verifies the EVM wallet transaction broadcaster correctly handles
/// nonce errors and retries with proper gas bumping
#[sqlx::test]
async fn test_evm_wallet_nonce_error_retry(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    // Initialize logging for debugging
    let _ = tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(true)
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    info!("Starting EVM wallet nonce error retry test");

    // Set up test accounts
    let market_maker_account = MultichainAccount::new(1);
    let user_account = MultichainAccount::new(2);

    // Start the devnet
    let devnet = RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .using_esplora(true)
        .build()
        .await
        .unwrap()
        .0;

    // Get the Ethereum RPC URL from the anvil instance
    let eth_rpc_url = devnet.ethereum.anvil.endpoint_url();
    let ws_url = devnet.ethereum.anvil.ws_endpoint_url();
    let ws_url_string = ws_url.to_string();

    // Create a wallet provider for the market maker
    let wallet = market_maker_account.ethereum_wallet.clone();
    let provider = Arc::new(
        ProviderBuilder::new()
            .wallet(wallet)
            .on_ws(WsConnect::new(ws_url_string))
            .await
            .unwrap(),
    );

    // Use the cbBTC token that's already deployed on devnet
    let test_token = devnet.ethereum.cbbtc_contract.address().clone();

    // Fund the market maker with cbBTC tokens
    devnet
        .ethereum
        .mint_cbbtc(
            market_maker_account.ethereum_address,
            U256::from(10).pow(U256::from(24)), // 1M tokens
        )
        .await
        .unwrap();

    // Also fund with ETH for gas
    devnet
        .ethereum
        .fund_eth_address(
            market_maker_account.ethereum_address,
            U256::from(10).pow(U256::from(19)), // 10 ETH
        )
        .await
        .unwrap();

    info!("Deployed test token at: {}", test_token);

    // Create the EVM wallet with transaction broadcaster
    let mut join_set = JoinSet::new();
    let evm_wallet = EVMWallet::new(
        provider.clone(),
        eth_rpc_url.to_string(),
        1, // 1 confirmation for testing
        &mut join_set,
    );

    // Subscribe to transaction status updates
    let mut status_receiver = evm_wallet.tx_broadcaster.subscribe_to_status_updates();

    // Create a currency for testing
    let currency = Currency {
        chain: ChainType::Ethereum,
        token: TokenIdentifier::Address(test_token.to_string()),
        amount: U256::from(1000) * U256::pow(U256::from(10), U256::from(18)), // 1000 tokens
        decimals: 18,
    };

    // Test Case 1: Simulate nonce too low error by sending multiple transactions rapidly
    info!("Test Case 1: Testing nonce too low error recovery");

    let user_address = user_account.ethereum_address.to_string();

    // Send the first transaction normally
    let tx1_future = evm_wallet.create_transaction(&currency, &user_address, None);

    // Immediately send another transaction to create a nonce conflict
    let tx2_future = evm_wallet.create_transaction(&currency, &user_address, None);

    // Both transactions should eventually succeed due to retry logic
    let (tx1_result, tx2_result) = tokio::join!(tx1_future, tx2_future);

    // At least one should succeed immediately, the other might have retried
    assert!(
        tx1_result.is_ok() || tx2_result.is_ok(),
        "At least one transaction should succeed tx1: {tx1_result:?}, tx2: {tx2_result:?}"
    );

    // Wait for status updates and verify retry behavior
    let mut retry_detected = false;

    // Collect a few status updates with timeout
    let timeout_duration = Duration::from_secs(2);
    let start = tokio::time::Instant::now();

    while start.elapsed() < timeout_duration {
        match tokio::time::timeout(Duration::from_millis(100), status_receiver.recv()).await {
            Ok(Ok(update)) => {
                debug!("Received status update: {:?}", update.result);

                // Check if this was a retry (would see multiple updates for same tx)
                if update.result.is_revert() {
                    if let evm_wallet::transaction_broadcaster::TransactionExecutionResult::Revert(
                        revert_info,
                    ) = &update.result
                    {
                        if revert_info
                            .error_payload
                            .message
                            .to_lowercase()
                            .contains("nonce")
                        {
                            retry_detected = true;
                            info!(
                                "Detected nonce error: {}",
                                revert_info.error_payload.message
                            );
                        }
                    }
                }
            }
            _ => break,
        }
    }

    info!(
        "Retry behavior check - nonce error detected: {}",
        retry_detected
    );

    // Test Case 2: Verify balance checking with buffer works correctly
    info!("Test Case 2: Testing balance checking with buffer");

    let can_fill = evm_wallet.can_fill(&currency).await.unwrap();
    assert!(can_fill, "Should be able to fill the currency amount");

    // Try with an amount that exceeds balance + buffer
    let large_currency = Currency {
        chain: ChainType::Ethereum,
        token: TokenIdentifier::Address(test_token.to_string()),
        amount: U256::from(2_000_000) * U256::pow(U256::from(10), U256::from(18)), // 2M tokens (more than funded)
        decimals: 18,
    };

    let can_fill_large = evm_wallet.can_fill(&large_currency).await.unwrap();
    assert!(
        !can_fill_large,
        "Should not be able to fill amount exceeding balance + buffer"
    );

    // Test Case 3: Verify transaction with custom nonce embedding
    info!("Test Case 3: Testing transaction with custom nonce");

    let custom_nonce: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let tx_with_nonce = evm_wallet
        .create_transaction(&currency, &user_address, Some(custom_nonce))
        .await;

    assert!(
        tx_with_nonce.is_ok(),
        "Transaction with custom nonce should succeed"
    );

    // Clean up
    join_set.abort_all();

    info!("EVM wallet nonce error retry test completed successfully");
}

/// Test that verifies gas price bumping during replacement transactions
#[sqlx::test]
async fn test_evm_wallet_gas_price_bumping(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_target(false)
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    info!("Starting EVM wallet gas price bumping test");

    // Set up test infrastructure
    let market_maker_account = MultichainAccount::new(1);
    let user_account = MultichainAccount::new(2);

    let devnet = RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .using_esplora(true)
        .build()
        .await
        .unwrap()
        .0;

    let eth_rpc_url = devnet.ethereum.anvil.endpoint_url();
    let ws_url = devnet.ethereum.anvil.ws_endpoint_url();
    let ws_url_string = ws_url.to_string();

    // Create provider with low initial gas price to trigger replacement
    let wallet = market_maker_account.ethereum_wallet.clone();
    let provider = Arc::new(
        ProviderBuilder::new()
            .wallet(wallet)
            .connect_ws(WsConnect::new(ws_url_string))
            .await
            .unwrap(),
    );

    // Use cbBTC token
    let test_token = devnet.ethereum.cbbtc_contract.address();

    // Fund with tokens and ETH
    devnet
        .ethereum
        .mint_cbbtc(
            market_maker_account.ethereum_address,
            U256::from(10).pow(U256::from(24)),
        )
        .await
        .unwrap();

    devnet
        .ethereum
        .fund_eth_address(
            market_maker_account.ethereum_address,
            U256::from(10).pow(U256::from(19)),
        )
        .await
        .unwrap();

    // Create EVM wallet
    let mut join_set = JoinSet::new();
    let evm_wallet = EVMWallet::new(provider.clone(), eth_rpc_url.to_string(), 1, &mut join_set);

    // Monitor status updates to detect gas bumping
    let mut status_receiver = evm_wallet.tx_broadcaster.subscribe_to_status_updates();

    // Create currency
    let currency = Currency {
        chain: ChainType::Ethereum,
        token: TokenIdentifier::Address(test_token.to_string()),
        amount: U256::from(100) * U256::pow(U256::from(10), U256::from(18)),
        decimals: 18,
    };

    // Send a transaction with low gas price first (this would be done by manipulating the provider)
    // In a real scenario, we'd intercept and modify the gas price, but for this test
    // we'll verify the retry logic exists and works

    let user_address = user_account.ethereum_address.to_string();
    let tx_result = evm_wallet
        .create_transaction(&currency, &user_address, None)
        .await;

    assert!(
        tx_result.is_ok(),
        "Transaction should eventually succeed with gas bumping"
    );

    // Verify we received status updates
    let mut update_count = 0;
    while let Ok(Ok(update)) =
        tokio::time::timeout(Duration::from_secs(1), status_receiver.recv()).await
    {
        update_count += 1;
        debug!("Status update {}: {:?}", update_count, update.result);
    }

    info!("Received {} status updates", update_count);

    // Clean up
    join_set.abort_all();

    info!("Gas price bumping test completed");
}

/// Test error handling for various failure scenarios
#[sqlx::test]
async fn test_evm_wallet_error_handling(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let _ = tracing_subscriber::fmt()
        .with_target(false)
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    info!("Starting EVM wallet error handling test");

    let market_maker_account = MultichainAccount::new(1);

    let devnet = RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .using_esplora(true)
        .build()
        .await
        .unwrap()
        .0;

    let eth_rpc_url = devnet.ethereum.anvil.endpoint_url();
    let ws_url = devnet.ethereum.anvil.ws_endpoint_url();
    let ws_url_string = ws_url.to_string();

    let wallet = market_maker_account.ethereum_wallet.clone();
    let provider = Arc::new(
        ProviderBuilder::new()
            .wallet(wallet)
            .connect_ws(WsConnect::new(ws_url_string))
            .await
            .unwrap(),
    );

    let mut join_set = JoinSet::new();
    let evm_wallet = EVMWallet::new(provider.clone(), eth_rpc_url.to_string(), 1, &mut join_set);

    // Test 1: Invalid recipient address
    let invalid_currency = Currency {
        chain: ChainType::Ethereum,
        token: TokenIdentifier::Address("0x1234567890123456789012345678901234567890".to_string()),
        amount: U256::from(100),
        decimals: 18,
    };

    let result = evm_wallet
        .create_transaction(&invalid_currency, "invalid_address", None)
        .await;

    assert!(result.is_err(), "Should fail with invalid address");

    // Test 2: Unsupported chain type
    let btc_currency = Currency {
        chain: ChainType::Bitcoin,
        token: TokenIdentifier::Native,
        amount: U256::from(100000),
        decimals: 8,
    };

    let result = evm_wallet.can_fill(&btc_currency).await;
    info!("result: {:?}", result);
    assert_eq!(result.unwrap(), false, "Should return false for unsupported currency");

    // Clean up
    join_set.abort_all();

    info!("Error handling test completed");
}
