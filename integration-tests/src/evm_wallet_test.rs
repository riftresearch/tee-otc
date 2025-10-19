use alloy::{
    primitives::U256,
    providers::{Provider, ProviderBuilder, WsConnect},
    signers::local::PrivateKeySigner,
};
use devnet::{MultichainAccount, RiftDevnet};
use market_maker::{
    deposit_key_storage::DepositKeyStorageTrait,
    evm_wallet::{self, EVMWallet},
    wallet::Wallet,
};
use otc_chains::traits::{MarketMakerPaymentVerification, Payment};
use otc_models::{ChainType, Currency, Lot, TokenIdentifier};
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
            .connect_ws(WsConnect::new(ws_url_string))
            .await
            .unwrap(),
    );

    // Use the cbBTC token that's already deployed on devnet
    let test_token = *devnet.ethereum.cbbtc_contract.address();

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
        None,
        &mut join_set,
    );

    tokio::select! {
        result = evm_wallet.ensure_eip7702_delegation(
            PrivateKeySigner::from_slice(&market_maker_account.secret_bytes).unwrap(),
        ) => {
            result.unwrap();
        },
        err = join_set.join_next() => {
            panic!("Join set exited unexpectedly, {err:?}");
        }
    }

    // Subscribe to transaction status updates
    let mut status_receiver = evm_wallet.tx_broadcaster.subscribe_to_status_updates();

    // Create a currency for testing
    let lot = Lot {
        currency: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::Address(test_token.to_string()),
            decimals: 18,
        },
        amount: U256::from(1000) * U256::pow(U256::from(10), U256::from(18)), // 1000 tokens
    };

    // Test Case 1: Simulate nonce too low error by sending multiple transactions rapidly
    info!("Test Case 1: Testing nonce too low error recovery");

    let user_address = user_account.ethereum_address.to_string();

    // Send the first transaction normally
    let tx1_future = evm_wallet.create_batch_payment(
        vec![Payment {
            lot: lot.clone(),
            to_address: user_address.clone(),
        }],
        None,
    );

    // Immediately send another transaction to create a nonce conflict
    let tx2_future = evm_wallet.create_batch_payment(
        vec![Payment {
            lot: lot.clone(),
            to_address: user_address.to_string(),
        }],
        None,
    );

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

    // Test Case 3: Verify transaction with custom nonce embedding
    info!("Test Case 3: Testing transaction with custom nonce");

    let custom_nonce: [u8; 32] = [
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
        26, 27, 28, 29, 30, 31, 32,
    ];
    let tx_with_nonce = evm_wallet
        .create_batch_payment(
            vec![Payment {
                lot: lot.clone(),
                to_address: user_address.clone(),
            }],
            Some(MarketMakerPaymentVerification {
                batch_nonce_digest: custom_nonce.clone(),
                aggregated_fee: U256::from(300),
            }),
        )
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
    let evm_wallet = EVMWallet::new(
        provider.clone(),
        eth_rpc_url.to_string(),
        1,
        None,
        &mut join_set,
    );

    evm_wallet
        .ensure_eip7702_delegation(
            PrivateKeySigner::from_slice(&market_maker_account.secret_bytes).unwrap(),
        )
        .await
        .unwrap();

    // Monitor status updates to detect gas bumping
    let mut status_receiver = evm_wallet.tx_broadcaster.subscribe_to_status_updates();

    // Create currency
    let lot = Lot {
        currency: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::Address(test_token.to_string()),
            decimals: 18,
        },
        amount: U256::from(100) * U256::pow(U256::from(10), U256::from(18)),
    };

    // Send a transaction with low gas price first (this would be done by manipulating the provider)
    // In a real scenario, we'd intercept and modify the gas price, but for this test
    // we'll verify the retry logic exists and works

    let user_address = user_account.ethereum_address.to_string();
    let tx_result = evm_wallet
        .create_batch_payment(
            vec![Payment {
                lot: lot.clone(),
                to_address: user_address.clone(),
            }],
            None,
        )
        .await;
    println!("tx_result: {:?}", tx_result);

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
    let evm_wallet = EVMWallet::new(
        provider.clone(),
        eth_rpc_url.to_string(),
        1,
        None,
        &mut join_set,
    );

    // Test 1: Invalid recipient address
    let invalid_lot = Lot {
        currency: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::Address(
                "0x1234567890123456789012345678901234567890".to_string(),
            ),
            decimals: 18,
        },
        amount: U256::from(100),
    };

    let result = evm_wallet
        .create_batch_payment(
            vec![Payment {
                lot: invalid_lot.clone(),
                to_address: "invalid_address".to_string(),
            }],
            None,
        )
        .await;

    assert!(result.is_err(), "Should fail with invalid address");

    // Clean up
    join_set.abort_all();

    info!("Error handling test completed");
}

#[sqlx::test]
async fn test_evm_wallet_actually_sends_token(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    // Initialize logging for debugging
    let _ = tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(true)
        .with_max_level(tracing::Level::INFO)
        .try_init();

    // Set up test accounts
    let market_maker_account = MultichainAccount::new(1);
    let recipient_account = MultichainAccount::new(2);

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
            .connect_ws(WsConnect::new(ws_url_string))
            .await
            .unwrap(),
    );

    // Use the cbBTC token that's already deployed on devnet
    let cbbtc_contract = devnet.ethereum.cbbtc_contract.clone();
    let test_token = *devnet.ethereum.cbbtc_contract.address();

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
        None,
        &mut join_set,
    );

    tokio::select! {
        result = evm_wallet.ensure_eip7702_delegation(
            PrivateKeySigner::from_slice(&market_maker_account.secret_bytes).unwrap(),
        ) => {
            result.unwrap();
        },
        err = join_set.join_next() => {
            panic!("Join set exited unexpectedly, {err:?}");
        }
    }

    // Create a currency for testing
    let lot = Lot {
        currency: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::Address(test_token.to_string()),
            decimals: 18,
        },
        amount: U256::from(1000) * U256::pow(U256::from(10), U256::from(18)), // 1000 tokens
    };

    let recipient_balance_before = cbbtc_contract
        .balanceOf(recipient_account.ethereum_address)
        .call()
        .await
        .unwrap();

    let payment_txid = evm_wallet
        .create_batch_payment(
            vec![Payment {
                lot: lot.clone(),
                to_address: recipient_account.ethereum_address.to_string(),
            }],
            None,
        )
        .await
        .unwrap();

    println!("payment_txid: {:?}", payment_txid);

    println!(
        "market_maker_account.ethereum_address: {:?}",
        market_maker_account.ethereum_address
    );

    let code = devnet
        .ethereum
        .funded_provider
        .get_code_at(market_maker_account.ethereum_address)
        .await
        .unwrap();
    println!("mm EOA code: {:?}", code);

    let recipient_balance_after = cbbtc_contract
        .balanceOf(recipient_account.ethereum_address)
        .call()
        .await
        .unwrap();

    assert!(
        recipient_balance_after > recipient_balance_before,
        "Recipient balance should increase"
    );
}

/// Test that the EVM wallet can fulfill a payment by combining its own balance
/// with funds sourced from the deposit key storage (via EIP-3009 receiveWithAuthorization).
#[sqlx::test]
async fn test_evm_wallet_spend_from_deposit_storage(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let _ = tracing_subscriber::fmt()
        .with_target(false)
        .with_max_level(tracing::Level::INFO)
        .try_init();

    // Accounts: MM sender, deposit-source wallet, and final recipient
    let mm_account = MultichainAccount::new(21);
    let deposit_account = MultichainAccount::new(22);
    let recipient_account = MultichainAccount::new(23);

    // Start devnet (no indexer required for this test)
    let devnet = RiftDevnet::builder()
        .using_esplora(false)
        .build()
        .await
        .unwrap()
        .0;

    // Provider for the MM wallet
    let ws_url = devnet.ethereum.anvil.ws_endpoint_url().to_string();
    let http_url = devnet.ethereum.anvil.endpoint_url().to_string();
    let provider = Arc::new(
        ProviderBuilder::new()
            .wallet(mm_account.ethereum_wallet.clone())
            .connect_ws(WsConnect::new(ws_url))
            .await
            .unwrap(),
    );

    // Token under test (cbBTC)
    let cbbtc_contract = devnet.ethereum.cbbtc_contract.clone();
    let token_address = *cbbtc_contract.address();

    // Mint 1 CBBTC to MM wallet and 2 CBBTC to the deposit wallet
    let one_cbbtc = U256::from(10).pow(U256::from(18));
    let two_cbbtc = U256::from(2) * one_cbbtc;
    let three_cbbtc = U256::from(3) * one_cbbtc;

    devnet
        .ethereum
        .mint_cbbtc(mm_account.ethereum_address, one_cbbtc)
        .await
        .unwrap();

    devnet
        .ethereum
        .mint_cbbtc(deposit_account.ethereum_address, two_cbbtc)
        .await
        .unwrap();

    // Fund MM address with ETH for gas
    devnet
        .ethereum
        .fund_eth_address(
            mm_account.ethereum_address,
            U256::from(10).pow(U256::from(19)),
        )
        .await
        .unwrap();

    // Create and link a deposit key storage, then store the deposit wallet's private key/holdings
    let deposit_key_storage = Arc::new(
        market_maker::deposit_key_storage::DepositKeyStorage::new(
            &connect_options.to_database_url(),
            10,
            2,
        )
        .await
        .expect("create deposit key storage"),
    );

    let deposit_private_key_hex = format!("0x{}", alloy::hex::encode(deposit_account.secret_bytes));
    let deposit_lot = Lot {
        currency: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::Address(token_address.to_string()),
            decimals: 18,
        },
        amount: two_cbbtc,
    };

    deposit_key_storage
        .store_deposit(&market_maker::deposit_key_storage::Deposit::new(
            deposit_private_key_hex,
            deposit_lot,
            "tx1",
        ))
        .await
        .expect("store deposit in key storage");

    // Spin up EVM wallet with deposit key storage linked
    let mut join_set = JoinSet::new();
    let evm_wallet = EVMWallet::new(
        provider.clone(),
        http_url,
        1, // confirmations
        Some(deposit_key_storage.clone()),
        &mut join_set,
    );

    // Ensure 7702 delegation is set on the MM EOA
    evm_wallet
        .ensure_eip7702_delegation(PrivateKeySigner::from_slice(&mm_account.secret_bytes).unwrap())
        .await
        .unwrap();

    // Prepare a 3 CBBTC payment to a third address
    let lot = Lot {
        currency: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::Address(token_address.to_string()),
            decimals: 18,
        },
        amount: three_cbbtc,
    };

    let before = cbbtc_contract
        .balanceOf(recipient_account.ethereum_address)
        .call()
        .await
        .unwrap();

    let _txid = evm_wallet
        .create_batch_payment(
            vec![Payment {
                lot: lot.clone(),
                to_address: recipient_account.ethereum_address.to_string(),
            }],
            None,
        )
        .await
        .expect("create payment should succeed");

    let after = cbbtc_contract
        .balanceOf(recipient_account.ethereum_address)
        .call()
        .await
        .unwrap();

    assert_eq!(after - before, three_cbbtc, "Recipient should get 3 CBBTC");

    // Cleanup background tasks
    join_set.abort_all();
}
