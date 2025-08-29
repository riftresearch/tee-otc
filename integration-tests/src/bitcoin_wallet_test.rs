use alloy::{hex, primitives::U256};
use bitcoin::{Network, PrivateKey};
use bitcoincore_rpc_async::RpcApi;
use devnet::{MultichainAccount, RiftDevnet};
use market_maker::{bitcoin_wallet::BitcoinWallet, wallet::Wallet};
use otc_chains::traits::MarketMakerPaymentValidation;
use otc_models::{ChainType, Currency, Lot, TokenIdentifier};
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::{str::FromStr, time::Duration};
use tokio::task::JoinSet;
use tracing::info;

use crate::utils::PgConnectOptionsExt;

/// Test that verifies the Bitcoin wallet basic functionality
#[sqlx::test]
async fn test_bitcoin_wallet_basic_operations(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    // Initialize logging for debugging
    let _ = tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(true)
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    info!("Starting Bitcoin wallet basic operations test");

    // Set up test accounts
    let market_maker_account = MultichainAccount::new(1);
    let user_account = MultichainAccount::new(2);

    // Get Bitcoin addresses to fund
    let market_maker_btc_address = market_maker_account.bitcoin_wallet.address.to_string();
    let user_btc_address = user_account.bitcoin_wallet.address.to_string();

    // Start the devnet with Esplora enabled
    let devnet = RiftDevnet::builder()
        .using_esplora(true)
        .build()
        .await
        .unwrap()
        .0;

    devnet
        .bitcoin
        .deal_bitcoin(
            &market_maker_account.bitcoin_wallet.address,
            &bitcoin::Amount::from_sat(100_000_000),
        )
        .await
        .unwrap();

    // Get Esplora URL
    let esplora_url = devnet.bitcoin.esplora_url.as_ref().unwrap();
    info!("Using Esplora at: {}", esplora_url);

    // Create a temporary database for the wallet
    let db_path = format!("/tmp/bitcoin_wallet_test_{}.db", std::process::id());

    // Create the Bitcoin wallet with transaction broadcaster
    let mut join_set = JoinSet::new();
    let bitcoin_wallet = BitcoinWallet::new(
        &db_path,
        &market_maker_account.bitcoin_wallet.descriptor(),
        Network::Regtest,
        esplora_url,
        &mut join_set,
    )
    .await
    .unwrap();

    info!("Market maker Bitcoin address: {}", market_maker_btc_address);

    // Test Case 1: Check that wallet is created and can check balance (even if 0)
    info!("Test Case 1: Testing wallet creation and balance checking");

    let small_lot = Lot {
        currency: Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        },
        amount: U256::from(1u64), // 1 satoshi
    };

    // This should return false since wallet has no funds
    let can_fill_small = bitcoin_wallet
        .balance(&small_lot.currency.token)
        .await
        .unwrap();
    info!("Can fill 1 satoshi (unfunded wallet): {}", can_fill_small);
    assert!(
        can_fill_small == U256::from(0),
        "Unfunded wallet should not be able to fill any amount"
    );

    // Test Case 2: Test with unsupported currency
    info!("Test Case 2: Testing unsupported currency handling");

    let eth_lot = Lot {
        currency: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::Native,
            decimals: 18,
        },
        amount: U256::from(100_000u64),
    };

    let can_fill_eth = bitcoin_wallet
        .balance(&eth_lot.currency.token)
        .await
        .unwrap();
    assert!(
        can_fill_eth == U256::from(0),
        "Should return false for Ethereum currency"
    );

    // Test Case 3: Test error handling for invalid address
    info!("Test Case 3: Testing invalid address handling");

    let btc_lot = Lot {
        currency: Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        },
        amount: U256::from(100_000u64),
    };

    let invalid_tx_result = bitcoin_wallet
        .create_payment(&btc_lot, "invalid_bitcoin_address", None)
        .await;

    assert!(
        invalid_tx_result.is_err(),
        "Should fail with invalid address"
    );
    info!("Invalid address error: {:?}", invalid_tx_result.err());

    info!("Test Case 4: Testing that wallet create a transaction");
    println!(
        "current btc block height: {:?}",
        devnet.bitcoin.rpc_client.get_block_count().await.unwrap()
    );

    // Test Case 4: Test that wallet create a transaction

    // ensure the tx we sent to the market maker is detected by esplora
    devnet
        .bitcoin
        .wait_for_esplora_sync(Duration::from_secs(30))
        .await
        .unwrap();

    let tx_result1 = bitcoin_wallet
        .create_payment(&btc_lot, &user_btc_address, None)
        .await;
    let tx_result2 = bitcoin_wallet
        .create_payment(&btc_lot, &user_btc_address, None)
        .await;

    let mm_nonce = hex!("deadbeefdeadbeefdeadbeefdeadbeef");
    let tx_result3 = bitcoin_wallet
        .create_payment(
            &btc_lot,
            &user_btc_address,
            Some(MarketMakerPaymentValidation {
                embedded_nonce: mm_nonce,
                fee_amount: U256::from(300),
            }),
        )
        .await;

    assert!(
        tx_result1.is_ok() || tx_result2.is_ok(),
        "Should create a transaction {tx_result1:?} or {tx_result2:?}"
    );
    let txid1 = tx_result1.unwrap();
    let txid2 = tx_result2.unwrap();
    let txid3 = tx_result3.unwrap();
    info!("Transaction created: {:?}", txid1);
    info!("Transaction created: {:?}", txid2);
    // mine
    devnet.bitcoin.mine_blocks(1).await.unwrap();
    // check that the transaction has been mined
    let tx1 = devnet
        .bitcoin
        .rpc_client
        .get_raw_transaction_verbose(&txid1.parse::<bitcoin::Txid>().unwrap())
        .await
        .unwrap();

    let tx2 = devnet
        .bitcoin
        .rpc_client
        .get_raw_transaction_verbose(&txid2.parse::<bitcoin::Txid>().unwrap())
        .await
        .unwrap();

    let tx3 = devnet
        .bitcoin
        .rpc_client
        .get_raw_transaction_verbose(&txid3.parse::<bitcoin::Txid>().unwrap())
        .await
        .unwrap();

    if tx1.confirmations.unwrap_or(0) != 1 {
        panic!("tx1 should be mined {tx1:#?}");
    } else if tx2.confirmations.unwrap_or(0) != 1 {
        panic!("tx2 should be mined {tx2:#?}");
    } else if tx3.confirmations.unwrap_or(0) != 1 {
        panic!("tx3 should be mined {tx3:#?}");
    }

    if !tx3.hex.contains(&hex::encode(mm_nonce)) {
        panic!("tx3 should contain the mm_nonce {tx3:#?}");
    }

    // Clean up
    join_set.abort_all();
    let _ = std::fs::remove_file(&db_path);

    info!("Bitcoin wallet basic operations test completed successfully");
}

/// Test error handling for various failure scenarios
#[sqlx::test]
async fn test_bitcoin_wallet_error_handling(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let _ = tracing_subscriber::fmt()
        .with_target(false)
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    info!("Starting Bitcoin wallet error handling test");

    let market_maker_account = MultichainAccount::new(1);

    let devnet = RiftDevnet::builder()
        .using_esplora(true)
        .build()
        .await
        .unwrap()
        .0;

    let esplora_url = devnet.bitcoin.esplora_url.as_ref().unwrap();
    let db_path = format!("/tmp/bitcoin_wallet_error_test_{}.db", std::process::id());

    // Create descriptor from the wallet's private key in WIF format
    // Convert the secret key to WIF for use in descriptor
    let private_key = bitcoin::PrivateKey::new(
        market_maker_account.bitcoin_wallet.secret_key,
        Network::Regtest,
    );
    let descriptor = format!("wpkh({})", private_key);

    let mut join_set = JoinSet::new();
    let bitcoin_wallet = BitcoinWallet::new(
        &db_path,
        &descriptor,
        Network::Regtest,
        esplora_url,
        &mut join_set,
    )
    .await
    .unwrap();

    // Test 1: Invalid recipient address
    let lot = Lot {
        currency: Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        },
        amount: U256::from(100_000u64),
    };

    let result = bitcoin_wallet
        .create_payment(&lot, "invalid_btc_address", None)
        .await;

    assert!(result.is_err(), "Should fail with invalid address");

    // Test 2: Unsupported chain type
    let eth_lot = Lot {
        currency: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::Native,
            decimals: 18,
        },
        amount: U256::from(100_000u64),
    };

    let result = bitcoin_wallet.balance(&eth_lot.currency.token).await;
    assert!(
        result.is_err(),
        "Should return error for unsupported currency"
    );

    // Clean up
    join_set.abort_all();
    let _ = std::fs::remove_file(&db_path);

    info!("Error handling test completed");
}
