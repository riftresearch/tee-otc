use alloy::primitives::{Address as AlloyAddress, TxHash, U256};
use alloy::providers::ext::AnvilApi;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use bitcoin::consensus::encode::serialize_hex;
use bitcoin::{Address as BitcoinAddress, Amount, Transaction};
use bitcoincore_rpc_async::RpcApi;
use devnet::bitcoin_devnet::MiningMode;
use devnet::{MultichainAccount, RiftDevnet};
use market_maker::db::{BroadcastedTransactionRepository, Database};
use market_maker::{
    bitcoin_wallet::BitcoinWallet, evm_wallet::EVMWallet, run_market_maker, wallet::Wallet,
};
use otc_chains::traits::Payment;
use otc_models::{ChainType, Currency, Lot, QuoteRequest, TokenIdentifier};
use otc_protocols::rfq::RFQResult;
use otc_server::{
    api::{CreateSwapRequest, CreateSwapResponse},
    server::run_server,
};
use reqwest::StatusCode;
use sqlx::PgPool;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tracing::{info, warn};
use uuid::Uuid;

use crate::utils::{
    build_bitcoin_wallet_descriptor, build_mm_test_args, build_otc_server_test_args,
    build_rfq_server_test_args, build_test_user_ethereum_wallet, build_tmp_bitcoin_wallet_db_file,
    create_test_database, get_free_port, wait_for_market_maker_to_connect_to_rfq_server,
    wait_for_otc_server_to_be_ready, wait_for_rfq_server_to_be_ready, wait_for_swap_status,
    wait_for_swap_to_be_settled, PgConnectOptionsExt,
};

/// Test that a replaced user deposit transaction (RBF) is properly detected and tracked
#[sqlx::test]
async fn test_user_deposit_replacement_bitcoin_to_ethereum(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let market_maker_account = MultichainAccount::new(1);
    let user_account_1 = MultichainAccount::new(2);
    let user_account_2 = MultichainAccount::new(3);

    let devnet = RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .using_esplora(true)
        .bitcoin_mining_mode(MiningMode::Manual) // Auto-mine like simple_swap_test
        .build()
        .await
        .unwrap()
        .0;

    let mut wallet_join_set = JoinSet::new();

    let tx_repo_db_url = create_test_database(&connect_options).await.unwrap();
    let database = Arc::new(Database::connect(&tx_repo_db_url, 5, 1).await.unwrap());

    let broadcasted_transaction_repository = Arc::new(database.broadcasted_transactions());

    // Create first user Bitcoin wallet for the first transaction
    let user_bitcoin_wallet_1 = BitcoinWallet::new(
        &build_tmp_bitcoin_wallet_db_file(),
        &build_bitcoin_wallet_descriptor(&user_account_1.bitcoin_wallet.private_key),
        bitcoin::Network::Regtest,
        &devnet.bitcoin.esplora_url.as_ref().unwrap().to_string(),
        None,
        Some(broadcasted_transaction_repository.clone()),
        100,
        &mut wallet_join_set,
    )
    .await
    .unwrap();

    // Create second user Bitcoin wallet for the second transaction
    let user_bitcoin_wallet_2 = BitcoinWallet::new(
        &build_tmp_bitcoin_wallet_db_file(),
        &build_bitcoin_wallet_descriptor(&user_account_2.bitcoin_wallet.private_key),
        bitcoin::Network::Regtest,
        &devnet.bitcoin.esplora_url.as_ref().unwrap().to_string(),
        None,
        Some(broadcasted_transaction_repository),
        100,
        &mut wallet_join_set,
    )
    .await
    .unwrap();

    // Fund both user accounts
    devnet
        .bitcoin
        .deal_bitcoin(
            &user_account_1.bitcoin_wallet.address,
            &bitcoin::Amount::from_sat(500_000_000), // 5 BTC
        )
        .await
        .unwrap();

    devnet
        .bitcoin
        .deal_bitcoin(
            &user_account_2.bitcoin_wallet.address,
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
            U256::from(9_000_000_000i128), // 90 cbbtc
        )
        .await
        .unwrap();

    let mut service_join_set = JoinSet::new();

    // Start OTC server
    let otc_port = get_free_port().await;
    let otc_args = build_otc_server_test_args(otc_port, &devnet, &connect_options).await;

    service_join_set.spawn(async move {
        run_server(otc_args)
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

    // Start RFQ server
    let rfq_port = get_free_port().await;
    let rfq_args = build_rfq_server_test_args(rfq_port);
    service_join_set.spawn(async move {
        rfq_server::server::run_server(rfq_args)
            .await
            .expect("RFQ server should not crash");
    });

    wait_for_rfq_server_to_be_ready(rfq_port).await;

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

    devnet
        .bitcoin
        .wait_for_esplora_sync(Duration::from_secs(30))
        .await
        .unwrap();

    let client = reqwest::Client::new();

    // Request a quote
    let quote_request = QuoteRequest {
        input_hint: Some(U256::from(10_000_000)), // 0.1 BTC
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
        .post(format!("http://localhost:{rfq_port}/api/v1/quotes/request"))
        .json(&quote_request)
        .send()
        .await
        .unwrap();

    assert_eq!(quote_response.status(), 200);

    let quote_response: rfq_server::server::QuoteResponse = quote_response.json().await.unwrap();
    let quote = match quote_response.quote.unwrap() {
        RFQResult::Success(quote) => quote,
        _ => panic!("Quote should be a success"),
    };

    // Create swap (using user_account_1 for destination)
    let swap_request = CreateSwapRequest {
        quote: quote.clone(),
        user_destination_address: user_account_1.ethereum_address.to_string(),
        user_evm_account_address: user_account_1.ethereum_address,
        metadata: None,
    };

    let swap_response = client
        .post(format!("http://localhost:{otc_port}/api/v2/swap"))
        .json(&swap_request)
        .send()
        .await
        .unwrap();

    assert_eq!(swap_response.status(), StatusCode::OK);
    let CreateSwapResponse {
        swap_id,
        deposit_address,
        min_input,
        decimals,
        ..
    } = swap_response.json().await.unwrap();

    info!(
        "Created swap {} with deposit address {}",
        swap_id, deposit_address
    );

    // Create FIRST user deposit transaction using wallet 1
    let first_tx_hash = user_bitcoin_wallet_1
        .create_batch_payment(
            vec![Payment {
                lot: Lot {
                    currency: Currency {
                        chain: ChainType::Bitcoin,
                        token: TokenIdentifier::Native,
                        decimals,
                    },
                    amount: min_input,
                },
                to_address: deposit_address.clone(),
            }],
            None,
        )
        .await
        .unwrap();

    info!(
        "First user deposit tx broadcast from wallet 1: {}",
        first_tx_hash
    );

    // Don't mine the transaction yet - let monitoring detect it in mempool
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify swap detected the deposit
    wait_for_swap_status(&client, otc_port, swap_id, "WaitingUserDepositConfirmed").await;
    info!("Swap detected first deposit transaction");

    // Get swap details to verify the tx_hash
    let swap_response = client
        .get(format!("http://localhost:{otc_port}/api/v2/swap/{swap_id}"))
        .send()
        .await
        .unwrap();
    let swap: otc_server::api::SwapResponse = swap_response.json().await.unwrap();
    assert_eq!(
        swap.user_deposit.deposit_tx.as_ref().unwrap(),
        &first_tx_hash,
        "Swap should be tracking the first transaction"
    );

    // Now create a SECOND transaction from a different wallet to the same address
    info!("Creating second transaction from wallet 2...");

    let second_tx_hash = user_bitcoin_wallet_2
        .create_batch_payment(
            vec![Payment {
                lot: Lot {
                    currency: Currency {
                        chain: ChainType::Bitcoin,
                        token: TokenIdentifier::Native,
                        decimals,
                    },
                    amount: min_input,
                },
                to_address: deposit_address.clone(),
            }],
            None,
        )
        .await
        .unwrap();

    info!(
        "Second user deposit tx broadcast from wallet 2: {}",
        second_tx_hash
    );
    // explicitly cancel the first transaction
    user_bitcoin_wallet_1
        .cancel_tx(&first_tx_hash)
        .await
        .unwrap();

    // Mine the second transaction to confirm it
    devnet.bitcoin.mine_blocks(6).await.unwrap();

    // Wait for esplora to sync
    tokio::time::sleep(Duration::from_secs(3)).await;

    // The monitoring service should detect that we now have two transactions to the same address
    // and should use the confirmed one (second_tx_hash) as the actual deposit
    // We need to wait for the monitoring cycle to run
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Get swap details again to verify it now tracks the SECOND transaction
    let swap_response = client
        .get(format!("http://localhost:{otc_port}/api/v2/swap/{swap_id}"))
        .send()
        .await
        .unwrap();
    let swap: otc_server::api::SwapResponse = swap_response.json().await.unwrap();

    info!("Swap user deposit: {:?}", swap.user_deposit);

    // The swap should now be tracking the second transaction (which is confirmed)
    assert_eq!(
        swap.user_deposit.deposit_tx.as_ref().unwrap(),
        &second_tx_hash,
        "Swap should now be tracking the second (confirmed) transaction"
    );

    info!("✅ Second transaction successfully detected and tracked!");

    // Now wait for the swap to fully settle (like simple_swap_test does)
    wait_for_swap_to_be_settled(otc_port, swap_id).await;

    info!("✅ Swap fully settled with second transaction!");

    drop(devnet);
    tokio::join!(wallet_join_set.shutdown(), service_join_set.shutdown());
}
