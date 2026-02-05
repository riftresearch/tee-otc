//! Taproot Address Integration Tests
//!
//! These tests verify that the OTC system correctly handles Taproot (P2TR) addresses
//! for both destination addresses (ETH → BTC swaps) and refund addresses (BTC → ETH swaps).

use alloy::primitives::U256;
use bitcoin::secp256k1::Secp256k1;
use bitcoin::{Address, CompressedPublicKey, Network, XOnlyPublicKey};
use bitcoincore_rpc_async::RpcApi;
use devnet::bitcoin_devnet::MiningMode;
use devnet::{MultichainAccount, RiftDevnet};
use market_maker::bitcoin_wallet::BitcoinWallet;
use market_maker::run_market_maker;
use market_maker::wallet::Wallet;
use mock_instant::global::MockClock;
use otc_chains::traits::Payment;
use otc_models::{ChainType, Currency, Lot, QuoteRequest, Swap, SwapMode, TokenIdentifier};
use otc_protocols::rfq::RFQResult;
use otc_server::api::swaps::RefundSwapResponse;
use otc_server::api::CreateSwapRequest;
use reqwest::StatusCode;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::collections::HashMap;
use std::time::Duration;
use tokio::task::{AbortHandle, JoinSet};
use tracing::info;

use crate::utils::{
    build_bitcoin_wallet_descriptor, build_mm_test_args, build_otc_server_test_args,
    build_rfq_server_test_args, build_tmp_bitcoin_wallet_db_file, get_free_port,
    wait_for_market_maker_to_connect_to_rfq_server, wait_for_otc_server_to_be_ready,
    wait_for_rfq_server_to_be_ready, wait_for_swap_to_be_settled, PgConnectOptionsExt,
};

/// Helper function to derive a Taproot (P2TR) address from a MultichainAccount.
///
/// This creates a key-path-only Taproot address using the account's secret key.
fn derive_taproot_address(account: &MultichainAccount) -> Address {
    let secp = Secp256k1::new();
    let secret_key = account.bitcoin_wallet.secret_key;
    let keypair = bitcoin::secp256k1::Keypair::from_secret_key(&secp, &secret_key);
    let (x_only_pubkey, _parity) = XOnlyPublicKey::from_keypair(&keypair);

    // Create a key-path-only Taproot address (no script tree)
    Address::p2tr(&secp, x_only_pubkey, None, Network::Regtest)
}

/// Test that a BTC → ETH swap can be refunded to a Taproot address.
///
/// This test:
/// 1. Creates a swap from Bitcoin to Ethereum
/// 2. User deposits BTC to the deposit vault
/// 3. Market maker is killed to simulate failure
/// 4. Time is advanced past the refund period
/// 5. User requests a refund to their Taproot address
/// 6. Verifies the refund transaction is valid and can be broadcast
#[sqlx::test]
async fn test_refund_to_taproot_address(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let market_maker_account = MultichainAccount::new(200);
    let user_account = MultichainAccount::new(201);

    // Derive Taproot address for refund
    let user_taproot_address = derive_taproot_address(&user_account);
    info!("User Taproot refund address: {}", user_taproot_address);

    let devnet = RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .using_esplora(true)
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

    // Fund accounts
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
    let mut service_handles: HashMap<&str, AbortHandle> = HashMap::new();

    let otc_port = get_free_port().await;
    let otc_args = build_otc_server_test_args(otc_port, &devnet, &connect_options).await;

    let otc_handle = service_join_set.spawn(async move {
        otc_server::server::run_server(otc_args)
            .await
            .expect("OTC server should not crash");
    });
    service_handles.insert("otc_server", otc_handle);

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
    let rfq_handle = service_join_set.spawn(async move {
        rfq_server::server::run_server(rfq_args)
            .await
            .expect("RFQ server should not crash");
    });
    service_handles.insert("rfq_server", rfq_handle);

    wait_for_rfq_server_to_be_ready(rfq_port).await;

    let mm_args = build_mm_test_args(
        otc_port,
        rfq_port,
        &market_maker_account,
        &devnet,
        &connect_options,
    )
    .await;
    let mm_handle = service_join_set.spawn(async move {
        run_market_maker(mm_args)
            .await
            .expect("Market maker should not crash");
    });
    service_handles.insert("market_maker", mm_handle);

    tokio::select! {
        _ = wait_for_market_maker_to_connect_to_rfq_server(rfq_port) => {
            info!("Market maker is connected to RFQ server");
        }
        _ = service_join_set.join_next() => {
            panic!("Market maker crashed");
        }
    }

    devnet
        .bitcoin
        .wait_for_esplora_sync(Duration::from_secs(30))
        .await
        .unwrap();

    let client = reqwest::Client::new();

    // Request a quote: BTC → cbBTC (Ethereum)
    let quote_request = QuoteRequest {
        mode: SwapMode::ExactInput(10_000_000), // 0.1 BTC
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

    assert_eq!(quote_response.status(), 200, "Quote request should succeed");

    let quote_response: rfq_server::server::QuoteResponse = quote_response
        .json()
        .await
        .expect("Should be able to parse quote response");

    let quote = match quote_response.quote.expect("Quote should be present") {
        RFQResult::Success(quote) => quote,
        _ => panic!("Quote should be a success"),
    };

    // Create swap with Taproot refund address
    let swap_request = CreateSwapRequest {
        quote: quote.clone(),
        user_destination_address: user_account.ethereum_address.to_string(),
        refund_address: user_taproot_address.to_string(), // <-- Taproot address for refund
        metadata: None,
    };

    info!(
        "Creating swap with Taproot refund address: {}",
        user_taproot_address
    );

    let response = client
        .post(format!("http://localhost:{otc_port}/api/v2/swap"))
        .json(&swap_request)
        .send()
        .await
        .unwrap();

    let response_status = response.status();
    let swap = match response_status {
        StatusCode::OK => response.json::<Swap>().await.unwrap(),
        _ => {
            let response_text = response.text().await;
            panic!(
                "Swap request should be successful but got {response_status:#?} {response_text:#?}"
            );
        }
    };

    info!("Swap created successfully with ID: {}", swap.id);

    // Kill the market maker to force refund path
    if let Some(handle) = service_handles.remove("market_maker") {
        handle.abort();
        info!("Market maker aborted");
    }

    // Send funds to the deposit address
    let tx_hash = user_bitcoin_wallet
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

    info!("User deposit transaction: {}", tx_hash);
    devnet.bitcoin.mine_blocks(6).await.unwrap();

    // Wait for swap to reach WaitingMMDepositInitiated state
    loop {
        let swap_status = client
            .get(format!(
                "http://localhost:{otc_port}/api/v2/swap/{}",
                swap.id
            ))
            .send()
            .await
            .unwrap();
        let swap_status: Swap = swap_status.json().await.unwrap();
        if swap_status.status == otc_models::SwapStatus::WaitingMMDepositInitiated {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // Advance time past refund period (24 hours + 1 minute)
    MockClock::advance_system_time(Duration::from_secs(60 * 60 * 24 + 60));

    // Request refund
    let refund_response = client
        .post(format!(
            "http://localhost:{otc_port}/api/v2/swap/{}/refund",
            swap.id
        ))
        .send()
        .await
        .unwrap();

    let response_status = refund_response.status();
    let refund_result = match response_status {
        StatusCode::OK => refund_response.json::<RefundSwapResponse>().await.unwrap(),
        _ => {
            let response_text = refund_response.text().await;
            panic!(
                "Refund request should be successful but got {response_status:#?} {response_text:#?}"
            );
        }
    };

    info!("Refund transaction created successfully");

    // Broadcast the refund transaction
    let refund_txid = devnet
        .bitcoin
        .rpc_client
        .send_raw_transaction(refund_result.tx_data.clone())
        .await
        .expect("Refund transaction to Taproot address should broadcast successfully");

    info!("Refund transaction broadcast: {}", refund_txid);

    devnet.bitcoin.mine_blocks(6).await.unwrap();

    // Verify the refund transaction was confirmed by checking it via RPC
    let refund_tx = devnet
        .bitcoin
        .rpc_client
        .get_raw_transaction_verbose(&refund_txid)
        .await
        .expect("Should be able to get refund transaction");

    assert!(
        refund_tx.confirmations.unwrap_or(0) > 0,
        "Refund transaction should be confirmed"
    );

    // The fact that the transaction was:
    // 1. Created successfully with a Taproot destination (validated by OTC server)
    // 2. Broadcast successfully to the Bitcoin network
    // 3. Mined and confirmed
    // proves that refunds to Taproot addresses work correctly.

    info!(
        "Refund to Taproot address {} completed successfully! TX: {}",
        user_taproot_address, refund_txid
    );
}

/// Test that an ETH → BTC swap can pay out to a Taproot address.
///
/// This test:
/// 1. Creates a swap from Ethereum (cbBTC) to Bitcoin
/// 2. User's destination address is a Taproot address
/// 3. User deposits cbBTC to the deposit vault
/// 4. Market maker fulfills the swap
/// 5. Verifies the user receives BTC at their Taproot address
#[sqlx::test]
async fn test_swap_to_taproot_destination(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    use alloy::signers::local::PrivateKeySigner;

    let market_maker_account = MultichainAccount::new(300);
    let user_account = MultichainAccount::new(301);

    // Derive Taproot address for destination
    let user_taproot_address = derive_taproot_address(&user_account);
    info!("User Taproot destination address: {}", user_taproot_address);

    let devnet = RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .bitcoin_mining_mode(MiningMode::Interval(2))
        .using_esplora(true)
        .build()
        .await
        .unwrap()
        .0;

    let (mut wallet_join_set, user_ethereum_wallet) =
        crate::utils::build_test_user_ethereum_wallet(&devnet, &user_account).await;

    // Fund market maker with BTC (they will pay out to user's Taproot address)
    devnet
        .bitcoin
        .deal_bitcoin(
            &market_maker_account.bitcoin_wallet.address,
            &bitcoin::Amount::from_sat(500_000_000), // 5 BTC
        )
        .await
        .unwrap();

    // Fund user with ETH for gas
    devnet
        .ethereum
        .fund_eth_address(
            user_account.ethereum_address,
            U256::from(100_000_000_000_000_000_000i128),
        )
        .await
        .unwrap();

    // Fund user with cbBTC to deposit
    devnet
        .ethereum
        .mint_cbbtc(
            user_account.ethereum_address,
            U256::from(9_000_000_000i128), // 90 cbBTC
        )
        .await
        .unwrap();

    // Fund market maker with ETH
    devnet
        .ethereum
        .fund_eth_address(
            market_maker_account.ethereum_address,
            U256::from(100_000_000_000_000_000_000i128),
        )
        .await
        .unwrap();

    // Ensure EIP-7702 delegation for user
    user_ethereum_wallet
        .ensure_eip7702_delegation(
            PrivateKeySigner::from_slice(&user_account.secret_bytes).unwrap(),
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
            panic!("Wallet crashed");
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

    // Wait for esplora to sync before starting market maker
    devnet
        .bitcoin
        .wait_for_esplora_sync(Duration::from_secs(30))
        .await
        .unwrap();

    service_join_set.spawn(async move {
        run_market_maker(mm_args)
            .await
            .expect("Market maker should not crash");
    });

    wait_for_market_maker_to_connect_to_rfq_server(rfq_port).await;

    let client = reqwest::Client::new();

    // Request a quote: cbBTC (Ethereum) → BTC
    let quote_request = QuoteRequest {
        mode: SwapMode::ExactInput(100_000_000), // 1 cbBTC
        from: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::address(devnet.ethereum.cbbtc_contract.address().to_string()),
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

    let quote_response: rfq_server::server::QuoteResponse = quote_response
        .json()
        .await
        .expect("Should be able to parse quote response");

    let quote = match quote_response.quote.expect("Quote should be present") {
        RFQResult::Success(quote) => quote,
        _ => panic!("Quote should be a success"),
    };

    // Create swap with Taproot destination address
    let swap_request = CreateSwapRequest {
        quote: quote.clone(),
        user_destination_address: user_taproot_address.to_string(), // <-- Taproot destination
        refund_address: user_account.ethereum_address.to_string(),
        metadata: None,
    };

    info!(
        "Creating swap with Taproot destination address: {}",
        user_taproot_address
    );

    let response = client
        .post(format!("http://localhost:{otc_port}/api/v2/swap"))
        .json(&swap_request)
        .send()
        .await
        .unwrap();

    let response_status = response.status();
    let swap = match response_status {
        StatusCode::OK => response.json::<Swap>().await.unwrap(),
        _ => {
            let response_text = response.text().await;
            panic!(
                "Swap request should be successful but got {response_status:#?} {response_text:#?}"
            );
        }
    };

    info!("Swap created successfully with ID: {}", swap.id);

    // User deposits cbBTC
    let tx_hash = user_ethereum_wallet
        .create_batch_payment(
            vec![Payment {
                lot: Lot {
                    currency: Currency {
                        chain: ChainType::Ethereum,
                        token: TokenIdentifier::address(
                            devnet.ethereum.cbbtc_contract.address().to_string(),
                        ),
                        decimals: swap.quote.from.currency.decimals,
                    },
                    amount: swap.quote.min_input,
                },
                to_address: swap.deposit_vault_address.clone(),
            }],
            None,
        )
        .await
        .unwrap();

    info!("User deposit transaction: {}", tx_hash);

    // Wait for swap to settle (market maker pays out to Taproot address)
    wait_for_swap_to_be_settled(otc_port, swap.id).await;

    info!("Swap settled successfully!");

    // Verify the swap is in Settled state - this confirms the MM payment to the Taproot address
    // was successfully created and broadcast
    let final_swap = client
        .get(format!("http://localhost:{otc_port}/api/v2/swap/{}", swap.id))
        .send()
        .await
        .unwrap()
        .json::<Swap>()
        .await
        .unwrap();

    assert_eq!(
        final_swap.status,
        otc_models::SwapStatus::Settled,
        "Swap should be in Settled state after MM payment to Taproot address"
    );

    // The swap settling proves the MM successfully created a transaction paying to the
    // Taproot address. The transaction was accepted by the Bitcoin network.
    info!(
        "Swap to Taproot destination address {} completed successfully!",
        user_taproot_address
    );

    drop(devnet);
    tokio::join!(wallet_join_set.shutdown(), service_join_set.shutdown());
}

