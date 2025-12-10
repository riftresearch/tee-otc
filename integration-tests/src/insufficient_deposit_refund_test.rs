use alloy::signers::k256::ecdsa::SigningKey;
use alloy::signers::local::PrivateKeySigner;
use alloy::{network::TransactionBuilder, primitives::U256, providers::{Provider, ProviderBuilder, WsConnect}};
use bitcoincore_rpc_async::RpcApi;
use devnet::{MultichainAccount, RiftDevnet};
use market_maker::{bitcoin_wallet::BitcoinWallet, run_market_maker, wallet::Wallet};
use otc_chains::traits::Payment;
use otc_models::{ChainType, Currency, Lot, QuoteRequest, TokenIdentifier};
use otc_protocols::rfq::RFQResult;
use otc_server::api::{
    swaps::{RefundPayload, RefundSwapRequest, RefundSwapResponse},
    CreateSwapRequest, CreateSwapResponse, SwapResponse,
};
use reqwest::StatusCode;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::{AbortHandle, JoinSet};
use tracing::info;

use crate::utils::{
    build_bitcoin_wallet_descriptor, build_mm_test_args, build_otc_server_test_args,
    build_rfq_server_test_args, build_tmp_bitcoin_wallet_db_file, get_free_port,
    wait_for_market_maker_to_connect_to_rfq_server, wait_for_otc_server_to_be_ready,
    wait_for_rfq_server_to_be_ready, PgConnectOptionsExt, RefundRequestSignature,
};

/// Test that a user can immediately refund when they send insufficient Bitcoin deposit
#[sqlx::test]
async fn test_insufficient_bitcoin_deposit_refund(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let market_maker_account = MultichainAccount::new(100);
    let user_account = MultichainAccount::new(101);

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
            U256::from(100_000_000_000_000_000_000i128), // 100 ETH
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

    // Start OTC server
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

    // Start RFQ server
    let rfq_port = get_free_port().await;
    let rfq_args = build_rfq_server_test_args(rfq_port);
    let rfq_handle = service_join_set.spawn(async move {
        rfq_server::server::run_server(rfq_args)
            .await
            .expect("RFQ server should not crash");
    });
    service_handles.insert("rfq_server", rfq_handle);

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
    let mm_handle = service_join_set.spawn(async move {
        run_market_maker(mm_args)
            .await
            .expect("Market maker should not crash");
    });
    service_handles.insert("market_maker", mm_handle);

    tokio::select! {
        _ = wait_for_market_maker_to_connect_to_rfq_server(rfq_port) => {
            info!("Market maker connected to RFQ server");
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

    let quote_response: rfq_server::server::QuoteResponse =
        quote_response.json().await.unwrap();

    let quote = match quote_response.quote.as_ref().unwrap() {
        RFQResult::Success(quote) => quote.clone(),
        _ => panic!("Quote should be a success"),
    };

    // Create a swap
    let swap_request = CreateSwapRequest {
        quote: quote.clone(),
        user_destination_address: user_account.ethereum_address.to_string(),
        user_evm_account_address: user_account.ethereum_address,
        metadata: None,
    };

    let response = client
        .post(format!("http://localhost:{otc_port}/api/v2/swap"))
        .json(&swap_request)
        .send()
        .await
        .unwrap();

    let response_json = match response.status() {
        StatusCode::OK => response.json::<CreateSwapResponse>().await.unwrap(),
        status => {
            let text = response.text().await;
            panic!("Swap request failed: {status:#?} {text:#?}");
        }
    };

    // Send INSUFFICIENT funds (expected_amount - 1 satoshi)
    let insufficient_amount = response_json.min_input - U256::from(1);
    
    info!(
        "Sending insufficient amount: {} (expected: {})",
        insufficient_amount, response_json.min_input
    );

    let tx_hash = user_bitcoin_wallet
        .create_batch_payment(
            vec![Payment {
                lot: Lot {
                    currency: Currency {
                        chain: ChainType::Bitcoin,
                        token: TokenIdentifier::Native,
                        decimals: response_json.decimals,
                    },
                    amount: insufficient_amount,
                },
                to_address: response_json.deposit_address.clone(),
            }],
            None,
        )
        .await
        .unwrap();

    info!("Broadcasted insufficient deposit tx: {}", tx_hash);

    // Mine blocks to confirm the transaction
    devnet.bitcoin.mine_blocks(6).await.unwrap();
    info!("Mined blocks to confirm transaction");

    // Wait for swap to be detected (it should stay in WaitingUserDepositInitiated)
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Check swap status
    let swap = client
        .get(format!(
            "http://localhost:{otc_port}/api/v2/swap/{}",
            response_json.swap_id
        ))
        .send()
        .await
        .unwrap()
        .json::<SwapResponse>()
        .await
        .unwrap();

    info!("Swap status after insufficient deposit: {}", swap.status);
    assert_eq!(
        swap.status, "WaitingUserDepositInitiated",
        "Swap should remain in WaitingUserDepositInitiated due to insufficient deposit"
    );

    // Now attempt immediate refund (no time advancement needed for early refund)
    let refund_payload = RefundPayload {
        swap_id: response_json.swap_id,
        refund_recipient: user_account.bitcoin_wallet.address.to_string(),
        refund_transaction_fee: U256::from(2000),
    };

    let signer = PrivateKeySigner::from_signing_key(
        SigningKey::from_bytes(&user_account.secret_bytes.into()).unwrap(),
    );
    let signature = refund_payload.sign(&signer).to_vec();

    let refund_request = RefundSwapRequest {
        payload: refund_payload,
        signature,
    };

    let refund_response = client
        .post(format!("http://localhost:{otc_port}/api/v1/refund"))
        .json(&refund_request)
        .send()
        .await
        .unwrap();

    let refund_status = refund_response.status();
    let refund_result = match refund_status {
        StatusCode::OK => refund_response.json::<RefundSwapResponse>().await.unwrap(),
        _ => {
            let text = refund_response.text().await;
            panic!("Refund should succeed but got {refund_status:#?} {text:#?}");
        }
    };

    info!("Refund reason: {:?}", refund_result.reason);

    // Get user balance before broadcast
    let bal_before = devnet
        .bitcoin
        .esplora_client
        .as_ref()
        .unwrap()
        .get_address_utxo(&user_account.bitcoin_wallet.address)
        .await
        .unwrap()
        .iter()
        .map(|utxo| utxo.value)
        .sum::<u64>();

    info!("User balance before refund broadcast: {}", bal_before);

    // Broadcast refund transaction
    devnet
        .bitcoin
        .rpc_client
        .send_raw_transaction(refund_result.tx_data)
        .await
        .unwrap();

    devnet.bitcoin.mine_blocks(6).await.unwrap();

    devnet
        .bitcoin
        .wait_for_esplora_sync(Duration::from_secs(30))
        .await
        .unwrap();

    // Check user balance after refund
    let bal_after = devnet
        .bitcoin
        .esplora_client
        .as_ref()
        .unwrap()
        .get_address_utxo(&user_account.bitcoin_wallet.address)
        .await
        .unwrap()
        .iter()
        .map(|utxo| utxo.value)
        .sum::<u64>();

    info!("User balance after refund broadcast: {}", bal_after);
    assert!(bal_after > bal_before, "User should receive refund");

    // Verify swap is now in RefundingUser status
    let final_swap = client
        .get(format!(
            "http://localhost:{otc_port}/api/v2/swap/{}",
            response_json.swap_id
        ))
        .send()
        .await
        .unwrap()
        .json::<SwapResponse>()
        .await
        .unwrap();

    assert_eq!(
        final_swap.status, "RefundingUser",
        "Swap should be in RefundingUser status"
    );

    info!("Test completed successfully!");

    // Cleanup
    for (_, handle) in service_handles {
        handle.abort();
    }
    wallet_join_set.abort_all();
}

/// Test that a user can immediately refund when they send insufficient EVM token deposit
#[sqlx::test]
async fn test_insufficient_evm_deposit_refund(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let market_maker_account = MultichainAccount::new(110);
    let user_account = MultichainAccount::new(111);

    let devnet = RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .using_esplora(true)
        .build()
        .await
        .unwrap()
        .0;

    // Fund user with ETH for gas and cbBTC for deposit
    devnet
        .ethereum
        .fund_eth_address(
            user_account.ethereum_address,
            U256::from(10).pow(U256::from(19)), // 10 ETH
        )
        .await
        .unwrap();

    devnet
        .ethereum
        .fund_eth_address(
            market_maker_account.ethereum_address,
            U256::from(10).pow(U256::from(19)), // 10 ETH
        )
        .await
        .unwrap();

    let one_cbbtc = U256::from(1_000_000);
    devnet
        .ethereum
        .mint_cbbtc(user_account.ethereum_address, one_cbbtc)
        .await
        .unwrap();

    // Give MM some BTC for balance checks
    devnet
        .bitcoin
        .deal_bitcoin(
            &market_maker_account.bitcoin_wallet.address,
            &bitcoin::Amount::from_sat(500_000_000), // 5 BTC
        )
        .await
        .unwrap();

    devnet
        .bitcoin
        .wait_for_esplora_sync(Duration::from_secs(30))
        .await
        .unwrap();

    let mut service_join_set = JoinSet::new();
    let mut service_handles: HashMap<&str, AbortHandle> = HashMap::new();

    // Start OTC server
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
    }

    // Start RFQ server
    let rfq_port = get_free_port().await;
    let rfq_args = build_rfq_server_test_args(rfq_port);
    let rfq_handle = service_join_set.spawn(async move {
        rfq_server::server::run_server(rfq_args)
            .await
            .expect("RFQ server should not crash");
    });
    service_handles.insert("rfq_server", rfq_handle);

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
    let mm_handle = service_join_set.spawn(async move {
        run_market_maker(mm_args)
            .await
            .expect("Market maker should not crash");
    });
    service_handles.insert("market_maker", mm_handle);

    wait_for_market_maker_to_connect_to_rfq_server(rfq_port).await;

    let client = reqwest::Client::new();

    // Request a quote where user deposits cbBTC on Ethereum
    let quote_request = QuoteRequest {
        input_hint: Some(one_cbbtc),
        from: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::Address(devnet.ethereum.cbbtc_contract.address().to_string()),
            decimals: 8,
        },
        to: Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
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

    let quote_response: rfq_server::server::QuoteResponse =
        quote_response.json().await.unwrap();

    let quote = match quote_response.quote.expect("Quote should be present") {
        RFQResult::Success(q) => q,
        other => panic!("Quote should be success, got: {other:?}"),
    };

    // Create the swap
    let swap_request = CreateSwapRequest {
        quote,
        user_destination_address: user_account.bitcoin_wallet.address.to_string(),
        user_evm_account_address: user_account.ethereum_address,
        metadata: None,
    };

    let response = client
        .post(format!("http://localhost:{otc_port}/api/v2/swap"))
        .json(&swap_request)
        .send()
        .await
        .unwrap();

    let response_json = match response.status() {
        StatusCode::OK => response.json::<CreateSwapResponse>().await.unwrap(),
        status => {
            let text = response.text().await;
            panic!("Swap request failed: {status:#?} {text:#?}");
        }
    };

    // Send INSUFFICIENT funds (expected_amount - 1 wei)
    let insufficient_amount = response_json.min_input - U256::from(1);

    info!(
        "Sending insufficient amount: {} (expected: {})",
        insufficient_amount, response_json.min_input
    );

    // Setup user provider and token contract
    let ws_url = devnet.ethereum.anvil.ws_endpoint_url().to_string();
    let user_provider = Arc::new(
        ProviderBuilder::new()
            .wallet(user_account.ethereum_wallet.clone())
            .connect_ws(WsConnect::new(ws_url))
            .await
            .unwrap(),
    );

    let token_address = *devnet.ethereum.cbbtc_contract.address();
    let user_token_contract =
        eip3009_erc20_contract::GenericEIP3009ERC20::GenericEIP3009ERC20Instance::new(
            token_address,
            user_provider.clone(),
        );

    // Transfer insufficient amount to deposit address
    user_token_contract
        .transfer(
            response_json
                .deposit_address
                .parse()
                .expect("valid deposit address"),
            insufficient_amount,
        )
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();

    info!("Transferred insufficient cbBTC deposit");

    // Wait for detection
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Check swap status
    let swap = client
        .get(format!(
            "http://localhost:{otc_port}/api/v2/swap/{}",
            response_json.swap_id
        ))
        .send()
        .await
        .unwrap()
        .json::<SwapResponse>()
        .await
        .unwrap();

    info!("Swap status after insufficient deposit: {}", swap.status);
    assert_eq!(
        swap.status, "WaitingUserDepositInitiated",
        "Swap should remain in WaitingUserDepositInitiated due to insufficient deposit"
    );

    // Attempt immediate refund (no time advancement needed)
    let signer = PrivateKeySigner::from_signing_key(
        SigningKey::from_bytes(&user_account.secret_bytes.into()).unwrap(),
    );
    let refund_payload = RefundPayload {
        swap_id: response_json.swap_id,
        refund_recipient: user_account.ethereum_address.to_string(),
        refund_transaction_fee: U256::from(0), // EVM doesn't need explicit fee
    };
    let signature = refund_payload.sign(&signer).to_vec();

    let refund_request = RefundSwapRequest {
        payload: refund_payload,
        signature,
    };

    let refund_response = client
        .post(format!("http://localhost:{otc_port}/api/v1/refund"))
        .json(&refund_request)
        .send()
        .await
        .unwrap();

    let refund_status = refund_response.status();
    let refund_result = match refund_status {
        StatusCode::OK => refund_response.json::<RefundSwapResponse>().await.unwrap(),
        _ => {
            let text = refund_response.text().await;
            panic!("Refund should succeed but got {refund_status:#?} {text:#?}");
        }
    };

    info!("Refund reason: {:?}", refund_result.reason);

    // Get user balance before refund
    let bal_before = devnet
        .ethereum
        .cbbtc_contract
        .balanceOf(user_account.ethereum_address)
        .call()
        .await
        .unwrap();

    info!("User cbBTC balance before refund: {}", bal_before);

    // Broadcast refund transaction
    use alloy::primitives::Bytes;
    use alloy::rpc::types::TransactionRequest as AlloyTransactionRequest;
    use market_maker::evm_wallet::transaction_broadcaster::EVMTransactionBroadcaster;

    let mut join_set = JoinSet::new();
    let broadcaster = EVMTransactionBroadcaster::new(
        user_provider.clone(),
        devnet.ethereum.anvil.endpoint_url().to_string(),
        1,
        &mut join_set,
    );

    let tx_data_bytes = alloy::hex::decode(refund_result.tx_data).expect("valid hex calldata");
    let tx_request = AlloyTransactionRequest::default()
        .with_to(token_address)
        .with_input::<Bytes>(tx_data_bytes.into());

    let result = broadcaster
        .broadcast_transaction(
            tx_request,
            market_maker::evm_wallet::transaction_broadcaster::PreflightCheck::Simulate,
        )
        .await
        .expect("broadcast result");

    assert!(
        result.is_success(),
        "Refund transaction should succeed: {result:?}"
    );

    // Check user balance after refund
    let bal_after = devnet
        .ethereum
        .cbbtc_contract
        .balanceOf(user_account.ethereum_address)
        .call()
        .await
        .unwrap();

    info!("User cbBTC balance after refund: {}", bal_after);
    assert!(
        bal_after > bal_before,
        "User should receive refund of their insufficient deposit"
    );

    // Verify swap is in RefundingUser status
    let final_swap = client
        .get(format!(
            "http://localhost:{otc_port}/api/v2/swap/{}",
            response_json.swap_id
        ))
        .send()
        .await
        .unwrap()
        .json::<SwapResponse>()
        .await
        .unwrap();

    assert_eq!(
        final_swap.status, "RefundingUser",
        "Swap should be in RefundingUser status"
    );

    info!("Test completed successfully!");

    // Cleanup
    for (_, handle) in service_handles {
        handle.abort();
    }
    join_set.abort_all();
}

