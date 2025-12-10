use crate::utils::RefundRequestSignature;
use alloy::network::TransactionBuilder;
use alloy::serde::storage::from_bytes_to_b256;
use alloy::signers::k256::ecdsa::SigningKey;
use alloy::signers::k256::Secp256k1;
use alloy::signers::local::PrivateKeySigner;
use mock_instant::global::MockClock;
use otc_chains::traits::Payment;
use std::time::Duration;

use alloy::primitives::{Bytes, U256};
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::rpc::types::TransactionRequest as AlloyTransactionRequest;
use bitcoincore_rpc_async::RpcApi;
use devnet::{MultichainAccount, RiftDevnet};
use market_maker::evm_wallet::transaction_broadcaster::EVMTransactionBroadcaster;
use market_maker::{bitcoin_wallet::BitcoinWallet, run_market_maker, wallet::Wallet};
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
use tokio::task::{AbortHandle, JoinSet};
use tracing::info;

use crate::utils::{
    build_bitcoin_wallet_descriptor, build_mm_test_args, build_otc_server_test_args,
    build_rfq_server_test_args, build_tmp_bitcoin_wallet_db_file, get_free_port,
    wait_for_market_maker_to_connect_to_rfq_server, wait_for_otc_server_to_be_ready,
    wait_for_rfq_server_to_be_ready, PgConnectOptionsExt,
};

#[sqlx::test]
async fn test_refund_from_bitcoin_user_deposit(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let market_maker_account = MultichainAccount::new(1);
    let user_account = MultichainAccount::new(2);

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
        100, // max_deposits_per_lot
        &mut wallet_join_set,
    )
    .await
    .unwrap();

    // fund all accounts

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
            U256::from(9_000_000_000i128), // 90 cbbtc
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
    // at this point, the user should have a confirmed BTC balance
    // and our market maker should have plenty of cbbtc to fill their order

    let client = reqwest::Client::new();

    // Request a quote from the RFQ server
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

    assert_eq!(quote_response.status(), 200, "Quote request should succeed");

    let quote_response: rfq_server::server::QuoteResponse = quote_response
        .json()
        .await
        .expect("Should be able to parse quote response");

    let quote = quote_response.quote;
    info!("Received quote: {:?}", quote);

    assert!(quote.is_some(), "Quote should be present");
    let quote = match quote.as_ref().unwrap() {
        RFQResult::Success(quote) => quote.clone(),
        _ => panic!("Quote should be a success"),
    };

    // create a swap request
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

    let response_status = response.status();
    let response_json = match response_status {
        StatusCode::OK => {
            let response_json: CreateSwapResponse = response.json().await.unwrap();
            response_json
        }
        _ => {
            let response_text = response.text().await;
            panic!(
                "Swap request should be successful but got {response_status:#?} {response_text:#?}"
            );
        }
    };
    // now kill the market maker specifically
    if let Some(handle) = service_handles.remove("market_maker") {
        handle.abort();
        info!("Market maker aborted");
    }
    // now send funds to the deposit address
    let tx_hash = user_bitcoin_wallet
        .create_batch_payment(
            vec![Payment {
                lot: Lot {
                    currency: Currency {
                        chain: ChainType::Bitcoin,
                        token: TokenIdentifier::Native,
                        decimals: response_json.decimals,
                    },
                    amount: response_json.min_input,
                },
                to_address: response_json.deposit_address,
            }],
            None,
        )
        .await
        .unwrap();

    info!(
        "Broadcasting transaction from user wallet to deposit address: {}",
        tx_hash
    );
    devnet.bitcoin.mine_blocks(6).await.unwrap();
    info!("Mined block");

    let get_tx_status = devnet
        .bitcoin
        .rpc_client
        .get_raw_transaction_verbose(&tx_hash.parse::<bitcoin::Txid>().unwrap())
        .await
        .unwrap();
    info!("Tx status: {:#?}", get_tx_status);

    // verify the swap state is WaitingMMDepositInitiated
    loop {
        let swap = client
            .get(format!(
                "http://localhost:{otc_port}/api/v2/swap/{}",
                response_json.swap_id
            ))
            .send()
            .await
            .unwrap();
        let swap: SwapResponse = swap.json().await.unwrap();
        if swap.status == "WaitingMMDepositInitiated" {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    // now refund the swap (advance time so that the swap can be refunded)
    MockClock::advance_system_time(Duration::from_secs(60 * 60 * 24 + 60)); // 24 hours + 1 minute
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
        signature: signature,
    };

    let refund_response = client
        .post(format!("http://localhost:{otc_port}/api/v1/refund",))
        .json(&refund_request)
        .send()
        .await
        .unwrap();

    let response_status = refund_response.status();
    let response_json = match response_status {
        StatusCode::OK => refund_response.json::<RefundSwapResponse>().await.unwrap(),
        _ => {
            let response_text = refund_response.text().await;
            panic!(
                "Refund request should be successful but got {response_status:#?} {response_text:#?}"
            );
        }
    };

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

    devnet
        .bitcoin
        .rpc_client
        .send_raw_transaction(response_json.tx_data)
        .await
        .unwrap();
    devnet.bitcoin.mine_blocks(6).await.unwrap();

    devnet
        .bitcoin
        .wait_for_esplora_sync(Duration::from_secs(30))
        .await
        .unwrap();

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
    assert_ne!(bal_before, bal_after, "Balance should change");
}

#[sqlx::test]
async fn test_refund_from_evm_user_deposit(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let market_maker_account = MultichainAccount::new(11);
    let user_account = MultichainAccount::new(12);

    let devnet = RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .using_esplora(true)
        .build()
        .await
        .unwrap()
        .0;

    // Fund accounts: user with ETH for gas and cbBTC for deposit; MM with some BTC for general readiness
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

    // Mint cbBTC to the user for deposit
    let one_cbbtc = U256::from(1_000_000);
    let deposit_amount = one_cbbtc;
    devnet
        .ethereum
        .mint_cbbtc(user_account.ethereum_address, deposit_amount)
        .await
        .unwrap();

    // MM gets some BTC just to mirror general funding (needed so quotes can pass balance checks)
    devnet
        .bitcoin
        .deal_bitcoin(
            &market_maker_account.bitcoin_wallet.address,
            &bitcoin::Amount::from_sat(500_000_000), // 5 BTC
        )
        .await
        .unwrap();

    // Ensure Esplora is synced so the MM wallet sees the funded balance before quoting
    devnet
        .bitcoin
        .wait_for_esplora_sync(Duration::from_secs(30))
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

    wait_for_market_maker_to_connect_to_rfq_server(rfq_port).await;

    let client = reqwest::Client::new();

    // Request a quote where the user deposits cbBTC on Ethereum and receives BTC
    let quote_request = QuoteRequest {
        input_hint: Some(deposit_amount), // in cbBTC base units (18 decimals)
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
    assert_eq!(quote_response.status(), 200, "Quote request should succeed");

    let quote_response: rfq_server::server::QuoteResponse = quote_response
        .json()
        .await
        .expect("Should be able to parse quote response");

    let quote = match quote_response.quote.expect("Quote should be present") {
        otc_protocols::rfq::RFQResult::Success(q) => q,
        other => panic!("Quote should be a success, got: {other:?}"),
    };

    // Create the swap (user will receive BTC to this address, but we will refund the cbBTC deposit)
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

    let response_status = response.status();
    let response_json = match response_status {
        StatusCode::OK => response.json::<CreateSwapResponse>().await.unwrap(),
        _ => {
            let response_text = response.text().await;
            panic!(
                "Swap request should be successful but got {response_status:#?} {response_text:#?}"
            );
        }
    };

    // Stop the market maker to force refund path
    if let Some(handle) = service_handles.remove("market_maker") {
        handle.abort();
        info!("Market maker aborted");
    }

    // Send the user's cbBTC deposit to the provided deposit address
    let ws_url = devnet.ethereum.anvil.ws_endpoint_url().to_string();
    let http_url = devnet.ethereum.anvil.endpoint_url().to_string();
    let user_provider = Arc::new(
        ProviderBuilder::new()
            .wallet(user_account.ethereum_wallet.clone())
            .connect_ws(WsConnect::new(ws_url))
            .await
            .unwrap(),
    );

    // Rebind token contract to the user's provider
    let token_address = *devnet.ethereum.cbbtc_contract.address();
    let user_token_contract =
        eip3009_erc20_contract::GenericEIP3009ERC20::GenericEIP3009ERC20Instance::new(
            token_address,
            user_provider.clone(),
        );

    // Transfer exactly the expected amount to the deposit address
    user_token_contract
        .transfer(
            response_json
                .deposit_address
                .parse()
                .expect("valid deposit address"),
            response_json.min_input,
        )
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();

    // Wait until swap reaches WaitingMMDepositInitiated after detecting the user deposit
    loop {
        let swap = client
            .get(format!(
                "http://localhost:{otc_port}/api/v2/swap/{}",
                response_json.swap_id
            ))
            .send()
            .await
            .unwrap();
        let swap: SwapResponse = swap.json().await.unwrap();
        if swap.status == "WaitingMMDepositInitiated" {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // Now refund the swap (advance time so that the swap can be refunded)
    MockClock::advance_system_time(Duration::from_secs(60 * 60 * 24 + 60)); // 24 hours + 1 minute
    let signer = PrivateKeySigner::from_signing_key(
        SigningKey::from_bytes(&user_account.secret_bytes.into()).unwrap(),
    );
    let refund_payload = RefundPayload {
        swap_id: response_json.swap_id,
        refund_recipient: user_account.ethereum_address.to_string(),
        refund_transaction_fee: U256::from(0),
    };
    let signature = refund_payload.sign(&signer).to_vec();
    let refund_request = RefundSwapRequest {
        payload: refund_payload,
        signature: signature,
    };

    let refund_response = client
        .post(format!("http://localhost:{otc_port}/api/v1/refund"))
        .json(&refund_request)
        .send()
        .await
        .expect("Refund request should succeed")
        .json::<RefundSwapResponse>()
        .await
        .unwrap();

    // Prepare EVM transaction broadcaster for the user and broadcast the unsigned calldata
    let mut join_set = JoinSet::new();
    let broadcaster = EVMTransactionBroadcaster::new(
        user_provider.clone(),
        http_url,
        1, // confirmations
        &mut join_set,
    );

    // Check recipient cbBTC balance before
    let recipient_balance_before = devnet
        .ethereum
        .cbbtc_contract
        .balanceOf(user_account.ethereum_address)
        .call()
        .await
        .unwrap();

    // Build transaction to token contract with the returned calldata
    let tx_data_bytes = alloy::hex::decode(refund_response.tx_data).expect("valid hex calldata");
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

    // Check recipient cbBTC balance after; it should increase
    let recipient_balance_after = devnet
        .ethereum
        .cbbtc_contract
        .balanceOf(user_account.ethereum_address)
        .call()
        .await
        .unwrap();
    assert!(
        recipient_balance_after > recipient_balance_before,
        "Recipient balance should increase after refund"
    );
    info!("Recipient balance before: {recipient_balance_before}");
    info!("Recipient balance after: {recipient_balance_after}");

    // Cleanup background task
    join_set.abort_all();
}
