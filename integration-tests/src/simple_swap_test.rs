use alloy::primitives::TxHash;
use alloy::providers::ext::AnvilApi;
use alloy::{primitives::U256, providers::Provider};

use bitcoincore_rpc_async::RpcApi;
use devnet::bitcoin_devnet::MiningMode;
use devnet::{MultichainAccount, RiftDevnet};
use evm_token_indexer_client::TokenIndexerClient;
use market_maker::evm_wallet::EVMWallet;
use market_maker::wallet::Wallet;
use market_maker::{bitcoin_wallet::BitcoinWallet, run_market_maker, MarketMakerArgs};
use otc_models::{ChainType, Currency, Lot, Quote, QuoteMode, QuoteRequest, TokenIdentifier};
use otc_rfq_protocol::RFQResult;
use otc_server::api::SwapResponse;
use otc_server::{
    api::{CreateSwapRequest, CreateSwapResponse},
    server::run_server,
    OtcServerArgs,
};
use reqwest::StatusCode;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::info;

use crate::utils::{
    build_bitcoin_wallet_descriptor, build_mm_test_args, build_otc_server_test_args,
    build_rfq_server_test_args, build_test_user_ethereum_wallet, build_tmp_bitcoin_wallet_db_file,
    get_free_port, wait_for_market_maker_to_connect_to_rfq_server, wait_for_otc_server_to_be_ready,
    wait_for_rfq_server_to_be_ready, wait_for_swap_to_be_settled, PgConnectOptionsExt,
    TEST_MARKET_MAKER_ID,
};

#[sqlx::test]
async fn test_swap_from_bitcoin_to_ethereum(
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
    // at this point, the user should have a confirmed BTC balance
    // and our market maker should have plenty of cbbtc to fill their order

    let client = reqwest::Client::new();

    // Request a quote from the RFQ server
    let quote_request = QuoteRequest {
        mode: QuoteMode::ExactInput,
        amount: U256::from(10_000_000), // 0.1 BTC
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
        RFQResult::Success(quote) => quote.quote.clone(),
        _ => panic!("Quote should be a success"),
    };

    // create a swap request
    let swap_request = CreateSwapRequest {
        quote,
        user_destination_address: user_account.ethereum_address.to_string(),
        user_evm_account_address: user_account.ethereum_address,
    };

    let response = client
        .post(format!("http://localhost:{otc_port}/api/v1/swaps"))
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
            unreachable!()
        }
    };
    let tx_hash = user_bitcoin_wallet
        .create_transaction(
            &Lot {
                currency: Currency {
                    chain: ChainType::Bitcoin,
                    token: TokenIdentifier::Native,
                    decimals: response_json.decimals,
                },
                amount: response_json.expected_amount,
            },
            &response_json.deposit_address,
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
    wait_for_swap_to_be_settled(otc_port, response_json.swap_id).await;

    drop(devnet);
    tokio::join!(wallet_join_set.shutdown(), service_join_set.shutdown());
}

#[sqlx::test]
async fn test_swap_from_ethereum_to_bitcoin(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let market_maker_account = MultichainAccount::new(1);
    let user_account = MultichainAccount::new(2);

    let devnet = RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .bitcoin_mining_mode(MiningMode::Interval(2))
        .using_esplora(true)
        .build()
        .await
        .unwrap()
        .0;

    let (mut wallet_join_set, user_ethereum_wallet) =
        build_test_user_ethereum_wallet(&devnet, &user_account).await;

    // fund all accounts
    devnet
        .bitcoin
        .deal_bitcoin(
            &market_maker_account.bitcoin_wallet.address,
            &bitcoin::Amount::from_sat(500_000_000), // 5 BTC
        )
        .await
        .unwrap();

    devnet
        .ethereum
        .fund_eth_address(
            user_account.ethereum_address,
            U256::from(100_000_000_000_000_000_000i128),
        )
        .await
        .unwrap();

    devnet
        .ethereum
        .mint_cbbtc(
            user_account.ethereum_address,
            U256::from(9_000_000_000i128), // 90 cbbtc
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
            panic!("Bitcoin wallet crashed");
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
    // at this point, the user should have a confirmed BTC balance
    // and our market maker should have plenty of cbbtc to fill their order

    let client = reqwest::Client::new();

    // Request a quote from the RFQ server
    let quote_request = QuoteRequest {
        mode: QuoteMode::ExactInput,
        amount: U256::from(100_000_000i128), // 1 cbbtc
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

    let quote = quote_response.quote;
    info!("Received quote: {:?}", quote);

    assert!(quote.is_some(), "Quote should be present");
    let quote = match quote.as_ref().unwrap() {
        RFQResult::Success(quote) => quote.quote.clone(),
        _ => panic!("Quote should be a success"),
    };

    // create a swap request
    let swap_request = CreateSwapRequest {
        quote,
        user_destination_address: user_account.bitcoin_wallet.address.to_string(),
        user_evm_account_address: user_account.ethereum_address,
    };

    let response = client
        .post(format!("http://localhost:{otc_port}/api/v1/swaps"))
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
            unreachable!()
        }
    };
    let tx_hash = user_ethereum_wallet
        .create_transaction(
            &Lot {
                currency: Currency {
                    chain: ChainType::Ethereum,
                    token: TokenIdentifier::Address(
                        devnet.ethereum.cbbtc_contract.address().to_string(),
                    ),
                    decimals: response_json.decimals,
                },
                amount: response_json.expected_amount,
            },
            &response_json.deposit_address,
            None,
        )
        .await
        .unwrap();

    info!(
        "Broadcasting transaction from user wallet to deposit address: {}",
        tx_hash
    );
    devnet
        .ethereum
        .funded_provider
        .anvil_mine(Some(2), None)
        .await
        .unwrap();
    info!("Mined 2 blocks");

    let get_tx_status = devnet
        .ethereum
        .funded_provider
        .get_transaction_receipt(tx_hash.parse::<TxHash>().unwrap())
        .await
        .unwrap();

    info!("Tx status: {:#?}", get_tx_status);
    wait_for_swap_to_be_settled(otc_port, response_json.swap_id).await;

    drop(devnet);
    tokio::join!(wallet_join_set.shutdown(), service_join_set.shutdown());
}
