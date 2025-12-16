use alloy::primitives::TxHash;
use alloy::providers::ext::AnvilApi;
use alloy::signers::local::PrivateKeySigner;
use alloy::{primitives::U256, providers::Provider};

use bitcoincore_rpc_async::RpcApi;
use devnet::bitcoin_devnet::MiningMode;
use devnet::{MultichainAccount, RiftDevnet};
use evm_token_indexer_client::TokenIndexerClient;
use market_maker::db::Database;
use market_maker::evm_wallet::EVMWallet;
use market_maker::wallet::Wallet;
use market_maker::{bitcoin_wallet::BitcoinWallet, run_market_maker, MarketMakerArgs};
use mock_instant::global::MockClock;
use otc_chains::traits::Payment;
use otc_models::{SwapMode, ChainType, Currency, Lot, Quote, QuoteRequest, TokenIdentifier};
use otc_protocols::rfq::RFQResult;
use otc_server::api::SwapResponse;
use otc_server::{
    api::{CreateSwapRequest, CreateSwapResponse},
    server::run_server,
    OtcServerArgs,
};
use reqwest::StatusCode;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tracing::info;
use uuid::Uuid;

use crate::utils::{
    build_bitcoin_wallet_descriptor, build_mm_test_args, build_otc_server_test_args,
    build_rfq_server_test_args, build_test_user_ethereum_wallet, build_tmp_bitcoin_wallet_db_file,
    get_free_port, wait_for_market_maker_to_connect_to_rfq_server, wait_for_otc_server_to_be_ready,
    wait_for_rfq_server_to_be_ready, wait_for_swap_status, wait_for_swap_to_be_settled,
    PgConnectOptionsExt,
};
#[sqlx::test]
async fn test_swap_from_ethereum_to_bitcoin_mm_timeout_triggers_cancel(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let market_maker_account = MultichainAccount::new(1);
    let user_account = MultichainAccount::new(2);

    let devnet = RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .bitcoin_mining_mode(MiningMode::Manual)
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

    user_ethereum_wallet
        .ensure_eip7702_delegation(
            PrivateKeySigner::from_slice(&user_account.secret_bytes).unwrap(),
        )
        .await
        .unwrap();

    devnet.bitcoin.mine_blocks(1).await.unwrap();

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

    // Wait for esplora to sync before starting market maker
    devnet
        .bitcoin
        .wait_for_esplora_sync(Duration::from_secs(30))
        .await
        .unwrap();

    let mm_db_url = mm_args.database_url.clone();
    service_join_set.spawn(async move {
        run_market_maker(mm_args)
            .await
            .expect("Market maker should not crash");
    });

    wait_for_market_maker_to_connect_to_rfq_server(rfq_port).await;

    let client = reqwest::Client::new();

    // Request a quote from the RFQ server
    let quote_request = QuoteRequest {
        mode: SwapMode::ExactInput(100_000_000i128 as u64), // 1 cbbtc
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
        user_destination_address: user_account.bitcoin_wallet.address.to_string(),
        refund_address: user_account.ethereum_address.to_string(),
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
            unreachable!()
        }
    };
    let tx_hash = user_ethereum_wallet
        .create_batch_payment(
            vec![Payment {
                lot: Lot {
                    currency: Currency {
                        chain: ChainType::Ethereum,
                        token: TokenIdentifier::Address(
                            devnet.ethereum.cbbtc_contract.address().to_string(),
                        ),
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
    let swap_response = wait_for_swap_status(
        &client,
        otc_port,
        response_json.swap_id,
        "WaitingMMDepositConfirmed",
    )
    .await;

    let mm_deposit_tx = swap_response
        .mm_deposit
        .deposit_tx
        .expect("MM deposit tx should be present");

    // mm_deposit_tx is bitcoin Txid
    let mm_deposit_tx = bitcoin::Txid::from_str(&mm_deposit_tx).unwrap();
    let mm_deposit_tx_status = devnet
        .bitcoin
        .rpc_client
        .get_raw_transaction_verbose(&mm_deposit_tx)
        .await
        .unwrap();

    info!(
        "MM deposit tx status before cancellation: {:#?}",
        mm_deposit_tx_status
    );

    let database = Database::connect(&mm_db_url, 10, 2).await.unwrap();
    database
        .payments()
        .has_payment_been_made(response_json.swap_id)
        .await
        .unwrap()
        .expect("Payment storage should have recorded the txid");

    // at this point the WaitingMMDepositConfirmed status will wait indefinitely b/c
    // bitcoin blocks are not being mined

    // advance time so that the swap can be cancelled
    // 23 hours + 51 minutes
    let hour = 60 * 60;
    let minute = 60;
    let utc_time_before_advance = utc::now();
    MockClock::advance_system_time(Duration::from_secs((hour * 23) + (minute * 51)));
    let utc_time_after_advance = utc::now();
    info!("UTC TIME BEFORE ADVANCE: {:#?}", utc_time_before_advance);
    info!("UTC TIME AFTER ADVANCE: {:#?}", utc_time_after_advance);
    info!("SYSTEM TIME ADVANCED TO: {:#?}", MockClock::system_time());

    loop {
        let mm_deposit_tx_status = devnet
            .bitcoin
            .rpc_client
            .get_raw_transaction_verbose(&mm_deposit_tx)
            .await;
        if mm_deposit_tx_status.is_err() {
            let err = mm_deposit_tx_status.unwrap_err();
            let err_str = err.to_string();
            if err_str.contains("No such mempool or blockchain transaction") {
                // implied that the original payment was cancelled
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    info!("Swap cancelled successfully");

    drop(devnet);
    tokio::join!(wallet_join_set.shutdown(), service_join_set.shutdown());
}
