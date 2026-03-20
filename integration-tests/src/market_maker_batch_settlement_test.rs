use alloy::primitives::TxHash;
use alloy::primitives::U256;
use alloy::providers::ext::AnvilApi;
use alloy::providers::Provider;
use alloy::signers::local::PrivateKeySigner;
use bitcoincore_rpc_async::RpcApi;
use devnet::bitcoin_devnet::MiningMode;
use devnet::{MultichainAccount, RiftDevnet};
use market_maker::run_market_maker;
use market_maker::wallet::Wallet;
use otc_chains::traits::Payment;
use otc_models::{ChainType, Currency, Lot, QuoteRequest, Swap, SwapMode, TokenIdentifier};
use otc_protocols::rfq::RFQResult;
use otc_server::{api::CreateSwapRequest, server::run_server, OtcServerArgs};
use reqwest::StatusCode;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::str::FromStr;
use tokio::task::{JoinHandle, JoinSet};
use tracing::info;

use crate::utils::{
    build_mm_test_args, build_otc_server_test_args, build_rfq_server_test_args,
    build_test_user_ethereum_wallet, get_free_port, wait_for_market_maker_to_connect_to_rfq_server,
    wait_for_otc_server_to_be_ready, wait_for_rfq_server_to_be_ready, wait_for_swap_status,
    wait_for_swap_to_be_settled, PgConnectOptionsExt,
};

fn spawn_otc_server(otc_args: OtcServerArgs) -> JoinHandle<()> {
    tokio::spawn(async move {
        run_server(otc_args)
            .await
            .expect("OTC server should not crash");
    })
}

#[sqlx::test]
async fn test_market_maker_batch_settlement_recovers_after_otc_restart(
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

    devnet
        .bitcoin
        .deal_bitcoin(
            &market_maker_account.bitcoin_wallet.address,
            &bitcoin::Amount::from_sat(500_000_000),
        )
        .await
        .unwrap();

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
        .mint_cbbtc(user_account.ethereum_address, U256::from(9_000_000_000u64))
        .await
        .unwrap();

    devnet
        .ethereum
        .fund_eth_address(
            market_maker_account.ethereum_address,
            U256::from(100_000_000_000_000_000_000u128),
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
    let mut otc_handle = spawn_otc_server(otc_args.clone());
    wait_for_otc_server_to_be_ready(otc_port).await;

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

    devnet
        .bitcoin
        .wait_for_esplora_sync(std::time::Duration::from_secs(30))
        .await
        .unwrap();

    service_join_set.spawn(async move {
        run_market_maker(mm_args)
            .await
            .expect("Market maker should not crash");
    });
    wait_for_market_maker_to_connect_to_rfq_server(rfq_port).await;

    let client = reqwest::Client::new();
    let quote_request = QuoteRequest {
        mode: SwapMode::ExactInput(100_000_000),
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

    assert_eq!(quote_response.status(), 200);
    let quote_response: rfq_server::server::QuoteResponse = quote_response.json().await.unwrap();
    let quote = match quote_response.quote.expect("Quote should be present") {
        RFQResult::Success(quote) => quote,
        other => panic!("Expected successful quote, got {other:?}"),
    };

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
    let swap = match response_status {
        StatusCode::OK => response.json::<Swap>().await.unwrap(),
        status => {
            let body = response.text().await.unwrap();
            panic!("Swap request failed with status {status}: {body}");
        }
    };

    let user_tx_hash = user_ethereum_wallet
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

    info!(
        swap_id = %swap.id,
        tx_hash = %user_tx_hash,
        "Broadcast user deposit for MM batch settlement recovery test"
    );

    devnet
        .ethereum
        .funded_provider
        .anvil_mine(Some(2), None)
        .await
        .unwrap();

    let _user_receipt = devnet
        .ethereum
        .funded_provider
        .get_transaction_receipt(user_tx_hash.parse::<TxHash>().unwrap())
        .await
        .unwrap()
        .expect("User deposit receipt should be available");

    let waiting_mm_confirmed =
        wait_for_swap_status(&client, otc_port, swap.id, "WaitingMMDepositConfirmed").await;
    let mm_deposit_tx_hash = waiting_mm_confirmed
        .mm_deposit_status
        .as_ref()
        .expect("MM deposit status should be present")
        .tx_hash
        .clone();
    let mm_deposit_txid = bitcoin::Txid::from_str(&mm_deposit_tx_hash).unwrap();

    let mm_deposit_tx_status = devnet
        .bitcoin
        .rpc_client
        .get_raw_transaction_verbose(&mm_deposit_txid)
        .await
        .unwrap();
    assert_eq!(mm_deposit_tx_status.confirmations.unwrap_or(0), 0);

    otc_handle.abort();
    let _ = otc_handle.await;

    otc_handle = spawn_otc_server(otc_args);
    wait_for_otc_server_to_be_ready(otc_port).await;

    devnet.bitcoin.mine_blocks(2).await.unwrap();
    wait_for_swap_to_be_settled(otc_port, swap.id).await;

    otc_handle.abort();
    let _ = otc_handle.await;

    drop(devnet);
    tokio::join!(wallet_join_set.shutdown(), service_join_set.shutdown());
}
