use std::{str::FromStr, time::Duration};

use alloy::{
    primitives::{Address, U256},
    providers::Provider,
};
use coinbase_exchange_client::CoinbaseClient;
use devnet::{MultichainAccount, RiftDevnet, WithdrawalProcessingMode};
use market_maker::run_market_maker;
use otc_models::ChainType;
use reqwest::StatusCode;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use tokio::task::JoinSet;
use tracing::info;

use crate::utils::{
    build_mm_test_args, build_otc_server_test_args, build_rfq_server_test_args, get_free_port,
    wait_for_market_maker_to_connect_to_rfq_server, wait_for_otc_server_to_be_ready,
    wait_for_rfq_server_to_be_ready, PgConnectOptionsExt,
};

async fn bitcoin_address_balance_sats(
    devnet: &RiftDevnet,
    address: &bitcoin::Address<bitcoin::address::NetworkChecked>,
) -> Result<u64, String> {
    let esplora_client = devnet
        .bitcoin
        .esplora_client
        .as_ref()
        .ok_or_else(|| "Esplora client not available".to_string())?;
    let script_pubkey = address.script_pubkey();
    let txs = esplora_client
        .scripthash_txs(&script_pubkey, None)
        .await
        .map_err(|e| format!("Failed to get address transactions from esplora: {e}"))?;

    let mut balance = 0u64;
    for tx in txs {
        for (vout, output) in tx.vout.iter().enumerate() {
            if output.scriptpubkey != script_pubkey {
                continue;
            }

            let outpoint = bitcoin::OutPoint {
                txid: tx.txid,
                vout: vout as u32,
            };
            let is_spent = esplora_client
                .get_output_status(&outpoint.txid, outpoint.vout as u64)
                .await
                .map_err(|e| format!("Failed to get output status from esplora: {e}"))?
                .map(|status| status.spent)
                .unwrap_or(false);

            if !is_spent {
                balance += output.value;
            }
        }
    }

    Ok(balance)
}

async fn wait_for_admin_api_ready(admin_port: u16) {
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{admin_port}/quoting");

    for _ in 0..50 {
        if let Ok(response) = client.get(&url).send().await {
            if response.status() == StatusCode::OK {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    panic!("Admin API did not become ready on port {}", admin_port);
}

#[sqlx::test]
async fn test_market_maker_admin_drain_to_coinbase(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let market_maker_account = MultichainAccount::new(1);
    let btc_amount_sats = 75_000_000u64;
    let cbbtc_amount_sats = 25_000_000u64;

    let devnet = RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .using_esplora(true)
        .with_coinbase_mock_server(WithdrawalProcessingMode::Instant)
        .bitcoin_mining_mode(devnet::bitcoin_devnet::MiningMode::Interval(2))
        .build()
        .await
        .unwrap()
        .0;

    let coinbase_client = CoinbaseClient::new(
        format!(
            "http://127.0.0.1:{}",
            devnet
                .coinbase_mock_server_port
                .expect("Coinbase mock server should be running")
        )
        .parse()
        .unwrap(),
        String::new(),
        String::new(),
        String::new(),
    )
    .unwrap();
    let btc_account_id = coinbase_client.get_btc_account_id().await.unwrap();
    let btc_deposit_address = coinbase_client
        .get_bitcoin_deposit_address(&btc_account_id)
        .await
        .unwrap();
    let cbbtc_deposit_address = coinbase_client
        .get_cbbtc_deposit_address(&btc_account_id, ChainType::Ethereum)
        .await
        .unwrap();

    info!(
        %btc_deposit_address,
        %cbbtc_deposit_address,
        "Prepared Coinbase deposit addresses for admin drain test"
    );

    devnet
        .bitcoin
        .deal_bitcoin(
            &market_maker_account.bitcoin_wallet.address,
            &bitcoin::Amount::from_sat(btc_amount_sats),
        )
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

    devnet
        .ethereum
        .mint_cbbtc(
            market_maker_account.ethereum_address,
            U256::from(cbbtc_amount_sats),
        )
        .await
        .unwrap();

    devnet
        .bitcoin
        .wait_for_esplora_sync(Duration::from_secs(30))
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
    }

    let rfq_port = get_free_port().await;
    let rfq_args = build_rfq_server_test_args(rfq_port);
    service_join_set.spawn(async move {
        rfq_server::server::run_server(rfq_args)
            .await
            .expect("RFQ server should not crash");
    });
    wait_for_rfq_server_to_be_ready(rfq_port).await;

    let admin_port = get_free_port().await;
    let mut mm_args = build_mm_test_args(
        otc_port,
        rfq_port,
        &market_maker_account,
        &devnet,
        &connect_options,
    )
    .await;
    mm_args.admin_api_listen_addr = Some(format!("127.0.0.1:{admin_port}").parse().unwrap());
    mm_args.auto_manage_inventory = false;
    mm_args.btc_coinbase_confirmations = 100;
    mm_args.cbbtc_coinbase_confirmations = 100;

    service_join_set.spawn(async move {
        run_market_maker(mm_args)
            .await
            .expect("Market maker should not crash");
    });

    wait_for_market_maker_to_connect_to_rfq_server(rfq_port).await;
    wait_for_admin_api_ready(admin_port).await;

    let admin_client = reqwest::Client::new();
    let drain_response = tokio::time::timeout(
        Duration::from_secs(10),
        admin_client
            .post(format!("http://127.0.0.1:{admin_port}/coinbase/drain"))
            .send(),
    )
    .await
    .expect("Drain endpoint should return without waiting for confirmations")
    .unwrap();
    let drain_status = drain_response.status();
    let drain_body = drain_response.text().await.unwrap();
    assert_eq!(
        drain_status,
        StatusCode::OK,
        "Drain endpoint should succeed, body: {}",
        drain_body
    );

    devnet
        .bitcoin
        .wait_for_esplora_sync(Duration::from_secs(30))
        .await
        .unwrap();

    let mm_btc_balance =
        bitcoin_address_balance_sats(&devnet, &market_maker_account.bitcoin_wallet.address)
            .await
            .unwrap();
    let coinbase_btc_balance = bitcoin_address_balance_sats(
        &devnet,
        &bitcoin::Address::from_str(&btc_deposit_address)
            .unwrap()
            .assume_checked(),
    )
    .await
    .unwrap();

    let mm_cbbtc_balance = devnet
        .ethereum
        .cbbtc_contract
        .balanceOf(market_maker_account.ethereum_address)
        .call()
        .await
        .unwrap();
    let coinbase_cbbtc_balance = devnet
        .ethereum
        .cbbtc_contract
        .balanceOf(Address::from_str(&cbbtc_deposit_address).unwrap())
        .call()
        .await
        .unwrap();

    assert_eq!(
        mm_btc_balance, 0,
        "market maker BTC wallet should be fully drained"
    );
    assert!(
        coinbase_btc_balance > 0 && coinbase_btc_balance <= btc_amount_sats,
        "Coinbase BTC deposit address should receive drained BTC less network fees: {}",
        coinbase_btc_balance
    );

    assert_eq!(
        mm_cbbtc_balance,
        U256::ZERO,
        "market maker cbBTC wallet should be fully drained"
    );
    assert_eq!(
        coinbase_cbbtc_balance,
        U256::from(cbbtc_amount_sats),
        "Coinbase cbBTC deposit address should receive the full cbBTC balance"
    );

    service_join_set.abort_all();
}

#[sqlx::test]
async fn test_market_maker_admin_drain_to_coinbase_eth_uses_eth_account_address(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let market_maker_account = MultichainAccount::new(2);
    let eth_amount_wei = U256::from(100_000_000_000_000_000_000u128);

    let devnet = RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .using_esplora(true)
        .with_coinbase_mock_server(WithdrawalProcessingMode::Instant)
        .bitcoin_mining_mode(devnet::bitcoin_devnet::MiningMode::Interval(2))
        .build()
        .await
        .unwrap()
        .0;

    let coinbase_client = CoinbaseClient::new(
        format!(
            "http://127.0.0.1:{}",
            devnet
                .coinbase_mock_server_port
                .expect("Coinbase mock server should be running")
        )
        .parse()
        .unwrap(),
        String::new(),
        String::new(),
        String::new(),
    )
    .unwrap();
    let btc_account_id = coinbase_client.get_btc_account_id().await.unwrap();
    let eth_account_id = coinbase_client.get_eth_account_id().await.unwrap();
    let cbbtc_deposit_address = coinbase_client
        .get_cbbtc_deposit_address(&btc_account_id, ChainType::Ethereum)
        .await
        .unwrap();
    let eth_deposit_address = coinbase_client
        .get_eth_deposit_address(&eth_account_id, ChainType::Ethereum)
        .await
        .unwrap();

    assert_ne!(
        cbbtc_deposit_address, eth_deposit_address,
        "ETH drain must not reuse the Coinbase BTC-account EVM deposit address"
    );

    info!(
        %cbbtc_deposit_address,
        %eth_deposit_address,
        "Prepared Coinbase EVM deposit addresses for native ETH drain test"
    );

    devnet
        .ethereum
        .fund_eth_address(market_maker_account.ethereum_address, eth_amount_wei)
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
    }

    let rfq_port = get_free_port().await;
    let rfq_args = build_rfq_server_test_args(rfq_port);
    service_join_set.spawn(async move {
        rfq_server::server::run_server(rfq_args)
            .await
            .expect("RFQ server should not crash");
    });
    wait_for_rfq_server_to_be_ready(rfq_port).await;

    let admin_port = get_free_port().await;
    let mut mm_args = build_mm_test_args(
        otc_port,
        rfq_port,
        &market_maker_account,
        &devnet,
        &connect_options,
    )
    .await;
    mm_args.admin_api_listen_addr = Some(format!("127.0.0.1:{admin_port}").parse().unwrap());
    mm_args.auto_manage_inventory = false;
    mm_args.cbbtc_coinbase_confirmations = 100;

    service_join_set.spawn(async move {
        run_market_maker(mm_args)
            .await
            .expect("Market maker should not crash");
    });

    wait_for_market_maker_to_connect_to_rfq_server(rfq_port).await;
    wait_for_admin_api_ready(admin_port).await;

    let admin_client = reqwest::Client::new();
    let drain_response = tokio::time::timeout(
        Duration::from_secs(10),
        admin_client
            .post(format!(
                "http://127.0.0.1:{admin_port}/coinbase/drain?test_mode=true&drain_eth=true"
            ))
            .send(),
    )
    .await
    .expect("Drain endpoint should return without waiting for confirmations")
    .unwrap();
    let drain_status = drain_response.status();
    let drain_body = drain_response.text().await.unwrap();
    assert_eq!(
        drain_status,
        StatusCode::OK,
        "Drain endpoint should succeed, body: {}",
        drain_body
    );

    let mm_eth_balance = devnet
        .ethereum
        .funded_provider
        .get_balance(market_maker_account.ethereum_address)
        .await
        .unwrap();
    let cbbtc_deposit_eth_balance = devnet
        .ethereum
        .funded_provider
        .get_balance(Address::from_str(&cbbtc_deposit_address).unwrap())
        .await
        .unwrap();
    let eth_deposit_eth_balance = devnet
        .ethereum
        .funded_provider
        .get_balance(Address::from_str(&eth_deposit_address).unwrap())
        .await
        .unwrap();

    assert!(
        mm_eth_balance < eth_amount_wei,
        "market maker ETH wallet should have spent native ETH during drain"
    );
    assert_eq!(
        cbbtc_deposit_eth_balance,
        U256::ZERO,
        "Coinbase BTC-account EVM deposit address must not receive native ETH"
    );
    assert!(
        eth_deposit_eth_balance > U256::ZERO && eth_deposit_eth_balance < eth_amount_wei,
        "Coinbase ETH deposit address should receive drained ETH less gas reserve: {}",
        eth_deposit_eth_balance
    );

    service_join_set.abort_all();
}

#[sqlx::test]
async fn test_market_maker_admin_drain_exact_eth_only_sends_eth(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let market_maker_account = MultichainAccount::new(3);
    let btc_amount_sats = 75_000_000u64;
    let cbbtc_amount_sats = 25_000_000u64;
    let eth_amount_wei = U256::from(100_000_000_000_000_000_000u128);
    let exact_eth_amount_wei = U256::from(25_000_000_000_000_000u128);

    let devnet = RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .using_esplora(true)
        .with_coinbase_mock_server(WithdrawalProcessingMode::Instant)
        .bitcoin_mining_mode(devnet::bitcoin_devnet::MiningMode::Interval(2))
        .build()
        .await
        .unwrap()
        .0;

    let coinbase_client = CoinbaseClient::new(
        format!(
            "http://127.0.0.1:{}",
            devnet
                .coinbase_mock_server_port
                .expect("Coinbase mock server should be running")
        )
        .parse()
        .unwrap(),
        String::new(),
        String::new(),
        String::new(),
    )
    .unwrap();
    let btc_account_id = coinbase_client.get_btc_account_id().await.unwrap();
    let eth_account_id = coinbase_client.get_eth_account_id().await.unwrap();
    let btc_deposit_address = coinbase_client
        .get_bitcoin_deposit_address(&btc_account_id)
        .await
        .unwrap();
    let cbbtc_deposit_address = coinbase_client
        .get_cbbtc_deposit_address(&btc_account_id, ChainType::Ethereum)
        .await
        .unwrap();
    let eth_deposit_address = coinbase_client
        .get_eth_deposit_address(&eth_account_id, ChainType::Ethereum)
        .await
        .unwrap();

    devnet
        .bitcoin
        .deal_bitcoin(
            &market_maker_account.bitcoin_wallet.address,
            &bitcoin::Amount::from_sat(btc_amount_sats),
        )
        .await
        .unwrap();

    devnet
        .ethereum
        .fund_eth_address(market_maker_account.ethereum_address, eth_amount_wei)
        .await
        .unwrap();

    devnet
        .ethereum
        .mint_cbbtc(
            market_maker_account.ethereum_address,
            U256::from(cbbtc_amount_sats),
        )
        .await
        .unwrap();

    devnet
        .bitcoin
        .wait_for_esplora_sync(Duration::from_secs(30))
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
    }

    let rfq_port = get_free_port().await;
    let rfq_args = build_rfq_server_test_args(rfq_port);
    service_join_set.spawn(async move {
        rfq_server::server::run_server(rfq_args)
            .await
            .expect("RFQ server should not crash");
    });
    wait_for_rfq_server_to_be_ready(rfq_port).await;

    let admin_port = get_free_port().await;
    let mut mm_args = build_mm_test_args(
        otc_port,
        rfq_port,
        &market_maker_account,
        &devnet,
        &connect_options,
    )
    .await;
    mm_args.admin_api_listen_addr = Some(format!("127.0.0.1:{admin_port}").parse().unwrap());
    mm_args.auto_manage_inventory = false;
    mm_args.cbbtc_coinbase_confirmations = 100;

    service_join_set.spawn(async move {
        run_market_maker(mm_args)
            .await
            .expect("Market maker should not crash");
    });

    wait_for_market_maker_to_connect_to_rfq_server(rfq_port).await;
    wait_for_admin_api_ready(admin_port).await;

    let admin_client = reqwest::Client::new();
    let drain_response = tokio::time::timeout(
        Duration::from_secs(10),
        admin_client
            .post(format!(
                "http://127.0.0.1:{admin_port}/coinbase/drain?asset=eth&amount_wei={exact_eth_amount_wei}"
            ))
            .send(),
    )
    .await
    .expect("Drain endpoint should return without waiting for confirmations")
    .unwrap();
    let drain_status = drain_response.status();
    let drain_body: serde_json::Value = drain_response.json().await.unwrap();
    assert_eq!(
        drain_status,
        StatusCode::OK,
        "Exact ETH drain endpoint should succeed, body: {}",
        drain_body
    );
    assert_eq!(drain_body["test_mode"], false);
    assert_eq!(drain_body["drain_eth"], true);
    assert_eq!(drain_body["btc_requested_sats"], 0);
    assert_eq!(drain_body["btc_tx_hash"], serde_json::Value::Null);
    assert_eq!(drain_body["cbbtc_requested_sats"], 0);
    assert_eq!(drain_body["cbbtc_tx_hash"], serde_json::Value::Null);
    assert_eq!(
        drain_body["eth_requested_wei"],
        exact_eth_amount_wei.to_string()
    );
    assert!(
        drain_body["eth_tx_hash"]
            .as_str()
            .is_some_and(|tx_hash| !tx_hash.is_empty()),
        "exact ETH drain should return an ETH tx hash: {drain_body}"
    );

    devnet
        .bitcoin
        .wait_for_esplora_sync(Duration::from_secs(30))
        .await
        .unwrap();

    let mm_btc_balance =
        bitcoin_address_balance_sats(&devnet, &market_maker_account.bitcoin_wallet.address)
            .await
            .unwrap();
    let coinbase_btc_balance = bitcoin_address_balance_sats(
        &devnet,
        &bitcoin::Address::from_str(&btc_deposit_address)
            .unwrap()
            .assume_checked(),
    )
    .await
    .unwrap();

    let mm_cbbtc_balance = devnet
        .ethereum
        .cbbtc_contract
        .balanceOf(market_maker_account.ethereum_address)
        .call()
        .await
        .unwrap();
    let coinbase_cbbtc_balance = devnet
        .ethereum
        .cbbtc_contract
        .balanceOf(Address::from_str(&cbbtc_deposit_address).unwrap())
        .call()
        .await
        .unwrap();

    let mm_eth_balance = devnet
        .ethereum
        .funded_provider
        .get_balance(market_maker_account.ethereum_address)
        .await
        .unwrap();
    let cbbtc_deposit_eth_balance = devnet
        .ethereum
        .funded_provider
        .get_balance(Address::from_str(&cbbtc_deposit_address).unwrap())
        .await
        .unwrap();
    let eth_deposit_eth_balance = devnet
        .ethereum
        .funded_provider
        .get_balance(Address::from_str(&eth_deposit_address).unwrap())
        .await
        .unwrap();

    assert_eq!(
        mm_btc_balance, btc_amount_sats,
        "exact ETH drain must not move BTC"
    );
    assert_eq!(
        coinbase_btc_balance, 0,
        "Coinbase BTC deposit address must not receive BTC during exact ETH drain"
    );
    assert_eq!(
        mm_cbbtc_balance,
        U256::from(cbbtc_amount_sats),
        "exact ETH drain must not move cbBTC"
    );
    assert_eq!(
        coinbase_cbbtc_balance,
        U256::ZERO,
        "Coinbase cbBTC deposit address must not receive cbBTC during exact ETH drain"
    );
    assert_eq!(
        cbbtc_deposit_eth_balance,
        U256::ZERO,
        "Coinbase BTC-account EVM deposit address must not receive native ETH during exact ETH drain"
    );
    assert_eq!(
        eth_deposit_eth_balance, exact_eth_amount_wei,
        "Coinbase ETH deposit address should receive the exact requested ETH amount"
    );
    assert!(
        mm_eth_balance < eth_amount_wei - exact_eth_amount_wei,
        "market maker ETH wallet should pay the exact ETH amount plus gas"
    );

    service_join_set.abort_all();
}
