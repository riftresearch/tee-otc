use alloy::primitives::U256;
use market_maker::{run_market_maker, MarketMakerArgs};
use otc_server::{server::run_server, OtcServerArgs};
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;
use tokio::task::JoinSet;

use crate::utils::{
    build_mm_test_args, build_otc_server_test_args, get_free_port, get_whitelist_file_path,
    wait_for_otc_server_to_be_ready, PgConnectOptionsExt, INTEGRATION_TEST_TIMEOUT_SECS,
    TEST_API_KEY, TEST_API_KEY_ID, TEST_MARKET_MAKER_ID,
};

#[sqlx::test]
async fn test_market_maker_otc_auth(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let market_maker_account = devnet::MultichainAccount::new(0);
    let devnet = devnet::RiftDevnet::builder()
        .using_esplora(true)
        .using_token_indexer(connect_options.to_database_url())
        .build()
        .await
        .unwrap()
        .0;

    devnet
        .ethereum
        .fund_eth_address(
            market_maker_account.ethereum_address,
            U256::from(100_000_000_000_000_000_000i128),
        )
        .await
        .unwrap();

    let mut join_set = JoinSet::new();
    let otc_port = get_free_port().await;
    let otc_args = build_otc_server_test_args(otc_port, &devnet, &connect_options).await;

    join_set.spawn(async move {
        run_server(otc_args)
            .await
            .expect("OTC server should not crash");
    });

    wait_for_otc_server_to_be_ready(otc_port).await;

    let rfq_port = get_free_port().await; // Get a dummy RFQ port (not used in this test)
    let mm_args = build_mm_test_args(
        otc_port,
        rfq_port,
        &market_maker_account,
        &devnet,
        &connect_options,
    )
    .await;

    join_set.spawn(async move {
        run_market_maker(mm_args)
            .await
            .expect("Market maker should not crash");
    });

    let connected_url = format!("http://127.0.0.1:{otc_port}/api/v1/market-makers/connected");

    let client = reqwest::Client::new();
    let start_time = std::time::Instant::now();
    let timeout = Duration::from_secs(INTEGRATION_TEST_TIMEOUT_SECS);

    loop {
        assert!(
            (start_time.elapsed() <= timeout),
            "Timeout waiting for market maker to connect"
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        if let Ok(response) = client.get(&connected_url).send().await {
            if response.status() == 200 {
                if let Ok(body) = response.json::<serde_json::Value>().await {
                    if let Some(market_makers) = body["market_makers"].as_array() {
                        if market_makers.len() == 1
                            && market_makers[0].as_str() == Some(TEST_MARKET_MAKER_ID)
                        {
                            println!("Market maker is connected!");
                            break;
                        }
                    }
                }
            }
        }
    }
}
