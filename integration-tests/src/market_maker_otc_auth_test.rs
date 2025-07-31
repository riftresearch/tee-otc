use market_maker::{run_market_maker, MarketMakerArgs};
use otc_server::{server::run_server, OtcServerArgs};
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;
use tokio::task::JoinSet;

use crate::utils::{
    get_free_port, get_whitelist_file_path, PgConnectOptionsExt, INTEGRATION_TEST_TIMEOUT_SECS,
    TEST_API_KEY, TEST_API_KEY_ID, TEST_MARKET_MAKER_ID,
};

#[sqlx::test]
async fn test_market_maker_otc_auth(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let devnet = devnet::RiftDevnet::builder()
        .using_esplora(true)
        .using_token_indexer(connect_options.to_database_url())
        .build()
        .await
        .unwrap()
        .0;

    let mut join_set = JoinSet::new();
    let otc_port = get_free_port().await;

    join_set.spawn(async move {
        run_server(OtcServerArgs {
            port: otc_port,
            database_url: connect_options.to_database_url(),
            whitelist_file: get_whitelist_file_path(),
            host: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            log_level: "info".to_string(),
            ethereum_mainnet_rpc_url: devnet.ethereum.anvil.endpoint(),
            ethereum_mainnet_token_indexer_url: devnet
                .ethereum
                .token_indexer
                .as_ref()
                .unwrap()
                .api_server_url
                .clone(),
            ethereum_mainnet_chain_id: devnet.ethereum.anvil.chain_id(),
            bitcoin_rpc_url: devnet.bitcoin.rpc_url_with_cookie.clone(),
            esplora_http_server_url: devnet.bitcoin.esplora_url.as_ref().unwrap().to_string(),
            bitcoin_network: bitcoin::network::Network::Testnet,
        })
        .await
        .expect("OTC server should not crash");
    });

    // Hit the otc server status endpoint every 100ms until it returns 200
    let client = reqwest::Client::new();
    let status_url = format!("http://127.0.0.1:{otc_port}/status");

    let start_time = std::time::Instant::now();
    let timeout = Duration::from_secs(INTEGRATION_TEST_TIMEOUT_SECS);

    loop {
        assert!(
            (start_time.elapsed() <= timeout),
            "Timeout waiting for OTC server to become ready"
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        if let Ok(response) = client.get(&status_url).send().await {
            if response.status() == 200 {
                println!("OTC server is ready!");
                break;
            }
        }
    }

    join_set.spawn(async move {
        run_market_maker(MarketMakerArgs {
            market_maker_id: TEST_MARKET_MAKER_ID.to_string(),
            api_key_id: TEST_API_KEY_ID.to_string(),
            api_key: TEST_API_KEY.to_string(),
            otc_ws_url: format!("ws://127.0.0.1:{otc_port}/ws/mm"),
            auto_accept: true,
            log_level: "info".to_string(),
        })
        .await
        .expect("Market maker should not crash");
    });

    let connected_url = format!("http://127.0.0.1:{otc_port}/api/v1/market-makers/connected");

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
