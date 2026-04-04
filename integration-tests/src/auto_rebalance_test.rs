use alloy::primitives::U256;
use devnet::bitcoin_devnet::MiningMode;
use devnet::{MultichainAccount, RiftDevnet, WithdrawalProcessingMode};
use market_maker::run_market_maker;
use market_maker::{bitcoin_wallet::BitcoinWallet, wallet::Wallet};
use otc_chains::traits::Payment;
use otc_models::{ChainType, Currency, Quote, QuoteRequest, Swap, SwapMode, TokenIdentifier};
use otc_protocols::rfq::RFQResult;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::info;

use crate::utils::{
    build_bitcoin_wallet_descriptor, build_mm_test_args, build_otc_server_test_args,
    build_rfq_server_test_args, build_tmp_bitcoin_wallet_db_file, get_free_port,
    wait_for_market_maker_to_connect_to_rfq_server, wait_for_otc_server_to_be_ready,
    wait_for_rfq_server_to_be_ready, wait_for_swap_to_be_settled, PgConnectOptionsExt,
};

/// Helper to calculate BTC ratio in basis points
fn calculate_btc_ratio_bps(btc_sats: u64, cbbtc_sats: u64) -> u64 {
    let total = btc_sats.saturating_add(cbbtc_sats);
    if total == 0 {
        return 0;
    }
    ((btc_sats as u128 * 10_000) / total as u128).min(10_000) as u64
}

/// Helper to read the MM's current on-chain BTC/cbBTC inventory split.
async fn current_inventory(
    devnet: &RiftDevnet,
    mm_account: &MultichainAccount,
) -> Result<(u64, u64, u64), String> {
    // Get BTC balance for the specific market maker address using esplora scripthash
    // (can't use get_balance() because it returns the entire wallet balance including miner rewards)
    let btc_sats = if let Some(ref esplora_client) = devnet.bitcoin.esplora_client {
        use bitcoin::ScriptBuf;
        let script_pubkey: ScriptBuf = mm_account.bitcoin_wallet.address.script_pubkey();
        tokio::time::sleep(Duration::from_millis(500)).await;

        match esplora_client.scripthash_txs(&script_pubkey, None).await {
            Ok(txs) => {
                let mut balance = 0u64;
                for tx in txs {
                    for (vout, output) in tx.vout.iter().enumerate() {
                        if output.scriptpubkey == script_pubkey {
                            let outpoint = bitcoin::OutPoint {
                                txid: tx.txid,
                                vout: vout as u32,
                            };
                            let is_spent = esplora_client
                                .get_output_status(&outpoint.txid, outpoint.vout as u64)
                                .await
                                .ok()
                                .and_then(|status| status)
                                .map(|status| status.spent)
                                .unwrap_or(false);

                            if !is_spent {
                                balance += output.value;
                            }
                        }
                    }
                }
                balance
            }
            Err(e) => {
                return Err(format!("Failed to get BTC balance from esplora: {}", e));
            }
        }
    } else {
        return Err("Esplora client not available".to_string());
    };

    let cbbtc_balance = devnet
        .ethereum
        .cbbtc_contract
        .balanceOf(mm_account.ethereum_address)
        .call()
        .await
        .map_err(|e| format!("Failed to get cbBTC balance: {}", e))?;
    let cbbtc_sats = cbbtc_balance.to::<u64>();
    let current_ratio_bps = calculate_btc_ratio_bps(btc_sats, cbbtc_sats);

    Ok((btc_sats, cbbtc_sats, current_ratio_bps))
}

async fn request_successful_btc_to_cbbtc_quote(
    client: &reqwest::Client,
    rfq_port: u16,
    cbbtc_address: &str,
    desired_output_sats: u64,
) -> Quote {
    let quote_request = QuoteRequest {
        mode: SwapMode::ExactOutput(desired_output_sats),
        from: Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        },
        to: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::address(cbbtc_address.to_string()),
            decimals: 8,
        },
        affiliate: None,
    };

    let response = client
        .post(format!("http://127.0.0.1:{rfq_port}/api/v2/quote"))
        .json(&quote_request)
        .send()
        .await
        .unwrap();

    let quote_response: rfq_server::server::QuoteResponse = response.json().await.unwrap();
    match quote_response.quote.expect("Quote should be present") {
        RFQResult::Success(quote) => quote,
        other => panic!("Expected successful quote, got {other:?}"),
    }
}

async fn create_swap(
    client: &reqwest::Client,
    otc_port: u16,
    quote: Quote,
    user_account: &MultichainAccount,
) -> Swap {
    client
        .post(format!("http://127.0.0.1:{otc_port}/api/v2/swap"))
        .json(&otc_server::api::CreateSwapRequest {
            quote,
            user_destination_address: user_account.ethereum_address.to_string(),
            refund_address: user_account.bitcoin_wallet.address.to_string(),
            metadata: None,
        })
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn deposit_into_swap(wallet: &BitcoinWallet, swap: &Swap, amount: U256) {
    wallet
        .create_batch_payment(
            vec![Payment {
                lot: otc_models::Lot {
                    currency: swap.quote.from.currency.clone(),
                    amount,
                },
                to_address: swap.deposit_vault_address.clone(),
            }],
            None,
        )
        .await
        .unwrap();
}

#[sqlx::test]
async fn test_no_proactive_rebalance_btc_to_cbbtc_without_blocked_demand(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let market_maker_account = MultichainAccount::new(1);

    // Build devnet with Coinbase mock server enabled (Manual mode for controlled testing)
    let devnet = RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .using_esplora(true)
        .with_coinbase_mock_server(WithdrawalProcessingMode::Instant)
        .bitcoin_mining_mode(devnet::bitcoin_devnet::MiningMode::Interval(3))
        .build()
        .await
        .unwrap()
        .0;

    let coinbase_mock_port = devnet
        .coinbase_mock_server_port
        .expect("Coinbase mock server should be running");
    info!(
        "Coinbase mock server running on port {}",
        coinbase_mock_port
    );

    // Fund the market maker with an IMBALANCED inventory:
    // - Lots of BTC (90% of total)
    // - Little cbBTC (10% of total)
    //
    // Under the planner model, rebalance only occurs when there is blocked minority demand.
    // Inventory skew alone is not enough to trigger autonomous rebalancing.
    let total_inventory_sats = 100_000_000; // 1 BTC total
    let btc_amount_sats = 90_000_000; // 0.9 BTC (90%)
    let cbbtc_amount_sats = 10_000_000; // 0.1 BTC (10%)

    info!(
        "Setting up imbalanced inventory: BTC={} sats ({}%), cbBTC={} sats ({}%)",
        btc_amount_sats,
        (btc_amount_sats * 100) / total_inventory_sats,
        cbbtc_amount_sats,
        (cbbtc_amount_sats * 100) / total_inventory_sats
    );

    devnet
        .bitcoin
        .deal_bitcoin(
            &market_maker_account.bitcoin_wallet.address,
            &bitcoin::Amount::from_sat(btc_amount_sats),
        )
        .await
        .unwrap();

    // Also fund with some ETH for gas
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
            U256::from(cbbtc_amount_sats),
        )
        .await
        .unwrap();

    // Wait for esplora to sync
    devnet
        .bitcoin
        .wait_for_esplora_sync(Duration::from_secs(30))
        .await
        .unwrap();

    let mut service_join_set = JoinSet::new();

    // Start OTC server
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

    // Start RFQ server
    let rfq_port = get_free_port().await;
    let rfq_args = build_rfq_server_test_args(rfq_port);
    service_join_set.spawn(async move {
        rfq_server::server::run_server(rfq_args)
            .await
            .expect("RFQ server should not crash");
    });
    wait_for_rfq_server_to_be_ready(rfq_port).await;

    // Start market maker with auto_manage_inventory enabled. Even though the inventory is far
    // outside the old tolerance band, the planner should not proactively rebalance without
    // blocked minority demand.
    let mut mm_args = build_mm_test_args(
        otc_port,
        rfq_port,
        &market_maker_account,
        &devnet,
        &connect_options,
    )
    .await;

    // Override the rebalancing parameters for faster testing
    mm_args.inventory_target_ratio_bps = 5000; // 50% BTC
    mm_args.rebalance_tolerance_bps = 2500; // ±25%

    info!(
        "Starting market maker with auto_manage_inventory=true, target={}%, tolerance=±{}%",
        mm_args.inventory_target_ratio_bps / 100,
        mm_args.rebalance_tolerance_bps / 100
    );

    service_join_set.spawn(async move {
        run_market_maker(mm_args)
            .await
            .expect("Market maker should not crash");
    });

    wait_for_market_maker_to_connect_to_rfq_server(rfq_port).await;

    info!("Market maker connected, verifying no proactive rebalance occurs...");

    tokio::time::sleep(Duration::from_secs(10)).await;
    let (final_btc, final_cbbtc, final_ratio) = current_inventory(&devnet, &market_maker_account)
        .await
        .expect("Should be able to read current inventory");

    info!(
        "Final inventory without blocked demand: BTC={} sats, cbBTC={} sats, ratio={} bps",
        final_btc, final_cbbtc, final_ratio
    );

    assert!(
        final_ratio > 7500,
        "Planner should not proactively rebalance skewed inventory without blocked demand"
    );

    let btc_diff = (final_btc as i64 - btc_amount_sats as i64).abs() as u64;
    let cbbtc_diff = (final_cbbtc as i64 - cbbtc_amount_sats as i64).abs() as u64;
    assert!(
        btc_diff < btc_amount_sats / 20,
        "BTC balance should not have changed significantly (changed by {} sats)",
        btc_diff
    );
    assert!(
        cbbtc_diff < cbbtc_amount_sats / 20,
        "cbBTC balance should not have changed significantly (changed by {} sats)",
        cbbtc_diff
    );

    drop(devnet);
    service_join_set.shutdown().await;
}

#[sqlx::test]
async fn test_no_proactive_rebalance_cbbtc_to_btc_without_blocked_demand(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let market_maker_account = MultichainAccount::new(1);

    // Build devnet with Coinbase mock server enabled
    let devnet = RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .using_esplora(true)
        .with_coinbase_mock_server(WithdrawalProcessingMode::Instant)
        .build()
        .await
        .unwrap()
        .0;

    let coinbase_mock_port = devnet
        .coinbase_mock_server_port
        .expect("Coinbase mock server should be running");
    info!(
        "Coinbase mock server running on port {}",
        coinbase_mock_port
    );

    // Inverse imbalance to the prior test. The planner should still remain inert without
    // blocked BTC-side demand.
    let total_inventory_sats = 100_000_000; // 1 BTC total
    let btc_amount_sats = 10_000_000; // 0.1 BTC (10%)
    let cbbtc_amount_sats = 90_000_000; // 0.9 BTC (90%)

    info!(
        "Setting up imbalanced inventory: BTC={} sats ({}%), cbBTC={} sats ({}%)",
        btc_amount_sats,
        (btc_amount_sats * 100) / total_inventory_sats,
        cbbtc_amount_sats,
        (cbbtc_amount_sats * 100) / total_inventory_sats
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
            U256::from(100_000_000_000_000_000_000i128), // 100 ETH
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

    let mut mm_args = build_mm_test_args(
        otc_port,
        rfq_port,
        &market_maker_account,
        &devnet,
        &connect_options,
    )
    .await;

    mm_args.inventory_target_ratio_bps = 5000; // 50% BTC
    mm_args.rebalance_tolerance_bps = 2500; // ±25%

    info!(
        "Starting market maker with auto_manage_inventory=true, target={}%, tolerance=±{}%",
        mm_args.inventory_target_ratio_bps / 100,
        mm_args.rebalance_tolerance_bps / 100
    );

    service_join_set.spawn(async move {
        run_market_maker(mm_args)
            .await
            .expect("Market maker should not crash");
    });

    wait_for_market_maker_to_connect_to_rfq_server(rfq_port).await;

    info!("Market maker connected, verifying no proactive rebalance occurs...");

    tokio::time::sleep(Duration::from_secs(10)).await;
    let (final_btc, final_cbbtc, final_ratio) = current_inventory(&devnet, &market_maker_account)
        .await
        .expect("Should be able to read current inventory");

    info!(
        "Final inventory without blocked demand: BTC={} sats, cbBTC={} sats, ratio={} bps",
        final_btc, final_cbbtc, final_ratio
    );

    assert!(
        final_ratio < 2500,
        "Planner should not proactively rebalance skewed inventory without blocked demand"
    );

    let btc_diff = (final_btc as i64 - btc_amount_sats as i64).abs() as u64;
    let cbbtc_diff = (final_cbbtc as i64 - cbbtc_amount_sats as i64).abs() as u64;
    assert!(
        btc_diff < btc_amount_sats / 20,
        "BTC balance should not have changed significantly (changed by {} sats)",
        btc_diff
    );
    assert!(
        cbbtc_diff < cbbtc_amount_sats / 20,
        "cbBTC balance should not have changed significantly (changed by {} sats)",
        cbbtc_diff
    );

    drop(devnet);
    service_join_set.shutdown().await;
}

#[sqlx::test]
async fn test_no_rebalance_when_within_tolerance(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let market_maker_account = MultichainAccount::new(1);

    // Build devnet with Coinbase mock server enabled
    let devnet = RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .using_esplora(true)
        .with_coinbase_mock_server(WithdrawalProcessingMode::Instant)
        .build()
        .await
        .unwrap()
        .0;

    let coinbase_mock_port = devnet
        .coinbase_mock_server_port
        .expect("Coinbase mock server should be running");
    info!(
        "Coinbase mock server running on port {}",
        coinbase_mock_port
    );

    // Fund the market maker with a BALANCED inventory
    // 55% BTC, 45% cbBTC - within the 50% ± 25% tolerance band
    let total_inventory_sats = 100_000_000; // 1 BTC total
    let btc_amount_sats = 55_000_000; // 0.55 BTC (55%)
    let cbbtc_amount_sats = 45_000_000; // 0.45 BTC (45%)

    info!(
        "Setting up balanced inventory: BTC={} sats ({}%), cbBTC={} sats ({}%)",
        btc_amount_sats,
        (btc_amount_sats * 100) / total_inventory_sats,
        cbbtc_amount_sats,
        (cbbtc_amount_sats * 100) / total_inventory_sats
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
            U256::from(100_000_000_000_000_000_000i128), // 100 ETH
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

    // Start OTC server
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

    info!("Market maker connected, inventory should remain stable...");

    // Wait for a reasonable amount of time (2 rebalancing cycles at 5 seconds each)
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Verify inventory hasn't changed significantly
    let (final_btc_balance, final_cbbtc_balance, final_ratio) =
        current_inventory(&devnet, &market_maker_account)
            .await
            .expect("Should be able to read current inventory");

    info!(
        "Final inventory after 10 seconds (2 rebalancing cycles): BTC={} sats, cbBTC={} sats, ratio={} bps",
        final_btc_balance, final_cbbtc_balance, final_ratio
    );

    // The inventory should still be roughly the same (allowing for small fluctuations due to fees)
    // Since we're within the tolerance band, no rebalancing should have occurred
    assert!(
        final_ratio >= 2500 && final_ratio <= 7500,
        "Ratio should still be within tolerance band"
    );

    // Verify balances haven't changed by more than 5% (allowing for consolidation fees)
    let btc_diff = (final_btc_balance as i64 - btc_amount_sats as i64).abs() as u64;
    let cbbtc_diff = (final_cbbtc_balance as i64 - cbbtc_amount_sats as i64).abs() as u64;

    assert!(
        btc_diff < btc_amount_sats / 20,
        "BTC balance should not have changed significantly (changed by {} sats)",
        btc_diff
    );
    assert!(
        cbbtc_diff < cbbtc_amount_sats / 20,
        "cbBTC balance should not have changed significantly (changed by {} sats)",
        cbbtc_diff
    );

    drop(devnet);
    service_join_set.shutdown().await;
}

#[sqlx::test]
async fn test_rebalance_executes_btc_to_cbbtc_when_blocked_minority_demand_exists(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    let market_maker_account = MultichainAccount::new(1);
    let user_account = MultichainAccount::new(2);

    let devnet = RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .using_esplora(true)
        .with_coinbase_mock_server(WithdrawalProcessingMode::Instant)
        .bitcoin_mining_mode(MiningMode::Manual)
        .build()
        .await
        .unwrap()
        .0;

    devnet
        .bitcoin
        .deal_bitcoin(
            &market_maker_account.bitcoin_wallet.address,
            &bitcoin::Amount::from_sat(90_000_000),
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
            U256::from(10_000_000u64),
        )
        .await
        .unwrap();
    devnet
        .bitcoin
        .deal_bitcoin(
            &user_account.bitcoin_wallet.address,
            &bitcoin::Amount::from_sat(100_000_000),
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
    service_join_set.spawn(async move {
        run_market_maker(mm_args)
            .await
            .expect("Market maker should not crash");
    });

    wait_for_market_maker_to_connect_to_rfq_server(rfq_port).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

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

    let client = reqwest::Client::new();
    let cbbtc_address = devnet.ethereum.cbbtc_contract.address().to_string();

    let first_quote =
        request_successful_btc_to_cbbtc_quote(&client, rfq_port, &cbbtc_address, 8_000_000).await;
    let first_deposit = first_quote.from.amount;
    let first_swap = create_swap(&client, otc_port, first_quote, &user_account).await;
    deposit_into_swap(&user_bitcoin_wallet, &first_swap, first_deposit).await;
    devnet.bitcoin.mine_blocks(2).await.unwrap();
    wait_for_swap_to_be_settled(otc_port, first_swap.id).await;

    let second_quote =
        request_successful_btc_to_cbbtc_quote(&client, rfq_port, &cbbtc_address, 8_000_000).await;
    let second_deposit = second_quote.from.amount;
    let second_swap = create_swap(&client, otc_port, second_quote, &user_account).await;
    deposit_into_swap(&user_bitcoin_wallet, &second_swap, second_deposit).await;
    devnet.bitcoin.mine_blocks(2).await.unwrap();

    let bitcoin_devnet = devnet.bitcoin.clone();
    let miner_handle = tokio::spawn(async move {
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            bitcoin_devnet.mine_blocks(1).await.unwrap();
        }
    });

    wait_for_swap_to_be_settled(otc_port, second_swap.id).await;
    miner_handle.abort();

    let (final_btc, final_cbbtc, final_ratio) = current_inventory(&devnet, &market_maker_account)
        .await
        .expect("Should be able to read current inventory");
    info!(
        "Final inventory after successful blocked-demand rebalance: BTC={} sats, cbBTC={} sats, ratio={} bps",
        final_btc, final_cbbtc, final_ratio
    );

    assert!(
        final_cbbtc < 8_000_000,
        "second swap could only have settled if the planner actually rebalanced into cbBTC first"
    );

    tokio::join!(wallet_join_set.shutdown(), service_join_set.shutdown());
}
