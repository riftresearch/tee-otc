use alloy::primitives::U256;
use devnet::{MultichainAccount, RiftDevnet, WithdrawalProcessingMode};
use market_maker::{run_market_maker, MarketMakerArgs};
use otc_models::TokenIdentifier;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::info;

use crate::utils::{
    build_bitcoin_wallet_descriptor, build_mm_test_args, build_otc_server_test_args,
    build_rfq_server_test_args, build_tmp_bitcoin_wallet_db_file, get_free_port,
    wait_for_market_maker_to_connect_to_rfq_server, wait_for_otc_server_to_be_ready,
    wait_for_rfq_server_to_be_ready, PgConnectOptionsExt,
};

/// Helper to calculate BTC ratio in basis points
fn calculate_btc_ratio_bps(btc_sats: u64, cbbtc_sats: u64) -> u64 {
    let total = btc_sats.saturating_add(cbbtc_sats);
    if total == 0 {
        return 0;
    }
    ((btc_sats as u128 * 10_000) / total as u128).min(10_000) as u64
}

/// Helper to wait for rebalancing to complete by polling the wallet balances
async fn wait_for_rebalancing(
    devnet: &RiftDevnet,
    mm_account: &MultichainAccount,
    target_ratio_bps: u64,
    tolerance_bps: u64,
    timeout: Duration,
) -> Result<(u64, u64, u64), String> {
    let start = std::time::Instant::now();
    let poll_interval = Duration::from_secs(2);

    info!(
        "Waiting for rebalancing to target ratio {} bps with tolerance {} bps",
        target_ratio_bps, tolerance_bps
    );

    loop {
        if start.elapsed() > timeout {
            return Err(format!(
                "Timeout waiting for rebalancing after {:?}",
                timeout
            ));
        }

        // Get BTC balance for the specific market maker address using esplora scripthash
        // (can't use get_balance() because it returns the entire wallet balance including miner rewards)
        let btc_sats = if let Some(ref esplora_client) = devnet.bitcoin.esplora_client {
            use bitcoin::ScriptBuf;
            let script_pubkey: ScriptBuf = mm_account.bitcoin_wallet.address.script_pubkey();
            // Wait for esplora to sync
            tokio::time::sleep(Duration::from_millis(500)).await;
            
            // Get all UTXOs for this address and sum them
            match esplora_client.scripthash_txs(&script_pubkey, None).await {
                Ok(txs) => {
                    let mut balance = 0u64;
                    // For each transaction, check outputs to our address
                    for tx in txs {
                        for (vout, output) in tx.vout.iter().enumerate() {
                            if output.scriptpubkey == script_pubkey {
                                // Check if this output is spent
                                let outpoint = bitcoin::OutPoint {
                                    txid: tx.txid,
                                    vout: vout as u32,
                                };
                                let is_spent = esplora_client.get_output_status(&outpoint.txid, outpoint.vout as u64)
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

        // Get cbBTC balance
        let cbbtc_balance = devnet
            .ethereum
            .cbbtc_contract
            .balanceOf(mm_account.ethereum_address)
            .call()
            .await
            .map_err(|e| format!("Failed to get cbBTC balance: {}", e))?;
        let cbbtc_sats = cbbtc_balance.to::<u64>();

        let current_ratio_bps = calculate_btc_ratio_bps(btc_sats, cbbtc_sats);
        let lower_bound = target_ratio_bps.saturating_sub(tolerance_bps);
        let upper_bound = (target_ratio_bps + tolerance_bps).min(10_000);

        info!(
            "Current inventory: BTC={} sats, cbBTC={} sats, ratio={} bps (target: {} bps, bounds: [{}, {}])",
            btc_sats, cbbtc_sats, current_ratio_bps, target_ratio_bps, lower_bound, upper_bound
        );

        // Check if we're within the tolerance band
        if current_ratio_bps >= lower_bound && current_ratio_bps <= upper_bound {
            info!(
                "Rebalancing complete! Final ratio: {} bps (within [{}, {}])",
                current_ratio_bps, lower_bound, upper_bound
            );
            return Ok((btc_sats, cbbtc_sats, current_ratio_bps));
        }

        tokio::time::sleep(poll_interval).await;
    }
}

#[sqlx::test]
async fn test_auto_rebalance_btc_to_cbbtc(
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
    // This should trigger rebalancing from BTC -> cbBTC
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

    // Start market maker with auto_manage_inventory enabled
    // Target: 50% BTC / 50% cbBTC (5000 bps)
    // Tolerance: ±25% (2500 bps), so triggers when ratio is outside [2500, 7500]
    // Current ratio is 90% BTC, which is way outside the band, so should trigger rebalance
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

    info!("Market maker connected, waiting for auto-rebalancing to occur...");

    // Wait for rebalancing to complete
    // The market maker polls every 5 seconds in tests (configured in build_mm_test_args)
    // Give it 5 minutes to complete (includes time for deposits to confirm and withdrawals to process)
    let (final_btc, final_cbbtc, final_ratio) = wait_for_rebalancing(
        &devnet,
        &market_maker_account,
        5000, // target 50%
        2500, // tolerance ±25%
        Duration::from_secs(300),
    )
    .await
    .expect("Rebalancing should complete within timeout");

    info!(
        "Rebalancing successful! Final inventory: BTC={} sats, cbBTC={} sats, ratio={} bps",
        final_btc, final_cbbtc, final_ratio
    );

    // Verify the final ratio is within the expected tolerance band
    assert!(
        final_ratio >= 2500 && final_ratio <= 7500,
        "Final ratio {} bps should be within tolerance band [2500, 7500]",
        final_ratio
    );

    drop(devnet);
    service_join_set.shutdown().await;
}

#[sqlx::test]
async fn test_auto_rebalance_cbbtc_to_btc(
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

    // Fund the market maker with an IMBALANCED inventory (opposite direction):
    // - Little BTC (10% of total)
    // - Lots of cbBTC (90% of total)
    // This should trigger rebalancing from cbBTC -> BTC
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

    // Fund with ETH for gas
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

    // Start market maker with auto_manage_inventory enabled
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

    info!("Market maker connected, waiting for auto-rebalancing to occur...");

    // Wait for rebalancing to complete
    let (final_btc, final_cbbtc, final_ratio) = wait_for_rebalancing(
        &devnet,
        &market_maker_account,
        5000, // target 50%
        2500, // tolerance ±25%
        Duration::from_secs(300),
    )
    .await
    .expect("Rebalancing should complete within timeout");

    info!(
        "Rebalancing successful! Final inventory: BTC={} sats, cbBTC={} sats, ratio={} bps",
        final_btc, final_cbbtc, final_ratio
    );

    // Verify the final ratio is within the expected tolerance band
    assert!(
        final_ratio >= 2500 && final_ratio <= 7500,
        "Final ratio {} bps should be within tolerance band [2500, 7500]",
        final_ratio
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
    let final_btc_balance = if let Some(ref esplora_client) = devnet.bitcoin.esplora_client {
        use bitcoin::ScriptBuf;
        let script_pubkey: ScriptBuf = market_maker_account.bitcoin_wallet.address.script_pubkey();
        tokio::time::sleep(Duration::from_millis(500)).await;
        
        // Get all UTXOs for this address and sum them
        match esplora_client.scripthash_txs(&script_pubkey, None).await {
            Ok(txs) => {
                let mut balance = 0u64;
                // For each transaction, check outputs to our address
                for tx in txs {
                    for (vout, output) in tx.vout.iter().enumerate() {
                        if output.scriptpubkey == script_pubkey {
                            // Check if this output is spent
                            let outpoint = bitcoin::OutPoint {
                                txid: tx.txid,
                                vout: vout as u32,
                            };
                            let is_spent = esplora_client.get_output_status(&outpoint.txid, outpoint.vout as u64)
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
            Err(e) => panic!("Failed to get BTC balance from esplora: {}", e),
        }
    } else {
        panic!("Esplora client not available");
    };

    let final_cbbtc_balance = devnet
        .ethereum
        .cbbtc_contract
        .balanceOf(market_maker_account.ethereum_address)
        .call()
        .await
        .unwrap()
        .to::<u64>();

    let final_ratio = calculate_btc_ratio_bps(final_btc_balance, final_cbbtc_balance);

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

