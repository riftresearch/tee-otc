use alloy::signers::local::PrivateKeySigner;
use alloy::{primitives::U256, providers::ext::AnvilApi};
use devnet::{MultichainAccount, RiftDevnet, WithdrawalProcessingMode};
use market_maker::{run_market_maker, wallet::Wallet, MarketMakerArgs};
use otc_models::{ChainType, Currency, Lot, QuoteRequest, TokenIdentifier};
use otc_protocols::rfq::RFQResult;
use otc_server::api::{CreateSwapRequest, CreateSwapResponse};
use reqwest::StatusCode;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tracing::{info, warn};
use uuid::Uuid;

use crate::utils::{
    build_mm_test_args, build_otc_server_test_args, build_rfq_server_test_args,
    build_test_user_ethereum_wallet, get_free_port, wait_for_market_maker_to_connect_to_rfq_server,
    wait_for_otc_server_to_be_ready, wait_for_rfq_server_to_be_ready, PgConnectOptionsExt,
};

use otc_chains::traits::Payment;

const NUM_USERS: usize = 25;
const SWAP_AMOUNT_SATS: u64 = 10_000_000; // 0.1 BTC per swap
const TOTAL_DEMAND_SATS: u64 = SWAP_AMOUNT_SATS * NUM_USERS as u64; // 2.5 BTC total
const MM_INITIAL_BTC_SATS: u64 = 25_000_000; // 0.25 BTC (10% of demand)
const MM_INITIAL_CBBTC_SATS: u64 = 25_000_000; // 0.25 BTC (symmetric with BTC)
const USER_CBBTC_FUNDING_SATS: u64 = 15_000_000; // 0.15 BTC per user (with buffer)

const TEST_TIMEOUT_SECS: u64 = 300; // 5 minutes

/// Result from a single concurrent swap attempt
struct SwapResult {
    user_index: usize,
    swap_id: Option<Uuid>,
    success: bool,
    error_message: Option<String>,
}

/// Execute a single swap for a user
async fn execute_user_swap(
    user_index: usize,
    user_account: MultichainAccount,
    devnet: Arc<RiftDevnet>,
    otc_port: u16,
    rfq_port: u16,
) -> SwapResult {
    let client = reqwest::Client::new();
    let cbbtc_address = devnet.ethereum.cbbtc_contract.address().to_string();

    // Request a quote
    let quote_request = QuoteRequest {
        input_hint: Some(U256::from(SWAP_AMOUNT_SATS)),
        from: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::Address(cbbtc_address.clone()),
            decimals: 8,
        },
        to: Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        },
    };

    let quote_response = match client
        .post(format!("http://localhost:{rfq_port}/api/v2/quote"))
        .json(&quote_request)
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(e) => {
            return SwapResult {
                user_index,
                swap_id: None,
                success: false,
                error_message: Some(format!("Quote request failed: {}", e)),
            };
        }
    };

    if quote_response.status() != 200 {
        return SwapResult {
            user_index,
            swap_id: None,
            success: false,
            error_message: Some(format!(
                "Quote request returned status {}",
                quote_response.status()
            )),
        };
    }

    let quote_response: rfq_server::server::QuoteResponse = match quote_response.json().await {
        Ok(q) => q,
        Err(e) => {
            return SwapResult {
                user_index,
                swap_id: None,
                success: false,
                error_message: Some(format!("Failed to parse quote response: {}", e)),
            };
        }
    };

    let quote = match quote_response.quote {
        Some(RFQResult::Success(q)) => q,
        Some(other) => {
            return SwapResult {
                user_index,
                swap_id: None,
                success: false,
                error_message: Some(format!("Quote not successful: {:?}", other)),
            };
        }
        None => {
            return SwapResult {
                user_index,
                swap_id: None,
                success: false,
                error_message: Some("No quote returned".to_string()),
            };
        }
    };

    // Create swap
    let swap_request = CreateSwapRequest {
        quote: quote.clone(),
        user_destination_address: user_account.bitcoin_wallet.address.to_string(),
        user_evm_account_address: user_account.ethereum_address,
        metadata: None,
    };

    let swap_response = match client
        .post(format!("http://localhost:{otc_port}/api/v2/swap"))
        .json(&swap_request)
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(e) => {
            return SwapResult {
                user_index,
                swap_id: None,
                success: false,
                error_message: Some(format!("Swap creation request failed: {}", e)),
            };
        }
    };

    if swap_response.status() != StatusCode::OK {
        let error_text = swap_response.text().await.unwrap_or_default();
        return SwapResult {
            user_index,
            swap_id: None,
            success: false,
            error_message: Some(format!("Swap creation failed: {}", error_text)),
        };
    }

    let CreateSwapResponse {
        swap_id,
        deposit_address,
        min_input,
        decimals,
        ..
    } = match swap_response.json().await {
        Ok(r) => r,
        Err(e) => {
            return SwapResult {
                user_index,
                swap_id: None,
                success: false,
                error_message: Some(format!("Failed to parse swap response: {}", e)),
            };
        }
    };

    info!("User {} created swap {}", user_index, swap_id);

    // Create user wallet and deposit cbBTC
    let (mut wallet_join_set, user_wallet) =
        build_test_user_ethereum_wallet(devnet.as_ref(), &user_account).await;

    // Ensure EIP-7702 delegation
    if let Err(e) = user_wallet
        .ensure_eip7702_delegation(
            PrivateKeySigner::from_slice(&user_account.secret_bytes).unwrap(),
        )
        .await
    {
        wallet_join_set.shutdown().await;
        return SwapResult {
            user_index,
            swap_id: Some(swap_id),
            success: false,
            error_message: Some(format!("EIP-7702 delegation failed: {}", e)),
        };
    }

    // Create and broadcast payment
    let tx_hash = match user_wallet
        .create_batch_payment(
            vec![Payment {
                lot: Lot {
                    currency: Currency {
                        chain: ChainType::Ethereum,
                        token: TokenIdentifier::Address(cbbtc_address.clone()),
                        decimals,
                    },
                    amount: min_input,
                },
                to_address: deposit_address,
            }],
            None,
        )
        .await
    {
        Ok(hash) => hash,
        Err(e) => {
            wallet_join_set.shutdown().await;
            return SwapResult {
                user_index,
                swap_id: Some(swap_id),
                success: false,
                error_message: Some(format!("Payment creation failed: {}", e)),
            };
        }
    };

    info!("User {} deposited cbBTC (tx: {})", user_index, tx_hash);

    wallet_join_set.shutdown().await;

    SwapResult {
        user_index,
        swap_id: Some(swap_id),
        success: true,
        error_message: None,
    }
}

/// Poll a swap until it reaches "Settled" status or times out
async fn wait_for_swap_settlement(
    client: &reqwest::Client,
    otc_port: u16,
    swap_id: Uuid,
    user_index: usize,
    timeout: Duration,
) -> bool {
    let start = Instant::now();
    let poll_interval = Duration::from_secs(2);

    loop {
        if start.elapsed() > timeout {
            warn!(
                "User {} swap {} timed out waiting for settlement",
                user_index, swap_id
            );
            return false;
        }

        let response = match client
            .get(format!("http://localhost:{otc_port}/api/v2/swap/{swap_id}"))
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!("Failed to query swap status for user {}: {}", user_index, e);
                tokio::time::sleep(poll_interval).await;
                continue;
            }
        };

        let swap_status: otc_server::api::SwapResponse = match response.json().await {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to parse swap status for user {}: {}", user_index, e);
                tokio::time::sleep(poll_interval).await;
                continue;
            }
        };

        if swap_status.status == "Settled" {
            info!(
                "User {} swap {} settled after {:?}",
                user_index,
                swap_id,
                start.elapsed()
            );
            return true;
        }

        if swap_status.status.contains("Failed") || swap_status.status.contains("Cancelled") {
            warn!(
                "User {} swap {} ended in state: {}",
                user_index, swap_id, swap_status.status
            );
            return false;
        }

        tokio::time::sleep(poll_interval).await;
    }
}

#[sqlx::test]
#[ignore]
async fn test_concurrent_swaps_with_aggressive_rebalancing(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) {
    info!("=== Starting Concurrent Rebalancing Integration Test ===");
    info!("Configuration:");
    info!("  - Users: {}", NUM_USERS);
    info!("  - Swap size: {} sats per user", SWAP_AMOUNT_SATS);
    info!("  - Total demand: {} sats", TOTAL_DEMAND_SATS);
    info!("  - MM initial BTC: {} sats", MM_INITIAL_BTC_SATS);
    info!("  - MM initial cbBTC: {} sats", MM_INITIAL_CBBTC_SATS);
    info!("  - Timeout: {} seconds", TEST_TIMEOUT_SECS);

    let market_maker_account = MultichainAccount::new(1);

    // Create user accounts (seeds 2-26)
    let user_accounts: Vec<MultichainAccount> = (2..=(NUM_USERS + 1))
        .map(|seed| MultichainAccount::new(seed as u32))
        .collect();

    // Build devnet with Coinbase mock server
    let devnet = RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .using_esplora(true)
        .with_coinbase_mock_server(WithdrawalProcessingMode::Instant)
        .bitcoin_mining_mode(devnet::bitcoin_devnet::MiningMode::Interval(2))
        .build()
        .await
        .unwrap()
        .0;
    let devnet = Arc::new(devnet);

    let coinbase_mock_port = devnet
        .coinbase_mock_server_port
        .expect("Coinbase mock server should be running");
    info!(
        "Coinbase mock server running on port {}",
        coinbase_mock_port
    );

    // Fund market maker with limited, symmetric inventory
    info!("Funding market maker with limited inventory...");
    devnet
        .bitcoin
        .deal_bitcoin(
            &market_maker_account.bitcoin_wallet.address,
            &bitcoin::Amount::from_sat(MM_INITIAL_BTC_SATS),
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
            U256::from(MM_INITIAL_CBBTC_SATS),
        )
        .await
        .unwrap();

    // Fund all users
    info!("Funding {} users...", NUM_USERS);
    for (i, user) in user_accounts.iter().enumerate() {
        devnet
            .ethereum
            .fund_eth_address(
                user.ethereum_address,
                U256::from(1_000_000_000_000_000_000i128), // 1 ETH
            )
            .await
            .unwrap();

        devnet
            .ethereum
            .mint_cbbtc(user.ethereum_address, U256::from(USER_CBBTC_FUNDING_SATS))
            .await
            .unwrap();

        if (i + 1) % 5 == 0 {
            info!("  Funded {} users...", i + 1);
        }
    }

    // Wait for esplora sync
    devnet
        .bitcoin
        .wait_for_esplora_sync(Duration::from_secs(30))
        .await
        .unwrap();

    let mut service_join_set = JoinSet::new();

    // Start OTC server
    info!("Starting OTC server...");
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
            panic!("OTC server crashed during startup");
        }
    }

    // Start RFQ server
    info!("Starting RFQ server...");
    let rfq_port = get_free_port().await;
    let rfq_args = build_rfq_server_test_args(rfq_port);
    service_join_set.spawn(async move {
        rfq_server::server::run_server(rfq_args)
            .await
            .expect("RFQ server should not crash");
    });
    wait_for_rfq_server_to_be_ready(rfq_port).await;

    // Start market maker with aggressive rebalancing settings
    info!("Starting market maker with aggressive rebalancing...");
    let mut mm_args = build_mm_test_args(
        otc_port,
        rfq_port,
        &market_maker_account,
        &devnet,
        &connect_options,
    )
    .await;

    // Aggressive rebalancing configuration
    mm_args.inventory_target_ratio_bps = 5000; // 50% BTC
    mm_args.rebalance_tolerance_bps = 1500; // ±15%
    mm_args.rebalance_poll_interval_secs = 3; // Check every 3 seconds

    info!("MM rebalancing config:");
    info!(
        "  - Target ratio: {}% BTC",
        mm_args.inventory_target_ratio_bps / 100
    );
    info!("  - Tolerance: ±{}%", mm_args.rebalance_tolerance_bps / 100);
    info!(
        "  - Poll interval: {}s",
        mm_args.rebalance_poll_interval_secs
    );

    service_join_set.spawn(async move {
        run_market_maker(mm_args)
            .await
            .expect("Market maker should not crash");
    });

    wait_for_market_maker_to_connect_to_rfq_server(rfq_port).await;
    info!("Market maker connected and ready");

    // Execute all swaps concurrently
    info!("Spawning {} concurrent swap tasks...", NUM_USERS);
    let swap_start = Instant::now();
    let mut swap_tasks = JoinSet::new();

    for (i, user_account) in user_accounts.into_iter().enumerate() {
        let devnet_clone = devnet.clone();
        swap_tasks.spawn(execute_user_swap(
            i,
            user_account,
            devnet_clone,
            otc_port,
            rfq_port,
        ));
    }

    // Collect swap results
    let mut swap_results = Vec::new();
    while let Some(result) = swap_tasks.join_next().await {
        match result {
            Ok(swap_result) => swap_results.push(swap_result),
            Err(e) => {
                warn!("Swap task panicked: {}", e);
            }
        }
    }

    info!("All swap tasks completed after {:?}", swap_start.elapsed());

    // Analyze swap initiation results
    let successful_initiations: Vec<_> = swap_results
        .iter()
        .filter(|r| r.success && r.swap_id.is_some())
        .collect();

    let failed_initiations: Vec<_> = swap_results.iter().filter(|r| !r.success).collect();

    info!("Swap initiation summary:");
    info!("  - Successful: {}", successful_initiations.len());
    info!("  - Failed: {}", failed_initiations.len());

    if !failed_initiations.is_empty() {
        for failure in &failed_initiations {
            warn!(
                "User {} failed: {}",
                failure.user_index,
                failure.error_message.as_deref().unwrap_or("Unknown error")
            );
        }
    }

    // Mine some blocks to confirm deposits
    info!("Mining blocks to confirm user deposits...");
    devnet
        .ethereum
        .funded_provider
        .anvil_mine(Some(10), None)
        .await
        .unwrap();

    // Wait for all successful swaps to settle
    info!(
        "Waiting for all swaps to settle (timeout: {}s)...",
        TEST_TIMEOUT_SECS
    );
    let settlement_start = Instant::now();
    let client = reqwest::Client::new();
    let timeout = Duration::from_secs(TEST_TIMEOUT_SECS);

    let mut settlement_tasks = JoinSet::new();
    for swap_result in &successful_initiations {
        if let Some(swap_id) = swap_result.swap_id {
            let client_clone = client.clone();
            let user_index = swap_result.user_index;
            settlement_tasks.spawn(async move {
                (
                    user_index,
                    swap_id,
                    wait_for_swap_settlement(&client_clone, otc_port, swap_id, user_index, timeout)
                        .await,
                )
            });
        }
    }

    let mut settled_swaps = Vec::new();
    let mut failed_settlements = Vec::new();

    while let Some(result) = settlement_tasks.join_next().await {
        match result {
            Ok((user_index, swap_id, settled)) => {
                if settled {
                    settled_swaps.push((user_index, swap_id));
                } else {
                    failed_settlements.push((user_index, swap_id));
                }

                // Log progress
                let total_complete = settled_swaps.len() + failed_settlements.len();
                if total_complete % 5 == 0 {
                    info!(
                        "Progress: {}/{} swaps completed ({} settled, {} failed)",
                        total_complete,
                        successful_initiations.len(),
                        settled_swaps.len(),
                        failed_settlements.len()
                    );
                }
            }
            Err(e) => {
                warn!("Settlement monitoring task panicked: {}", e);
            }
        }
    }

    let total_duration = settlement_start.elapsed();

    // Final summary
    info!("=== Test Complete ===");
    info!("Total duration: {:?}", total_duration);
    info!(
        "Swap initiation: {}/{} successful",
        successful_initiations.len(),
        NUM_USERS
    );
    info!(
        "Swap settlement: {}/{} successful",
        settled_swaps.len(),
        successful_initiations.len()
    );

    if !failed_settlements.is_empty() {
        warn!("Failed settlements:");
        for (user_index, swap_id) in &failed_settlements {
            warn!("  - User {} swap {}", user_index, swap_id);
        }
    }

    // Assertions
    assert_eq!(
        successful_initiations.len(),
        NUM_USERS,
        "All {} users should successfully initiate swaps",
        NUM_USERS
    );

    assert_eq!(
        settled_swaps.len(),
        NUM_USERS,
        "All {} swaps should settle successfully. {} failed to settle.",
        NUM_USERS,
        failed_settlements.len()
    );

    info!("✓ All swaps completed successfully!");
    info!("✓ Market maker successfully rebalanced to fulfill all orders!");

    drop(devnet);
    service_join_set.shutdown().await;
}
