use alloy::{primitives::U256, providers::Provider};
use devnet::{MultichainAccount, RiftDevnet};
use evm_token_indexer_client::TokenIndexerClient;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::time::Duration;
use tracing::info;

use crate::utils::PgConnectOptionsExt;

#[sqlx::test]
async fn test_indexer_client(_: PoolOptions<sqlx::Postgres>, connect_options: PgConnectOptions) {
    let to = MultichainAccount::new(1);
    let devnet = RiftDevnet::builder()
        .using_token_indexer(connect_options.to_database_url())
        .build()
        .await
        .unwrap()
        .0;
    // now mint some token

    info!("Minting CB-BTC to funded address");
    devnet
        .ethereum
        .mint_cbbtc(devnet.ethereum.funded_address, U256::from(1_000_000))
        .await
        .unwrap();

    info!("Transferring CB-BTC to to address");
    println!("to.ethereum_address: {}", to.ethereum_address);
    devnet
        .ethereum
        .cbbtc_contract
        .transfer(to.ethereum_address, U256::from(100))
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();

    // print the current block number
    let block_number = devnet
        .ethereum
        .funded_provider
        .get_block_number()
        .await
        .unwrap();
    info!("Current block number: {}", block_number);

    // print hte balance of the to address
    let balance = devnet
        .ethereum
        .cbbtc_contract
        .balanceOf(to.ethereum_address)
        .call()
        .await
        .unwrap();
    info!("Balance of to address: {}", balance);

    // Get the token indexer URL
    let indexer_url = devnet
        .ethereum
        .token_indexer
        .as_ref()
        .expect("Token indexer should be enabled")
        .api_server_url
        .clone();

    info!("Token indexer URL: {}", indexer_url);

    // Create the indexer client
    let indexer_client =
        TokenIndexerClient::new(&indexer_url).expect("Failed to create indexer client");

    // Wait for the indexer to process the transfer with retries
    info!("Waiting for token indexer to index the transfer...");
    let mut transfers = None;
    let max_retries = 10;
    let retry_delay = Duration::from_millis(500);

    for i in 0..max_retries {
        let result = indexer_client
            .get_transfers_to(to.ethereum_address, Some(1), None)
            .await;

        if let Ok(transfer_response) = result {
            if !transfer_response.transfers.is_empty() {
                info!(
                    "Found {} transfers after {} retries",
                    transfer_response.pagination.total, i
                );
                transfers = Some(transfer_response);
                break;
            }
        } else {
            info!("Result: {:?}", result);
        }

        if i < max_retries - 1 {
            tokio::time::sleep(retry_delay).await;
        }
    }

    let transfers = transfers.expect("Failed to find transfers after retries");

    // Validate that we have at least one transfer
    assert!(
        !transfers.transfers.is_empty(),
        "Expected at least one transfer"
    );

    // Get the most recent transfer (transfers are sorted by timestamp descending)
    let latest_transfer = &transfers.transfers[0];

    // Validate the transfer details
    assert_eq!(
        latest_transfer.to, to.ethereum_address,
        "Transfer 'to' address mismatch"
    );
    assert_eq!(
        latest_transfer.from, devnet.ethereum.funded_address,
        "Transfer 'from' address mismatch"
    );

    // Validate the amount - we sent 100 tokens
    let expected_amount = U256::from(100);
    let actual_amount =
        U256::from_str_radix(&latest_transfer.amount, 10).expect("Failed to parse transfer amount");

    assert_eq!(
        actual_amount, expected_amount,
        "Transfer amount mismatch: expected {}, got {}",
        expected_amount, actual_amount
    );

    info!("✅ Transfer validation successful!");
    info!("  - From: {:?}", latest_transfer.from);
    info!("  - To: {:?}", latest_transfer.to);
    info!("  - Amount: {}", latest_transfer.amount);
    info!("  - Tx Hash: {:?}", latest_transfer.transaction_hash);
    info!(
        "  - Block: {} (hash: {:?})",
        latest_transfer.block_number, latest_transfer.block_hash
    );
}
