use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::post,
    Router,
};
use otc_chains::traits::Payment;
use otc_models::{Lot, TokenIdentifier};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::{
    bitcoin_wallet::BitcoinWallet,
    db::{DepositRepository, DepositStore},
    evm_wallet::EVMWallet,
    wallet::Wallet,
};

#[derive(Clone)]
pub struct AdminApiState {
    pub deposit_repository: Arc<DepositRepository>,
    pub bitcoin_wallet: Arc<BitcoinWallet>,
    pub evm_wallet: Arc<EVMWallet>,
}

#[derive(Debug, Deserialize)]
pub struct ConsolidateRequest {
    pub chain: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ConsolidateResponse {
    pub results: Vec<ChainConsolidationResult>,
}

#[derive(Debug, Serialize)]
pub struct ChainConsolidationResult {
    pub chain: String,
    pub status: String,
    pub tx_hash: Option<String>,
    pub amount: String,
    pub deposit_count: usize,
    pub error: Option<String>,
}

pub fn create_admin_router(state: AdminApiState) -> Router {
    Router::new()
        .route("/consolidate", post(consolidate_handler))
        .with_state(state)
}

async fn consolidate_handler(
    State(state): State<AdminApiState>,
    Query(request): Query<ConsolidateRequest>,
) -> impl IntoResponse {
    info!(
        "Consolidation request received for chain: {:?}",
        request.chain
    );

    let mut results = Vec::new();

    // Determine which chains to consolidate
    let chains_to_process = if let Some(ref chain) = request.chain {
        vec![chain.to_lowercase()]
    } else {
        vec!["bitcoin".to_string(), "ethereum".to_string()]
    };

    for chain in chains_to_process {
        let result = match chain.as_str() {
            "bitcoin" => {
                consolidate_bitcoin(
                    state.deposit_repository.clone(),
                    state.bitcoin_wallet.clone(),
                )
                .await
            }
            "ethereum" => {
                consolidate_ethereum(
                    state.deposit_repository.clone(),
                    state.evm_wallet.clone(),
                )
                .await
            }
            _ => ChainConsolidationResult {
                chain: chain.clone(),
                status: "error".to_string(),
                tx_hash: None,
                amount: "0".to_string(),
                deposit_count: 0,
                error: Some(format!("Unknown chain: {}", chain)),
            },
        };
        results.push(result);
    }

    (StatusCode::OK, Json(ConsolidateResponse { results }))
}

async fn consolidate_bitcoin(
    deposit_repository: Arc<DepositRepository>,
    bitcoin_wallet: Arc<BitcoinWallet>,
) -> ChainConsolidationResult {
    info!("Starting Bitcoin deposit consolidation");

    // Peek at all available Bitcoin deposits (read-only)
    let deposits = match deposit_repository
        .peek_all_available_deposits(Some("bitcoin".to_string()))
        .await
    {
        Ok(deps) => deps,
        Err(e) => {
            error!("Failed to fetch Bitcoin deposits: {}", e);
            return ChainConsolidationResult {
                chain: "bitcoin".to_string(),
                status: "error".to_string(),
                tx_hash: None,
                amount: "0".to_string(),
                deposit_count: 0,
                error: Some(format!("Database error: {}", e)),
            };
        }
    };

    if deposits.is_empty() {
        info!("No Bitcoin deposits to consolidate");
        return ChainConsolidationResult {
            chain: "bitcoin".to_string(),
            status: "success".to_string(),
            tx_hash: None,
            amount: "0".to_string(),
            deposit_count: 0,
            error: None,
        };
    }

    info!("Found {} Bitcoin deposits to consolidate", deposits.len());

    // Sum up total amount and get currency from first deposit
    let mut total_amount = alloy::primitives::U256::from(0);
    let currency = deposits[0].holdings.currency.clone();

    for deposit in &deposits {
        total_amount = total_amount.saturating_add(deposit.holdings.amount);
    }

    // Get the wallet's receive address
    let receive_address = bitcoin_wallet.receive_address(&TokenIdentifier::Native);

    // Create a single payment to ourselves for the total amount
    let payment = Payment {
        lot: Lot {
            currency,
            amount: total_amount,
        },
        to_address: receive_address,
    };

    info!(
        "Creating consolidation payment for {} sats to {}",
        total_amount, payment.to_address
    );

    // Use the existing payment infrastructure to handle consolidation
    // This will automatically pull from deposit keys via take_deposits_that_fill_lot
    match bitcoin_wallet
        .create_batch_payment(vec![payment], None)
        .await
    {
        Ok(tx_hash) => {
            info!("Bitcoin consolidation successful: {}", tx_hash);
            ChainConsolidationResult {
                chain: "bitcoin".to_string(),
                status: "success".to_string(),
                tx_hash: Some(tx_hash),
                amount: total_amount.to_string(),
                deposit_count: deposits.len(),
                error: None,
            }
        }
        Err(e) => {
            error!("Failed to create Bitcoin consolidation payment: {}", e);
            ChainConsolidationResult {
                chain: "bitcoin".to_string(),
                status: "error".to_string(),
                tx_hash: None,
                amount: total_amount.to_string(),
                deposit_count: deposits.len(),
                error: Some(format!("Payment error: {}", e)),
            }
        }
    }
}

async fn consolidate_ethereum(
    deposit_repository: Arc<DepositRepository>,
    evm_wallet: Arc<EVMWallet>,
) -> ChainConsolidationResult {
    info!("Starting Ethereum deposit consolidation");

    // Peek at all available Ethereum deposits (read-only)
    let deposits = match deposit_repository
        .peek_all_available_deposits(Some("ethereum".to_string()))
        .await
    {
        Ok(deps) => deps,
        Err(e) => {
            error!("Failed to fetch Ethereum deposits: {}", e);
            return ChainConsolidationResult {
                chain: "ethereum".to_string(),
                status: "error".to_string(),
                tx_hash: None,
                amount: "0".to_string(),
                deposit_count: 0,
                error: Some(format!("Database error: {}", e)),
            };
        }
    };

    if deposits.is_empty() {
        info!("No Ethereum deposits to consolidate");
        return ChainConsolidationResult {
            chain: "ethereum".to_string(),
            status: "success".to_string(),
            tx_hash: None,
            amount: "0".to_string(),
            deposit_count: 0,
            error: None,
        };
    }

    info!("Found {} Ethereum deposits to consolidate", deposits.len());

    // Group deposits by token
    let mut deposits_by_token: std::collections::HashMap<String, Vec<crate::db::Deposit>> =
        std::collections::HashMap::new();

    for deposit in deposits {
        let token_key = match &deposit.holdings.currency.token {
            TokenIdentifier::Native => "native".to_string(),
            TokenIdentifier::Address(addr) => addr.clone(),
        };
        deposits_by_token
            .entry(token_key)
            .or_insert_with(Vec::new)
            .push(deposit);
    }

    let mut all_tx_hashes = Vec::new();
    let mut total_amount = alloy::primitives::U256::from(0);
    let mut total_deposit_count = 0;
    let mut errors = Vec::new();

    // Get the wallet's receive address
    let receive_address = evm_wallet.receive_address(&TokenIdentifier::Native);

    // Process each token group
    for (token_key, token_deposits) in deposits_by_token {
        info!(
            "Consolidating {} deposits for token: {}",
            token_deposits.len(),
            token_key
        );

        total_deposit_count += token_deposits.len();

        // Sum up total for this token
        let mut token_total = alloy::primitives::U256::from(0);
        let currency = token_deposits[0].holdings.currency.clone();

        for deposit in &token_deposits {
            token_total = token_total.saturating_add(deposit.holdings.amount);
        }

        total_amount = total_amount.saturating_add(token_total);

        // Create a single payment to ourselves for this token
        let payment = Payment {
            lot: Lot {
                currency,
                amount: token_total,
            },
            to_address: receive_address.clone(),
        };

        info!(
            "Creating consolidation payment for token {} amount {} to {}",
            token_key, token_total, payment.to_address
        );

        // Use the existing payment infrastructure
        match evm_wallet
            .create_batch_payment(vec![payment], None)
            .await
        {
            Ok(tx_hash) => {
                info!("Token {} consolidation successful: {}", token_key, tx_hash);
                all_tx_hashes.push(tx_hash);
            }
            Err(e) => {
                error!("Failed to consolidate token {}: {}", token_key, e);
                errors.push(format!("{}: {}", token_key, e));
            }
        }
    }

    if all_tx_hashes.is_empty() && !errors.is_empty() {
        ChainConsolidationResult {
            chain: "ethereum".to_string(),
            status: "error".to_string(),
            tx_hash: None,
            amount: total_amount.to_string(),
            deposit_count: total_deposit_count,
            error: Some(errors.join("; ")),
        }
    } else {
        ChainConsolidationResult {
            chain: "ethereum".to_string(),
            status: if errors.is_empty() {
                "success".to_string()
            } else {
                "partial".to_string()
            },
            tx_hash: if all_tx_hashes.is_empty() {
                None
            } else {
                Some(all_tx_hashes.join(", "))
            },
            amount: total_amount.to_string(),
            deposit_count: total_deposit_count,
            error: if errors.is_empty() {
                None
            } else {
                Some(errors.join("; "))
            },
        }
    }
}
