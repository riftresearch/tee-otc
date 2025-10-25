use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::post,
    Router,
};
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
    _deposit_repository: Arc<DepositRepository>,
    bitcoin_wallet: Arc<BitcoinWallet>,
) -> ChainConsolidationResult {
    info!("Starting Bitcoin deposit consolidation");

    // Create a lot representing Bitcoin native currency
    // The consolidate method will find all available deposits
    let lot = Lot {
        currency: otc_models::Currency {
            chain: otc_models::ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        },
        amount: alloy::primitives::U256::from(1), // Amount doesn't matter for consolidation
    };

    // Use the wallet's consolidate method
    match bitcoin_wallet.consolidate(&lot, 100).await {
        Ok(summary) => {
            if summary.iterations == 0 {
                info!("No Bitcoin deposits to consolidate");
                ChainConsolidationResult {
                    chain: "bitcoin".to_string(),
                    status: "success".to_string(),
                    tx_hash: None,
                    amount: "0".to_string(),
                    deposit_count: 0,
                    error: None,
                }
            } else {
                info!(
                    "Bitcoin consolidation successful: {} iterations, {} total amount, {} txs",
                    summary.iterations,
                    summary.total_amount,
                    summary.tx_hashes.len()
                );
                ChainConsolidationResult {
                    chain: "bitcoin".to_string(),
                    status: "success".to_string(),
                    tx_hash: Some(summary.tx_hashes.join(", ")),
                    amount: summary.total_amount.to_string(),
                    deposit_count: summary.iterations,
                    error: None,
                }
            }
        }
        Err(e) => {
            error!("Failed to consolidate Bitcoin deposits: {}", e);
            ChainConsolidationResult {
                chain: "bitcoin".to_string(),
                status: "error".to_string(),
                tx_hash: None,
                amount: "0".to_string(),
                deposit_count: 0,
                error: Some(format!("Consolidation error: {}", e)),
            }
        }
    }
}

async fn consolidate_ethereum(
    deposit_repository: Arc<DepositRepository>,
    evm_wallet: Arc<EVMWallet>,
) -> ChainConsolidationResult {
    info!("Starting Ethereum deposit consolidation");

    // Peek at all available Ethereum deposits to identify unique tokens
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

    // Group deposits by token to get unique currencies
    let mut unique_currencies: std::collections::HashMap<String, otc_models::Currency> =
        std::collections::HashMap::new();

    for deposit in deposits {
        let token_key = match &deposit.holdings.currency.token {
            TokenIdentifier::Native => "native".to_string(),
            TokenIdentifier::Address(addr) => addr.clone(),
        };
        unique_currencies
            .entry(token_key)
            .or_insert_with(|| deposit.holdings.currency.clone());
    }

    let mut all_tx_hashes = Vec::new();
    let mut total_amount = alloy::primitives::U256::from(0);
    let mut total_iterations = 0;
    let mut errors = Vec::new();

    // Process each token separately
    for (token_key, currency) in unique_currencies {
        info!("Consolidating deposits for token: {}", token_key);

        let lot = Lot {
            currency: currency.clone(),
            amount: alloy::primitives::U256::from(1), // Amount doesn't matter for consolidation
        };

        // Use the wallet's consolidate method
        match evm_wallet.consolidate(&lot, 350).await {
            Ok(summary) => {
                if summary.iterations > 0 {
                    info!(
                        "Token {} consolidation successful: {} iterations, {} total amount, {} txs",
                        token_key,
                        summary.iterations,
                        summary.total_amount,
                        summary.tx_hashes.len()
                    );
                    total_amount = total_amount.saturating_add(summary.total_amount);
                    total_iterations += summary.iterations;
                    all_tx_hashes.extend(summary.tx_hashes);
                } else {
                    info!("No deposits found for token {}", token_key);
                }
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
            deposit_count: total_iterations,
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
            deposit_count: total_iterations,
            error: if errors.is_empty() {
                None
            } else {
                Some(errors.join("; "))
            },
        }
    }
}
