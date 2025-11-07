use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, U256};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use bitcoin::Address as BitcoinAddress;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::{info, warn};
use uuid::Uuid;

use bitcoincore_rpc_async::RpcApi;
use chrono::Utc;

use crate::{bitcoin_devnet::BitcoinDevnet, evm_devnet::EthDevnet, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoinbaseAccount {
    pub id: String,
    pub currency: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateAddressRequest {
    pub network: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateAddressResponse {
    pub address: String,
    pub network: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithdrawalRequest {
    pub amount: String,
    pub currency: String,
    pub crypto_address: String,
    pub network: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithdrawalResponse {
    pub id: String,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct Withdrawal {
    pub id: String,
    pub amount_sats: u64,
    pub recipient_address: String,
    pub network: String,
    pub status: WithdrawalState,
    pub created_at: String,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum WithdrawalState {
    /// Pending manual approval (tx not yet processed)
    Pending,
    /// Processing with automatic completion at specified time (tx not yet processed)
    Processing { completion_time: std::time::Instant },
    /// Completed withdrawal (tx has been processed on-chain)
    Completed { tx_hash: String, completed_at: String },
    /// Cancelled withdrawal
    Cancelled,
}

#[derive(Debug, Clone, Copy)]
pub enum WithdrawalProcessingMode {
    /// Withdrawals complete instantly
    Instant,
    /// Withdrawals complete after a fixed delay
    Fixed(Duration),
    /// Realistic delays: BTC ~15 min, cbBTC ~45 sec
    Realistic,
    /// Withdrawals stay pending until admin manually approves them
    Manual,
}

impl Default for WithdrawalProcessingMode {
    fn default() -> Self {
        Self::Instant
    }
}

#[derive(Clone)]
struct AppState {
    bitcoin_devnet: Arc<BitcoinDevnet>,
    ethereum_devnet: Arc<EthDevnet>,
    btc_account_id: String,
    deposit_addresses: Arc<DashMap<String, String>>, // network -> address
    withdrawals: Arc<DashMap<String, Withdrawal>>,
    processing_mode: WithdrawalProcessingMode,
}

impl AppState {
    fn new(
        bitcoin_devnet: Arc<BitcoinDevnet>,
        ethereum_devnet: Arc<EthDevnet>,
        processing_mode: WithdrawalProcessingMode,
    ) -> Self {
        Self {
            bitcoin_devnet,
            ethereum_devnet,
            btc_account_id: Uuid::new_v4().to_string(),
            deposit_addresses: Arc::new(DashMap::new()),
            withdrawals: Arc::new(DashMap::new()),
            processing_mode,
        }
    }
}

/// Fixed port for Coinbase mock server in interactive mode
pub const COINBASE_MOCK_SERVER_PORT: u16 = 8080;

pub struct CoinbaseMockServer {
    pub processing_mode: WithdrawalProcessingMode,
}

impl CoinbaseMockServer {
    pub fn new() -> Self {
        Self {
            processing_mode: WithdrawalProcessingMode::default(),
        }
    }

    pub fn with_processing_mode(mut self, mode: WithdrawalProcessingMode) -> Self {
        self.processing_mode = mode;
        self
    }

    pub async fn start(
        &self,
        listener: tokio::net::TcpListener,
        bitcoin_devnet: Arc<BitcoinDevnet>,
        ethereum_devnet: Arc<EthDevnet>,
    ) -> Result<tokio::task::JoinHandle<Result<()>>> {
        let state = AppState::new(bitcoin_devnet, ethereum_devnet, self.processing_mode);
        let state_for_task = state.clone();

        let addr = listener.local_addr()
            .map_err(|e| eyre::eyre!("Failed to get local address: {}", e))?;
        info!("Coinbase mock server starting on {}", addr);

        // Spawn background task to process withdrawal state transitions
        tokio::spawn(async move {
            process_withdrawal_states(state_for_task).await;
        });

        let app = axum::Router::new()
            .route("/coinbase-accounts", axum::routing::get(get_accounts))
            .route(
                "/coinbase-accounts/:account_id/addresses",
                axum::routing::post(create_deposit_address),
            )
            .route("/withdrawals/crypto", axum::routing::post(create_withdrawal))
            .route("/transfers/:withdrawal_id", axum::routing::get(get_transfer_status))
            .route("/admin/process-withdrawals", axum::routing::post(admin_process_withdrawals))
            .with_state(state);

        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .map_err(|e| eyre::eyre!("Coinbase mock server error: {}", e))?;
            Ok(())
        });

        Ok(handle)
    }
}

impl Default for CoinbaseMockServer {
    fn default() -> Self {
        Self::new()
    }
}

/// Execute a withdrawal transaction on the devnet
async fn execute_withdrawal(
    state: &AppState,
    network: &str,
    crypto_address: &str,
    amount_sats: u64,
) -> Result<String, (StatusCode, Json<serde_json::Value>)> {
    // Detect address type to determine actual destination
    // This simulates Coinbase's automatic BTC->cbBTC conversion when withdrawing to Ethereum
    let is_ethereum_address = Address::from_str(crypto_address).is_ok();
    
    match (network, is_ethereum_address) {
        // Bitcoin network with Bitcoin address -> send native BTC
        ("bitcoin", false) => {
            let recipient_addr = BitcoinAddress::from_str(crypto_address)
                .expect("Address already validated")
                .assume_checked();

            match state
                .bitcoin_devnet
                .deal_bitcoin(&recipient_addr, &bitcoin::Amount::from_sat(amount_sats))
                .await
            {
                Ok(tx) => Ok(tx.txid.to_string()),
                Err(e) => {
                    warn!("Failed to process Bitcoin withdrawal: {}", e);
                    Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": "Failed to process withdrawal"})),
                    ))
                }
            }
        }
        // Bitcoin network with Ethereum address OR Ethereum network -> mint cbBTC
        ("bitcoin", true) | ("ethereum", _) => {
            let recipient_addr = Address::from_str(crypto_address)
                .expect("Address already validated");

            match state
                .ethereum_devnet
                .mint_cbbtc(recipient_addr, U256::from(amount_sats))
                .await
            {
                Ok(tx_hash) => Ok(tx_hash),
                Err(e) => {
                    warn!("Failed to process cbBTC withdrawal: {}", e);
                    Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": "Failed to process withdrawal"})),
                    ))
                }
            }
        }
        _ => unreachable!(),
    }
}

async fn get_accounts(State(state): State<AppState>) -> impl IntoResponse {
    let account = CoinbaseAccount {
        id: state.btc_account_id.clone(),
        currency: "BTC".to_string(),
    };
    (StatusCode::OK, Json(json!([account])))
}

async fn create_deposit_address(
    State(state): State<AppState>,
    Path(account_id): Path<String>,
    Json(req): Json<CreateAddressRequest>,
) -> impl IntoResponse {
    if account_id != state.btc_account_id {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "Account not found"})),
        );
    }

    let network = req.network.to_lowercase();
    let address_key = format!("{}_{}", account_id, network);

    // Check if we already have an address for this network
    if let Some(addr) = state.deposit_addresses.get(&address_key) {
        return (
            StatusCode::OK,
            Json(json!(CreateAddressResponse {
                address: addr.clone(),
                network: req.network.clone(),
            })),
        );
    }

    // Generate new address based on network
    let address = match network.as_str() {
        "bitcoin" => {
            // Generate Bitcoin address using RPC
            match state
                .bitcoin_devnet
                .rpc_client
                .get_new_address(None, None)
                .await
            {
                Ok(addr) => addr.assume_checked().to_string(),
                Err(e) => {
                    warn!("Failed to generate Bitcoin address: {}", e);
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": "Failed to generate address"})),
                    );
                }
            }
        }
        "ethereum" => {
            // Generate Ethereum address - use a simple random address
            // For devnet, we can use any valid address format
            use rand::Rng;
            let mut rng = rand::thread_rng();
            let bytes: [u8; 20] = rng.gen();
            let random_addr = Address::from(bytes);
            format!("{:?}", random_addr)
        }
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("Unsupported network: {}", network)})),
            );
        }
    };

    state.deposit_addresses.insert(address_key, address.clone());

    (
        StatusCode::OK,
        Json(json!(CreateAddressResponse {
            address,
            network: req.network.clone(),
        })),
    )
}

async fn create_withdrawal(
    State(state): State<AppState>,
    Json(req): Json<WithdrawalRequest>,
) -> impl IntoResponse {
    if req.currency != "BTC" {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Only BTC currency is supported"})),
        );
    }

    let network = req.network.to_lowercase();
    if network != "bitcoin" && network != "ethereum" {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": format!("Unsupported network: {}", network)})),
        );
    }

    // Parse amount from BTC string to satoshis
    let amount_btc: f64 = match req.amount.parse() {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "Invalid amount format"})),
            );
        }
    };
    let amount_sats = (amount_btc * 100_000_000.0) as u64;

    if amount_sats == 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Invalid amount"})),
        );
    }

    // Validate address format - accept both Bitcoin and Ethereum addresses
    // This simulates Coinbase's feature where you can withdraw BTC to an Ethereum address
    // (which automatically converts to cbBTC)
    let is_bitcoin_address = BitcoinAddress::from_str(&req.crypto_address).is_ok();
    let is_ethereum_address = Address::from_str(&req.crypto_address).is_ok();
    
    if !is_bitcoin_address && !is_ethereum_address {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Invalid address format"})),
        );
    }

    let withdrawal_id = Uuid::new_v4().to_string();
    let created_at = Utc::now().to_rfc3339();

    // Determine delay based on configuration
    let delay = match state.processing_mode {
        WithdrawalProcessingMode::Instant => Duration::from_secs(0),
        WithdrawalProcessingMode::Fixed(d) => d,
        WithdrawalProcessingMode::Realistic => match network.as_str() {
            "bitcoin" => Duration::from_secs(15 * 60), // 15 minutes
            "ethereum" => Duration::from_secs(45),      // 45 seconds
            _ => Duration::from_secs(0),
        },
        WithdrawalProcessingMode::Manual => Duration::from_secs(0), // Will stay pending
    };

    // Create withdrawal with appropriate state
    let withdrawal = match state.processing_mode {
        WithdrawalProcessingMode::Instant => {
            // For Instant mode, process the transaction immediately
            let tx_hash = match execute_withdrawal(&state, &network, &req.crypto_address, amount_sats).await {
                Ok(hash) => hash,
                Err(response) => return response,
            };
            
            Withdrawal {
                id: withdrawal_id.clone(),
                amount_sats,
                recipient_address: req.crypto_address.clone(),
                network: req.network.clone(),
                status: WithdrawalState::Completed {
                    tx_hash: tx_hash.clone(),
                    completed_at: Utc::now().to_rfc3339(),
                },
                created_at,
            }
        },
        WithdrawalProcessingMode::Manual => Withdrawal {
            id: withdrawal_id.clone(),
            amount_sats,
            recipient_address: req.crypto_address.clone(),
            network: req.network.clone(),
            status: WithdrawalState::Pending,
            created_at,
        },
        WithdrawalProcessingMode::Fixed(_) | WithdrawalProcessingMode::Realistic => Withdrawal {
            id: withdrawal_id.clone(),
            amount_sats,
            recipient_address: req.crypto_address.clone(),
            network: req.network.clone(),
            status: WithdrawalState::Processing {
                completion_time: std::time::Instant::now() + delay,
            },
            created_at,
        },
    };

    state.withdrawals.insert(withdrawal_id.clone(), withdrawal.clone());

    info!(
        "Created withdrawal {}: {} sats to {} on {} (mode: {:?})",
        withdrawal_id, amount_sats, req.crypto_address, network, state.processing_mode
    );

    (
        StatusCode::OK,
        Json(json!(WithdrawalResponse { id: withdrawal_id })),
    )
}

async fn get_transfer_status(
    State(state): State<AppState>,
    Path(withdrawal_id): Path<String>,
) -> impl IntoResponse {
    let withdrawal = match state.withdrawals.get(&withdrawal_id) {
        Some(w) => w.clone(),
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "Transfer not found"})),
            );
        }
    };

    let response = match &withdrawal.status {
        WithdrawalState::Pending => json!({
            "id": withdrawal.id,
            "cancelled": serde_json::Value::Null,
            "completed_at": serde_json::Value::Null,
            "details": {
                "crypto_transaction_hash": serde_json::Value::Null
            }
        }),
        WithdrawalState::Processing { .. } => json!({
            "id": withdrawal.id,
            "cancelled": serde_json::Value::Null,
            "completed_at": serde_json::Value::Null,
            "details": {
                "crypto_transaction_hash": serde_json::Value::Null
            }
        }),
        WithdrawalState::Completed { tx_hash, completed_at } => json!({
            "id": withdrawal.id,
            "cancelled": serde_json::Value::Null,
            "completed_at": completed_at,
            "details": {
                "crypto_transaction_hash": tx_hash
            }
        }),
        WithdrawalState::Cancelled => json!({
            "id": withdrawal.id,
            "cancelled": "true",
            "completed_at": serde_json::Value::Null,
            "details": {
                "crypto_transaction_hash": serde_json::Value::Null
            }
        }),
    };

    (StatusCode::OK, Json(response))
}

/// Admin endpoint to manually process pending withdrawals
async fn admin_process_withdrawals(State(state): State<AppState>) -> impl IntoResponse {
    let mut processed = Vec::new();
    let mut failed = Vec::new();
    
    // First, collect all pending withdrawals
    let pending: Vec<_> = state.withdrawals.iter()
        .filter_map(|entry| {
            if matches!(entry.value().status, WithdrawalState::Pending) {
                Some((entry.key().clone(), entry.value().clone()))
            } else {
                None
            }
        })
        .collect();
    
    // Process each pending withdrawal
    for (withdrawal_id, withdrawal) in pending {
        // Execute the withdrawal transaction
        match execute_withdrawal(
            &state,
            &withdrawal.network,
            &withdrawal.recipient_address,
            withdrawal.amount_sats,
        ).await {
            Ok(tx_hash) => {
                // Update withdrawal to completed
                if let Some(mut entry) = state.withdrawals.get_mut(&withdrawal_id) {
                    let completed_at = Utc::now().to_rfc3339();
                    entry.status = WithdrawalState::Completed {
                        tx_hash,
                        completed_at,
                    };
                    processed.push(withdrawal_id);
                }
            }
            Err(e) => {
                warn!("Failed to execute withdrawal {}: {:?}", withdrawal_id, e);
                failed.push(withdrawal_id);
            }
        }
    }
    
    info!("Admin processed {} withdrawals, {} failed", processed.len(), failed.len());
    
    (
        StatusCode::OK,
        Json(json!({
            "processed": processed.len(),
            "failed": failed.len(),
            "withdrawal_ids": processed,
            "failed_ids": failed
        })),
    )
}

/// Background task that processes withdrawal state transitions
async fn process_withdrawal_states(state: AppState) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    
    loop {
        interval.tick().await;
        
        // Collect withdrawals ready to be processed
        let ready_to_process: Vec<_> = state.withdrawals.iter()
            .filter_map(|entry| {
                if let WithdrawalState::Processing { completion_time } = &entry.value().status {
                    if std::time::Instant::now() >= *completion_time {
                        Some((entry.key().clone(), entry.value().clone()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
        
        // Execute and transition each ready withdrawal
        for (withdrawal_id, withdrawal) in ready_to_process {
            match execute_withdrawal(
                &state,
                &withdrawal.network,
                &withdrawal.recipient_address,
                withdrawal.amount_sats,
            ).await {
                Ok(tx_hash) => {
                    if let Some(mut entry) = state.withdrawals.get_mut(&withdrawal_id) {
                        let completed_at = Utc::now().to_rfc3339();
                        entry.status = WithdrawalState::Completed {
                            tx_hash,
                            completed_at,
                        };
                        info!("Auto-processed withdrawal {} with tx {}", withdrawal_id, entry.status.tx_hash().unwrap_or("none"));
                    }
                }
                Err(e) => {
                    warn!("Failed to auto-process withdrawal {}: {:?}", withdrawal_id, e);
                    // Keep it in Processing state, will retry on next tick
                }
            }
        }
    }
}

impl WithdrawalState {
    fn tx_hash(&self) -> Option<&str> {
        match self {
            WithdrawalState::Completed { tx_hash, .. } => Some(tx_hash),
            _ => None,
        }
    }
}
