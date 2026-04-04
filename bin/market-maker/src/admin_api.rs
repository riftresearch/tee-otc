use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use alloy::primitives::U256;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use otc_models::{ChainType, Lot, TokenIdentifier};
use serde::{Deserialize, Serialize};
use tracing::{error, info};
use uuid::Uuid;

use crate::{
    db::{BatchStatus, DepositRepository, DepositStore, PaymentRepository},
    liquidity_lock::LiquidityLockManager,
    payment_manager::PaymentManager,
    planner::PlannerService,
    wallet::{ConsolidationSummary, WalletManager},
};

#[derive(Clone)]
pub struct AdminApiState {
    pub deposit_repository: Arc<DepositRepository>,
    pub payment_repository: Arc<PaymentRepository>,
    pub payment_manager: Arc<PaymentManager>,
    pub planner_service: Arc<PlannerService>,
    pub liquidity_lock_manager: Arc<LiquidityLockManager>,
    pub wallet_manager: Arc<WalletManager>,
    pub quoting_enabled: Arc<AtomicBool>,
    pub evm_chain: ChainType,
    pub bitcoin_max_deposits_per_iteration: usize,
    pub evm_max_deposits_per_iteration: usize,
}

#[derive(Debug, Deserialize)]
pub struct ConsolidateRequest {
    pub chain: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SetQuotingRequest {
    pub enabled: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsolidateResponse {
    pub results: Vec<ChainConsolidationResult>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChainConsolidationResult {
    pub chain: String,
    pub status: String,
    pub tx_hash: Option<String>,
    pub amount: String,
    pub deposit_count: usize,
    pub error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QuotingStateResponse {
    pub enabled: bool,
}

#[derive(Debug, Serialize)]
pub struct AdminApiErrorResponse {
    pub error: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InFlightSwapSummary {
    pub awaiting_deposit_confirmation_swaps: usize,
    pub queued_for_payment_swaps: usize,
    pub broadcast_pending_confirmation_swaps: usize,
    pub broadcast_pending_confirmation_batches: usize,
    pub total_blocking_swaps: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AwaitingDepositConfirmationSwap {
    pub swap_id: Uuid,
    pub quote_id: Uuid,
    pub from_chain: String,
    pub to_chain: String,
    pub amount: String,
    pub locked_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueuedPaymentSwap {
    pub swap_id: Uuid,
    pub quote_id: Uuid,
    pub payment_chain: String,
    pub amount: String,
    pub queued_at: DateTime<Utc>,
    pub user_deposit_confirmed_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BroadcastPendingConfirmationBatch {
    pub tx_hash: String,
    pub chain: String,
    pub swap_ids: Vec<Uuid>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InFlightSwapsResponse {
    pub quoting_enabled: bool,
    pub safe_to_take_down: bool,
    pub summary: InFlightSwapSummary,
    pub awaiting_deposit_confirmation: Vec<AwaitingDepositConfirmationSwap>,
    pub queued_for_payment: Vec<QueuedPaymentSwap>,
    pub broadcast_pending_confirmation: Vec<BroadcastPendingConfirmationBatch>,
}

#[derive(Debug, Deserialize)]
pub struct DeterministicDrainPlanQuery {
    pub max_rounds: Option<usize>,
}

pub fn create_admin_router(state: AdminApiState) -> Router {
    Router::new()
        .route("/consolidate", post(consolidate_handler))
        .route(
            "/quoting",
            get(get_quoting_handler).post(set_quoting_handler),
        )
        .route(
            "/planner/drain-plan",
            get(get_deterministic_drain_plan_handler),
        )
        .route("/swaps/in-flight", get(get_in_flight_swaps_handler))
        .with_state(state)
}

async fn get_quoting_handler(State(state): State<AdminApiState>) -> impl IntoResponse {
    Json(QuotingStateResponse {
        enabled: state.quoting_enabled.load(Ordering::Relaxed),
    })
}

async fn set_quoting_handler(
    State(state): State<AdminApiState>,
    Json(request): Json<SetQuotingRequest>,
) -> impl IntoResponse {
    state
        .quoting_enabled
        .store(request.enabled, Ordering::Relaxed);

    info!("Updated quoting state: enabled={}", request.enabled);

    (
        StatusCode::OK,
        Json(QuotingStateResponse {
            enabled: request.enabled,
        }),
    )
}

async fn get_in_flight_swaps_handler(State(state): State<AdminApiState>) -> impl IntoResponse {
    let quoting_enabled = state.quoting_enabled.load(Ordering::Relaxed);

    let awaiting_deposit_confirmation = state
        .liquidity_lock_manager
        .snapshot_locks()
        .await
        .into_iter()
        .map(|lock| AwaitingDepositConfirmationSwap {
            swap_id: lock.swap_id,
            quote_id: lock.quote_id,
            from_chain: lock.from.chain.to_db_string().to_string(),
            to_chain: lock.to.chain.to_db_string().to_string(),
            amount: lock.amount.to_string(),
            locked_at: lock.created_at,
        })
        .collect::<Vec<_>>();

    let queued_for_payment = state
        .payment_manager
        .in_flight_payments()
        .into_iter()
        .map(|payment| QueuedPaymentSwap {
            swap_id: payment.swap_id,
            quote_id: payment.quote_id,
            payment_chain: payment.chain.to_db_string().to_string(),
            amount: payment.amount.to_string(),
            queued_at: payment.queued_at,
            user_deposit_confirmed_at: payment.user_deposit_confirmed_at,
        })
        .collect::<Vec<_>>();

    let created_batches = match state
        .payment_repository
        .get_batches_by_status(BatchStatus::Created)
        .await
    {
        Ok(batches) => batches,
        Err(error) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(AdminApiErrorResponse {
                    error: format!("Failed to load in-flight batches: {error}"),
                }),
            )
                .into_response();
        }
    };

    let broadcast_pending_confirmation = created_batches
        .iter()
        .map(|batch| BroadcastPendingConfirmationBatch {
            tx_hash: batch.txid.clone(),
            chain: batch.chain.to_db_string().to_string(),
            swap_ids: batch.swap_ids.clone(),
            created_at: batch.created_at,
        })
        .collect::<Vec<_>>();

    let broadcast_pending_confirmation_swaps = created_batches
        .iter()
        .map(|batch| batch.swap_ids.len())
        .sum::<usize>();

    let total_blocking_swaps = awaiting_deposit_confirmation.len()
        + queued_for_payment.len()
        + broadcast_pending_confirmation_swaps;

    let response = InFlightSwapsResponse {
        quoting_enabled,
        safe_to_take_down: !quoting_enabled && total_blocking_swaps == 0,
        summary: InFlightSwapSummary {
            awaiting_deposit_confirmation_swaps: awaiting_deposit_confirmation.len(),
            queued_for_payment_swaps: queued_for_payment.len(),
            broadcast_pending_confirmation_swaps,
            broadcast_pending_confirmation_batches: broadcast_pending_confirmation.len(),
            total_blocking_swaps,
        },
        awaiting_deposit_confirmation,
        queued_for_payment,
        broadcast_pending_confirmation,
    };

    (StatusCode::OK, Json(response)).into_response()
}

async fn get_deterministic_drain_plan_handler(
    State(state): State<AdminApiState>,
    Query(query): Query<DeterministicDrainPlanQuery>,
) -> impl IntoResponse {
    let max_rounds = query.max_rounds.unwrap_or(32).min(256);

    match state
        .planner_service
        .deterministic_drain_plan(max_rounds)
        .await
    {
        Ok(plan) => (StatusCode::OK, Json(plan)).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(AdminApiErrorResponse {
                error: format!("Failed to build deterministic drain plan: {error}"),
            }),
        )
            .into_response(),
    }
}

async fn consolidate_handler(
    State(state): State<AdminApiState>,
    Query(request): Query<ConsolidateRequest>,
) -> impl IntoResponse {
    info!(
        "Consolidation request received for chain: {:?}",
        request.chain
    );

    let chains_to_process = match request.chain.as_deref() {
        Some(chain) => match resolve_requested_chain(chain, state.evm_chain) {
            Ok(chain) => vec![chain],
            Err(error) => {
                return (
                    StatusCode::OK,
                    Json(ConsolidateResponse {
                        results: vec![ChainConsolidationResult {
                            chain: chain.to_lowercase(),
                            status: "error".to_string(),
                            tx_hash: None,
                            amount: "0".to_string(),
                            deposit_count: 0,
                            error: Some(error),
                        }],
                    }),
                );
            }
        },
        None => vec![ChainType::Bitcoin, state.evm_chain],
    };

    let mut results = Vec::with_capacity(chains_to_process.len());

    for chain in chains_to_process {
        let result = match chain {
            ChainType::Bitcoin => {
                consolidate_bitcoin(
                    state.wallet_manager.clone(),
                    state.bitcoin_max_deposits_per_iteration,
                )
                .await
            }
            ChainType::Ethereum | ChainType::Base => {
                consolidate_evm_chain(
                    state.deposit_repository.clone(),
                    state.wallet_manager.clone(),
                    chain,
                    state.evm_max_deposits_per_iteration,
                )
                .await
            }
        };
        results.push(result);
    }

    (StatusCode::OK, Json(ConsolidateResponse { results }))
}

fn resolve_requested_chain(
    chain: &str,
    configured_evm_chain: ChainType,
) -> Result<ChainType, String> {
    let normalized = chain.to_lowercase();
    match ChainType::from_db_string(&normalized) {
        Some(ChainType::Bitcoin) => Ok(ChainType::Bitcoin),
        Some(requested) if requested == configured_evm_chain => Ok(requested),
        Some(_) => Err(format!(
            "Chain '{}' is not configured for this market maker",
            normalized
        )),
        None => Err(format!("Unknown chain: {}", normalized)),
    }
}

async fn consolidate_bitcoin(
    wallet_manager: Arc<WalletManager>,
    max_deposits_per_iteration: usize,
) -> ChainConsolidationResult {
    let Some(wallet) = wallet_manager.get(ChainType::Bitcoin) else {
        return ChainConsolidationResult {
            chain: "bitcoin".to_string(),
            status: "error".to_string(),
            tx_hash: None,
            amount: "0".to_string(),
            deposit_count: 0,
            error: Some("Bitcoin wallet not configured".to_string()),
        };
    };

    info!("Starting Bitcoin deposit consolidation");

    let lot = Lot {
        currency: otc_models::Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        },
        amount: U256::MAX,
    };

    summarize_consolidation(
        "bitcoin",
        wallet.consolidate(&lot, max_deposits_per_iteration).await,
    )
}

async fn consolidate_evm_chain(
    deposit_repository: Arc<DepositRepository>,
    wallet_manager: Arc<WalletManager>,
    chain: ChainType,
    max_deposits_per_iteration: usize,
) -> ChainConsolidationResult {
    let chain_name = chain.to_db_string().to_string();
    let Some(wallet) = wallet_manager.get(chain) else {
        return ChainConsolidationResult {
            chain: chain_name,
            status: "error".to_string(),
            tx_hash: None,
            amount: "0".to_string(),
            deposit_count: 0,
            error: Some(format!("{} wallet not configured", chain.to_db_string())),
        };
    };

    info!("Starting {} deposit consolidation", chain.to_db_string());

    let deposits = match deposit_repository
        .peek_all_available_deposits(Some(chain.to_db_string().to_string()))
        .await
    {
        Ok(deps) => deps,
        Err(error) => {
            error!(
                "Failed to fetch {} deposits: {}",
                chain.to_db_string(),
                error
            );
            return ChainConsolidationResult {
                chain: chain.to_db_string().to_string(),
                status: "error".to_string(),
                tx_hash: None,
                amount: "0".to_string(),
                deposit_count: 0,
                error: Some(format!("Database error: {}", error)),
            };
        }
    };

    if deposits.is_empty() {
        info!("No {} deposits to consolidate", chain.to_db_string());
        return ChainConsolidationResult {
            chain: chain.to_db_string().to_string(),
            status: "success".to_string(),
            tx_hash: None,
            amount: "0".to_string(),
            deposit_count: 0,
            error: None,
        };
    }

    let mut unique_currencies = HashMap::new();

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
    let mut total_amount = U256::ZERO;
    let mut total_iterations = 0;
    let mut errors = Vec::new();

    for (token_key, currency) in unique_currencies {
        info!(
            "Consolidating {} deposits for token: {}",
            chain.to_db_string(),
            token_key
        );

        let lot = Lot {
            currency,
            amount: U256::MAX,
        };

        match wallet.consolidate(&lot, max_deposits_per_iteration).await {
            Ok(summary) => {
                if summary.iterations > 0 {
                    total_amount = total_amount.saturating_add(summary.total_amount);
                    total_iterations += summary.iterations;
                    all_tx_hashes.extend(summary.tx_hashes);
                }
            }
            Err(error) => {
                error!(
                    "Failed to consolidate {} token {}: {}",
                    chain.to_db_string(),
                    token_key,
                    error
                );
                errors.push(format!("{}: {}", token_key, error));
            }
        }
    }

    if all_tx_hashes.is_empty() && !errors.is_empty() {
        ChainConsolidationResult {
            chain: chain.to_db_string().to_string(),
            status: "error".to_string(),
            tx_hash: None,
            amount: total_amount.to_string(),
            deposit_count: total_iterations,
            error: Some(errors.join("; ")),
        }
    } else {
        ChainConsolidationResult {
            chain: chain.to_db_string().to_string(),
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

fn summarize_consolidation(
    chain: &str,
    result: crate::WalletResult<ConsolidationSummary>,
) -> ChainConsolidationResult {
    match result {
        Ok(summary) => ChainConsolidationResult {
            chain: chain.to_string(),
            status: "success".to_string(),
            tx_hash: if summary.tx_hashes.is_empty() {
                None
            } else {
                Some(summary.tx_hashes.join(", "))
            },
            amount: summary.total_amount.to_string(),
            deposit_count: summary.iterations,
            error: None,
        },
        Err(error) => {
            error!("Failed to consolidate {} deposits: {}", chain, error);
            ChainConsolidationResult {
                chain: chain.to_string(),
                status: "error".to_string(),
                tx_hash: None,
                amount: "0".to_string(),
                deposit_count: 0,
                error: Some(format!("Consolidation error: {}", error)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::{
        body::{to_bytes, Body},
        http::Request,
    };
    use otc_chains::traits::{MarketMakerPaymentVerification, Payment};
    use std::{sync::Mutex, time::Duration};
    use tokio::{sync::mpsc, task::JoinSet};
    use tokio_util::sync::CancellationToken;
    use tower::util::ServiceExt;

    use crate::{
        balance_strat::QuoteBalanceStrategy,
        db::Database,
        db::Deposit,
        liquidity_lock::LockedLiquidity,
        planner::PlannerPolicy,
        wallet::{Wallet, WalletBalance, WalletError, WalletResult},
    };

    struct MockWallet {
        chain: ChainType,
        consolidations: Mutex<Vec<(ChainType, TokenIdentifier)>>,
        summary: ConsolidationSummary,
    }

    impl MockWallet {
        fn new(chain: ChainType, tx_hash: &str, amount: u64) -> Self {
            Self {
                chain,
                consolidations: Mutex::new(Vec::new()),
                summary: ConsolidationSummary {
                    total_amount: U256::from(amount),
                    iterations: 1,
                    tx_hashes: vec![tx_hash.to_string()],
                },
            }
        }

        fn consolidations(&self) -> Vec<(ChainType, TokenIdentifier)> {
            self.consolidations.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Wallet for MockWallet {
        async fn prepare_batch_payment(
            &self,
            _payments: Vec<Payment>,
            _mm_payment_validation: Option<MarketMakerPaymentVerification>,
        ) -> WalletResult<crate::wallet::PreparedBatchPayment> {
            Ok(crate::wallet::PreparedBatchPayment::Bitcoin {
                txid: "mock_txid".to_string(),
                txdata: vec![1, 2, 3],
                foreign_utxos: Vec::new(),
                absolute_fee: 0,
                reserved_deposit_keys: Vec::new(),
            })
        }

        async fn broadcast_prepared_batch(
            &self,
            _prepared_batch: &crate::wallet::PreparedBatchPayment,
        ) -> WalletResult<String> {
            Err(WalletError::TransactionCreationFailed {
                reason: "not implemented in test".to_string(),
            })
        }

        async fn guarantee_confirmations(
            &self,
            _tx_hash: &str,
            _confirmations: u64,
            _poll_interval: Duration,
        ) -> WalletResult<()> {
            Ok(())
        }

        async fn balance(&self, _token: &TokenIdentifier) -> WalletResult<WalletBalance> {
            Ok(WalletBalance {
                total_balance: U256::from(1_000_000u64),
                native_balance: U256::from(1_000_000u64),
                deposit_key_balance: U256::ZERO,
            })
        }

        async fn consolidate(
            &self,
            lot: &Lot,
            _max_deposits_per_iteration: usize,
        ) -> WalletResult<ConsolidationSummary> {
            self.consolidations
                .lock()
                .unwrap()
                .push((lot.currency.chain, lot.currency.token.clone()));
            Ok(self.summary.clone())
        }

        fn receive_address(&self, _token: &TokenIdentifier) -> String {
            "receive-address".to_string()
        }

        async fn cancel_tx(&self, _tx_hash: &str) -> WalletResult<String> {
            Ok("cancelled".to_string())
        }

        async fn check_tx_confirmations(&self, _tx_hash: &str) -> WalletResult<u64> {
            Ok(0)
        }

        async fn lookup_transaction_state(
            &self,
            _tx_hash: &str,
        ) -> WalletResult<crate::wallet::TransactionState> {
            Ok(crate::wallet::TransactionState::Missing)
        }

        fn chain_type(&self) -> ChainType {
            self.chain
        }
    }

    fn build_wallet_manager(wallets: Vec<Arc<dyn Wallet>>) -> Arc<WalletManager> {
        let mut manager = WalletManager::new();
        for wallet in wallets {
            manager.register(wallet.chain_type(), wallet);
        }
        Arc::new(manager)
    }

    fn build_payment_manager(
        wallet_manager: Arc<WalletManager>,
        payment_repository: Arc<PaymentRepository>,
        broadcasted_transaction_repository: Arc<crate::db::BroadcastedTransactionRepository>,
    ) -> Arc<PaymentManager> {
        let (otc_response_tx, _otc_response_rx) = mpsc::unbounded_channel();
        let mut join_set = JoinSet::new();

        Arc::new(PaymentManager::new(
            wallet_manager,
            payment_repository,
            broadcasted_transaction_repository,
            HashMap::new(),
            Arc::new(QuoteBalanceStrategy::new(9_500)),
            otc_response_tx,
            CancellationToken::new(),
            &mut join_set,
        ))
    }

    async fn build_planner_service(
        payment_manager: Arc<PaymentManager>,
        evm_chain: ChainType,
    ) -> Arc<PlannerService> {
        Arc::new(
            PlannerService::new(
                payment_manager,
                Arc::new(MockWallet::new(
                    ChainType::Bitcoin,
                    "planner-btc-tx",
                    1_000_000,
                )) as Arc<dyn Wallet>,
                Arc::new(MockWallet::new(evm_chain, "planner-evm-tx", 1_000_000))
                    as Arc<dyn Wallet>,
                PlannerPolicy::default(),
                evm_chain,
                CancellationToken::new(),
            )
            .await
            .unwrap(),
        )
    }

    async fn response_json<T: serde::de::DeserializeOwned>(
        response: axum::response::Response,
    ) -> T {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[sqlx::test]
    async fn test_consolidate_uses_configured_evm_chain(pool: sqlx::PgPool) -> sqlx::Result<()> {
        let db = Database::from_pool(pool).await.unwrap();
        let deposit_repository = Arc::new(db.deposits());
        let payment_repository = Arc::new(db.payments());
        let broadcasted_transaction_repository = Arc::new(db.broadcasted_transactions());

        let bitcoin_wallet = Arc::new(MockWallet::new(ChainType::Bitcoin, "btc-tx", 1));
        let base_wallet = Arc::new(MockWallet::new(ChainType::Base, "base-tx", 42));
        let wallet_manager = build_wallet_manager(vec![
            bitcoin_wallet.clone() as Arc<dyn Wallet>,
            base_wallet.clone() as Arc<dyn Wallet>,
        ]);
        let payment_manager =
            build_payment_manager(
                wallet_manager.clone(),
                payment_repository.clone(),
                broadcasted_transaction_repository,
            );
        let planner_service = build_planner_service(payment_manager.clone(), ChainType::Base).await;

        deposit_repository
            .store_deposit(
                &Deposit::new(
                    "base-private-key",
                    Lot {
                        currency: otc_models::Currency {
                            chain: ChainType::Base,
                            token: TokenIdentifier::Native,
                            decimals: 18,
                        },
                        amount: U256::from(42u64),
                    },
                    "base-funding-tx",
                ),
                utc::now(),
                Uuid::now_v7(),
            )
            .await
            .unwrap();

        let state = AdminApiState {
            deposit_repository,
            payment_repository,
            payment_manager,
            planner_service,
            liquidity_lock_manager: Arc::new(LiquidityLockManager::with_ttl(
                chrono::Duration::hours(1),
            )),
            wallet_manager,
            quoting_enabled: Arc::new(AtomicBool::new(true)),
            evm_chain: ChainType::Base,
            bitcoin_max_deposits_per_iteration: 100,
            evm_max_deposits_per_iteration: 200,
        };

        let response = create_admin_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/consolidate?chain=base")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let response: ConsolidateResponse = response_json(response).await;
        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].chain, "base");
        assert_eq!(response.results[0].status, "success");
        assert_eq!(response.results[0].amount, "42");

        assert!(bitcoin_wallet.consolidations().is_empty());
        assert_eq!(
            base_wallet.consolidations(),
            vec![(ChainType::Base, TokenIdentifier::Native)]
        );

        Ok(())
    }

    #[sqlx::test]
    async fn test_swaps_in_flight_endpoint_reports_blocking_work(
        pool: sqlx::PgPool,
    ) -> sqlx::Result<()> {
        let db = Database::from_pool(pool).await.unwrap();
        let payment_repository = Arc::new(db.payments());
        let deposit_repository = Arc::new(db.deposits());
        let broadcasted_transaction_repository = Arc::new(db.broadcasted_transactions());

        let bitcoin_wallet = Arc::new(MockWallet::new(ChainType::Bitcoin, "btc-tx", 1));
        let wallet_manager = build_wallet_manager(vec![bitcoin_wallet as Arc<dyn Wallet>]);
        let payment_manager =
            build_payment_manager(
                wallet_manager.clone(),
                payment_repository.clone(),
                broadcasted_transaction_repository,
            );
        let planner_service =
            build_planner_service(payment_manager.clone(), ChainType::Ethereum).await;
        let liquidity_lock_manager =
            Arc::new(LiquidityLockManager::with_ttl(chrono::Duration::hours(1)));

        let swap_id = Uuid::now_v7();
        let quote_id = Uuid::now_v7();

        liquidity_lock_manager
            .lock(
                swap_id,
                LockedLiquidity {
                    quote_id,
                    from: otc_models::Currency {
                        chain: ChainType::Bitcoin,
                        token: TokenIdentifier::Native,
                        decimals: 8,
                    },
                    to: otc_models::Currency {
                        chain: ChainType::Ethereum,
                        token: TokenIdentifier::Native,
                        decimals: 18,
                    },
                    amount: U256::from(50_000u64),
                    created_at: utc::now(),
                },
            )
            .await;

        payment_manager.insert_in_flight_payment_for_test(
            crate::payment_manager::InFlightPaymentSnapshot {
                swap_id: Uuid::now_v7(),
                quote_id: Uuid::now_v7(),
                chain: ChainType::Bitcoin,
                amount: U256::from(25_000u64),
                queued_at: utc::now(),
                user_deposit_confirmed_at: utc::now(),
            },
        );

        payment_repository
            .set_batch_payment(
                vec![Uuid::now_v7(), Uuid::now_v7()],
                "batch-created-tx",
                ChainType::Bitcoin,
                [7u8; 32],
                0,
            )
            .await
            .unwrap();

        let state = AdminApiState {
            deposit_repository,
            payment_repository,
            payment_manager,
            planner_service,
            liquidity_lock_manager,
            wallet_manager,
            quoting_enabled: Arc::new(AtomicBool::new(false)),
            evm_chain: ChainType::Ethereum,
            bitcoin_max_deposits_per_iteration: 100,
            evm_max_deposits_per_iteration: 200,
        };

        let response = create_admin_router(state)
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/swaps/in-flight")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let response: InFlightSwapsResponse = response_json(response).await;
        assert!(!response.safe_to_take_down);
        assert!(!response.quoting_enabled);
        assert_eq!(response.summary.awaiting_deposit_confirmation_swaps, 1);
        assert_eq!(response.summary.queued_for_payment_swaps, 1);
        assert_eq!(response.summary.broadcast_pending_confirmation_batches, 1);
        assert_eq!(response.summary.broadcast_pending_confirmation_swaps, 2);
        assert_eq!(response.summary.total_blocking_swaps, 4);
        assert_eq!(response.awaiting_deposit_confirmation.len(), 1);
        assert_eq!(response.queued_for_payment.len(), 1);
        assert_eq!(response.broadcast_pending_confirmation.len(), 1);

        Ok(())
    }

    #[sqlx::test]
    async fn test_swaps_in_flight_endpoint_safe_when_disabled_and_empty(
        pool: sqlx::PgPool,
    ) -> sqlx::Result<()> {
        let db = Database::from_pool(pool).await.unwrap();
        let deposit_repository = Arc::new(db.deposits());
        let payment_repository = Arc::new(db.payments());
        let broadcasted_transaction_repository = Arc::new(db.broadcasted_transactions());
        let bitcoin_wallet = Arc::new(MockWallet::new(ChainType::Bitcoin, "btc-tx", 1));
        let wallet_manager = build_wallet_manager(vec![bitcoin_wallet as Arc<dyn Wallet>]);
        let payment_manager =
            build_payment_manager(
                wallet_manager.clone(),
                payment_repository.clone(),
                broadcasted_transaction_repository,
            );
        let planner_service =
            build_planner_service(payment_manager.clone(), ChainType::Ethereum).await;

        let state = AdminApiState {
            deposit_repository,
            payment_repository,
            payment_manager,
            planner_service,
            liquidity_lock_manager: Arc::new(LiquidityLockManager::with_ttl(
                chrono::Duration::hours(1),
            )),
            wallet_manager,
            quoting_enabled: Arc::new(AtomicBool::new(false)),
            evm_chain: ChainType::Ethereum,
            bitcoin_max_deposits_per_iteration: 100,
            evm_max_deposits_per_iteration: 200,
        };

        let response = create_admin_router(state)
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/swaps/in-flight")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let response: InFlightSwapsResponse = response_json(response).await;
        assert!(response.safe_to_take_down);
        assert_eq!(response.summary.total_blocking_swaps, 0);

        Ok(())
    }

    #[sqlx::test]
    async fn test_planner_drain_plan_endpoint_returns_deterministic_plan(
        pool: sqlx::PgPool,
    ) -> sqlx::Result<()> {
        let db = Database::from_pool(pool).await.unwrap();
        let deposit_repository = Arc::new(db.deposits());
        let payment_repository = Arc::new(db.payments());
        let broadcasted_transaction_repository = Arc::new(db.broadcasted_transactions());
        let bitcoin_wallet = Arc::new(MockWallet::new(ChainType::Bitcoin, "btc-tx", 1));
        let wallet_manager = build_wallet_manager(vec![bitcoin_wallet as Arc<dyn Wallet>]);
        let payment_manager =
            build_payment_manager(
                wallet_manager.clone(),
                payment_repository.clone(),
                broadcasted_transaction_repository,
            );
        let planner_service =
            build_planner_service(payment_manager.clone(), ChainType::Ethereum).await;

        let state = AdminApiState {
            deposit_repository,
            payment_repository,
            payment_manager,
            planner_service,
            liquidity_lock_manager: Arc::new(LiquidityLockManager::with_ttl(
                chrono::Duration::hours(1),
            )),
            wallet_manager,
            quoting_enabled: Arc::new(AtomicBool::new(true)),
            evm_chain: ChainType::Ethereum,
            bitcoin_max_deposits_per_iteration: 100,
            evm_max_deposits_per_iteration: 200,
        };

        let response = create_admin_router(state)
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/planner/drain-plan?max_rounds=5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let response: crate::planner::DeterministicDrainPlan = response_json(response).await;
        assert_eq!(response.max_rounds, 5);
        assert!(response.fully_drained);
        assert!(!response.truncated);
        assert!(response.rounds.is_empty());
        assert_eq!(response.current_state.open_obligations, 0);
        assert_eq!(response.projected_final_state.open_obligations, 0);

        Ok(())
    }
}
