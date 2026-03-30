use crate::{
    api::swaps::{BlockHashResponse, CreateSwapRequest, RefundSwapResponse},
    config::Settings,
    db::{
        swap_repo::{SWAP_FEES_TOTAL_METRIC, SWAP_VOLUME_TOTAL_METRIC},
        Database, GOOD_STANDING_THRESHOLD_SATS, GOOD_STANDING_WINDOW, MM_FEE_DEBT_SATS_METRIC,
    },
    detection_ingest::{deposit_observation, ParticipantCooldownMap},
    http_metrics::{track_http_metrics, HTTP_REQUESTS_TOTAL, HTTP_REQUEST_DURATION_SECONDS},
    services::{
        market_maker_batch_settlement::{
            MarketMakerBatchSettlementStatus, ACTIVE_MARKET_MAKER_BATCHES_METRIC,
            MARKET_MAKER_BATCH_SETTLEMENT_DURATION_METRIC,
            MARKET_MAKER_BATCH_SETTLEMENT_EVENTS_TOTAL_METRIC,
        },
        user_deposit_confirmation_scheduler::{
            UserDepositSchedulerStatus, ACTIVE_USER_DEPOSIT_CONFIRMATIONS_METRIC,
            USER_DEPOSIT_CONFIRMATIONS_ADVANCED_TOTAL_METRIC,
            USER_DEPOSIT_CONFIRMATION_DURATION_METRIC,
            USER_DEPOSIT_HEAD_FETCH_FAILURES_TOTAL_METRIC, USER_DEPOSIT_REARMS_TOTAL_METRIC,
        },
        MMRegistry, MarketMakerBatchSettlementService, SwapManager,
        UserDepositConfirmationScheduler,
    },
    BackgroundTaskResult, OtcServerArgs, Result,
};
use async_trait::async_trait;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post, Router},
    Json,
};
use chainalysis_address_screener::{ChainalysisAddressScreener, RiskLevel};
use dstack_sdk::dstack_client::{DstackClient, GetQuoteResponse, InfoResponse};
use metrics::{describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use mm_websocket_server::{MessageHandler, MessageSender};
use otc_auth::{
    api_keys::{DETECTOR_API_KEY, MARKET_MAKER_API_KEYS},
    ApiKeyStore,
};
use otc_chains::{bitcoin::BitcoinChain, evm::EvmChain, ChainRegistry};
use otc_protocols::mm::{MMRequest, MMResponse, ProtocolMessage};
use serde::{Deserialize, Serialize};
use snafu::{prelude::*, FromString, Whatever};
use std::{
    net::SocketAddr,
    sync::{Arc, OnceLock},
    time::Instant,
};
use tokio::{sync::mpsc, task::JoinSet, time::Duration};
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::{error, info};
use uuid::Uuid;

#[derive(Clone)]
pub struct AppState {
    pub db: Database,
    pub swap_manager: Arc<SwapManager>,
    pub mm_registry: Arc<MMRegistry>,
    pub api_key_store: Arc<otc_auth::ApiKeyStore>,
    pub detector_api_key_store: Arc<otc_auth::ApiKeyStore>,
    pub address_screener: Option<ChainalysisAddressScreener>,
    pub chain_registry: Arc<ChainRegistry>,
    pub dstack_client: Arc<DstackClient>,
    pub market_maker_batch_settlement_service: Arc<MarketMakerBatchSettlementService>,
    pub user_deposit_confirmation_scheduler: Arc<UserDepositConfirmationScheduler>,
    pub participant_submission_cooldowns: Arc<ParticipantCooldownMap>,
    pub participant_detection_min_interval: Duration,
    pub participant_signed_detection_enabled: bool,
}

#[derive(Serialize, Deserialize)]
struct Status {
    status: String,
    version: String,
    last_batch_settlement_pass: u64,
    last_user_deposit_confirmation_pass: u64,
    market_maker_batch_settlement: MarketMakerBatchSettlementStatus,
    user_deposit_confirmation_scheduler: UserDepositSchedulerStatus,
    connected_market_makers: Vec<Uuid>,
}

#[derive(Serialize)]
struct SuspendedMarketMakersResponse {
    market_maker_ids: Vec<Uuid>,
}

const QUOTE_LATENCY_METRIC: &str = "otc_quote_response_seconds";
static PROMETHEUS_HANDLE: OnceLock<Arc<PrometheusHandle>> = OnceLock::new();

pub async fn run_server(args: OtcServerArgs) -> Result<()> {
    info!("Starting OTC server...");

    let addr = SocketAddr::from((args.host, args.port));
    let mut background_tasks: JoinSet<BackgroundTaskResult> = JoinSet::new();

    let settings =
        Arc::new(
            Settings::load(&args.config_dir).map_err(|e| crate::Error::DatabaseInit {
                source: crate::error::OtcServerError::InvalidData {
                    message: format!("Failed to load settings: {e}"),
                },
            })?,
        );
    let db = Database::connect(
        &args.database_url,
        args.db_max_connections,
        args.db_min_connections,
    )
    .await
    .context(crate::DatabaseInitSnafu)?;

    info!("Initializing chain registry...");
    let mut chain_registry = ChainRegistry::new();

    // Initialize Bitcoin chain
    let bitcoin_chain = BitcoinChain::new(
        &args.bitcoin_rpc_url,
        args.bitcoin_rpc_auth,
        &args.untrusted_esplora_http_server_url,
        args.bitcoin_network,
    )
    .map_err(|e| crate::Error::DatabaseInit {
        source: crate::error::OtcServerError::InvalidData {
            message: format!("Failed to initialize Bitcoin chain: {e}"),
        },
    })?;
    chain_registry.register(otc_models::ChainType::Bitcoin, Arc::new(bitcoin_chain));

    // Initialize Ethereum chain
    let ethereum_chain = Arc::new(
        EvmChain::new(
            &args.ethereum_mainnet_rpc_url,
            &args.ethereum_allowed_token,
            otc_models::ChainType::Ethereum,
            b"ethereum-wallet",
            4,
            Duration::from_secs(12),
        )
        .await
        .map_err(|e| crate::Error::DatabaseInit {
            source: crate::error::OtcServerError::InvalidData {
                message: format!("Failed to initialize Ethereum chain: {e}"),
            },
        })?,
    );
    chain_registry.register_evm(otc_models::ChainType::Ethereum, ethereum_chain);

    // Initialize Base chain
    let base_chain = Arc::new(
        EvmChain::new(
            &args.base_rpc_url,
            &args.base_allowed_token,
            otc_models::ChainType::Base,
            b"base-wallet",
            2,
            Duration::from_secs(2),
        )
        .await
        .map_err(|e| crate::Error::DatabaseInit {
            source: crate::error::OtcServerError::InvalidData {
                message: format!("Failed to initialize Base chain: {e}"),
            },
        })?,
    );
    chain_registry.register_evm(otc_models::ChainType::Base, base_chain);

    let chain_registry = Arc::new(chain_registry);

    info!("Initializing services...");

    let api_key_store = Arc::new(ApiKeyStore::new(MARKET_MAKER_API_KEYS.clone()).await?);
    let detector_api_key_store = Arc::new(ApiKeyStore::new(vec![DETECTOR_API_KEY.clone()]).await?);

    let mm_registry = Arc::new(MMRegistry::new());

    let swap_manager = Arc::new(SwapManager::new(
        db.clone(),
        settings.clone(),
        chain_registry.clone(),
        mm_registry.clone(),
    ));

    let market_maker_batch_settlement_service = Arc::new(MarketMakerBatchSettlementService::new(
        db.clone(),
        settings.clone(),
        chain_registry.clone(),
        mm_registry.clone(),
    ));
    let user_deposit_confirmation_scheduler = Arc::new(UserDepositConfirmationScheduler::new(
        db.clone(),
        chain_registry.clone(),
        mm_registry.clone(),
    ));

    info!("Starting market maker batch settlement service...");
    market_maker_batch_settlement_service.initialize().await;
    market_maker_batch_settlement_service.spawn_tasks(&mut background_tasks);

    info!("Starting user deposit confirmation scheduler...");
    user_deposit_confirmation_scheduler.initialize().await;
    user_deposit_confirmation_scheduler.spawn_tasks(&mut background_tasks);

    // Start periodic database metrics sync for Grafana
    info!("Starting database metrics sync...");
    spawn_database_metrics_sync(&mut background_tasks, db.clone());

    // Initialize optional Chainalysis address screener
    let address_screener = match (&args.chainalysis_host, &args.chainalysis_token) {
        (Some(host), Some(token)) if !host.is_empty() && !token.is_empty() => {
            match ChainalysisAddressScreener::new(host.clone(), token.clone()) {
                Ok(s) => Some(s),
                Err(e) => {
                    tracing::warn!("Failed to initialize address screener: {}", e);
                    None
                }
            }
        }
        _ => None,
    };

    if let Some(host) = &args.chainalysis_host {
        if address_screener.is_some() {
            info!(%host, "Chainalysis address screening: enabled");
        } else {
            info!(%host, "Chainalysis address screening: disabled (init failed or token missing)");
        }
    } else {
        info!("Chainalysis address screening: disabled (no host configured)");
    }

    let dstack_client = Arc::new(DstackClient::new(Some(&args.dstack_sock_path)));

    let state = AppState {
        db,
        swap_manager,
        mm_registry,
        api_key_store,
        detector_api_key_store,
        address_screener,
        chain_registry,
        dstack_client,
        market_maker_batch_settlement_service,
        user_deposit_confirmation_scheduler,
        participant_submission_cooldowns: Arc::new(ParticipantCooldownMap::new()),
        participant_detection_min_interval: Duration::from_secs(
            args.participant_detection_min_interval_seconds,
        ),
        participant_signed_detection_enabled: args.participant_signed_detection_enabled,
    };

    if let Some(metrics_addr) = args.metrics_listen_addr {
        setup_metrics(&mut background_tasks, metrics_addr).await?;
    } else {
        install_metrics_recorder()?;
    }

    let mut app = Router::new()
        // Health check
        .route("/status", get(status_handler))
        // WebSocket endpoints
        .route("/ws", get(websocket_handler))
        .route("/ws/mm", get(mm_websocket_handler))
        // API endpoints
        .route("/api/v2/swap", post(create_swap))
        .route("/api/v2/swap/:id", get(get_swap))
        .route("/api/v2/swap/:id/refund", post(refund_swap))
        .route("/api/v2/chains/bitcoin/tip", get(get_best_bitcoin_hash))
        .route("/api/v2/chains/ethereum/tip", get(get_best_ethereum_hash))
        .route("/api/v2/chains/base/tip", get(get_best_base_hash))
        .route(
            "/api/v2/market-makers/suspended",
            get(get_suspended_market_makers),
        )
        .route(
            "/api/v1/swaps/:swap_id/deposit-observation",
            post(deposit_observation),
        )
        .route("/api/v1/tdx/quote", get(get_tdx_quote))
        .route("/api/v1/tdx/info", get(get_tdx_info))
        .layer(axum::middleware::from_fn(track_http_metrics))
        .with_state(state);

    // Add CORS layer if cors_domain is specified
    if let Some(ref cors_domain_pattern) = args.cors_domain {
        let cors_domain = cors_domain_pattern.clone();
        let cors = if cors_domain == "*" {
            // Allow all origins
            CorsLayer::new()
                .allow_origin(tower_http::cors::Any)
                .allow_methods(tower_http::cors::Any)
                .allow_headers(tower_http::cors::Any)
        } else {
            // Handle specific patterns
            CorsLayer::new()
                .allow_origin(AllowOrigin::predicate(move |origin, _request_parts| {
                    let origin_str = origin.to_str().unwrap_or("");

                    // Support wildcard patterns
                    if cors_domain.contains('*') {
                        let pattern = cors_domain.replace("*", "");
                        if cors_domain.starts_with('*') {
                            origin_str.ends_with(&pattern)
                        } else if cors_domain.ends_with('*') {
                            origin_str.starts_with(&pattern[..pattern.len() - 1])
                        } else {
                            // Handle middle wildcards like "*.example.*"
                            let parts: Vec<&str> = cors_domain.split('*').collect();
                            parts.iter().all(|part| origin_str.contains(part))
                        }
                    } else {
                        origin_str == cors_domain
                    }
                }))
                .allow_methods(tower_http::cors::Any)
                .allow_headers(tower_http::cors::Any)
        };

        app = app.layer(cors);
        info!("CORS enabled for domain: {}", cors_domain_pattern);
    }

    info!("Listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context(crate::ServerBindSnafu)?;

    tokio::select! {
        serve_result = axum::serve(listener, app) => {
            serve_result.context(crate::ServerStartSnafu)?;
            Ok(())
        }
        background_result = background_tasks.join_next(), if !background_tasks.is_empty() => {
            handle_background_task_result(background_result)
        }
    }
}

fn spawn_database_metrics_sync(join_set: &mut JoinSet<BackgroundTaskResult>, db: Database) {
    join_set.spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;

            // Sync volume metrics
            if let Ok(volumes) = db.swaps().get_settled_volume_totals().await {
                for (market, total) in volumes {
                    gauge!(
                        SWAP_VOLUME_TOTAL_METRIC,
                        "market" => market,
                    )
                    .set(total as f64);
                }
            }

            // Sync fee metrics
            if let Ok(fees) = db.swaps().get_settled_fee_totals().await {
                for (market, total) in fees {
                    gauge!(
                        SWAP_FEES_TOTAL_METRIC,
                        "market" => market,
                    )
                    .set(total as f64);
                }
            }

            // Sync MM fee debt metrics
            if let Ok(states) = db.fees().list_all_fee_states().await {
                for (mm_id, debt_sats) in states {
                    gauge!(
                        MM_FEE_DEBT_SATS_METRIC,
                        "market_maker_id" => mm_id.to_string(),
                    )
                    .set(debt_sats as f64);
                }
            }
        }
        #[allow(unreachable_code)]
        Ok::<(), String>(())
    });
}

fn background_task_failed(message: String) -> crate::Error {
    crate::Error::Generic {
        source: Whatever::without_source(message),
    }
}

fn handle_background_task_result(
    background_result: Option<std::result::Result<BackgroundTaskResult, tokio::task::JoinError>>,
) -> Result<()> {
    match background_result {
        Some(Ok(Ok(()))) => Err(background_task_failed(
            "A background task exited unexpectedly".to_string(),
        )),
        Some(Ok(Err(err))) => Err(background_task_failed(err)),
        Some(Err(err)) => Err(background_task_failed(format!(
            "A background task panicked or was cancelled: {err}"
        ))),
        None => Err(background_task_failed(
            "The background task set terminated unexpectedly".to_string(),
        )),
    }
}

fn install_metrics_recorder() -> Result<Arc<PrometheusHandle>> {
    if let Some(handle) = PROMETHEUS_HANDLE.get() {
        return Ok(handle.clone());
    }

    // Custom histogram buckets for HTTP latency metrics (in seconds).
    // Provides good granularity for P50/P95/P99 calculations on typical web request latencies.
    let http_latency_buckets = vec![
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ];

    let handle = PrometheusBuilder::new()
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Full(HTTP_REQUEST_DURATION_SECONDS.to_string()),
            &http_latency_buckets,
        )
        .context(crate::MetricsRecorderSnafu)?
        .install_recorder()
        .context(crate::MetricsRecorderSnafu)?;
    let shared_handle = Arc::new(handle);

    describe_gauge!(
        "otc_metrics_exporter_up",
        "Set to 1 when the OTC server metrics recorder is installed."
    );
    gauge!("otc_metrics_exporter_up").set(1.0);

    describe_histogram!(
        QUOTE_LATENCY_METRIC,
        "Latency in seconds for responding to quote requests."
    );

    describe_histogram!(
        MARKET_MAKER_BATCH_SETTLEMENT_DURATION_METRIC,
        "Duration in seconds for refreshing a market maker batch settlement candidate."
    );

    describe_counter!(
        MARKET_MAKER_BATCH_SETTLEMENT_EVENTS_TOTAL_METRIC,
        "Market maker batch settlement lifecycle events by chain and event type."
    );

    describe_gauge!(
        ACTIVE_MARKET_MAKER_BATCHES_METRIC,
        "Current number of market maker deposit batches scheduled for confirmation tracking."
    );

    describe_histogram!(
        USER_DEPOSIT_CONFIRMATION_DURATION_METRIC,
        "Duration in seconds for refreshing a scheduled user deposit confirmation candidate."
    );

    describe_gauge!(
        ACTIVE_USER_DEPOSIT_CONFIRMATIONS_METRIC,
        "Current number of swaps scheduled for user-deposit confirmation tracking."
    );

    describe_counter!(
        USER_DEPOSIT_CONFIRMATIONS_ADVANCED_TOTAL_METRIC,
        "Total number of user deposits advanced to market-maker initiation after confirmation."
    );

    describe_counter!(
        USER_DEPOSIT_REARMS_TOTAL_METRIC,
        "Total number of accepted user deposits rearmed after disappearing from chain visibility."
    );

    describe_counter!(
        USER_DEPOSIT_HEAD_FETCH_FAILURES_TOTAL_METRIC,
        "Total number of user-deposit scheduler chain-head fetch failures by chain."
    );

    describe_gauge!(
        SWAP_VOLUME_TOTAL_METRIC,
        "Settled volume per market (sats), synced from database aggregate tables."
    );

    describe_gauge!(
        SWAP_FEES_TOTAL_METRIC,
        "Settled protocol fees per market (sats), synced from database aggregate tables."
    );

    describe_gauge!(
        MM_FEE_DEBT_SATS_METRIC,
        "Current protocol fee debt per market maker (sats). Negative values indicate credit."
    );

    describe_counter!(
        "ethereum_rpc_requests_total",
        "Total number of Ethereum RPC requests."
    );

    describe_histogram!(
        "ethereum_rpc_duration_seconds",
        "Duration in seconds of Ethereum RPC requests."
    );

    describe_counter!(
        HTTP_REQUESTS_TOTAL,
        "Total number of HTTP requests by method, path, and status."
    );

    describe_histogram!(
        HTTP_REQUEST_DURATION_SECONDS,
        "HTTP request latency in seconds by method, path, and status."
    );

    if PROMETHEUS_HANDLE.set(shared_handle.clone()).is_err() {
        if let Some(existing) = PROMETHEUS_HANDLE.get() {
            return Ok(existing.clone());
        }
    }

    Ok(shared_handle)
}

async fn setup_metrics(
    join_set: &mut JoinSet<BackgroundTaskResult>,
    addr: SocketAddr,
) -> Result<()> {
    let shared_handle = install_metrics_recorder()?;

    let upkeep_handle = shared_handle.clone();
    join_set.spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(10));
        loop {
            ticker.tick().await;
            upkeep_handle.run_upkeep();
        }
        #[allow(unreachable_code)]
        Ok::<(), String>(())
    });

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context(crate::MetricsServerBindSnafu { addr })?;

    let metrics_state = shared_handle.clone();
    join_set.spawn(async move {
        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .with_state(metrics_state);

        axum::serve(listener, app)
            .await
            .map_err(|error| format!("Metrics server error: {error}"))?;

        Ok::<(), String>(())
    });

    Ok(())
}

async fn metrics_handler(State(handle): State<Arc<PrometheusHandle>>) -> impl IntoResponse {
    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        handle.render(),
    )
}

async fn status_handler(State(state): State<AppState>) -> impl IntoResponse {
    let batch_status = state
        .market_maker_batch_settlement_service
        .status_snapshot();
    let user_status = state.user_deposit_confirmation_scheduler.status_snapshot();
    Json(Status {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        last_batch_settlement_pass: batch_status.last_pass,
        last_user_deposit_confirmation_pass: user_status.last_pass,
        market_maker_batch_settlement: batch_status,
        user_deposit_confirmation_scheduler: user_status,
        connected_market_makers: state.mm_registry.get_connected_market_makers(),
    })
}

async fn websocket_handler(ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(handle_socket)
}

async fn mm_websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // Extract and validate authentication headers
    let market_maker_id = match headers.get("x-api-id") {
        Some(value) => match value.to_str() {
            Ok(id_str) => match Uuid::parse_str(id_str) {
                Ok(id) => id,
                Err(_) => {
                    return (StatusCode::BAD_REQUEST, "Invalid API ID format").into_response();
                }
            },
            Err(_) => {
                return (StatusCode::BAD_REQUEST, "Invalid API ID header").into_response();
            }
        },
        None => {
            return (StatusCode::UNAUTHORIZED, "Missing X-API-ID header").into_response();
        }
    };

    let api_secret = match headers.get("x-api-secret") {
        Some(value) => match value.to_str() {
            Ok(key) => key,
            Err(_) => {
                return (StatusCode::BAD_REQUEST, "Invalid API secret header").into_response();
            }
        },
        None => {
            return (StatusCode::UNAUTHORIZED, "Missing X-API-SECRET header").into_response();
        }
    };

    // Validate the API key
    match state.api_key_store.validate(&market_maker_id, api_secret) {
        Ok(market_maker_tag) => {
            info!(
                "Market maker {} authenticated via headers",
                market_maker_tag
            );
            let mm_uuid = market_maker_id;
            ws.on_upgrade(move |socket| handle_mm_socket(socket, state, mm_uuid))
        }
        Err(e) => {
            error!("API key validation failed: {}", e);
            (StatusCode::UNAUTHORIZED, "Invalid API key").into_response()
        }
    }
}

async fn handle_socket(mut socket: WebSocket) {
    info!("WebSocket connection established");

    while let Some(msg) = socket.recv().await {
        if let Ok(msg) = msg {
            match msg {
                axum::extract::ws::Message::Text(text) => {
                    if socket
                        .send(axum::extract::ws::Message::Text(format!("Echo: {text}")))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                axum::extract::ws::Message::Close(_) => {
                    info!("WebSocket connection closed");
                    break;
                }
                _ => {}
            }
        } else {
            break;
        }
    }
}

async fn refund_swap(
    State(state): State<AppState>,
    Path(swap_id): Path<Uuid>,
) -> Result<Json<RefundSwapResponse>, crate::error::OtcServerError> {
    state
        .swap_manager
        .refund_swap(swap_id)
        .await
        .map(Json)
        .map_err(Into::into)
}

async fn get_best_bitcoin_hash(
    State(state): State<AppState>,
) -> Result<Json<BlockHashResponse>, crate::error::OtcServerError> {
    let chain = state
        .chain_registry
        .get(&otc_models::ChainType::Bitcoin)
        .unwrap();
    chain
        .get_best_hash()
        .await
        .map_err(Into::into)
        .map(|hash| Json(BlockHashResponse { block_hash: hash }))
}

async fn get_best_ethereum_hash(
    State(state): State<AppState>,
) -> Result<Json<BlockHashResponse>, crate::error::OtcServerError> {
    let chain = state
        .chain_registry
        .get(&otc_models::ChainType::Ethereum)
        .unwrap();
    chain.get_best_hash().await.map_err(Into::into).map(|hash| {
        if hash.starts_with("0x") {
            Json(BlockHashResponse { block_hash: hash })
        } else {
            Json(BlockHashResponse {
                block_hash: format!("0x{}", hash),
            })
        }
    })
}

async fn get_best_base_hash(
    State(state): State<AppState>,
) -> Result<Json<BlockHashResponse>, crate::error::OtcServerError> {
    let chain = state
        .chain_registry
        .get(&otc_models::ChainType::Base)
        .unwrap();
    chain.get_best_hash().await.map_err(Into::into).map(|hash| {
        if hash.starts_with("0x") {
            Json(BlockHashResponse { block_hash: hash })
        } else {
            Json(BlockHashResponse {
                block_hash: format!("0x{}", hash),
            })
        }
    })
}

async fn get_suspended_market_makers(
    State(state): State<AppState>,
) -> Result<Json<SuspendedMarketMakersResponse>, crate::error::OtcServerError> {
    let now = utc::now();
    let market_maker_ids = state.db.fees().list_bad_standing(now).await?;
    Ok(Json(SuspendedMarketMakersResponse { market_maker_ids }))
}

async fn create_swap(
    State(state): State<AppState>,
    Json(request): Json<CreateSwapRequest>,
) -> Result<Json<otc_models::Swap>, crate::error::OtcServerError> {
    // Address screening (only if configured). Block if either address is High/Severe risk.
    if let Some(screener) = &state.address_screener {
        let to_check: Vec<String> = vec![
            request.user_destination_address.clone(),
            request.refund_address.clone(),
        ];

        for addr in to_check {
            match screener.get_address_risk(&addr).await {
                Ok(r) => {
                    if matches!(r.risk, RiskLevel::High | RiskLevel::Severe) {
                        return Err(crate::error::OtcServerError::Authorization {
                            message: format!(
                                "Address {} blocked due to risk classification: {:?}",
                                addr, r.risk
                            ),
                        });
                    }
                }
                Err(e) => {
                    tracing::warn!("Address screening failed for {}: {}", addr, e);
                }
            }
        }
    }
    state
        .swap_manager
        .create_swap(request)
        .await
        .map(Json)
        .map_err(Into::into)
}

async fn get_swap(
    State(state): State<AppState>,
    Path(swap_id): Path<Uuid>,
) -> Result<Json<otc_models::Swap>, crate::error::OtcServerError> {
    state
        .swap_manager
        .get_swap(swap_id)
        .await
        .map(Json)
        .map_err(|e| match e {
            crate::services::swap_manager::SwapError::QuoteNotFound { .. } => {
                crate::error::OtcServerError::NotFound
            }
            crate::services::swap_manager::SwapError::Database { .. } => {
                crate::error::OtcServerError::Internal {
                    message: e.to_string(),
                }
            }
            crate::services::swap_manager::SwapError::ChainNotSupported { .. } => {
                crate::error::OtcServerError::Internal {
                    message: e.to_string(),
                }
            }
            crate::services::swap_manager::SwapError::WalletDerivation { .. } => {
                crate::error::OtcServerError::Internal {
                    message: e.to_string(),
                }
            }
            _ => crate::error::OtcServerError::Internal {
                message: e.to_string(),
            },
        })
}

// ========== MM WebSocket Handler Implementation ==========

/// Message handler for OTC protocol
struct OTCMessageHandler {
    db: Database,
    mm_registry: Arc<MMRegistry>,
    market_maker_batch_settlement_service: Arc<MarketMakerBatchSettlementService>,
    chain_registry: Arc<ChainRegistry>,
    swap_manager: Arc<SwapManager>,
}

#[async_trait]
impl MessageHandler for OTCMessageHandler {
    async fn handle_message(&self, mm_id: Uuid, text: &str) -> Option<String> {
        match serde_json::from_str::<ProtocolMessage<MMResponse>>(text) {
            Ok(msg) => {
                let ProtocolMessage {
                    version: _,
                    sequence: _,
                    payload,
                } = msg;

                match payload {
                    MMResponse::QuoteValidated {
                        quote_id, accepted, ..
                    } => {
                        info!(
                            market_maker_id = %mm_id,
                            quote_id = %quote_id,
                            accepted = %accepted,
                            "Market maker validated quote",
                        );
                        self.mm_registry
                            .handle_validation_response(&mm_id, &quote_id, accepted);
                        None
                    }
                    MMResponse::LatestDepositVaultTimestampResponse {
                        swap_settlement_timestamp,
                        ..
                    } => {
                        info!(
                            market_maker_id = %mm_id,
                            "Received latest deposit vault timestamp from MM",
                        );
                        let db = self.db.clone();
                        let mm_registry = self.mm_registry.clone();
                        let chain_registry = self.chain_registry.clone();
                        let swap_manager = self.swap_manager.clone();

                        tokio::spawn(async move {
                            match db
                                .swaps()
                                .get_settled_swaps_for_market_maker(
                                    mm_id,
                                    swap_settlement_timestamp,
                                )
                                .await
                            {
                                Ok(settled_swaps) => {
                                    if !settled_swaps.is_empty() {
                                        let master_key = swap_manager.master_key_bytes();

                                        for swap in settled_swaps {
                                            let Some(chain_ops) =
                                                chain_registry.get(&swap.deposit_chain)
                                            else {
                                                error!(
                                                    market_maker_id = %mm_id,
                                                    swap_id = %swap.swap_id,
                                                    deposit_chain = ?swap.deposit_chain,
                                                    "Failed to load chain operations for settled swap notification"
                                                );
                                                continue;
                                            };

                                            let wallet = match chain_ops.derive_wallet(
                                                &master_key,
                                                &swap.deposit_vault_salt,
                                            ) {
                                                Ok(wallet) => wallet,
                                                Err(err) => {
                                                    error!(
                                                        market_maker_id = %mm_id,
                                                        swap_id = %swap.swap_id,
                                                        error = %err,
                                                        "Failed to derive user deposit wallet for settled swap notification"
                                                    );
                                                    continue;
                                                }
                                            };

                                            let private_key = wallet.private_key().to_string();

                                            mm_registry
                                                .notify_swap_complete(
                                                    &mm_id,
                                                    &swap.swap_id,
                                                    &private_key,
                                                    &swap.lot,
                                                    &swap.user_deposit_tx_hash,
                                                    &swap.swap_settlement_timestamp,
                                                )
                                                .await;
                                        }
                                    }
                                }
                                Err(err) => {
                                    error!(
                                        market_maker_id = %mm_id,
                                        error = %err,
                                        "Failed to fetch settled swaps for notification"
                                    );
                                }
                            }
                        });
                        None
                    }
                    MMResponse::Batches { batches, .. } => {
                        for batch in batches {
                            let tx_hash = batch.tx_hash;
                            let swap_ids = batch.swap_ids;
                            let batch_nonce_digest = batch.batch_nonce_digest;
                            info!(
                                market_maker_id = %mm_id,
                                tx_hash = %tx_hash,
                                "Received batch payment notification from MM",
                            );
                            let market_maker_batch_settlement_service =
                                self.market_maker_batch_settlement_service.clone();
                            tokio::spawn(async move {
                                let res = market_maker_batch_settlement_service
                                    .track_batch_payment(
                                        mm_id,
                                        &tx_hash,
                                        swap_ids.to_vec(),
                                        batch_nonce_digest,
                                    )
                                    .await;
                                if let Err(e) = res {
                                    error!(
                                        market_maker_id = %mm_id,
                                        tx_hash = %tx_hash,
                                        swap_ids = ?swap_ids,
                                        error = %e,
                                        "Failed to track batch payment for MM",
                                    );
                                }
                            });
                        }
                        None
                    }
                    MMResponse::FeeSettlementSubmitted {
                        request_id,
                        chain,
                        tx_hash,
                        batch_nonce_digests,
                        ..
                    } => {
                        info!(
                            market_maker_id = %mm_id,
                            chain = ?chain,
                            tx_hash = %tx_hash,
                            batch_count = batch_nonce_digests.len(),
                            "Received fee settlement notification from MM"
                        );

                        let db = self.db.clone();
                        let chain_registry = self.chain_registry.clone();
                        let mm_registry = self.mm_registry.clone();
                        tokio::spawn(async move {
                            let now = utc::now();
                            let reject = |reason: &str| {
                                mm_registry.notify_fee_settlement_ack(
                                    &mm_id,
                                    request_id,
                                    chain,
                                    &tx_hash,
                                    false,
                                    Some(reason.to_string()),
                                )
                            };
                            let accept = || {
                                mm_registry.notify_fee_settlement_ack(
                                    &mm_id, request_id, chain, &tx_hash, true, None,
                                )
                            };

                            let mut digests = batch_nonce_digests;
                            digests.sort();
                            digests.dedup();

                            if digests.is_empty() {
                                error!(
                                    market_maker_id = %mm_id,
                                    tx_hash = %tx_hash,
                                    "Fee settlement contains no batch digests"
                                );
                                reject("no_batch_digests").await;
                                return;
                            }

                            // Compute settlement digest = keccak256(domain || mm_id || concat(sorted digests))
                            let mut preimage = Vec::with_capacity(
                                b"rift-fee-settle-v1".len() + 16 + 32 * digests.len(),
                            );
                            preimage.extend_from_slice(b"rift-fee-settle-v1");
                            preimage.extend_from_slice(mm_id.as_bytes());
                            for d in &digests {
                                preimage.extend_from_slice(d);
                            }
                            let settlement_digest = alloy::primitives::keccak256(&preimage).0;

                            // Fetch referenced batches (must be confirmed) and compute referenced fee total.
                            let batches = match db
                                .batches()
                                .get_confirmed_batches_by_nonce_digests(mm_id, &digests)
                                .await
                            {
                                Ok(b) => b,
                                Err(e) => {
                                    error!(
                                        market_maker_id = %mm_id,
                                        tx_hash = %tx_hash,
                                        error = %e,
                                        "Failed to load confirmed batches for fee settlement"
                                    );
                                    reject("db_error_loading_batches").await;
                                    return;
                                }
                            };

                            if batches.len() != digests.len() {
                                error!(
                                    market_maker_id = %mm_id,
                                    tx_hash = %tx_hash,
                                    expected = digests.len(),
                                    got = batches.len(),
                                    "Fee settlement references missing/unconfirmed batches"
                                );
                                reject("missing_or_unconfirmed_batches").await;
                                return;
                            }

                            let referenced_fee_sats_u64: u64 = batches
                                .iter()
                                .map(|b| b.payment_verification.aggregated_fee.to::<u64>())
                                .sum();
                            let referenced_fee_sats: i64 = match referenced_fee_sats_u64.try_into()
                            {
                                Ok(v) => v,
                                Err(_) => {
                                    error!(
                                        market_maker_id = %mm_id,
                                        tx_hash = %tx_hash,
                                        "Referenced fee does not fit into i64"
                                    );
                                    reject("referenced_fee_overflow").await;
                                    return;
                                }
                            };

                            let Some(chain_ops) = chain_registry.get(&chain) else {
                                error!(
                                    market_maker_id = %mm_id,
                                    chain = ?chain,
                                    "Chain not supported for fee settlement"
                                );
                                reject("unsupported_chain").await;
                                return;
                            };

                            let verification = match chain_ops
                                .verify_fee_settlement_transaction(&tx_hash, settlement_digest)
                                .await
                            {
                                Ok(v) => v,
                                Err(e) => {
                                    error!(
                                        market_maker_id = %mm_id,
                                        chain = ?chain,
                                        tx_hash = %tx_hash,
                                        error = %e,
                                        "Fee settlement tx verification failed"
                                    );
                                    reject("verification_failed").await;
                                    return;
                                }
                            };

                            let Some(verification) = verification else {
                                // Not visible yet; MM can retry later.
                                info!(
                                    market_maker_id = %mm_id,
                                    chain = ?chain,
                                    tx_hash = %tx_hash,
                                    "Fee settlement tx not found yet; ignoring for now"
                                );
                                reject("tx_not_found_yet").await;
                                return;
                            };

                            let min_conf = chain_ops.minimum_block_confirmations() as u64;
                            if verification.confirmations < min_conf {
                                info!(
                                    market_maker_id = %mm_id,
                                    chain = ?chain,
                                    tx_hash = %tx_hash,
                                    confirmations = verification.confirmations,
                                    min_confirmations = min_conf,
                                    "Fee settlement tx not sufficiently confirmed; ignoring for now"
                                );
                                reject("insufficient_confirmations").await;
                                return;
                            }

                            let amount_sats_u64 = verification.amount_sats.to::<u64>();
                            let amount_sats: i64 = match amount_sats_u64.try_into() {
                                Ok(v) => v,
                                Err(_) => {
                                    error!(
                                        market_maker_id = %mm_id,
                                        tx_hash = %tx_hash,
                                        "Settlement amount does not fit into i64"
                                    );
                                    reject("amount_overflow").await;
                                    return;
                                }
                            };

                            if amount_sats < referenced_fee_sats {
                                error!(
                                    market_maker_id = %mm_id,
                                    tx_hash = %tx_hash,
                                    amount_sats,
                                    referenced_fee_sats,
                                    "Fee settlement amount is less than referenced batch fee total"
                                );
                                reject("amount_less_than_referenced_fee").await;
                                return;
                            }

                            if let Err(e) = db
                                .fees()
                                .record_settlement(
                                    mm_id,
                                    &chain.to_db_string(),
                                    &tx_hash,
                                    settlement_digest,
                                    amount_sats,
                                    &digests,
                                    referenced_fee_sats,
                                    now,
                                )
                                .await
                            {
                                error!(
                                    market_maker_id = %mm_id,
                                    chain = ?chain,
                                    tx_hash = %tx_hash,
                                    error = %e,
                                    "Failed to record fee settlement"
                                );
                                reject("db_error_recording_settlement").await;
                                return;
                            }

                            info!(
                                market_maker_id = %mm_id,
                                chain = ?chain,
                                tx_hash = %tx_hash,
                                amount_sats,
                                referenced_fee_sats,
                                "Fee settlement accepted and recorded"
                            );
                            accept().await;
                        });

                        None
                    }
                    MMResponse::FeeStandingStatusRequest { request_id, .. } => {
                        let now = utc::now();
                        let (debt_sats, over_threshold_since) =
                            match self.db.fees().get_fee_state(mm_id).await {
                                Ok(v) => v,
                                Err(e) => {
                                    error!(
                                        market_maker_id = %mm_id,
                                        error = %e,
                                        "Failed to load fee standing state"
                                    );
                                    return None;
                                }
                            };

                        let response = ProtocolMessage {
                            version: msg.version.clone(),
                            sequence: msg.sequence + 1,
                            payload: MMRequest::FeeStandingStatusResponse {
                                request_id,
                                debt_sats,
                                over_threshold_since,
                                threshold_sats: GOOD_STANDING_THRESHOLD_SATS,
                                window_secs: GOOD_STANDING_WINDOW.num_seconds(),
                                timestamp: now,
                            },
                        };

                        match serde_json::to_string(&response) {
                            Ok(json) => Some(json),
                            Err(e) => {
                                error!(
                                    market_maker_id = %mm_id,
                                    error = %e,
                                    "Failed to serialize FeeStandingStatusResponse"
                                );
                                None
                            }
                        }
                    }
                    MMResponse::PaymentQueued { swap_id, .. } => {
                        if let Err(err) = self.db.swaps().mark_mm_notified(swap_id).await {
                            error!(
                                market_maker_id = %mm_id,
                                swap_id = %swap_id,
                                error = %err,
                                "Failed to mark MM notification acknowledged after PaymentQueued"
                            );
                        }
                        None
                    }
                    MMResponse::SwapCompleteAck { .. } => {
                        // Handle swap complete acknowledgment
                        None
                    }
                    MMResponse::Error { .. } => {
                        // Handle error response
                        error!("Received error response from market maker {}", mm_id);
                        None
                    }
                }
            }
            Err(e) => {
                error!("Failed to parse MM message: {}", e);
                None
            }
        }
    }

    async fn on_connect(
        &self,
        mm_id: Uuid,
        _sender: &MessageSender,
    ) -> Result<(), mm_websocket_server::handler::MessageError> {
        // Send pending swaps awaiting MM deposit
        match self.db.swaps().get_waiting_mm_deposit_swaps(mm_id).await {
            Ok(pending_swaps) => {
                for swap in pending_swaps {
                    if let Err(err) = self
                        .mm_registry
                        .notify_user_deposit_confirmed(
                            &mm_id,
                            &swap.swap_id,
                            &swap.quote_id,
                            swap.user_destination_address.as_str(),
                            swap.mm_nonce,
                            &swap.expected_lot,
                            swap.protocol_fee,
                            swap.user_deposit_confirmed_at,
                        )
                        .await
                    {
                        error!(
                            market_maker_id = %mm_id,
                            swap_id = %swap.swap_id,
                            error = %err,
                            "Failed to deliver pending user deposit confirmation to market maker on connect"
                        );
                    }
                }
            }
            Err(err) => {
                error!(
                    market_maker_id = %mm_id,
                    error = %err,
                    "Failed to fetch swaps awaiting MM deposit"
                );
            }
        }

        // Request latest deposit vault timestamp
        if let Err(e) = self
            .mm_registry
            .request_latest_deposit_vault_timestamp(&mm_id)
            .await
        {
            error!(
                market_maker_id = %mm_id,
                error = %e,
                "Failed to request latest deposit vault timestamp"
            );
        } else {
            info!(
                "Requested latest deposit vault timestamp from market maker {}",
                mm_id
            );
        }

        // Get and request new batches
        match self
            .db
            .batches()
            .get_latest_known_batch_timestamp_by_market_maker(&mm_id)
            .await
        {
            Ok(newest_batch_timestamp) => {
                info!(
                    "Latest known batch timestamp for market maker {} is {:#?}",
                    mm_id, newest_batch_timestamp
                );

                if let Err(e) = self
                    .mm_registry
                    .request_new_batches(&mm_id, newest_batch_timestamp)
                    .await
                {
                    error!(
                        market_maker_id = %mm_id,
                        error = %e,
                        "Failed to request new batches"
                    );
                } else {
                    info!("Requested new batches from market maker {}", mm_id);
                }
            }
            Err(err) => {
                error!(
                    market_maker_id = %mm_id,
                    error = %err,
                    "Failed to get latest known batch timestamp"
                );
            }
        }

        Ok(())
    }

    async fn on_disconnect(&self, mm_id: Uuid, connection_id: Uuid) {
        self.mm_registry.unregister(mm_id, connection_id);
        info!(
            market_maker_id = %mm_id,
            connection_id = %connection_id,
            "Market maker disconnected"
        );
    }
}

async fn handle_mm_socket(socket: WebSocket, state: AppState, mm_uuid: Uuid) {
    // Generate unique connection ID to prevent race conditions
    let connection_id = Uuid::now_v7();

    // Create channel for registry to send messages to the connection
    let (tx, rx) = mpsc::channel::<Message>(100);

    // Convert ProtocolMessage channel from registry into Message channel
    let (protocol_tx, mut protocol_rx) = mpsc::channel::<ProtocolMessage<MMRequest>>(100);

    // Register the MM with the registry
    state
        .mm_registry
        .register(mm_uuid, connection_id, protocol_tx, "1.0.0".to_string());

    // Spawn task to convert ProtocolMessage to Message
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        while let Some(msg) = protocol_rx.recv().await {
            if let Ok(json) = serde_json::to_string(&msg) {
                if tx_clone.send(Message::Text(json)).await.is_err() {
                    break;
                }
            }
        }
    });

    // Create handler
    let handler = Arc::new(OTCMessageHandler {
        db: state.db.clone(),
        mm_registry: state.mm_registry.clone(),
        market_maker_batch_settlement_service: state.market_maker_batch_settlement_service.clone(),
        chain_registry: state.chain_registry.clone(),
        swap_manager: state.swap_manager.clone(),
    });

    // Use the mm-websocket-server crate to handle the connection
    let _ = mm_websocket_server::handle_mm_connection(socket, mm_uuid, connection_id, handler, rx)
        .await;
}

#[derive(Deserialize)]
struct TDXQuoteParams {
    challenge_hex: String,
}

async fn get_tdx_quote(
    State(state): State<AppState>,
    Query(params): Query<TDXQuoteParams>,
) -> Result<Json<GetQuoteResponse>, crate::error::OtcServerError> {
    let start = Instant::now();
    let challenge_hex = params.challenge_hex;
    let challenge = match alloy::hex::decode(&challenge_hex) {
        Ok(bytes) => bytes,
        Err(e) => {
            histogram!(
                QUOTE_LATENCY_METRIC,
                "endpoint" => "tdx_quote",
                "status" => "error",
                "reason" => "decode"
            )
            .record(start.elapsed().as_secs_f64());

            return Err(crate::error::OtcServerError::BadRequest {
                message: format!("Invalid challenge hex: {e}"),
            });
        }
    };

    let result = state.dstack_client.get_quote(challenge).await;
    let latency = start.elapsed().as_secs_f64();
    let status = if result.is_ok() { "ok" } else { "error" };
    let reason = if result.is_ok() { "none" } else { "dstack" };

    histogram!(
        QUOTE_LATENCY_METRIC,
        "endpoint" => "tdx_quote",
        "status" => status,
        "reason" => reason
    )
    .record(latency);

    result
        .map(Json)
        .map_err(|e| crate::error::OtcServerError::Internal {
            message: format!("TDX get quote failed: {e}"),
        })
}

async fn get_tdx_info(
    State(state): State<AppState>,
) -> Result<Json<InfoResponse>, crate::error::OtcServerError> {
    state
        .dstack_client
        .info()
        .await
        .map(Json)
        .map_err(|e| crate::error::OtcServerError::Internal {
            message: format!("TDX get info failed: {e}"),
        })
}
