use crate::{
    api::swaps::{
        BlockHashResponse, CreateSwapRequest, CreateSwapResponse, RefundSwapRequest,
        RefundSwapResponse, SwapResponse,
    },
    config::Settings,
    db::{
        swap_repo::{SWAP_FEES_TOTAL_METRIC, SWAP_VOLUME_TOTAL_METRIC},
        Database,
    },
    services::{
        swap_monitoring::{
            ACTIVE_INDIVIDUAL_SWAPS_METRIC, ACTIVE_SWAP_BATCHES_METRIC,
            SWAP_MONITORING_DURATION_METRIC,
        },
        MMRegistry, SwapManager, SwapMonitoringService,
    },
    OtcServerArgs, Result,
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
use mm_websocket_server::{MessageHandler, MessageSender};
use chainalysis_address_screener::{ChainalysisAddressScreener, RiskLevel};
use dstack_sdk::dstack_client::{DstackClient, GetQuoteResponse, InfoResponse};
use metrics::{describe_gauge, describe_histogram, describe_counter, gauge, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use otc_auth::{api_keys::API_KEYS, ApiKeyStore};
use otc_chains::{bitcoin::BitcoinChain, ethereum::EthereumChain, ChainRegistry};
use otc_protocols::mm::{MMRequest, MMResponse, ProtocolMessage};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::{
    net::SocketAddr,
    sync::{Arc, OnceLock},
    time::Instant,
};
use tokio::{sync::mpsc, time::Duration};
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::{error, info};
use uuid::Uuid;

#[derive(Clone)]
pub struct AppState {
    pub db: Database,
    pub swap_manager: Arc<SwapManager>,
    pub mm_registry: Arc<MMRegistry>,
    pub api_key_store: Arc<otc_auth::ApiKeyStore>,
    pub address_screener: Option<ChainalysisAddressScreener>,
    pub chain_registry: Arc<ChainRegistry>,
    pub dstack_client: Arc<DstackClient>,
    pub swap_monitoring_service: Arc<SwapMonitoringService>,
}

#[derive(Serialize, Deserialize)]
struct Status {
    status: String,
    version: String,
}

const QUOTE_LATENCY_METRIC: &str = "otc_quote_response_seconds";
static PROMETHEUS_HANDLE: OnceLock<Arc<PrometheusHandle>> = OnceLock::new();

pub async fn run_server(args: OtcServerArgs) -> Result<()> {
    info!("Starting OTC server...");

    let addr = SocketAddr::from((args.host, args.port));

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
    let ethereum_chain = EthereumChain::new(
        &args.ethereum_mainnet_rpc_url,
        &args.untrusted_ethereum_mainnet_token_indexer_url,
    )
    .await
    .map_err(|e| crate::Error::DatabaseInit {
        source: crate::error::OtcServerError::InvalidData {
            message: format!("Failed to initialize Ethereum chain: {e}"),
        },
    })?;
    chain_registry.register(otc_models::ChainType::Ethereum, Arc::new(ethereum_chain));

    let chain_registry = Arc::new(chain_registry);

    info!("Initializing services...");

    let api_key_store = Arc::new(ApiKeyStore::new(API_KEYS.clone()).await?);

    let mm_registry = Arc::new(MMRegistry::new());

    let swap_manager = Arc::new(SwapManager::new(
        db.clone(),
        settings.clone(),
        chain_registry.clone(),
        mm_registry.clone(),
    ));

    // Start the swap monitoring service
    let swap_monitoring_service = Arc::new(SwapMonitoringService::new(
        db.clone(),
        settings.clone(),
        chain_registry.clone(),
        mm_registry.clone(),
        args.chain_monitor_interval_seconds,
        args.max_concurrent_swaps,
    ));

    info!("Starting swap monitoring service...");
    tokio::spawn({
        let monitoring_service = swap_monitoring_service.clone();
        async move {
            monitoring_service.run().await;
        }
    });

    // Start periodic database metrics sync for Grafana
    info!("Starting database metrics sync...");
    tokio::spawn({
        let db = db.clone();
        async move {
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
            }
        }
    });

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
        address_screener,
        chain_registry,
        dstack_client,
        swap_monitoring_service,
    };

    if let Some(metrics_addr) = args.metrics_listen_addr {
        setup_metrics(metrics_addr).await?;
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
        .route("/api/v1/swaps", post(create_swap))
        .route("/api/v1/swaps/:id", get(get_swap))
        .route(
            "/api/v1/market-makers/connected",
            get(get_connected_market_makers),
        )
        .route("/api/v1/refund", post(refund_swap))
        .route(
            "/api/v1/chains/bitcoin/best-hash",
            get(get_best_bitcoin_hash),
        )
        .route(
            "/api/v1/chains/ethereum/best-hash",
            get(get_best_ethereum_hash),
        )
        .route("/api/v1/tdx/quote", get(get_tdx_quote))
        .route("/api/v1/tdx/info", get(get_tdx_info))
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

    axum::serve(listener, app)
        .await
        .context(crate::ServerStartSnafu)?;

    Ok(())
}

fn install_metrics_recorder() -> Result<Arc<PrometheusHandle>> {
    if let Some(handle) = PROMETHEUS_HANDLE.get() {
        return Ok(handle.clone());
    }

    let handle = PrometheusBuilder::new()
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
        SWAP_MONITORING_DURATION_METRIC,
        "Duration in seconds for monitoring all active swaps in a single iteration."
    );

    describe_gauge!(
        ACTIVE_INDIVIDUAL_SWAPS_METRIC,
        "Current number of swaps monitored individually in the latest monitoring cycle."
    );

    describe_gauge!(
        ACTIVE_SWAP_BATCHES_METRIC,
        "Current number of market maker deposit batches monitored in the latest monitoring cycle."
    );

    describe_gauge!(
        SWAP_VOLUME_TOTAL_METRIC,
        "Settled volume per market (sats), synced from database aggregate tables."
    );

    describe_gauge!(
        SWAP_FEES_TOTAL_METRIC,
        "Settled protocol fees per market (sats), synced from database aggregate tables."
    );

    describe_counter!(
        "ethereum_rpc_requests_total",
        "Total number of Ethereum RPC requests."
    );

    describe_histogram!(
        "ethereum_rpc_duration_seconds",
        "Duration in seconds of Ethereum RPC requests."
    );

    if PROMETHEUS_HANDLE.set(shared_handle.clone()).is_err() {
        if let Some(existing) = PROMETHEUS_HANDLE.get() {
            return Ok(existing.clone());
        }
    }

    Ok(shared_handle)
}

async fn setup_metrics(addr: SocketAddr) -> Result<()> {
    let shared_handle = install_metrics_recorder()?;

    let upkeep_handle = shared_handle.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(10));
        loop {
            ticker.tick().await;
            upkeep_handle.run_upkeep();
        }
    });

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context(crate::MetricsServerBindSnafu { addr })?;

    let metrics_state = shared_handle.clone();
    tokio::spawn(async move {
        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .with_state(metrics_state);

        if let Err(error) = axum::serve(listener, app).await {
            error!("Metrics server error: {}", error);
        }
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

async fn status_handler() -> impl IntoResponse {
    Json(Status {
        status: "online".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
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
                    info!("Received: {}", text);

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
    Json(request): Json<RefundSwapRequest>,
) -> Result<Json<RefundSwapResponse>, crate::error::OtcServerError> {
    state
        .swap_manager
        .refund_swap(request)
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

async fn create_swap(
    State(state): State<AppState>,
    Json(request): Json<CreateSwapRequest>,
) -> Result<Json<CreateSwapResponse>, crate::error::OtcServerError> {
    // Address screening (only if configured). Block if either address is High/Severe risk.
    if let Some(screener) = &state.address_screener {
        let mut to_check: Vec<String> = vec![request.user_destination_address.clone()];
        to_check.push(request.user_evm_account_address.to_string());

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
) -> Result<Json<SwapResponse>, crate::error::OtcServerError> {
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

#[derive(Serialize)]
struct ConnectedMarketMakersResponse {
    market_makers: Vec<Uuid>,
}

async fn get_connected_market_makers(
    State(state): State<AppState>,
) -> Json<ConnectedMarketMakersResponse> {
    let market_makers = state.mm_registry.get_connected_market_makers();
    Json(ConnectedMarketMakersResponse { market_makers })
}

// ========== MM WebSocket Handler Implementation ==========

/// Message handler for OTC protocol
struct OTCMessageHandler {
    db: Database,
    mm_registry: Arc<MMRegistry>,
    swap_monitoring_service: Arc<SwapMonitoringService>,
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
                                .get_settled_swaps_for_market_maker(mm_id, swap_settlement_timestamp)
                                .await
                            {
                                Ok(settled_swaps) => {
                                    if !settled_swaps.is_empty() {
                                        let master_key = swap_manager.master_key_bytes();

                                        for swap in settled_swaps {
                                            let Some(chain_ops) = chain_registry.get(&swap.deposit_chain) else {
                                                error!(
                                                    market_maker_id = %mm_id,
                                                    swap_id = %swap.swap_id,
                                                    deposit_chain = ?swap.deposit_chain,
                                                    "Failed to load chain operations for settled swap notification"
                                                );
                                                continue;
                                            };

                                            let wallet = match chain_ops.derive_wallet(&master_key, &swap.user_deposit_salt)
                                            {
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
                    MMResponse::Batches {
                        batches,
                        ..
                    } => {
                        for batch in batches {
                            let tx_hash = batch.tx_hash;
                            let swap_ids = batch.swap_ids;
                            let batch_nonce_digest = batch.batch_nonce_digest;
                            info!(
                                market_maker_id = %mm_id,
                                tx_hash = %tx_hash,
                                "Received batch payment notification from MM",
                            );
                            let swap_monitoring_service = self.swap_monitoring_service.clone();
                            tokio::spawn(async move {
                                let res = swap_monitoring_service
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
                    MMResponse::PaymentQueued { .. } => {
                        // Handle payment queued notification
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

    async fn on_connect(&self, mm_id: Uuid, _sender: &MessageSender) -> Result<(), mm_websocket_server::handler::MessageError> {
        // Send pending swaps awaiting MM deposit
        match self.db.swaps().get_waiting_mm_deposit_swaps(mm_id).await {
            Ok(pending_swaps) => {
                for swap in pending_swaps {
                    self.mm_registry
                        .notify_user_deposit_confirmed(
                            &mm_id,
                            &swap.swap_id,
                            &swap.quote_id,
                            swap.user_destination_address.as_str(),
                            swap.mm_nonce,
                            &swap.expected_lot,
                            swap.user_deposit_confirmed_at,
                        )
                        .await;
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
        if let Err(e) = self.mm_registry.request_latest_deposit_vault_timestamp(&mm_id).await {
            error!(
                market_maker_id = %mm_id,
                error = %e,
                "Failed to request latest deposit vault timestamp"
            );
        } else {
            info!("Requested latest deposit vault timestamp from market maker {}", mm_id);
        }

        // Get and request new batches
        match self.db.batches().get_latest_known_batch_timestamp_by_market_maker(&mm_id).await {
            Ok(newest_batch_timestamp) => {
                info!("Latest known batch timestamp for market maker {} is {:#?}", mm_id, newest_batch_timestamp);
                
                if let Err(e) = self.mm_registry.request_new_batches(&mm_id, newest_batch_timestamp).await {
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
    let connection_id = Uuid::new_v4();
    
    // Create channel for registry to send messages to the connection
    let (tx, rx) = mpsc::channel::<Message>(100);

    // Convert ProtocolMessage channel from registry into Message channel
    let (protocol_tx, mut protocol_rx) = mpsc::channel::<ProtocolMessage<MMRequest>>(100);
    
    // Register the MM with the registry
    state.mm_registry.register(
        mm_uuid,
        connection_id,
        protocol_tx,
        "1.0.0".to_string(),
    );

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
        swap_monitoring_service: state.swap_monitoring_service.clone(),
        chain_registry: state.chain_registry.clone(),
        swap_manager: state.swap_manager.clone(),
    });

    // Use the mm-websocket-server crate to handle the connection
    let _ = mm_websocket_server::handle_mm_connection(
        socket,
        mm_uuid,
        connection_id,
        handler,
        rx,
    )
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
