use crate::{
    api::swaps::{
        CreateSwapRequest, CreateSwapResponse, RefundSwapRequest, RefundSwapResponse, SwapResponse,
    },
    config::Settings,
    db::Database,
    services::{MMRegistry, SwapManager, SwapMonitoringService},
    OtcServerArgs, Result,
};
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
use futures_util::{SinkExt, StreamExt};
use otc_auth::{api_keys::API_KEYS, ApiKeyStore};
use otc_chains::{bitcoin::BitcoinChain, ethereum::EthereumChain, ChainRegistry};
use otc_protocols::mm::{Connected, MMRequest, MMResponse, ProtocolMessage};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::{net::SocketAddr, sync::Arc};
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
}

#[derive(Serialize, Deserialize)]
struct Status {
    status: String,
    version: String,
}

pub async fn run_server(args: OtcServerArgs) -> Result<()> {
    info!("Starting OTC server...");

    let addr = SocketAddr::from((args.host, args.port));

    // Load configuration
    let settings =
        Arc::new(
            Settings::load(&args.config_dir).map_err(|e| crate::Error::DatabaseInit {
                source: crate::error::OtcServerError::InvalidData {
                    message: format!("Failed to load settings: {e}"),
                },
            })?,
        );
    let db = Database::connect(&args.database_url)
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
    .await
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
        args.ethereum_mainnet_chain_id,
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

    // Initialize API key store
    let api_key_store = Arc::new(ApiKeyStore::new(API_KEYS.clone()).await?);

    // Initialize MM registry with 5-second validation timeout
    let mm_registry = Arc::new(MMRegistry::new(Duration::from_secs(5)));

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
    ));

    info!("Starting swap monitoring service...");
    tokio::spawn({
        let monitoring_service = swap_monitoring_service.clone();
        async move {
            monitoring_service.run().await;
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
    };

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
            let mm_uuid = market_maker_id.clone();
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
) -> Result<Json<String>, crate::error::OtcServerError> {
    let chain = state
        .chain_registry
        .get(&otc_models::ChainType::Bitcoin)
        .unwrap();
    chain.get_best_hash().await.map_err(Into::into).map(Json)
}

async fn get_best_ethereum_hash(
    State(state): State<AppState>,
) -> Result<Json<String>, crate::error::OtcServerError> {
    let chain = state
        .chain_registry
        .get(&otc_models::ChainType::Ethereum)
        .unwrap();
    chain.get_best_hash().await.map_err(Into::into).map(Json)
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

async fn handle_mm_socket(socket: WebSocket, state: AppState, mm_uuid: Uuid) {
    info!("Market maker {} WebSocket connection established", mm_uuid);

    // Channel for sending messages to the MM
    let (tx, mut rx) = mpsc::channel::<ProtocolMessage<MMRequest>>(100);

    // Split the socket for bidirectional communication
    let (sender, mut receiver) = socket.split();

    // Register the MM immediately (already authenticated via headers)
    state.mm_registry.register(
        mm_uuid,
        tx.clone(),
        "1.0.0".to_string(), // Default protocol version
    );

    let mm_id = mm_uuid.to_string();

    // Send Connected response
    let connected_response = Connected {
        session_id: Uuid::new_v4(),
        server_version: env!("CARGO_PKG_VERSION").to_string(),
        timestamp: utc::now(),
    };

    let response = serde_json::json!({
        "Connected": connected_response
    });

    let (sender_tx, mut sender_rx) = mpsc::channel::<Message>(100);

    // Send initial connected response
    if sender_tx
        .send(Message::Text(response.to_string()))
        .await
        .is_err()
    {
        error!("Failed to send Connected response");
        return;
    }

    // Spawn task to handle outgoing messages from the registry
    let mm_id_clone = mm_id.clone();
    let sender_tx_clone = sender_tx.clone();
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if let Ok(json) = serde_json::to_string(&msg) {
                if sender_tx_clone.send(Message::Text(json)).await.is_err() {
                    error!("Failed to send message to market maker {}", mm_id_clone);
                    break;
                }
            }
        }
    });

    // Spawn task to forward messages to the socket
    let mm_id_clone = mm_id.clone();
    let mut sender = sender;
    tokio::spawn(async move {
        while let Some(msg) = sender_rx.recv().await {
            if sender.send(msg).await.is_err() {
                error!(
                    "Failed to send message to market maker {} socket",
                    mm_id_clone
                );
                break;
            }
        }
    });

    // Handle incoming messages
    while let Some(msg) = receiver.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                match serde_json::from_str::<ProtocolMessage<MMResponse>>(&text) {
                    Ok(msg) => {
                        match &msg.payload {
                            MMResponse::QuoteValidated {
                                quote_id, accepted, ..
                            } => {
                                info!(
                                    "Market maker {} validated quote {}: accepted={}",
                                    mm_id, quote_id, accepted
                                );
                                state
                                    .mm_registry
                                    .handle_validation_response(&mm_uuid, quote_id, *accepted);
                            }
                            MMResponse::Pong { .. } => {
                                // Handle pong for keepalive
                            }
                            MMResponse::DepositInitiated { .. } => {
                                // Handle deposit notification - will be implemented when needed
                            }
                            MMResponse::SwapCompleteAck { .. } => {
                                // Handle swap complete acknowledgment
                            }
                            MMResponse::Error { .. } => {
                                // Handle error response
                                error!("Received error response from market maker {}", mm_id);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to parse MM message: {}", e);
                    }
                }
            }
            Ok(Message::Close(_)) => {
                info!("Market maker {} disconnected", mm_id);
                break;
            }
            Err(e) => {
                error!("WebSocket error for market maker {}: {}", mm_id, e);
                break;
            }
            _ => {}
        }
    }

    // Unregister on disconnect
    state.mm_registry.unregister(mm_uuid);
    info!("Market maker {} unregistered", mm_id);
}

async fn get_tdx_quote(
    State(state): State<AppState>,
    Query(challenge_hex): Query<String>,
) -> Result<Json<GetQuoteResponse>, crate::error::OtcServerError> {
    let challenge = alloy::hex::decode(challenge_hex).map_err(|e| {
        crate::error::OtcServerError::BadRequest {
            message: format!("Invalid challenge hex: {e}"),
        }
    })?;

    state
        .dstack_client
        .get_quote(challenge)
        .await
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
