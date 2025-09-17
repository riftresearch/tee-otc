use crate::{
    error::RfqServerError, mm_registry::RfqMMRegistry, quote_aggregator::QuoteAggregator, Result,
    RfqServerArgs,
};
use alloy::primitives::U256;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use chainalysis_address_screener::{ChainalysisAddressScreener, RiskLevel};
use futures_util::{SinkExt, StreamExt};
use otc_auth::{api_keys::API_KEYS, ApiKeyStore};
use otc_models::{Currency, Lot, Quote, QuoteRequest};
use otc_protocols::rfq::{
    Connected, ProtocolMessage, QuoteWithFees, RFQRequest, RFQResponse, RFQResult,
};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::mpsc;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Clone)]
pub struct AppState {
    pub mm_registry: Arc<RfqMMRegistry>,
    pub api_key_store: Arc<ApiKeyStore>,
    pub quote_aggregator: Arc<QuoteAggregator>,
    pub address_screener: Option<ChainalysisAddressScreener>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Status {
    pub status: String,
    pub version: String,
    pub connected_market_makers: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QuoteResponse {
    pub request_id: Uuid,
    pub quote: Option<RFQResult<QuoteWithFees>>,
    pub total_quotes_received: usize,
    pub market_makers_contacted: usize,
}

pub async fn run_server(args: RfqServerArgs) -> Result<()> {
    info!("Starting RFQ server...");
    let addr = SocketAddr::from((args.host, args.port));

    // Initialize API key store
    let api_key_store = Arc::new(
        ApiKeyStore::new(API_KEYS.clone())
            .await
            .map_err(|e| crate::Error::ApiKeyLoad { source: e })?,
    );

    // Initialize MM registry
    let mm_registry = Arc::new(RfqMMRegistry::new());

    // Initialize quote aggregator
    let quote_aggregator = Arc::new(QuoteAggregator::new(
        mm_registry.clone(),
        args.quote_timeout_milliseconds,
    ));

    // Initialize optional Chainalysis address screener
    let address_screener = match (&args.chainalysis_host, &args.chainalysis_token) {
        (Some(host), Some(token)) if !host.is_empty() && !token.is_empty() => {
            match ChainalysisAddressScreener::new(host.clone(), token.clone()) {
                Ok(s) => Some(s),
                Err(e) => {
                    warn!("Failed to initialize address screener: {}", e);
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

    let state = AppState {
        mm_registry,
        api_key_store,
        quote_aggregator,
        address_screener,
    };

    let mut app = Router::new()
        // Health check
        .route("/status", get(status_handler))
        // WebSocket endpoint for market makers
        .route("/ws/mm", get(mm_websocket_handler))
        // API endpoints
        .route("/api/v1/quotes/request", post(request_quotes))
        .route(
            "/api/v1/market-makers/connected",
            get(get_connected_market_makers),
        )
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

async fn status_handler(State(state): State<AppState>) -> Json<Status> {
    Json(Status {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        connected_market_makers: state.mm_registry.get_connection_count(),
    })
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

async fn handle_mm_socket(socket: WebSocket, state: AppState, mm_uuid: Uuid) {
    info!(
        "RFQ Market maker {} WebSocket connection established",
        mm_uuid
    );

    // Channel for sending messages to the MM
    let (tx, mut rx) = mpsc::channel::<ProtocolMessage<RFQRequest>>(100);

    // Split the socket for bidirectional communication
    let (sender, mut receiver) = socket.split();

    // Register the MM
    state.mm_registry.register(
        mm_uuid,
        tx.clone(),
        "1.0.0".to_string(), // Default protocol version
    );

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
    let mm_id_clone = mm_uuid.clone();
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
    let mm_id_clone = mm_uuid.clone();
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
                match serde_json::from_str::<ProtocolMessage<RFQResponse>>(&text) {
                    Ok(msg) => match &msg.payload {
                        RFQResponse::QuoteResponse { request_id, .. } => {
                            // Route the response to the appropriate aggregator
                            state
                                .mm_registry
                                .handle_quote_response(*request_id, msg.payload.clone())
                                .await;
                        }
                        RFQResponse::Pong { .. } => {
                            // Handle pong for keepalive
                        }
                        RFQResponse::Error {
                            error_code,
                            message,
                            ..
                        } => {
                            warn!(
                                "Received error from market maker {}: {:?} - {}",
                                mm_uuid, error_code, message
                            );
                        }
                    },
                    Err(e) => {
                        error!("Failed to parse RFQ message: {}", e);
                    }
                }
            }
            Ok(Message::Close(_)) => {
                info!("Market maker {} disconnected", mm_uuid);
                break;
            }
            Err(e) => {
                error!("WebSocket error for market maker {}: {}", mm_uuid, e);
                break;
            }
            _ => {}
        }
    }

    // Unregister on disconnect
    state.mm_registry.unregister(mm_uuid);
    info!("Market maker {} unregistered", mm_uuid);
}

async fn request_quotes(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<QuoteRequest>,
) -> Result<Json<QuoteResponse>, RfqServerError> {
    info!(
        from_chain = ?request.from.chain,
        to_chain = ?request.to.chain,
        amount = %request.amount,
        quote_mode = ?request.mode,
        "Received quote request"
    );

    // Optional address screening via headers if screener is configured
    if let Some(screener) = &state.address_screener {
        let mut addresses_to_check: Vec<String> = Vec::new();
        if let Some(v) = headers.get("x-user-address").and_then(|v| v.to_str().ok()) {
            addresses_to_check.push(v.to_string());
        }
        if let Some(v) = headers
            .get("x-user-evm-account-address")
            .and_then(|v| v.to_str().ok())
        {
            addresses_to_check.push(v.to_string());
        }
        if let Some(v) = headers
            .get("x-user-destination-address")
            .and_then(|v| v.to_str().ok())
        {
            addresses_to_check.push(v.to_string());
        }

        for addr in addresses_to_check {
            match screener.get_address_risk(&addr).await {
                Ok(r) => {
                    if matches!(r.risk, RiskLevel::High | RiskLevel::Severe) {
                        warn!("Address {} blocked due to risk: {:?}", addr, r.risk);
                        return Err(RfqServerError::Forbidden {
                            message: format!("Address risk too high: {:?}", r.risk),
                        });
                    }
                }
                Err(e) => {
                    warn!("Address screening failed for {}: {}", addr, e);
                }
            }
        }
    }

    match state.quote_aggregator.request_quotes(request).await {
        Ok(result) => {
            info!(
                request_id = %result.request_id,
                "Quote aggregation successful"
            );

            Ok(Json(QuoteResponse {
                request_id: result.request_id,
                quote: result.best_quote,
                total_quotes_received: result.total_quotes_received,
                market_makers_contacted: result.market_makers_contacted,
            }))
        }
        Err(e) => {
            error!("Quote aggregation failed: {}", e);
            match e {
                crate::quote_aggregator::QuoteAggregatorError::NoMarketMakersConnected => {
                    Err(RfqServerError::ServiceUnavailable {
                        service: "market_makers".to_string(),
                    })
                }
                crate::quote_aggregator::QuoteAggregatorError::NoQuotesReceived => {
                    Err(RfqServerError::NoQuotesAvailable)
                }
                crate::quote_aggregator::QuoteAggregatorError::AggregationTimeout => {
                    Err(RfqServerError::Timeout {
                        message: "Quote collection timeout".to_string(),
                    })
                }
            }
        }
    }
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
