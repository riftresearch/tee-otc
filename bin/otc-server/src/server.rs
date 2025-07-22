use crate::{
    api::swaps::{CreateSwapRequest, CreateSwapResponse, SwapResponse},
    config::Settings,
    db::Database,
    services::SwapManager,
    Result,
};
use axum::{
    extract::{ws::{WebSocket, WebSocketUpgrade}, Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post, Router},
    Json,
};
use clap::Parser;
use otc_chains::{bitcoin::BitcoinChain, ethereum::EthereumChain, ChainRegistry};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::{net::{IpAddr, SocketAddr}, sync::Arc};
use tracing::info;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "otc-server")]
#[command(about = "TEE-OTC server for cross-chain swaps")]
pub struct Args {
    /// Host to bind to
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    pub host: IpAddr,
    
    /// Port to bind to
    #[arg(short, long, default_value = "3000")]
    pub port: u16,
    
    /// Database URL
    #[arg(long, env = "DATABASE_URL", default_value = "postgres://otc_user:otc_password@localhost:5432/otc_db")]
    pub database_url: String,
}

#[derive(Clone)]
pub struct AppState {
    pub db: Database,
    pub swap_manager: Arc<SwapManager>,
}

#[derive(Serialize, Deserialize)]
struct Status {
    status: String,
    version: String,
}

pub async fn run_server(addr: SocketAddr, database_url: &str) -> Result<()> {
    info!("Starting OTC server...");
    
    // Load configuration
    let settings = Arc::new(Settings::load().map_err(|e| crate::Error::DatabaseInit { 
        source: crate::db::DbError::InvalidData { 
            message: format!("Failed to load settings: {}", e) 
        }
    })?);
    
    let db = Database::connect(database_url)
        .await
        .context(crate::DatabaseInitSnafu)?;
    
    info!("Initializing chain registry...");
    let mut chain_registry = ChainRegistry::new();
    
    // Initialize Bitcoin chain (mock for now)
    let bitcoin_chain = BitcoinChain::new(
        "http://localhost:8332",
        bitcoincore_rpc::Auth::UserPass("user".to_string(), "pass".to_string()),
        bitcoin::Network::Testnet,
    ).map_err(|e| crate::Error::DatabaseInit { 
        source: crate::db::DbError::InvalidData { 
            message: format!("Failed to initialize Bitcoin chain: {}", e) 
        }
    })?;
    chain_registry.register(otc_models::ChainType::Bitcoin, Arc::new(bitcoin_chain));
    
    // Initialize Ethereum chain (mock for now)
    let ethereum_chain = EthereumChain::new(
        "http://localhost:8545",
        1, // mainnet chain ID
    ).await.map_err(|e| crate::Error::DatabaseInit { 
        source: crate::db::DbError::InvalidData { 
            message: format!("Failed to initialize Ethereum chain: {}", e) 
        }
    })?;
    chain_registry.register(otc_models::ChainType::Ethereum, Arc::new(ethereum_chain));
    
    let chain_registry = Arc::new(chain_registry);
    
    info!("Initializing services...");
    let swap_manager = Arc::new(SwapManager::new(db.clone(), settings, chain_registry));
    
    let state = AppState { 
        db,
        swap_manager,
    };
    
    let app = Router::new()
        // Health check
        .route("/status", get(status_handler))
        // WebSocket endpoint
        .route("/ws", get(websocket_handler))
        // API endpoints
        .route("/api/v1/swaps", post(create_swap))
        .route("/api/v1/swaps/:id", get(get_swap))
        .with_state(state);
    
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

async fn handle_socket(mut socket: WebSocket) {
    info!("WebSocket connection established");
    
    while let Some(msg) = socket.recv().await {
        if let Ok(msg) = msg {
            match msg {
                axum::extract::ws::Message::Text(text) => {
                    info!("Received: {}", text);
                    
                    if socket
                        .send(axum::extract::ws::Message::Text(format!("Echo: {}", text)))
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

async fn create_swap(
    State(state): State<AppState>,
    Json(request): Json<CreateSwapRequest>,
) -> Result<Json<CreateSwapResponse>, (StatusCode, String)> {
    state.swap_manager
        .create_swap(request)
        .await
        .map(Json)
        .map_err(|e| {
            let status = match e {
                crate::services::swap_manager::SwapError::QuoteNotFound { .. } => StatusCode::NOT_FOUND,
                crate::services::swap_manager::SwapError::QuoteExpired => StatusCode::BAD_REQUEST,
                crate::services::swap_manager::SwapError::MarketMakerMismatch { .. } => StatusCode::BAD_REQUEST,
                crate::services::swap_manager::SwapError::MarketMakerRejected => StatusCode::CONFLICT,
                crate::services::swap_manager::SwapError::Database { .. } => StatusCode::INTERNAL_SERVER_ERROR,
                crate::services::swap_manager::SwapError::ChainNotSupported { .. } => StatusCode::BAD_REQUEST,
                crate::services::swap_manager::SwapError::WalletDerivation { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, e.to_string())
        })
}

async fn get_swap(
    State(state): State<AppState>,
    Path(swap_id): Path<Uuid>,
) -> Result<Json<SwapResponse>, (StatusCode, String)> {
    state.swap_manager
        .get_swap(swap_id)
        .await
        .map(Json)
        .map_err(|e| {
            let status = match e {
                crate::services::swap_manager::SwapError::QuoteNotFound { .. } => StatusCode::NOT_FOUND,
                crate::services::swap_manager::SwapError::Database { .. } => StatusCode::INTERNAL_SERVER_ERROR,
                crate::services::swap_manager::SwapError::ChainNotSupported { .. } => StatusCode::INTERNAL_SERVER_ERROR,
                crate::services::swap_manager::SwapError::WalletDerivation { .. } => StatusCode::INTERNAL_SERVER_ERROR,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, e.to_string())
        })
}