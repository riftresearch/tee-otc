use crate::{
    Result,
    ServerBindSnafu,
    ServerStartSnafu,
};
use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, State},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use futures::{stream::StreamExt, SinkExt};
use otc_mm_protocol::{MMRequest, MMResponse, ProtocolMessage};
use otc_models::{ChainType, TokenIdentifier};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::IpAddr};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::{mpsc, RwLock, Semaphore};
use tokio::time::Duration;
use tracing::{error, info, warn, debug};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use alloy::primitives::U256;
use snafu::prelude::*;
use tokio::pin;

#[derive(Parser, Debug)]
#[command(name = "rfq-server")]
pub struct Args {
    /// Host to bind to
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    pub host: IpAddr,
    
    /// Port to bind to
    #[arg(short, long, default_value = "3000")]
    pub port: u16,
}

#[derive(Clone)]
struct AppState {
    mm_connections: Arc<RwLock<HashMap<Uuid, mpsc::Sender<(Uuid, Message)>>>>,
    collectors: Arc<RwLock<HashMap<Uuid, (mpsc::UnboundedSender<MMResponse>, Instant)>>>,
    broadcast_semaphore: Arc<Semaphore>,
    messages_sent: Arc<AtomicUsize>,
    messages_failed: Arc<AtomicUsize>,
}

#[derive(Deserialize)]
struct QuoteRequest {
    from_chain: ChainType,
    from_token: TokenIdentifier,
    from_amount: U256,
    to_chain: ChainType,
    to_token: TokenIdentifier,
}

#[derive(Serialize)]
struct QuoteResponse {
    quote_id: Uuid,
    to_amount: U256,
    expires_at: DateTime<Utc>,
}

pub async fn run_server(addr: SocketAddr) -> Result<()> {
    info!("Starting RFQ server...");

    let mm_connections = Arc::new(RwLock::new(HashMap::new()));
    let collectors = Arc::new(RwLock::new(HashMap::new()));
    let broadcast_semaphore = Arc::new(Semaphore::new(200));
    let messages_sent = Arc::new(AtomicUsize::new(0));
    let messages_failed = Arc::new(AtomicUsize::new(0));

    let collectors_cleanup = collectors.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            let now = Instant::now();
            let mut collectors_guard = collectors_cleanup.write().await;
            collectors_guard.retain(|_, (_, deadline)| {
                // Keep collectors that haven't expired with some buffer?
                now < *deadline + StdDuration::from_millis(100)
            });
        }
    });

    let state = AppState {
        mm_connections,
        collectors,
        broadcast_semaphore,
        messages_sent,
        messages_failed,
    };

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .route("/api/v1/quotes", post(request_quote))
        .with_state(state);

    info!("RFQ server listening on {}", addr);
    
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context(ServerBindSnafu)?;

    axum::serve(listener, app)
        .await
        .context(ServerStartSnafu)?;
    
    Ok(())
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_ws(socket, state))
}

async fn handle_ws(socket: WebSocket, state: AppState) {
    let mm_id = Uuid::new_v4();
    info!("New MM connected: {}", mm_id);

    let (tx, mut rx) = mpsc::channel::<(Uuid, Message)>(1000);
    
    // Register the MM connection
    {
        let mut connections = state.mm_connections.write().await;
        connections.insert(mm_id, tx);
    }

    let (mut sender, mut receiver) = socket.split();

    let send_task = tokio::spawn(async move {
        while let Some((req_id, msg)) = rx.recv().await {
            debug!("Sending message for request: {:?}", req_id);
            match sender.send(msg).await {
                Ok(_) => {
                    if let Err(e) = sender.flush().await {
                        error!("Failed to flush WebSocket: {:?}", e);
                        break;
                    }
                    debug!("Successfully sent message for request: {:?}", req_id);
                }
                Err(e) => {
                    error!("Failed to send message to MM WebSocket: {:?}", e);
                    break;
                }
            }
        }
        debug!("WebSocket send task ended for MM");
    });

    let collectors = state.collectors.clone();
    let recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            if let Message::Text(text) = msg {
                match serde_json::from_str::<ProtocolMessage<MMResponse>>(&text) {
                    Ok(proto_msg) => {
                        if let MMResponse::QuoteResponse { request_id, .. } = &proto_msg.payload {
                            let collectors_guard = collectors.read().await;
                            if let Some((tx, deadline)) = collectors_guard.get(request_id) {
                                if Instant::now() < *deadline {
                                    let _ = tx.send(proto_msg.payload);
                                }
                            }
                        }
                    }
                    Err(e) => error!("Invalid message: {}", e),
                }
            }
        }
    });

    tokio::join!(send_task, recv_task);
    
    {
        let mut connections = state.mm_connections.write().await;
        connections.remove(&mm_id);
    }
    
    info!("MM disconnected: {}", mm_id);
}

async fn request_quote(
    State(state): State<AppState>,
    Json(req): Json<QuoteRequest>,
) -> impl IntoResponse {
    let request_id = Uuid::new_v4();
    let get_quote = MMRequest::GetQuote {
        request_id,
        from_chain: req.from_chain,
        from_token: req.from_token,
        from_amount: req.from_amount,
        to_chain: req.to_chain,
        to_token: req.to_token,
        timestamp: Utc::now(),
    };

    debug!("Processing quote request: {:?}", request_id);

    let proto_msg = ProtocolMessage {
        version: "1.0.0".to_string(),
        sequence: 0,
        payload: get_quote,
    };

    let json = serde_json::to_string(&proto_msg).unwrap();
    let message = Message::Text(json);
    
    {
        let connections = state.mm_connections.read().await;
        let num_mms = connections.len();
        info!("Broadcasting quote request {} to {} connected MMs", request_id, num_mms);
        
        let mut send_tasks = Vec::new();
        
        for (mm_id, tx) in connections.iter() {
            let mm_id = *mm_id;
            let tx = tx.clone();
            let message = message.clone();
            let request_id = request_id;
            let semaphore = state.broadcast_semaphore.clone();
            let messages_sent = state.messages_sent.clone();
            let messages_failed = state.messages_failed.clone();
            
            let task = tokio::spawn(async move {
                let _permit = semaphore.acquire().await.ok()?;
                
                match tx.send((request_id, message)).await {
                    Ok(_) => {
                        messages_sent.fetch_add(1, Ordering::Relaxed);
                        debug!("Successfully queued message for MM {}", mm_id);
                        Some(())
                    }
                    Err(e) => {
                        messages_failed.fetch_add(1, Ordering::Relaxed);
                        warn!("Failed to queue message for MM {}: {:?}", mm_id, e);
                        None
                    }
                }
            });
            
            send_tasks.push(task);
        }
        
        // Wait for all broadcasts to complete
        let results = futures::future::join_all(send_tasks).await;
        let successful_sends = results.iter().filter(|r| r.is_ok() && r.as_ref().unwrap().is_some()).count();
        
        info!("Quote request {} broadcast to {}/{} MMs successfully", 
                request_id, successful_sends, num_mms);
        
        // Log stats
        let total_sent = state.messages_sent.load(Ordering::Relaxed);
        let total_failed = state.messages_failed.load(Ordering::Relaxed);
        debug!("Message statistics - Sent: {}, Failed: {}", total_sent, total_failed);
    }

    let (tx, mut rx) = mpsc::unbounded_channel::<MMResponse>();
    let collection_timeout = StdDuration::from_millis(500);
    
    // Register collector
    {
        let mut collectors = state.collectors.write().await;
        collectors.insert(request_id, (tx, Instant::now() + collection_timeout));
    }

    let mut quotes = Vec::new();
    let deadline = tokio::time::sleep(tokio::time::Duration::from(collection_timeout));
    pin!(deadline);

    loop {
        tokio::select! {
            Some(resp) = rx.recv() => {
                quotes.push(resp);
            }
            _ = &mut deadline => {
                break;
            }
        }
    }

    if let Some(best) = quotes.into_iter().max_by_key(|q| {
        if let MMResponse::QuoteResponse { to_amount, .. } = q { 
        *to_amount
        } else { 
        U256::ZERO
        }
    }) {
        if let MMResponse::QuoteResponse { quote_id, to_amount, expires_at, .. } = best {
            let response = QuoteResponse { quote_id, to_amount, expires_at };
            info!("Sending quote response: {}", serde_json::to_string(&response).unwrap());
            return Json(response).into_response();
        }
    }

    axum::http::StatusCode::NO_CONTENT.into_response()
}