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
use std::sync::{Arc, Mutex};
use std::time::{Duration as StdDuration, Instant};
use crossbeam_channel::{unbounded, Select, Sender as CrossbeamSender, Receiver as CrossbeamReceiver};
use tokio::sync::mpsc;
use tokio::task::spawn_blocking;
use tokio::sync::broadcast;
use tracing::{error, info};
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
  tx: broadcast::Sender<(Uuid, Message)>,  // Broadcast channel for sending to MMs
  response_tx: Arc<CrossbeamSender<(Uuid, MMResponse)>>,
  registration_tx: Arc<CrossbeamSender<(Uuid, mpsc::UnboundedSender<MMResponse>, StdDuration)>>,
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

  let (tx, _) = broadcast::channel(100);

  let (response_tx, response_rx) = unbounded::<(Uuid, MMResponse)>();
  let (registration_tx, registration_rx) = unbounded::<(Uuid, mpsc::UnboundedSender<MMResponse>, StdDuration)>();

  let response_tx_arc = Arc::new(response_tx);
  let registration_tx_arc = Arc::new(registration_tx);

  spawn_blocking(move || {
      let mut collectors: HashMap<Uuid, (mpsc::UnboundedSender<MMResponse>, Instant)> = HashMap::new();

      let mut sel = Select::new();
      let response_oper = sel.recv(&response_rx);
      let registration_oper = sel.recv(&registration_rx);

      loop {
          let oper = sel.select();

          match oper.index() {
              i if i == response_oper => {
                  if let Ok((req_id, resp)) = oper.recv(&response_rx) {
                      if let Some((tx, deadline)) = collectors.get(&req_id) {
                          if Instant::now() < *deadline {
                              let _ = tx.send(resp);
                          }
                      }
                  }
              }
              i if i == registration_oper => {
                  if let Ok((req_id, tx, duration)) = oper.recv(&registration_rx) {
                      collectors.insert(req_id, (tx, Instant::now() + duration));
                  }
              }
              _ => unreachable!(),
          }

          // Clean up expired collectors
          collectors.retain(|_, (_, deadline)| Instant::now() < *deadline);
      }
  });

  let state = AppState {
      tx,
      response_tx: response_tx_arc,
      registration_tx: registration_tx_arc,
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
  info!("New MM connected");

  let mut rx = state.tx.subscribe();

  let (mut sender, mut receiver) = socket.split();

  let send_task = tokio::spawn(async move {
      while let Ok((req_id, msg)) = rx.recv().await {
          info!("Request id: {:?}", req_id);
          info!("Sending message to MM: {:#?}", msg);
          if sender.send(msg).await.is_err() {
              break;
          }
      }
  });

  let responses = state.response_tx.clone();
  let recv_task = tokio::spawn(async move {
      while let Some(Ok(msg)) = receiver.next().await {
          if let Message::Text(text) = msg {
              match serde_json::from_str::<ProtocolMessage<MMResponse>>(&text) {
                  Ok(proto_msg) => {
                      if let MMResponse::QuoteResponse { request_id, .. } = &proto_msg.payload {
                          let _ = responses.send((*request_id, proto_msg.payload));
                      }
                  }
                  Err(e) => error!("Invalid message: {}", e),
              }
          }
      }
  });

  tokio::join!(send_task, recv_task);
  info!("MM disconnected");
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

  info!("Sending quote request to MM: {:#?}", get_quote);

  let proto_msg = ProtocolMessage {
      version: "1.0.0".to_string(),
      sequence: 0,
      payload: get_quote,
  };

  let json = serde_json::to_string(&proto_msg).unwrap();
  state.tx.send((request_id, Message::Text(json))).ok();

  let (tx, mut rx) = mpsc::unbounded_channel::<MMResponse>();
  let collection_timeout = StdDuration::from_millis(1000);
  state.registration_tx.send((request_id, tx, collection_timeout)).ok();

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

  // Pick best (highest to_amount)
  if let Some(best) = quotes.into_iter().max_by_key(|q| 
    if let MMResponse::QuoteResponse { to_amount, .. } = q { 
      *to_amount
    } else { 
      U256::ZERO
    }) {
      if let MMResponse::QuoteResponse { quote_id, to_amount, expires_at, .. } = best {
        let response = QuoteResponse { quote_id, to_amount, expires_at };
        info!("Sending quote response: {}", serde_json::to_string(&response).unwrap());
        return Json(response).into_response();
      }
  }

  axum::http::StatusCode::NO_CONTENT.into_response()
}