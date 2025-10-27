use crate::config::Config;
use crate::rfq_handler::RFQMessageHandler;
use futures_util::{SinkExt, StreamExt};
use otc_protocols::rfq::{ProtocolMessage, RFQRequest, RFQResponse};
use snafu::prelude::*;
use tokio::task::JoinSet;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex, Notify};
use tokio::time::{sleep, Duration, Instant};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{http, Message},
};
use tracing::{debug, error, info, warn};
use url::Url;
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum RfqClientError {
    #[snafu(display("WebSocket connection error: {}", source))]
    WebSocketConnection {
        source: tokio_tungstenite::tungstenite::Error,
    },

    #[snafu(display("URL parse error: {}", source))]
    UrlParse { source: url::ParseError },

    #[snafu(display("Message send error: {}", source))]
    MessageSend {
        source: tokio_tungstenite::tungstenite::Error,
    },

    #[snafu(display("Message serialization error: {}", source))]
    Serialization { source: serde_json::Error },

    #[snafu(display("Maximum reconnection attempts reached"))]
    MaxReconnectAttempts,
}

type Result<T, E = RfqClientError> = std::result::Result<T, E>;

type WebSocketMessage = Message;

const PING_INTERVAL_SECS: u64 = 10;  // Reduced from 30 - faster detection
const PING_TIMEOUT_SECS: u64 = 5;    // Reduced from 10 - faster timeout
const READ_TIMEOUT_SECS: u64 = 20;   // Reduced from 60 - faster fallback
const WS_PROTOCOL_PING_INTERVAL_SECS: u64 = 20;

struct PendingPing {
    id: Uuid,
    sent_at: Instant,
}

#[derive(Clone)]
pub struct WebSocketSender {
    tx: mpsc::Sender<WebSocketMessage>,
}

impl WebSocketSender {
    #[allow(dead_code)]
    pub async fn send(
        &self,
        message: WebSocketMessage,
    ) -> Result<(), mpsc::error::SendError<WebSocketMessage>> {
        self.tx.send(message).await
    }

    #[allow(dead_code)]
    pub async fn send_protocol_message<T>(&self, message: &ProtocolMessage<T>) -> Result<()>
    where
        T: serde::Serialize,
    {
        let json = serde_json::to_string(message).context(SerializationSnafu)?;
        self.tx
            .send(Message::Text(json))
            .await
            .map_err(|_| RfqClientError::MessageSend {
                source: tokio_tungstenite::tungstenite::Error::ConnectionClosed,
            })?;
        Ok(())
    }
}

pub struct RfqClient {
    config: Config,
    handler: RFQMessageHandler,
    rfq_ws_url: String,
    sender: Option<WebSocketSender>,
}

impl RfqClient {
    pub fn new(config: Config, rfq_handler: RFQMessageHandler, rfq_ws_url: String) -> Self {
        Self {
            config,
            handler: rfq_handler,
            rfq_ws_url,
            sender: None,
        }
    }

    #[allow(dead_code)]
    pub fn get_sender(&self) -> Option<WebSocketSender> {
        self.sender.clone()
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut reconnect_attempts = 0;

        loop {
            match self.connect_and_run().await {
                Ok(()) => {
                    info!("RFQ WebSocket connection closed normally, will reconnect");
                    reconnect_attempts = 0;
                }
                Err(e) => {
                    error!(
                        "RFQ WebSocket error: {}, reconnecting (attempt {}/{})",
                        e, reconnect_attempts + 1, self.config.max_reconnect_attempts
                    );
                    reconnect_attempts += 1;

                    if reconnect_attempts >= self.config.max_reconnect_attempts {
                        return Err(RfqClientError::MaxReconnectAttempts);
                    }

                    let delay = Duration::from_secs(
                        self.config.reconnect_interval_secs * u64::from(reconnect_attempts),
                    );
                    warn!(
                        "Reconnecting to RFQ server in {} seconds (attempt {}/{})",
                        delay.as_secs(),
                        reconnect_attempts,
                        self.config.max_reconnect_attempts
                    );
                    sleep(delay).await;
                }
            }
        }
    }

    async fn connect_and_run(&mut self) -> Result<()> {
        let url = Url::parse(&self.rfq_ws_url).context(UrlParseSnafu)?;
        info!("Connecting to RFQ server at {}", url);

        // Build request with authentication headers
        let request = http::Request::builder()
            .method("GET")
            .uri(url.as_str())
            .header("Host", url.host_str().unwrap_or("localhost"))
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header(
                "Sec-WebSocket-Key",
                tokio_tungstenite::tungstenite::handshake::client::generate_key(),
            )
            .header("X-API-ID", &self.config.market_maker_id.to_string())
            .header("X-API-SECRET", &self.config.api_secret)
            .body(())
            .map_err(|e| RfqClientError::WebSocketConnection {
                source: tokio_tungstenite::tungstenite::Error::Http(
                    http::Response::builder()
                        .status(400)
                        .body(Some(format!("Failed to build request: {e}").into_bytes()))
                        .unwrap(),
                ),
            })?;

        let (ws_stream, _) = connect_async_with_config(request, None, false)
            .await
            .context(WebSocketConnectionSnafu)?;

        info!("RFQ WebSocket connected and authenticated - will verify with ping/pong");

        let (ws_sink, mut ws_stream) = ws_stream.split();

        // Create channel for writer task (buffer size of 1024 messages)
        let (websocket_tx, mut websocket_rx) = mpsc::channel::<WebSocketMessage>(1024);

        // Store sender in self for external access
        self.sender = Some(WebSocketSender {
            tx: websocket_tx.clone(),
        });

        let mut rfq_join_set = JoinSet::new();

        // Spawn single writer task that owns the sink
        // This ensures all messages are sent in the order they are queued,
        // without interleaving that could corrupt the protocol
        rfq_join_set.spawn(async move {
            let mut sink = ws_sink;
            while let Some(message) = websocket_rx.recv().await {
                if let Err(e) = sink.send(message).await {
                    error!("Failed to send RFQ WebSocket message: {}", e);
                    break;
                }
            }
            // Gracefully close the sink
            let _ = sink.flush().await;
            let _ = sink.close().await;
            info!("RFQ WebSocket writer task finished");
        });

        let pending_ping = Arc::new(Mutex::new(None::<PendingPing>));
        let disconnect_notify = Arc::new(Notify::new());

        let ping_sender = websocket_tx.clone();
        let pending_for_ping = pending_ping.clone();
        let notify_for_ping = disconnect_notify.clone();
        let (ping_shutdown_tx, ping_shutdown_rx) = oneshot::channel();
        let mut ping_shutdown_rx = ping_shutdown_rx;
        rfq_join_set.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(PING_INTERVAL_SECS));
            let mut sequence: u64 = 0;
            let version = env!("CARGO_PKG_VERSION").to_string();

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let mut new_ping_id = None;
                        let mut timed_out = false;
                        let mut last_ping_id = None;
                        {
                            let guard = pending_for_ping.lock().await;
                            if let Some(pending) = guard.as_ref() {
                                if pending.sent_at.elapsed() >= Duration::from_secs(PING_TIMEOUT_SECS) {
                                    timed_out = true;
                                    last_ping_id = Some(pending.id);
                                }
                            } else {
                                new_ping_id = Some(Uuid::new_v4());
                            }
                        }

                        if timed_out {
                            warn!(
                                "RFQ ping watchdog detected no pong within {}s; closing connection (last ping: {:?})",
                                PING_TIMEOUT_SECS,
                                last_ping_id
                            );
                            notify_for_ping.notify_waiters();
                            break;
                        }

                        if let Some(ping_id) = new_ping_id {
                            let ping_message = ProtocolMessage {
                                version: version.clone(),
                                sequence,
                                payload: RFQResponse::Ping {
                                    request_id: ping_id,
                                    timestamp: utc::now(),
                                },
                            };

                            match serde_json::to_string(&ping_message) {
                                Ok(json) => {
                                    if ping_sender.send(Message::Text(json)).await.is_err() {
                                        warn!("RFQ ping sender channel closed; stopping ping task");
                                        notify_for_ping.notify_waiters();
                                        break;
                                    }

                                    debug!("Sent RFQ ping {}", ping_id);
                                    let mut guard = pending_for_ping.lock().await;
                                    *guard = Some(PendingPing {
                                        id: ping_id,
                                        sent_at: Instant::now(),
                                    });
                                    sequence = sequence.wrapping_add(1);
                                }
                                Err(e) => {
                                    error!("Failed to serialize RFQ ping: {}", e);
                                }
                            }
                        }
                    }
                    _ = &mut ping_shutdown_rx => {
                        break;
                    }
                }
            }
        });

        // Spawn WebSocket protocol-level ping task for proxy keepalive
        let ws_ping_sender = websocket_tx.clone();
        let notify_for_ws_ping = disconnect_notify.clone();
        let (ws_ping_shutdown_tx, ws_ping_shutdown_rx) = oneshot::channel();
        let mut ws_ping_shutdown_rx = ws_ping_shutdown_rx;
        rfq_join_set.spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(WS_PROTOCOL_PING_INTERVAL_SECS));
            // Skip first tick to avoid immediate ping
            interval.tick().await;

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if ws_ping_sender.send(Message::Ping(vec![])).await.is_err() {
                            info!("WebSocket protocol ping sender channel closed");
                            notify_for_ws_ping.notify_waiters();
                            break;
                        }
                    }
                    _ = &mut ws_ping_shutdown_rx => {
                        break;
                    }
                }
            }
        });

        let message_handler = Arc::new(self.handler.clone());
        let pending_ping_reader = pending_ping.clone();
        let read_timeout = tokio::time::sleep(Duration::from_secs(READ_TIMEOUT_SECS));
        tokio::pin!(read_timeout);

        loop {
            tokio::select! {
                _ = disconnect_notify.notified() => {
                    warn!("RFQ disconnect requested by watchdog (ping timeout or task failure)");
                    break;
                }
                _ = &mut read_timeout => {
                    warn!(
                        "RFQ WebSocket read timed out after {}s; disconnecting",
                        READ_TIMEOUT_SECS
                    );
                    disconnect_notify.notify_waiters();
                    break;
                }
                res = rfq_join_set.join_next() => { 
                    warn!("RFQ join set task finished: {:?}", res);
                    break;
                }
                maybe_msg = ws_stream.next() => {
                    match maybe_msg {
                        Some(msg) => {
                            read_timeout.as_mut().reset(Instant::now() + Duration::from_secs(READ_TIMEOUT_SECS));
                            match msg {
                                Ok(Message::Text(text)) => {
                                    match serde_json::from_str::<ProtocolMessage<RFQRequest>>(&text) {
                                        Ok(protocol_msg) => {
                                            if let RFQRequest::Pong { request_id, .. } = &protocol_msg.payload {
                                                let mut guard = pending_ping_reader.lock().await;
                                                if guard
                                                    .as_ref()
                                                    .map(|pending| pending.id == *request_id)
                                                    .unwrap_or(false)
                                                {
                                                    debug!("Received pong for ping {}, connection healthy", request_id);
                                                    *guard = None;
                                                }

                                                continue;
                                            }

                                            let websocket_sender = websocket_tx.clone();
                                            let handler = message_handler.clone();
                                            tokio::spawn(async move {
                                                if let Some(response) =
                                                    handler.handle_request(&protocol_msg).await
                                                {
                                                    let response_json = match serde_json::to_string(&response) {
                                                        Ok(json) => json,
                                                        Err(e) => {
                                                            error!("Failed to serialize response: {}", e);
                                                            return;
                                                        }
                                                    };

                                                    if let Err(e) = websocket_sender.send(Message::Text(response_json)).await {
                                                        error!("Failed to queue response message: {}", e);
                                                    }
                                                }
                                            });
                                        }
                                        Err(e) => {
                                            error!("Failed to parse RFQ message: {}", e);
                                        }
                                    }
                                }
                                Ok(Message::Close(_)) => {
                                    info!("RFQ server closed connection");
                                    disconnect_notify.notify_waiters();
                                    break;
                                }
                                Err(e) => {
                                    error!("RFQ WebSocket error: {}", e);
                                    disconnect_notify.notify_waiters();
                                    break;
                                }
                                _ => {}
                            }
                        }
                        None => {
                            info!("RFQ WebSocket stream ended by server (graceful shutdown or connection lost)");
                            disconnect_notify.notify_waiters();
                            break;
                        }
                    }
                }
            }
        }

        let _ = ping_shutdown_tx.send(());

        let _ = ws_ping_shutdown_tx.send(());

        info!("Shutting down RFQ join set");
        let s = tokio::time::Instant::now();
        rfq_join_set.shutdown().await;
        info!("RFQ join set shutdown completed in {:?}", s.elapsed());

        // Clean up: drop sender to signal writer task to exit
        self.sender = None;
        drop(websocket_tx);

        Ok(())
    }
}
