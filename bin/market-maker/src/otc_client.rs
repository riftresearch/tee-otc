use crate::config::Config;
use crate::db::{DepositRepository, PaymentRepository, QuoteRepository};
use crate::otc_handler::OTCMessageHandler;
use crate::payment_manager::PaymentManager;
use futures_util::{SinkExt, StreamExt};
use otc_protocols::mm::{MMRequest, MMResponse, MMStatus, ProtocolMessage};
use snafu::prelude::*;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use tokio::time::{sleep, Duration, Instant};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{http, Message},
};
use tracing::{debug, error, info, warn};
use url::Url;
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum ClientError {
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
    #[snafu(display("Background thread exited: {}", source))]
    BackgroundThreadExited {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

type Result<T, E = ClientError> = std::result::Result<T, E>;

type WebSocketMessage = Message;

const PING_INTERVAL_SECS: u64 = 30;
const PING_TIMEOUT_SECS: u64 = 10;
const READ_TIMEOUT_SECS: u64 = 60;
const WS_PROTOCOL_PING_INTERVAL_SECS: u64 = 20;

struct PendingPing {
    id: Uuid,
    sent_at: Instant,
}

pub struct OtcFillClient {
    config: Config,
    handler: OTCMessageHandler,
    /// Broadcasts the currently active websocket sender (if any) to the persistent forwarder
    websocket_tx_watch: watch::Sender<Option<mpsc::Sender<WebSocketMessage>>>,
}

impl OtcFillClient {
    pub fn new(
        config: Config,
        quote_repository: Arc<QuoteRepository>,
        deposit_repository: Arc<DepositRepository>,
        payment_manager: Arc<PaymentManager>,
        payment_repository: Arc<PaymentRepository>,
        otc_response_rx: mpsc::UnboundedReceiver<ProtocolMessage<MMResponse>>,
    ) -> Self {
        let handler = OTCMessageHandler::new(quote_repository, deposit_repository, payment_manager, payment_repository);
        let (websocket_tx_watch, mut websocket_tx_watch_rx) =
            watch::channel::<Option<mpsc::Sender<WebSocketMessage>>>(None);

        tokio::spawn(async move {
            let mut otc_response_rx = otc_response_rx;
            while let Some(response_msg) = otc_response_rx.recv().await {
                let response_json = match serde_json::to_string(&response_msg) {
                    Ok(json) => json,
                    Err(e) => {
                        error!("Failed to serialize unsolicited MMResponse: {}", e);
                        continue;
                    }
                };

                let message = Message::Text(response_json);

                loop {
                    let current_sender = websocket_tx_watch_rx.borrow().clone();
                    match current_sender {
                        Some(sender) => match sender.send(message.clone()).await {
                            Ok(_) => break,
                            Err(e) => {
                                error!("Failed to queue unsolicited MMResponse message: {}", e);
                                if websocket_tx_watch_rx.changed().await.is_err() {
                                    info!("WebSocket sender watch channel closed, stopping MMResponse forwarder");
                                    return;
                                }
                            }
                        },
                        None => {
                            if websocket_tx_watch_rx.changed().await.is_err() {
                                info!("WebSocket sender watch channel closed, stopping MMResponse forwarder");
                                return;
                            }
                        }
                    }
                }
            }
        });

        Self {
            config,
            handler,
            websocket_tx_watch,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let mut reconnect_attempts = 0;

        loop {
            match self.connect_and_run().await {
                Ok(()) => {
                    info!("WebSocket connection closed normally");
                    reconnect_attempts = 0;
                }
                Err(e) => {
                    error!("WebSocket error: {}", e);
                    reconnect_attempts += 1;

                    if reconnect_attempts >= self.config.max_reconnect_attempts {
                        return Err(ClientError::MaxReconnectAttempts);
                    }

                    let delay = Duration::from_secs(
                        self.config.reconnect_interval_secs * u64::from(reconnect_attempts),
                    );
                    warn!(
                        "Reconnecting in {} seconds (attempt {}/{})",
                        delay.as_secs(),
                        reconnect_attempts,
                        self.config.max_reconnect_attempts
                    );
                    sleep(delay).await;
                }
            }
        }
    }

    // TODO(tee): When TEE logic is implemented, we need a way to validate that we're connected to a valid TEE
    async fn connect_and_run(&self) -> Result<()> {
        let url = Url::parse(&self.config.otc_ws_url).context(UrlParseSnafu)?;
        info!("Connecting to {}", url);

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
            .map_err(|e| ClientError::WebSocketConnection {
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

        info!("WebSocket connected, authenticated via headers");

        let (ws_sink, mut ws_stream) = ws_stream.split();

        // Create channel for writer task (buffer size of 1024 messages)
        let (websocket_tx, mut websocket_rx) = mpsc::channel::<WebSocketMessage>(1024);

        // Spawn single writer task that owns the sink
        // This ensures all messages are sent in the order they are queued,
        // without interleaving that could corrupt the protocol
        let writer_handle = tokio::spawn(async move {
            let mut sink = ws_sink;
            while let Some(message) = websocket_rx.recv().await {
                if let Err(e) = sink.send(message).await {
                    error!("Failed to send WebSocket message: {}", e);
                    break;
                }
            }
            // Gracefully close the sink
            let _ = sink.flush().await;
            let _ = sink.close().await;
            info!("WebSocket writer task finished");
        });

        let pending_ping = Arc::new(Mutex::new(None::<PendingPing>));
        let disconnect_notify = Arc::new(Notify::new());

        let ping_sender = websocket_tx.clone();
        let pending_for_ping = pending_ping.clone();
        let notify_for_ping = disconnect_notify.clone();
        let (ping_shutdown_tx, ping_shutdown_rx) = oneshot::channel();
        let mut ping_shutdown_rx = ping_shutdown_rx;
        let ping_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(PING_INTERVAL_SECS));
            let mut sequence: u64 = 0;
            let version = env!("CARGO_PKG_VERSION").to_string();

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let mut new_ping_id = None;
                        let mut timed_out = false;
                        {
                            let guard = pending_for_ping.lock().await;
                            if let Some(pending) = guard.as_ref() {
                                if pending.sent_at.elapsed() >= Duration::from_secs(PING_TIMEOUT_SECS) {
                                    timed_out = true;
                                }
                            } else {
                                new_ping_id = Some(Uuid::new_v4());
                            }
                        }

                        if timed_out {
                            warn!(
                                "OTC ping watchdog detected no pong within {}s; closing connection",
                                PING_TIMEOUT_SECS
                            );
                            notify_for_ping.notify_waiters();
                            break;
                        }

                        if let Some(ping_id) = new_ping_id {
                            let ping_message = ProtocolMessage {
                                version: version.clone(),
                                sequence,
                                payload: MMResponse::Ping {
                                    request_id: ping_id,
                                    status: MMStatus::Active,
                                    version: version.clone(),
                                    timestamp: utc::now(),
                                },
                            };

                            match serde_json::to_string(&ping_message) {
                                Ok(json) => {
                                    if ping_sender.send(Message::Text(json)).await.is_err() {
                                        warn!("OTC ping sender channel closed; stopping ping task");
                                        notify_for_ping.notify_waiters();
                                        break;
                                    }

                                    let mut guard = pending_for_ping.lock().await;
                                    *guard = Some(PendingPing {
                                        id: ping_id,
                                        sent_at: Instant::now(),
                                    });
                                    sequence = sequence.wrapping_add(1);
                                }
                                Err(e) => {
                                    error!("Failed to serialize OTC ping: {}", e);
                                }
                            }
                        }
                    }
                    _ = &mut ping_shutdown_rx => {
                        debug!("OTC ping task received shutdown signal");
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
        let ws_ping_handle = tokio::spawn(async move {
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

        if self
            .websocket_tx_watch
            .send(Some(websocket_tx.clone()))
            .is_err()
        {
            warn!("Failed to publish websocket sender to MMResponse forwarder");
        }

        let message_handler = Arc::new(self.handler.clone());
        let read_timeout = tokio::time::sleep(Duration::from_secs(READ_TIMEOUT_SECS));
        tokio::pin!(read_timeout);

        // Handle incoming messages
        loop {
            tokio::select! {
                _ = disconnect_notify.notified() => {
                    warn!("OTC disconnect requested by watchdog");
                    break;
                }
                _ = &mut read_timeout => {
                    warn!(
                        "OTC WebSocket read timed out after {}s; disconnecting",
                        READ_TIMEOUT_SECS
                    );
                    disconnect_notify.notify_waiters();
                    break;
                }
                maybe_msg = ws_stream.next() => {
                    match maybe_msg {
                        Some(msg) => {
                            read_timeout.as_mut().reset(Instant::now() + Duration::from_secs(READ_TIMEOUT_SECS));
                            match msg {
                                Ok(Message::Text(text)) => {
                                    match serde_json::from_str::<ProtocolMessage<MMRequest>>(&text) {
                                        Ok(protocol_msg) => {
                                            if let MMRequest::Pong { request_id, .. } = &protocol_msg.payload {
                                                let mut guard = pending_ping.lock().await;
                                                if guard
                                                    .as_ref()
                                                    .map(|pending| pending.id == *request_id)
                                                    .unwrap_or(false)
                                                {
                                                    *guard = None;
                                                }
                                            }

                                            let websocket_sender = websocket_tx.clone();
                                            let message_handler = message_handler.clone();
                                            tokio::spawn(async move {
                                                if let Some(response) =
                                                    message_handler.handle_request(&protocol_msg).await
                                                {
                                                    let response_json =
                                                        match serde_json::to_string(&response) {
                                                            Ok(json) => json,
                                                            Err(e) => {
                                                                error!("Failed to serialize response: {}", e);
                                                                return;
                                                            }
                                                        };

                                                    if let Err(e) = websocket_sender
                                                        .send(Message::Text(response_json))
                                                        .await
                                                    {
                                                        error!("Failed to queue response message: {}", e);
                                                    }
                                                }
                                            });
                                        }
                                        Err(e) => {
                                            error!("Failed to parse message: {}", e);
                                        }
                                    }
                                }
                                Ok(Message::Close(_)) => {
                                    info!("Server closed connection");
                                    disconnect_notify.notify_waiters();
                                    break;
                                }
                                Err(e) => {
                                    error!("WebSocket error: {}", e);
                                    disconnect_notify.notify_waiters();
                                    break;
                                }
                                _ => {}
                            }
                        }
                        None => {
                            info!("OTC WebSocket stream ended by server");
                            disconnect_notify.notify_waiters();
                            break;
                        }
                    }
                }
            }
        }

        let _ = ping_shutdown_tx.send(());
        let _ = ping_handle.await;

        let _ = ws_ping_shutdown_tx.send(());
        let _ = ws_ping_handle.await;

        // Clean up: drop sender to signal writer task to exit
        drop(websocket_tx);

        if self.websocket_tx_watch.send(None).is_err() {
            warn!("Failed to clear websocket sender for MMResponse forwarder");
        }

        // Wait for writer task to finish
        let _ = writer_handle.await;

        Ok(())
    }
}
