use crate::config::Config;
use crate::deposit_key_storage::DepositKeyStorage;
use crate::otc_handler::OTCMessageHandler;
use crate::payment_manager::PaymentManager;
use crate::quote_storage::QuoteStorage;
use futures_util::{SinkExt, StreamExt};
use otc_protocols::mm::{MMRequest, MMResponse, ProtocolMessage};
use snafu::prelude::*;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{http, Message},
};
use tracing::{error, info, warn};
use url::Url;

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

pub struct OtcFillClient {
    config: Config,
    handler: OTCMessageHandler,
    /// Receiver for unsolicited MMResponse messages (taken on first run)
    otc_response_rx: Arc<tokio::sync::Mutex<Option<mpsc::UnboundedReceiver<ProtocolMessage<MMResponse>>>>>,
}

impl OtcFillClient {
    pub fn new(
        config: Config,
        quote_storage: Arc<QuoteStorage>,
        deposit_key_storage: Arc<DepositKeyStorage>,
        payment_manager: Arc<PaymentManager>,
        otc_response_rx: mpsc::UnboundedReceiver<ProtocolMessage<MMResponse>>,
    ) -> Self {
        let handler = OTCMessageHandler::new(quote_storage, deposit_key_storage, payment_manager);
        Self { 
            config, 
            handler,
            otc_response_rx: Arc::new(tokio::sync::Mutex::new(Some(otc_response_rx))),
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


        let message_handler = Arc::new(self.handler.clone());

        // Take the receiver for unsolicited MMResponse messages (only on first connection)
        let mut otc_response_rx = {
            let mut guard = self.otc_response_rx.lock().await;
            guard.take()
        };

        // Spawn task to forward unsolicited MMResponse messages from PaymentManager to websocket
        if let Some(mut rx) = otc_response_rx.take() {
            let websocket_tx_clone = websocket_tx.clone();
            tokio::spawn(async move {
                while let Some(response_msg) = rx.recv().await {
                    let response_json = match serde_json::to_string(&response_msg) {
                        Ok(json) => json,
                        Err(e) => {
                            error!("Failed to serialize unsolicited MMResponse: {}", e);
                            continue;
                        }
                    };

                    if let Err(e) = websocket_tx_clone.send(Message::Text(response_json)).await {
                        error!("Failed to queue unsolicited MMResponse message: {}", e);
                        break;
                    }
                }
            });
        }

        // Handle incoming messages
        while let Some(msg) = ws_stream.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    // Try to parse as a protocol message
                    match serde_json::from_str::<ProtocolMessage<MMRequest>>(&text) {
                        Ok(protocol_msg) => {
                            let websocket_sender = websocket_tx.clone();
                            let message_handler = message_handler.clone();
                            tokio::spawn(async move {
                                if let Some(response) = message_handler.handle_request(&protocol_msg).await
                                {
                                    let response_json = match serde_json::to_string(&response) {
                                        Ok(json) => json,
                                        Err(e) => {
                                            error!("Failed to serialize response: {}", e);
                                            return;
                                        }
                                    };

                                    // Send response through the fan-in channel to the writer task
                                    if let Err(e) =
                                        websocket_sender.send(Message::Text(response_json)).await
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
                    break;
                }
                Err(e) => {
                    error!("WebSocket error: {}", e);
                    break;
                }
                _ => {}
            }
        }

        // Clean up: drop sender to signal writer task to exit
        drop(websocket_tx);

        // Wait for writer task to finish
        let _ = writer_handle.await;

        Ok(())
    }
}
