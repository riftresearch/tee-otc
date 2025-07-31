use crate::handlers::MessageHandler;
use crate::{config::Config, wallet::WalletManager};
use futures_util::{SinkExt, StreamExt};
use otc_mm_protocol::{MMRequest, ProtocolMessage};
use snafu::prelude::*;
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

pub struct OtcFillClient {
    config: Config,
    handler: MessageHandler,
}

impl OtcFillClient {
    pub fn new(config: Config, wallet_manager: WalletManager) -> Self {
        let handler = MessageHandler::new(config.clone(), wallet_manager);
        Self { config, handler }
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
            .header("X-API-Key-ID", &self.config.api_key_id)
            .header("X-API-Key", &self.config.api_key)
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

        let (mut write, mut read) = ws_stream.split();

        // Handle messages
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    // First check if it's a Connected response
                    if text.contains("Connected") {
                        info!("Received Connected acknowledgment from server");
                        continue;
                    }

                    // Otherwise, try to parse as a protocol message
                    match serde_json::from_str::<ProtocolMessage<MMRequest>>(&text) {
                        Ok(protocol_msg) => {
                            if let Some(response) = self.handler.handle_request(&protocol_msg) {
                                let response_json =
                                    serde_json::to_string(&response).context(SerializationSnafu)?;
                                write
                                    .send(Message::Text(response_json))
                                    .await
                                    .context(MessageSendSnafu)?;
                            }
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

        Ok(())
    }
}
