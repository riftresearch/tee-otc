use crate::quote_storage::QuoteStorage;
use crate::rfq_handler::RFQMessageHandler;
use crate::wallet::WalletManager;
use crate::{config::Config, wrapped_bitcoin_quoter::WrappedBitcoinQuoter};
use futures_util::{SinkExt, StreamExt};
use otc_protocols::rfq::{ProtocolMessage, RFQRequest};
use snafu::prelude::*;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{http, Message},
};
use tracing::{error, info, warn};
use url::Url;

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

pub struct RfqClient {
    config: Config,
    handler: RFQMessageHandler,
    rfq_ws_url: String,
}

impl RfqClient {
    pub fn new(config: Config, rfq_handler: RFQMessageHandler, rfq_ws_url: String) -> Self {
        Self {
            config,
            handler: rfq_handler,
            rfq_ws_url,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let mut reconnect_attempts = 0;

        loop {
            match self.connect_and_run().await {
                Ok(()) => {
                    info!("RFQ WebSocket connection closed normally");
                    reconnect_attempts = 0;
                }
                Err(e) => {
                    error!("RFQ WebSocket error: {}", e);
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

    async fn connect_and_run(&self) -> Result<()> {
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

        info!("RFQ WebSocket connected, authenticated via headers");

        let (mut write, mut read) = ws_stream.split();

        // Handle messages
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    // First check if it's a Connected response
                    if text.contains("Connected") {
                        info!("Received Connected acknowledgment from RFQ server");
                        continue;
                    }

                    // Otherwise, try to parse as a protocol message
                    match serde_json::from_str::<ProtocolMessage<RFQRequest>>(&text) {
                        Ok(protocol_msg) => {
                            if let Some(response) = self.handler.handle_request(&protocol_msg).await
                            {
                                let response_json =
                                    serde_json::to_string(&response).context(SerializationSnafu)?;
                                write
                                    .send(Message::Text(response_json))
                                    .await
                                    .context(MessageSendSnafu)?;
                            }
                        }
                        Err(e) => {
                            error!("Failed to parse RFQ message: {}", e);
                        }
                    }
                }
                Ok(Message::Close(_)) => {
                    info!("RFQ server closed connection");
                    break;
                }
                Err(e) => {
                    error!("RFQ WebSocket error: {}", e);
                    break;
                }
                _ => {}
            }
        }

        Ok(())
    }
}
