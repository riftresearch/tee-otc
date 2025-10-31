use async_trait::async_trait;
use ezsockets::ClientConfig;
use serde::Serialize;
use snafu::prelude::*;
use std::sync::Arc;
use tracing::{Instrument, info, warn};

#[derive(Debug, Snafu)]
pub enum WebSocketError {
    #[snafu(display("WebSocket connection error: {}", source))]
    Connection { source: ezsockets::Error },

    #[snafu(display("Message serialization error: {}", source))]
    Serialization { source: serde_json::Error },

    #[snafu(display("URL parse error: {}", source))]
    UrlParse { source: url::ParseError },
}

pub type Result<T, E = WebSocketError> = std::result::Result<T, E>;

/// Generic trait for handling WebSocket protocol messages
/// 
/// The handler receives raw JSON text and returns optional JSON response text.
/// This allows each implementation to work with their own ProtocolMessage type.
#[async_trait]
pub trait MessageHandler: Send + Sync + Clone + 'static {
    /// Handle an incoming JSON message and optionally return a JSON response
    async fn handle_text(&self, text: &str) -> Option<String>;
}

/// Handle to send messages through the WebSocket connection
#[derive(Clone)]
pub struct WebSocketHandle {
    client: ezsockets::Client<EzSocketAdapter>,
}

impl WebSocketHandle {
    /// Send a serializable message through the WebSocket
    pub fn send<T: Serialize>(&self, message: &T) -> Result<()> {
        let json = serde_json::to_string(message).context(SerializationSnafu)?;
        let _ = self.client.call(json);
        Ok(())
    }
}

/// Generic WebSocket client for protocol message handling
pub struct WebSocketClient<H: MessageHandler> {
    url: String,
    api_key_id: String,
    api_secret: String,
    handler: Arc<H>,
}

impl<H: MessageHandler> WebSocketClient<H> {
    /// Create a new WebSocket client
    pub fn new(url: String, api_key_id: String, api_secret: String, handler: H) -> Self {
        Self {
            url,
            api_key_id,
            api_secret,
            handler: Arc::new(handler),
        }
    }

    /// Connect to the WebSocket server and return a handle and future
    pub async fn connect(
        &self,
    ) -> Result<(WebSocketHandle, impl std::future::Future<Output = Result<()>>)> {
        // Validate URL
        let _ = url::Url::parse(&self.url).context(UrlParseSnafu)?;
        
        info!("Connecting to WebSocket at {}", self.url);

        let mut config = ClientConfig::new(self.url.as_str());
        
        // Add authentication headers
        config = config
            .header("X-API-ID", &self.api_key_id)
            .header("X-API-SECRET", &self.api_secret);

        // let span = tracing::info_span!("ws_client", client = self.handler.handler_name());
        // let _span_guard = span.enter();

        let handler = self.handler.clone();
        let (client, future) = ezsockets::connect(
            move |client| EzSocketAdapter {
                handler: handler.clone(),
                client,
            },
            config,
        )
        .await;

        let handle = WebSocketHandle { client };

        let wrapped_future = async move {
            future.await.context(ConnectionSnafu)?;
            Ok(())
        };

        // drop(_span_guard);

        Ok((handle, wrapped_future))
    }
}

/// Internal adapter implementing ezsockets::ClientExt
struct EzSocketAdapter {
    handler: Arc<dyn MessageHandlerErased>,
    client: ezsockets::Client<Self>,
}

/// Type-erased message handler for use in EzSocketAdapter
#[async_trait]
trait MessageHandlerErased: Send + Sync {
    async fn handle_text_message(&self, text: &str) -> Option<String>;
}

#[async_trait]
impl<H> MessageHandlerErased for H
where
    H: MessageHandler,
{
    async fn handle_text_message(&self, text: &str) -> Option<String> {
        self.handle_text(text).await
    }
}

#[async_trait]
impl ezsockets::ClientExt for EzSocketAdapter {
    type Call = String;

    async fn on_text(&mut self, text: ezsockets::Utf8Bytes) -> std::result::Result<(), ezsockets::Error> {
        if let Some(response_json) = self.handler.handle_text_message(text.as_ref()).await {
            let _ = self.client.text(response_json);
        }
        Ok(())
    }

    async fn on_binary(&mut self, _bytes: ezsockets::Bytes) -> std::result::Result<(), ezsockets::Error> {
        warn!("Received unexpected binary message");
        Ok(())
    }

    async fn on_call(&mut self, call: Self::Call) -> std::result::Result<(), ezsockets::Error> {
        // This is called when handle.send() is used via client.call()
        let _ = self.client.text(call);
        Ok(())
    }
}

