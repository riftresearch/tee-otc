use async_trait::async_trait;
use ezsockets::ClientConfig;
use serde::Serialize;
use snafu::{location, prelude::*};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::mpsc;
use tracing::{info, warn};

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

const DEFAULT_ORDERED_INBOUND_QUEUE_CAPACITY: usize = 1024;

#[derive(Clone)]
pub enum InboundHandlingMode {
    Concurrent,
    Ordered {
        queue_label: &'static str,
        capacity: usize,
    },
}

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

impl Drop for WebSocketHandle {
    fn drop(&mut self) {
        let _ = self.client.close(None);
    }
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
    inbound_mode: InboundHandlingMode,
}

impl<H: MessageHandler> WebSocketClient<H> {
    /// Create a new WebSocket client
    pub fn new(url: String, api_key_id: String, api_secret: String, handler: H) -> Self {
        Self {
            url,
            api_key_id,
            api_secret,
            handler: Arc::new(handler),
            inbound_mode: InboundHandlingMode::Concurrent,
        }
    }

    /// Process inbound messages in strict receive order using a bounded queue.
    #[must_use]
    pub fn with_ordered_inbound(mut self, queue_label: &'static str) -> Self {
        self.inbound_mode = InboundHandlingMode::Ordered {
            queue_label,
            capacity: DEFAULT_ORDERED_INBOUND_QUEUE_CAPACITY,
        };
        self
    }

    /// Connect to the WebSocket server and return a handle and future
    pub async fn connect(
        &self,
    ) -> Result<(
        WebSocketHandle,
        impl std::future::Future<Output = Result<()>>,
    )> {
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
        let inbound_mode = self.inbound_mode.clone();
        let (client, future) = ezsockets::connect(
            move |client| EzSocketAdapter::new(handler.clone(), client, inbound_mode.clone()),
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
    ordered_processor: Option<OrderedInboundProcessor>,
}

struct OrderedInboundProcessor {
    tx: mpsc::Sender<String>,
    depth: Arc<AtomicUsize>,
    queue_label: &'static str,
}

impl OrderedInboundProcessor {
    fn new(
        handler: Arc<dyn MessageHandlerErased>,
        client: ezsockets::Client<EzSocketAdapter>,
        queue_label: &'static str,
        capacity: usize,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<String>(capacity);
        let depth = Arc::new(AtomicUsize::new(0));
        emit_inbound_queue_depth_metric(queue_label, 0);

        let worker_depth = depth.clone();
        tokio::spawn(async move {
            while let Some(text) = rx.recv().await {
                let remaining = worker_depth.fetch_sub(1, Ordering::Relaxed) - 1;
                emit_inbound_queue_depth_metric(queue_label, remaining);

                if let Some(response_json) = handler.handle_text_message(&text).await {
                    if let Err(e) = client.text(response_json) {
                        warn!(
                            "Failed to send ordered response for {}: {} at {}",
                            queue_label,
                            e,
                            location!()
                        );
                    }
                }
            }

            emit_inbound_queue_depth_metric(queue_label, 0);
        });

        Self {
            tx,
            depth,
            queue_label,
        }
    }

    async fn enqueue(&self, text: String) -> std::result::Result<(), mpsc::error::SendError<()>> {
        let permit = self
            .tx
            .reserve()
            .await
            .map_err(|_| mpsc::error::SendError(()))?;
        let queued = self.depth.fetch_add(1, Ordering::Relaxed) + 1;
        emit_inbound_queue_depth_metric(self.queue_label, queued);
        permit.send(text);
        Ok(())
    }
}

fn emit_inbound_queue_depth_metric(queue_label: &'static str, depth: usize) {
    metrics::gauge!(
        "mm_ws_inbound_queue_len",
        "client" => queue_label.to_string()
    )
    .set(depth as f64);
}

impl EzSocketAdapter {
    fn new(
        handler: Arc<dyn MessageHandlerErased>,
        client: ezsockets::Client<Self>,
        inbound_mode: InboundHandlingMode,
    ) -> Self {
        let ordered_processor = match inbound_mode {
            InboundHandlingMode::Concurrent => None,
            InboundHandlingMode::Ordered {
                queue_label,
                capacity,
            } => Some(OrderedInboundProcessor::new(
                handler.clone(),
                client.clone(),
                queue_label,
                capacity,
            )),
        };

        Self {
            handler,
            client,
            ordered_processor,
        }
    }
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

    async fn on_text(
        &mut self,
        text: ezsockets::Utf8Bytes,
    ) -> std::result::Result<(), ezsockets::Error> {
        if let Some(processor) = &self.ordered_processor {
            if processor.enqueue(text.to_string()).await.is_err() {
                warn!("Ordered inbound queue closed unexpectedly");
            }
            return Ok(());
        }

        let handler = self.handler.clone();
        let client = self.client.clone();
        tokio::spawn(async move {
            if let Some(response_json) = handler.handle_text_message(text.as_ref()).await {
                tokio::task::spawn_blocking(move || {
                    if let Err(e) = client.text(response_json) {
                        warn!("Failed to send response: {} at {}", e, location!());
                    }
                });
            }
        });
        Ok(())
    }

    async fn on_binary(
        &mut self,
        _bytes: ezsockets::Bytes,
    ) -> std::result::Result<(), ezsockets::Error> {
        warn!("Received unexpected binary message");
        Ok(())
    }

    async fn on_call(&mut self, call: Self::Call) -> std::result::Result<(), ezsockets::Error> {
        // This is called when handle.send() is used via client.call()
        let client = self.client.clone();
        tokio::task::spawn_blocking(move || {
            if let Err(e) = client.text(call) {
                warn!("Failed to send call: {} at {}", e, location!());
            }
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::SinkExt;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::sync::{Mutex, Notify};
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    #[derive(Clone)]
    struct RecordingHandler {
        processed: Arc<Mutex<Vec<String>>>,
        notify: Arc<Notify>,
    }

    #[async_trait]
    impl MessageHandler for RecordingHandler {
        async fn handle_text(&self, text: &str) -> Option<String> {
            if text.contains("snapshot") {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            let mut processed = self.processed.lock().await;
            processed.push(text.to_string());
            if processed.len() == 2 {
                self.notify.notify_waiters();
            }
            None
        }
    }

    #[tokio::test]
    async fn ordered_inbound_mode_processes_messages_in_receive_order() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = accept_async(stream).await.unwrap();
            websocket
                .send(Message::Text("snapshot".to_string()))
                .await
                .unwrap();
            websocket
                .send(Message::Text("deposit_confirmed".to_string()))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = websocket.close(None).await;
        });

        let processed = Arc::new(Mutex::new(Vec::new()));
        let notify = Arc::new(Notify::new());
        let client = WebSocketClient::new(
            format!("ws://{}", addr),
            "test-api-id".to_string(),
            "test-api-secret".to_string(),
            RecordingHandler {
                processed: processed.clone(),
                notify: notify.clone(),
            },
        )
        .with_ordered_inbound("test_otc");

        let (handle, future) = client.connect().await.unwrap();
        let client_task = tokio::spawn(future);

        tokio::time::timeout(Duration::from_secs(5), notify.notified())
            .await
            .unwrap();

        assert_eq!(
            *processed.lock().await,
            vec!["snapshot".to_string(), "deposit_confirmed".to_string()]
        );

        drop(handle);
        let _ = server.await;
        let _ = client_task.await;
    }
}
