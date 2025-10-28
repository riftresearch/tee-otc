//! Message handling traits and utilities

use async_trait::async_trait;
use axum::extract::ws::Message;
use serde::Serialize;
use snafu::prelude::*;
use tokio::sync::mpsc;
use uuid::Uuid;

/// Error types for message handling
#[derive(Debug, Snafu)]
pub enum MessageError {
    #[snafu(display("Failed to serialize message: {}", source))]
    SerializationError { source: serde_json::Error },

    #[snafu(display("Failed to send message: channel closed"))]
    ChannelClosed,

    #[snafu(display("Handler error: {}", message))]
    HandlerError { message: String },
}

pub type Result<T, E = MessageError> = std::result::Result<T, E>;

/// Handler for incoming WebSocket messages from market makers
///
/// Implement this trait to process messages for your specific protocol.
#[async_trait]
pub trait MessageHandler: Send + Sync + 'static {
    /// Handle an incoming JSON text message from a market maker
    ///
    /// # Arguments
    /// * `mm_id` - The UUID of the market maker
    /// * `text` - The raw JSON message text
    ///
    /// # Returns
    /// * `Some(String)` - A JSON response to send back to the market maker
    /// * `None` - No response needed
    async fn handle_message(&self, mm_id: Uuid, text: &str) -> Option<String>;

    /// Called when a market maker first connects (after authentication)
    ///
    /// Use this hook to send initial messages, such as pending requests,
    /// state synchronization, or welcome messages.
    ///
    /// # Arguments
    /// * `mm_id` - The UUID of the market maker
    /// * `sender` - A handle to send messages to this market maker
    async fn on_connect(&self, mm_id: Uuid, sender: &MessageSender) -> Result<()>;

    /// Called when a market maker disconnects
    ///
    /// Use this hook to clean up state, such as removing from registries
    /// or canceling pending requests.
    ///
    /// # Arguments
    /// * `mm_id` - The UUID of the market maker that disconnected
    /// * `connection_id` - The unique connection ID (use this to avoid race conditions when unregistering)
    async fn on_disconnect(&self, mm_id: Uuid, connection_id: Uuid);
}

/// Handle for sending messages to a market maker
///
/// This is provided in the `on_connect` hook to send initial messages.
pub struct MessageSender {
    tx: mpsc::Sender<Message>,
}

impl MessageSender {
    /// Create a new MessageSender
    pub(crate) fn new(tx: mpsc::Sender<Message>) -> Self {
        Self { tx }
    }

    /// Send a message to the market maker
    ///
    /// The message will be serialized to JSON automatically.
    pub async fn send<T: Serialize>(&self, message: &T) -> Result<()> {
        let json = serde_json::to_string(message).context(SerializationSnafu)?;
        self.send_json(json).await
    }

    /// Send a pre-serialized JSON string to the market maker
    pub async fn send_json(&self, json: String) -> Result<()> {
        self.tx
            .send(Message::Text(json))
            .await
            .map_err(|_| MessageError::ChannelClosed)
    }
}

