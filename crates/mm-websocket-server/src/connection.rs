//! WebSocket connection lifecycle management

use crate::handler::{MessageHandler, MessageSender};
use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use snafu::prelude::*;
use std::sync::Arc;
use tokio::{sync::mpsc, task::JoinSet};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Error types for connection handling
#[derive(Debug, Snafu)]
pub enum ConnectionError {
    #[snafu(display("Handler error: {}", source))]
    HandlerError { source: crate::handler::MessageError },

    #[snafu(display("WebSocket error: {}", message))]
    WebSocketError { message: String },
}

pub type Result<T, E = ConnectionError> = std::result::Result<T, E>;

/// Handle a market maker WebSocket connection
///
/// This function manages the complete lifecycle of a WebSocket connection:
/// - Splits the socket for bidirectional communication
/// - Spawns background tasks for message forwarding and keepalive
/// - Calls handler hooks (on_connect, on_disconnect, handle_message)
/// - Monitors background task health
/// - Performs graceful cleanup on disconnect
///
/// # Arguments
/// * `socket` - The WebSocket connection
/// * `mm_id` - The authenticated market maker UUID
/// * `connection_id` - Unique identifier for this specific connection instance (prevents race conditions)
/// * `handler` - Handler for processing messages
/// * `outgoing_rx` - Channel for receiving outgoing messages (from registry)
///
/// # Returns
/// `Ok(())` when the connection closes normally, or an error if something went wrong
pub async fn handle_mm_connection<H: MessageHandler>(
    socket: WebSocket,
    mm_id: Uuid,
    connection_id: Uuid,
    handler: Arc<H>,
    mut outgoing_rx: mpsc::Receiver<Message>,
) -> Result<()> {
    info!("MM {} WebSocket connection established", mm_id);

    // Split socket for bidirectional communication
    let (mut sender, mut receiver) = socket.split();

    // Internal channel for coordinating outgoing messages
    let (sender_tx, mut sender_rx) = mpsc::channel::<Message>(100);

    // Create message sender for the on_connect hook
    let message_sender = MessageSender::new(sender_tx.clone());

    // Call on_connect hook
    if let Err(e) = handler.on_connect(mm_id, &message_sender).await {
        error!("on_connect failed for MM {}: {}", mm_id, e);
    }

    // Track all background tasks
    let mut tasks = JoinSet::new();

    // Task 1: Forward messages from registry channel to sender_tx
    let mm_id_clone = mm_id;
    let sender_tx_clone = sender_tx.clone();
    tasks.spawn(async move {
        while let Some(msg) = outgoing_rx.recv().await {
            if sender_tx_clone.send(msg).await.is_err() {
                error!("Failed to forward message from registry for MM {}", mm_id_clone);
                break;
            }
        }
        info!("Registry-to-socket task exited for MM {}", mm_id_clone);
    });

    // Task 2: Forward messages from sender_rx to actual socket
    let mm_id_clone = mm_id;
    tasks.spawn(async move {
        while let Some(msg) = sender_rx.recv().await {
            if sender.send(msg).await.is_err() {
                error!("Failed to send message to MM {} socket", mm_id_clone);
                break;
            }
        }
        info!("Socket writer task exited for MM {}", mm_id_clone);
    });

    // Main loop: handle incoming messages and monitor background tasks
    loop {
        tokio::select! {
            // Check if any background task died
            Some(result) = tasks.join_next() => {
                match result {
                    Ok(_) => {
                        warn!("Background task exited for MM {}, closing connection", mm_id);
                    }
                    Err(e) => {
                        error!("Background task panicked for MM {}: {}", mm_id, e);
                    }
                }
                break;
            }
            // Handle incoming WebSocket messages
            Some(msg) = receiver.next() => {
                match msg {
                    Ok(Message::Text(text)) => {
                        // Spawn task to handle message asynchronously
                        let handler_clone = handler.clone();
                        let sender_tx_task = sender_tx.clone();
                        tokio::spawn(async move {
                            if let Some(response) = handler_clone.handle_message(mm_id, &text).await {
                                if let Err(e) = sender_tx_task.send(Message::Text(response)).await {
                                    warn!(
                                        "Failed to send response for MM {}: {}",
                                        mm_id, e
                                    );
                                }
                            }
                        });
                    }
                    Ok(Message::Close(_)) => {
                        info!("MM {} disconnected", mm_id);
                        break;
                    }
                    Err(e) => {
                        error!("WebSocket error for MM {}: {}", mm_id, e);
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    // Cleanup: shutdown all background tasks
    debug!(
        "MM {} (connection {}) handler exiting, shutting down background tasks",
        mm_id, connection_id
    );
    tasks.shutdown().await;

    // Call on_disconnect hook with connection_id to prevent race conditions
    handler.on_disconnect(mm_id, connection_id).await;

    info!(
        "MM {} (connection {}) disconnected, background tasks shut down",
        mm_id, connection_id
    );

    Ok(())
}

