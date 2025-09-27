use dashmap::DashMap;
use otc_models::QuoteRequest;
use otc_protocols::rfq::{ProtocolMessage, RFQRequest, RFQResponse};
use snafu::Snafu;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum MMRegistryError {
    #[snafu(display("Market maker '{}' not connected", market_maker_id))]
    MarketMakerNotConnected { market_maker_id: String },

    #[snafu(display("Failed to send message to market maker: {}", source))]
    MessageSendError {
        source: mpsc::error::SendError<ProtocolMessage<RFQRequest>>,
    },
}

type Result<T, E = MMRegistryError> = std::result::Result<T, E>;

pub struct MarketMakerConnection {
    pub id: Uuid,
    pub sender: mpsc::Sender<ProtocolMessage<RFQRequest>>,
    pub protocol_version: String,
}

#[derive(Clone)]
pub struct RfqMMRegistry {
    connections: Arc<DashMap<Uuid, MarketMakerConnection>>,
    pending_requests: Arc<DashMap<Uuid, mpsc::Sender<RFQResponse>>>,
}

impl RfqMMRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            connections: Arc::new(DashMap::new()),
            pending_requests: Arc::new(DashMap::new()),
        }
    }

    pub fn register(
        &self,
        market_maker_id: Uuid,
        sender: mpsc::Sender<ProtocolMessage<RFQRequest>>,
        protocol_version: String,
    ) {
        info!(
            market_maker_id = %market_maker_id,
            protocol_version = %protocol_version,
            "Registering RFQ market maker connection"
        );

        let connection = MarketMakerConnection {
            id: market_maker_id,
            sender,
            protocol_version,
        };

        self.connections.insert(market_maker_id, connection);
    }

    pub fn unregister(&self, market_maker_id: Uuid) {
        info!(market_maker_id = %market_maker_id, "Unregistering RFQ market maker connection");
        self.connections.remove(&market_maker_id);
    }

    #[must_use]
    pub fn is_connected(&self, market_maker_id: Uuid) -> bool {
        self.connections.contains_key(&market_maker_id)
    }

    /// Broadcast a quote request to all connected market makers
    pub async fn broadcast_quote_request(
        &self,
        request_id: &Uuid,
        request: &QuoteRequest,
    ) -> Vec<(Uuid, mpsc::Receiver<RFQResponse>)> {
        let mut receivers = Vec::new();

        for entry in self.connections.iter() {
            let mm_id = *entry.key();
            let connection = entry.value();

            // Create a channel for this MM's response
            let (response_tx, response_rx) = mpsc::channel::<RFQResponse>(1);

            // Store the response channel for this MM and request
            let mm_request_id = Uuid::new_v4(); // Unique ID for this MM's request
            self.pending_requests.insert(mm_request_id, response_tx);

            let request = ProtocolMessage {
                version: connection.protocol_version.clone(),
                sequence: 0, // TODO: Implement sequence tracking
                payload: RFQRequest::QuoteRequested {
                    request_id: mm_request_id, // Use unique ID per MM
                    request: request.clone(),
                    timestamp: utc::now(),
                },
            };

            // Send the request
            if let Err(e) = connection.sender.send(request).await {
                warn!(
                    market_maker_id = %mm_id,
                    error = %e,
                    "Failed to send quote request to market maker"
                );
                self.pending_requests.remove(&mm_request_id);
                continue;
            }

            receivers.push((mm_id, response_rx));
        }

        debug!(
            request_id = %request_id,
            market_makers_count = receivers.len(),
            "Broadcasted quote request to market makers"
        );

        receivers
    }

    pub async fn send_ping(&self, market_maker_id: &Uuid) -> Result<()> {
        let (protocol_version, sender) = {
            let connection = self.connections.get(market_maker_id).ok_or_else(|| {
                MMRegistryError::MarketMakerNotConnected {
                    market_maker_id: market_maker_id.to_string(),
                }
            })?;

            (
                connection.protocol_version.clone(),
                connection.sender.clone(),
            )
        };

        let ping = ProtocolMessage {
            version: protocol_version,
            sequence: 0,
            payload: RFQRequest::Ping {
                request_id: Uuid::new_v4(),
                timestamp: utc::now(),
            },
        };

        sender
            .send(ping)
            .await
            .map_err(|e| MMRegistryError::MessageSendError { source: e })?;

        Ok(())
    }

    /// Notify a market maker that their quote was selected
    pub async fn notify_quote_selected(
        &self,
        market_maker_id: Uuid,
        request_id: Uuid,
        quote_id: Uuid,
    ) -> Result<()> {
        let connection = self.connections.get(&market_maker_id).ok_or_else(|| {
            MMRegistryError::MarketMakerNotConnected {
                market_maker_id: market_maker_id.to_string(),
            }
        })?;

        let notification = ProtocolMessage {
            version: connection.protocol_version.clone(),
            sequence: 0,
            payload: RFQRequest::QuoteSelected {
                request_id,
                quote_id,
                timestamp: utc::now(),
            },
        };

        connection
            .sender
            .send(notification)
            .await
            .map_err(|e| MMRegistryError::MessageSendError { source: e })?;

        info!(
            market_maker_id = %market_maker_id,
            quote_id = %quote_id,
            "Notified market maker of quote selection"
        );

        Ok(())
    }

    #[must_use]
    pub fn get_connection_count(&self) -> usize {
        self.connections.len()
    }

    #[must_use]
    pub fn get_connected_market_makers(&self) -> Vec<Uuid> {
        self.connections.iter().map(|entry| *entry.key()).collect()
    }

    /// Handle incoming quote response from a market maker
    pub async fn handle_quote_response(&self, request_id: Uuid, response: RFQResponse) {
        if let Some((_, sender)) = self.pending_requests.remove(&request_id) {
            if let Err(e) = sender.send(response).await {
                warn!(
                    request_id = %request_id,
                    error = ?e,
                    "Failed to send quote response to aggregator"
                );
            }
        } else {
            warn!(
                request_id = %request_id,
                "Received quote response for unknown request"
            );
        }
    }
}

impl Default for RfqMMRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_register_unregister() {
        let registry = RfqMMRegistry::new();
        let (tx, _rx) = mpsc::channel(10);
        let mm_id = Uuid::new_v4();

        // Register a market maker
        registry.register(mm_id, tx, "1.0.0".to_string());
        assert!(registry.is_connected(mm_id));
        assert_eq!(registry.get_connection_count(), 1);

        // Unregister
        registry.unregister(mm_id);
        assert!(!registry.is_connected(mm_id));
        assert_eq!(registry.get_connection_count(), 0);
    }
}
