use crate::suspension_watcher::SuspensionWatcher;
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
    pub connection_id: Uuid, // Unique per connection instance
    pub sender: mpsc::Sender<ProtocolMessage<RFQRequest>>,
    pub protocol_version: String,
}

#[derive(Clone)]
pub struct RfqMMRegistry {
    connections: Arc<DashMap<Uuid, MarketMakerConnection>>,
    pending_requests: Arc<DashMap<Uuid, mpsc::Sender<RFQResponse>>>,
    suspension_watcher: Option<SuspensionWatcher>,
}

impl RfqMMRegistry {
    #[must_use]
    pub fn new(suspension_watcher: Option<SuspensionWatcher>) -> Self {
        Self {
            connections: Arc::new(DashMap::new()),
            pending_requests: Arc::new(DashMap::new()),
            suspension_watcher,
        }
    }

    pub fn register(
        &self,
        market_maker_id: Uuid,
        connection_id: Uuid,
        sender: mpsc::Sender<ProtocolMessage<RFQRequest>>,
        protocol_version: String,
    ) {
        info!(
            market_maker_id = %market_maker_id,
            connection_id = %connection_id,
            protocol_version = %protocol_version,
            "Registering RFQ market maker connection"
        );

        let connection = MarketMakerConnection {
            id: market_maker_id,
            connection_id,
            sender,
            protocol_version,
        };

        self.connections.insert(market_maker_id, connection);
    }

    /// Unregister a market maker connection only if the connection_id matches
    ///
    /// This prevents a race condition where:
    /// 1. Connection A registers
    /// 2. Connection B registers (overwrites A)
    /// 3. Connection A calls unregister (should NOT remove B)
    ///
    /// Returns true if the connection was removed, false if it didn't match or wasn't found
    pub fn unregister(&self, market_maker_id: Uuid, connection_id: Uuid) -> bool {
        // Use remove_if to atomically check and remove only if connection_id matches
        let removed = self.connections.remove_if(&market_maker_id, |_, conn| {
            conn.connection_id == connection_id
        }).is_some();

        if removed {
            info!(
                market_maker_id = %market_maker_id,
                connection_id = %connection_id,
                "Unregistered RFQ market maker connection"
            );
        } else {
            info!(
                market_maker_id = %market_maker_id,
                connection_id = %connection_id,
                "Skipped unregister - connection_id mismatch or not found (likely already replaced by new connection)"
            );
        }

        removed
    }

    #[must_use]
    pub fn is_connected(&self, market_maker_id: Uuid) -> bool {
        self.connections.contains_key(&market_maker_id)
    }

    /// Broadcast a quote request to all connected market makers (excluding suspended ones).
    pub async fn broadcast_quote_request(
        &self,
        request_id: &Uuid,
        request: &QuoteRequest,
    ) -> Vec<(Uuid, mpsc::Receiver<RFQResponse>)> {
        let mut receivers = Vec::new();
        let mut skipped_suspended = 0usize;

        for entry in self.connections.iter() {
            let mm_id = *entry.key();

            // Skip suspended market makers
            if let Some(watcher) = &self.suspension_watcher {
                if watcher.is_suspended(mm_id).await {
                    debug!(market_maker_id = %mm_id, "Skipping suspended market maker");
                    skipped_suspended += 1;
                    continue;
                }
            }

            let connection = entry.value();

            // Create a channel for this MM's response
            let (response_tx, response_rx) = mpsc::channel::<RFQResponse>(1);

            // Store the response channel for this MM and request
            let mm_request_id = Uuid::now_v7(); // Unique ID for this MM's request
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

        info!(
            request_id = %request_id,
            market_makers_count = receivers.len(),
            skipped_suspended,
            "Broadcasted quote request to market makers"
        );

        receivers
    }

    /// Broadcast a liquidity request to all connected market makers (excluding suspended ones).
    pub async fn broadcast_liquidity_request(
        &self,
        request_id: &Uuid,
    ) -> Vec<(Uuid, mpsc::Receiver<RFQResponse>)> {
        let mut receivers = Vec::new();
        let mut skipped_suspended = 0usize;

        for entry in self.connections.iter() {
            let mm_id = *entry.key();

            // Skip suspended market makers
            if let Some(watcher) = &self.suspension_watcher {
                if watcher.is_suspended(mm_id).await {
                    debug!(market_maker_id = %mm_id, "Skipping suspended market maker for liquidity");
                    skipped_suspended += 1;
                    continue;
                }
            }

            let connection = entry.value();

            // Create a channel for this MM's response
            let (response_tx, response_rx) = mpsc::channel::<RFQResponse>(1);

            // Store the response channel for this MM and request
            let mm_request_id = Uuid::now_v7(); // Unique ID for this MM's request
            self.pending_requests.insert(mm_request_id, response_tx);

            let request = ProtocolMessage {
                version: connection.protocol_version.clone(),
                sequence: 0,
                payload: RFQRequest::LiquidityRequest {
                    request_id: mm_request_id,
                    timestamp: utc::now(),
                },
            };

            // Send the request
            if let Err(e) = connection.sender.send(request).await {
                warn!(
                    market_maker_id = %mm_id,
                    error = %e,
                    "Failed to send liquidity request to market maker"
                );
                self.pending_requests.remove(&mm_request_id);
                continue;
            }

            receivers.push((mm_id, response_rx));
        }

        info!(
            request_id = %request_id,
            market_makers_count = receivers.len(),
            skipped_suspended,
            "Broadcasted liquidity request to market makers"
        );

        receivers
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
        Self::new(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_register_unregister() {
        let registry = RfqMMRegistry::new(None);
        let (tx, _rx) = mpsc::channel(10);
        let mm_id = Uuid::now_v7();
        let conn_id = Uuid::now_v7();

        // Register a market maker
        registry.register(mm_id, conn_id, tx, "1.0.0".to_string());
        assert!(registry.is_connected(mm_id));
        assert_eq!(registry.get_connection_count(), 1);

        // Unregister with correct connection_id
        assert!(registry.unregister(mm_id, conn_id));
        assert!(!registry.is_connected(mm_id));
        assert_eq!(registry.get_connection_count(), 0);
    }

    #[tokio::test]
    async fn test_unregister_race_condition() {
        let registry = RfqMMRegistry::new(None);
        let mm_id = Uuid::now_v7();
        
        // Connection A registers
        let (tx_a, _rx_a) = mpsc::channel(10);
        let conn_id_a = Uuid::now_v7();
        registry.register(mm_id, conn_id_a, tx_a, "1.0.0".to_string());
        assert!(registry.is_connected(mm_id));
        
        // Connection B registers (overwrites A)
        let (tx_b, _rx_b) = mpsc::channel(10);
        let conn_id_b = Uuid::now_v7();
        registry.register(mm_id, conn_id_b, tx_b, "1.0.0".to_string());
        assert!(registry.is_connected(mm_id));
        
        // Connection A tries to unregister - should NOT remove connection B
        assert!(!registry.unregister(mm_id, conn_id_a));
        assert!(registry.is_connected(mm_id), "Connection B should still be registered");
        
        // Connection B unregisters - should succeed
        assert!(registry.unregister(mm_id, conn_id_b));
        assert!(!registry.is_connected(mm_id));
    }
}
