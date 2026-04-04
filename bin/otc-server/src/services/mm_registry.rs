use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use otc_models::ChainType;
use otc_models::Lot;
use otc_protocols::mm::{ActiveObligation, MMRequest, ProtocolMessage};
use snafu::Snafu;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum MMRegistryError {
    #[snafu(display("Market maker '{}' not connected", market_maker_id))]
    MarketMakerNotConnected { market_maker_id: String },

    #[snafu(display("Validation request timed out for market maker '{}'", market_maker_id))]
    ValidationTimeout { market_maker_id: String },

    #[snafu(display("Failed to send message to market maker: {}", source))]
    MessageSendError {
        source: mpsc::error::SendError<ProtocolMessage<MMRequest>>,
    },

    #[snafu(display("Invalid quote ID: {}", quote_id))]
    InvalidQuoteId { quote_id: String },

    #[snafu(display("Failed to receive validation response: {}", source))]
    ResponseReceiveError { source: oneshot::error::RecvError },
}

type Result<T, E = MMRegistryError> = std::result::Result<T, E>;

pub struct MarketMakerConnection {
    pub id: Uuid,
    pub connection_id: Uuid, // Unique per connection instance
    pub sender: mpsc::Sender<ProtocolMessage<MMRequest>>,
    pub protocol_version: String,
    delivery: Mutex<ConnectionDeliveryState>,
}

#[derive(Default)]
struct ConnectionDeliveryState {
    ready: bool,
    bootstrap_queue: VecDeque<ProtocolMessage<MMRequest>>,
    live_queue: VecDeque<ProtocolMessage<MMRequest>>,
}

#[derive(Clone)]
pub struct MMRegistry {
    connections: Arc<DashMap<Uuid, Arc<MarketMakerConnection>>>,
    pending_validations: Arc<DashMap<Uuid, oneshot::Sender<Result<bool>>>>,
}

impl Default for MMRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl MMRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            connections: Arc::new(DashMap::new()),
            pending_validations: Arc::new(DashMap::new()),
        }
    }

    pub fn register(
        &self,
        market_maker_id: Uuid,
        connection_id: Uuid,
        sender: mpsc::Sender<ProtocolMessage<MMRequest>>,
        protocol_version: String,
    ) {
        self.insert_connection(
            market_maker_id,
            connection_id,
            sender,
            protocol_version,
            true,
        );
    }

    pub fn register_pending(
        &self,
        market_maker_id: Uuid,
        connection_id: Uuid,
        sender: mpsc::Sender<ProtocolMessage<MMRequest>>,
        protocol_version: String,
    ) {
        self.insert_connection(
            market_maker_id,
            connection_id,
            sender,
            protocol_version,
            false,
        );
    }

    fn insert_connection(
        &self,
        market_maker_id: Uuid,
        connection_id: Uuid,
        sender: mpsc::Sender<ProtocolMessage<MMRequest>>,
        protocol_version: String,
        ready: bool,
    ) {
        info!(
            market_maker_id = %market_maker_id,
            connection_id = %connection_id,
            protocol_version = %protocol_version,
            ready,
            "Registering market maker connection"
        );

        let connection = Arc::new(MarketMakerConnection {
            id: market_maker_id,
            connection_id,
            sender,
            protocol_version,
            delivery: Mutex::new(ConnectionDeliveryState {
                ready,
                ..ConnectionDeliveryState::default()
            }),
        });

        self.connections.insert(market_maker_id, connection);
    }

    pub async fn queue_bootstrap_requests(
        &self,
        market_maker_id: Uuid,
        connection_id: Uuid,
        requests: Vec<ProtocolMessage<MMRequest>>,
    ) -> Result<()> {
        let connection = self.get_connection(&market_maker_id)?;
        if connection.connection_id != connection_id {
            return Ok(());
        }

        let mut delivery = connection.delivery.lock().await;
        if delivery.ready {
            drop(delivery);
            for request in requests {
                connection
                    .sender
                    .send(request)
                    .await
                    .map_err(|source| MMRegistryError::MessageSendError { source })?;
            }
            return Ok(());
        }

        delivery.bootstrap_queue.extend(requests);
        Ok(())
    }

    pub async fn mark_ready(&self, market_maker_id: Uuid, connection_id: Uuid) -> Result<()> {
        let connection = self.get_connection(&market_maker_id)?;
        if connection.connection_id != connection_id {
            return Ok(());
        }

        let mut delivery = connection.delivery.lock().await;
        if delivery.ready {
            return Ok(());
        }

        while let Some(request) = delivery.bootstrap_queue.pop_front() {
            connection
                .sender
                .send(request)
                .await
                .map_err(|source| MMRegistryError::MessageSendError { source })?;
        }
        while let Some(request) = delivery.live_queue.pop_front() {
            connection
                .sender
                .send(request)
                .await
                .map_err(|source| MMRegistryError::MessageSendError { source })?;
        }
        delivery.ready = true;
        Ok(())
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
        let removed = self
            .connections
            .remove_if(&market_maker_id, |_, conn| {
                conn.connection_id == connection_id
            })
            .is_some();

        if removed {
            info!(
                market_maker_id = %market_maker_id,
                connection_id = %connection_id,
                "Unregistered market maker connection"
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

    pub async fn notify_user_deposit(
        &self,
        market_maker_id: &Uuid,
        swap_id: &Uuid,
        quote_id: &Uuid,
        deposit_vault_address: &str,
        user_tx_hash: &str,
        deposit_amount: U256,
    ) {
        let request = if let Ok(connection) = self.get_connection(market_maker_id) {
            ProtocolMessage {
                version: connection.protocol_version.clone(),
                sequence: 0,
                payload: MMRequest::UserDeposited {
                    request_id: Uuid::now_v7(),
                    swap_id: *swap_id,
                    quote_id: *quote_id,
                    user_tx_hash: user_tx_hash.to_string(),
                    deposit_address: deposit_vault_address.to_string(),
                    deposit_amount,
                    timestamp: utc::now(),
                },
            }
        } else {
            return;
        };

        if let Err(e) = self.send_live_request(market_maker_id, request).await {
            error!(market_maker_id = %market_maker_id, error = %e, "Failed to send user deposit notification");
        }
    }

    pub async fn request_new_batches(
        &self,
        market_maker_id: &Uuid,
        newest_batch_timestamp: Option<DateTime<Utc>>,
    ) -> Result<()> {
        let connection = self.get_connection(market_maker_id)?;

        let request = ProtocolMessage {
            version: connection.protocol_version.clone(),
            sequence: 0,
            payload: MMRequest::NewBatches {
                request_id: Uuid::now_v7(),
                newest_batch_timestamp,
            },
        };

        self.send_live_request(market_maker_id, request).await?;

        Ok(())
    }

    pub async fn request_latest_deposit_vault_timestamp(
        &self,
        market_maker_id: &Uuid,
    ) -> Result<()> {
        let connection = self.get_connection(market_maker_id)?;

        let request = ProtocolMessage {
            version: connection.protocol_version.clone(),
            sequence: 0,
            payload: MMRequest::LatestDepositVaultTimestamp {
                request_id: Uuid::now_v7(),
            },
        };

        self.send_live_request(market_maker_id, request).await?;

        info!(
            market_maker_id = %market_maker_id,
            "Requested latest deposit vault timestamp from market maker"
        );

        Ok(())
    }
    pub async fn notify_user_deposit_confirmed(
        &self,
        market_maker_id: &Uuid,
        swap_id: &Uuid,
        quote_id: &Uuid,
        user_destination_address: &str,
        mm_nonce: [u8; 16],
        expected_lot: &Lot,
        settlement_lot: &Lot,
        protocol_fee: U256,
        user_deposit_confirmed_at: DateTime<Utc>,
    ) -> Result<()> {
        let connection = self.get_connection(market_maker_id)?;

        let request = ProtocolMessage {
            version: connection.protocol_version.clone(),
            sequence: 0,
            payload: MMRequest::UserDepositConfirmed {
                request_id: Uuid::now_v7(),
                swap_id: *swap_id,
                quote_id: *quote_id,
                user_destination_address: user_destination_address.to_string(),
                mm_nonce,
                expected_lot: expected_lot.clone(),
                settlement_lot: settlement_lot.clone(),
                protocol_fee,
                user_deposit_confirmed_at,
                timestamp: utc::now(),
            },
        };

        info!(
            market_maker_id = %market_maker_id,
            swap_id = %swap_id,
            user_destination_address = %user_destination_address,
            "Notifying MM that user deposit is confirmed - MM should send payment with nonce"
        );

        self.send_live_request(market_maker_id, request).await
    }

    pub async fn notify_active_obligations_snapshot(
        &self,
        market_maker_id: &Uuid,
        obligations: Vec<ActiveObligation>,
    ) -> Result<()> {
        let connection = self.get_connection(market_maker_id)?;

        let request = ProtocolMessage {
            version: connection.protocol_version.clone(),
            sequence: 0,
            payload: MMRequest::ActiveObligationsSnapshot {
                request_id: Uuid::now_v7(),
                obligations,
                timestamp: utc::now(),
            },
        };

        self.send_live_request(market_maker_id, request).await
    }

    pub async fn notify_swap_complete(
        &self,
        market_maker_id: &Uuid,
        swap_id: &Uuid,
        user_deposit_private_key: &str,
        lot: &Lot,
        user_deposit_tx_hash: &str,
        swap_settlement_timestamp: &DateTime<Utc>,
    ) {
        let request = if let Ok(connection) = self.get_connection(market_maker_id) {
            ProtocolMessage {
                version: connection.protocol_version.clone(),
                sequence: 0,
                payload: MMRequest::SwapComplete {
                    request_id: Uuid::now_v7(),
                    swap_id: *swap_id,
                    user_deposit_private_key: user_deposit_private_key.to_string(),
                    lot: lot.clone(),
                    user_deposit_tx_hash: user_deposit_tx_hash.to_string(),
                    swap_settlement_timestamp: *swap_settlement_timestamp,
                    timestamp: utc::now(),
                },
            }
        } else {
            return;
        };

        if let Err(e) = self.send_live_request(market_maker_id, request).await {
            error!(market_maker_id = %market_maker_id, error = %e, "Failed to send swap complete notification");
        }
    }

    pub async fn notify_fee_settlement_ack(
        &self,
        market_maker_id: &Uuid,
        request_id: Uuid,
        chain: ChainType,
        tx_hash: &str,
        accepted: bool,
        rejection_reason: Option<String>,
    ) {
        let request = if let Ok(connection) = self.get_connection(market_maker_id) {
            ProtocolMessage {
                version: connection.protocol_version.clone(),
                sequence: 0,
                payload: MMRequest::FeeSettlementAck {
                    request_id,
                    chain,
                    tx_hash: tx_hash.to_string(),
                    accepted,
                    rejection_reason,
                    timestamp: utc::now(),
                },
            }
        } else {
            warn!(
                market_maker_id = %market_maker_id,
                "Cannot send fee settlement ack - market maker not connected"
            );
            return;
        };

        if let Err(e) = self.send_live_request(market_maker_id, request).await {
            error!(
                market_maker_id = %market_maker_id,
                error = %e,
                "Failed to send fee settlement ack to market maker"
            );
        }
    }

    pub async fn validate_quote(
        &self,
        market_maker_id: &Uuid,
        quote_id: &Uuid,
        quote_hash: &[u8; 32],
        user_destination_address: &str,
        response_tx: oneshot::Sender<Result<bool>>,
    ) {
        debug!(
            market_maker_id = %market_maker_id,
            quote_id = %quote_id,
            "Validating quote with market maker"
        );

        let mm_connection = if let Ok(conn) = self.get_connection(market_maker_id) {
            conn
        } else {
            warn!(
                market_maker_id = %market_maker_id,
                "Market maker not connected"
            );
            let _ = response_tx.send(Err(MMRegistryError::MarketMakerNotConnected {
                market_maker_id: market_maker_id.to_string(),
            }));
            return;
        };

        let request = ProtocolMessage {
            version: mm_connection.protocol_version.clone(),
            sequence: 0, // TODO: Implement sequence tracking
            payload: MMRequest::ValidateQuote {
                request_id: Uuid::now_v7(),
                quote_id: *quote_id,
                quote_hash: *quote_hash,
                user_destination_address: user_destination_address.to_string(),
                timestamp: utc::now(),
            },
        };

        // Store the response channel before sending the request
        self.pending_validations.insert(*quote_id, response_tx);

        // Send the validation request
        if let Err(e) = self.send_live_request(market_maker_id, request).await {
            error!(
                market_maker_id = %market_maker_id,
                error = %e,
                "Failed to send validation request"
            );
            // Remove the pending validation since we failed to send
            if let Some((_, tx)) = self.pending_validations.remove(quote_id) {
                let _ = tx.send(Err(e));
            }
        }
    }

    pub fn handle_validation_response(
        &self,
        market_maker_id: &Uuid,
        quote_id: &Uuid,
        accepted: bool,
    ) {
        debug!(
            market_maker_id = %market_maker_id,
            quote_id = %quote_id,
            accepted = %accepted,
            "Handling validation response"
        );

        // Find the pending validation for this quote
        if let Some((_, tx)) = self.pending_validations.remove(quote_id) {
            let _ = tx.send(Ok(accepted));
        } else {
            warn!(
                quote_id = %quote_id,
                "Received validation response for unknown quote"
            );
        }
    }

    #[must_use]
    pub fn get_connection_count(&self) -> usize {
        self.connections.len()
    }

    #[must_use]
    pub fn get_connected_market_makers(&self) -> Vec<Uuid> {
        self.connections.iter().map(|entry| *entry.key()).collect()
    }

    fn get_connection(&self, market_maker_id: &Uuid) -> Result<Arc<MarketMakerConnection>> {
        self.connections
            .get(market_maker_id)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| MMRegistryError::MarketMakerNotConnected {
                market_maker_id: market_maker_id.to_string(),
            })
    }

    async fn send_live_request(
        &self,
        market_maker_id: &Uuid,
        request: ProtocolMessage<MMRequest>,
    ) -> Result<()> {
        let connection = self.get_connection(market_maker_id)?;
        let mut delivery = connection.delivery.lock().await;
        if delivery.ready {
            connection
                .sender
                .send(request)
                .await
                .map_err(|source| MMRegistryError::MessageSendError { source })?;
        } else {
            delivery.live_queue.push_back(request);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otc_models::{Currency, TokenIdentifier};

    fn btc_lot(amount: u64) -> Lot {
        Lot {
            currency: Currency {
                chain: ChainType::Bitcoin,
                token: TokenIdentifier::Native,
                decimals: 8,
            },
            amount: U256::from(amount),
        }
    }

    #[tokio::test]
    async fn test_register_unregister() {
        let registry = MMRegistry::new();
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
        let registry = MMRegistry::new();
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
        assert!(
            registry.is_connected(mm_id),
            "Connection B should still be registered"
        );

        // Connection B unregisters - should succeed
        assert!(registry.unregister(mm_id, conn_id_b));
        assert!(!registry.is_connected(mm_id));
    }

    #[tokio::test]
    async fn test_validate_quote_not_connected() {
        let registry = MMRegistry::new();
        let (response_tx, response_rx) = oneshot::channel();
        let unknown_mm_id = Uuid::now_v7();

        let () = registry
            .validate_quote(
                &unknown_mm_id,
                &Uuid::now_v7(),
                &[0u8; 32],
                "0x123",
                response_tx,
            )
            .await;

        let result = response_rx.await.unwrap();
        assert!(matches!(
            result,
            Err(MMRegistryError::MarketMakerNotConnected { .. })
        ));
    }

    #[tokio::test]
    async fn test_pending_connection_flushes_bootstrap_before_live_messages() {
        let registry = MMRegistry::new();
        let mm_id = Uuid::now_v7();
        let conn_id = Uuid::now_v7();
        let (tx, mut rx) = mpsc::channel(10);

        registry.register_pending(mm_id, conn_id, tx, "1.0.0".to_string());

        let live_swap_id = Uuid::now_v7();
        registry
            .notify_user_deposit_confirmed(
                &mm_id,
                &live_swap_id,
                &Uuid::now_v7(),
                "dest-live",
                [7u8; 16],
                &btc_lot(10),
                &btc_lot(11),
                U256::ZERO,
                utc::now(),
            )
            .await
            .unwrap();

        let bootstrap_request = ProtocolMessage {
            version: "1.0.0".to_string(),
            sequence: 0,
            payload: MMRequest::ActiveObligationsSnapshot {
                request_id: Uuid::now_v7(),
                obligations: Vec::new(),
                timestamp: utc::now(),
            },
        };
        let latest_deposit_request = ProtocolMessage {
            version: "1.0.0".to_string(),
            sequence: 0,
            payload: MMRequest::LatestDepositVaultTimestamp {
                request_id: Uuid::now_v7(),
            },
        };

        registry
            .queue_bootstrap_requests(
                mm_id,
                conn_id,
                vec![bootstrap_request.clone(), latest_deposit_request.clone()],
            )
            .await
            .unwrap();
        registry.mark_ready(mm_id, conn_id).await.unwrap();

        let first = rx.recv().await.expect("bootstrap snapshot");
        let second = rx.recv().await.expect("bootstrap follow-up");
        let third = rx.recv().await.expect("buffered live notification");

        assert!(matches!(
            first.payload,
            MMRequest::ActiveObligationsSnapshot { .. }
        ));
        assert!(matches!(
            second.payload,
            MMRequest::LatestDepositVaultTimestamp { .. }
        ));
        match third.payload {
            MMRequest::UserDepositConfirmed { swap_id, .. } => {
                assert_eq!(swap_id, live_swap_id);
            }
            other => panic!("unexpected payload order: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_live_messages_after_ready_are_delivered_immediately() {
        let registry = MMRegistry::new();
        let mm_id = Uuid::now_v7();
        let conn_id = Uuid::now_v7();
        let (tx, mut rx) = mpsc::channel(10);

        registry.register_pending(mm_id, conn_id, tx, "1.0.0".to_string());
        registry.mark_ready(mm_id, conn_id).await.unwrap();

        let swap_id = Uuid::now_v7();
        registry
            .notify_user_deposit_confirmed(
                &mm_id,
                &swap_id,
                &Uuid::now_v7(),
                "dest-ready",
                [9u8; 16],
                &btc_lot(12),
                &btc_lot(13),
                U256::ZERO,
                utc::now(),
            )
            .await
            .unwrap();

        match rx.recv().await.expect("live notification after ready") {
            ProtocolMessage {
                payload: MMRequest::UserDepositConfirmed { swap_id: got, .. },
                ..
            } => assert_eq!(got, swap_id),
            other => panic!("unexpected payload: {other:?}"),
        }
    }
}
