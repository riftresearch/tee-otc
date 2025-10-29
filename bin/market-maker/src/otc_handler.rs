use async_trait::async_trait;
use crate::db::{Deposit, DepositRepository, DepositStore, PaymentRepository, QuoteRepository};
use crate::liquidity_lock::{LiquidityLockManager, LockedLiquidity};
use crate::payment_manager::PaymentManager;
use crate::websocket_client::MessageHandler;
use otc_protocols::mm::{MMRequest, MMResponse, NetworkBatch, ProtocolMessage};
use std::sync::Arc;
use tracing::{error, info, warn};

#[derive(Clone)]
pub struct OTCMessageHandler {
    quote_repository: Arc<QuoteRepository>,
    deposit_repository: Arc<DepositRepository>,
    payment_manager: Arc<PaymentManager>,
    payment_repository: Arc<PaymentRepository>,
    liquidity_lock_manager: Arc<LiquidityLockManager>,
}

impl OTCMessageHandler {
    pub fn new(
        quote_repository: Arc<QuoteRepository>,
        deposit_repository: Arc<DepositRepository>,
        payment_manager: Arc<PaymentManager>,
        payment_repository: Arc<PaymentRepository>,
        liquidity_lock_manager: Arc<LiquidityLockManager>,
    ) -> Self {
        Self {
            quote_repository,
            deposit_repository,
            payment_manager,
            payment_repository,
            liquidity_lock_manager,
        }
    }

    pub async fn handle_request(
        &self,
        msg: &ProtocolMessage<MMRequest>,
    ) -> Option<ProtocolMessage<MMResponse>> {
        match &msg.payload {
            MMRequest::NewBatches {
                request_id,
                newest_batch_timestamp,
                ..
            } => {
                info!(
                    "Received new batches request for newest batch timestamp {:#?}", newest_batch_timestamp,
                );

                let response = match self.payment_repository.list_batches(*newest_batch_timestamp).await { 
                    Ok(batches) => {
                        MMResponse::Batches {
                            request_id: *request_id,
                            batches: batches.into_iter().map(|batch| NetworkBatch {
                                tx_hash: batch.txid,
                                swap_ids: batch.swap_ids,
                                batch_nonce_digest: batch.batch_nonce_digest,
                            }).collect(),
                        }
                    }
                    Err(e) => {
                        error!(
                            "Failed to list batches: {}",
                            e
                        );
                        MMResponse::Error {
                            request_id: *request_id,
                            error_code: otc_protocols::mm::MMErrorCode::InternalError,
                            message: "Failed to list batches".to_string(),
                            timestamp: utc::now(),
                        }
                    }
                };
                Some(ProtocolMessage {
                    version: msg.version.clone(),
                    sequence: msg.sequence + 1, 
                    payload: response,
                })
            }
            MMRequest::LatestDepositVaultTimestamp {
                request_id,
                ..
            } => {
                info!(
                    "Received latest deposit vault timestamp request",
                );
                let swap_settlement_timestamp_res = self.deposit_repository.get_latest_deposit_vault_timestamp().await;
                let response = match swap_settlement_timestamp_res {
                    Ok(swap_settlement_timestamp) => {
                        MMResponse::LatestDepositVaultTimestampResponse  {
                            request_id: *request_id,
                            swap_settlement_timestamp,
                            timestamp: utc::now(),
                        }
                    }
                    Err(e) => {
                        error!(
                            "Failed to get latest deposit vault timestamp: {}",
                            e
                        );
                        MMResponse::Error {
                            request_id: *request_id,
                            error_code: otc_protocols::mm::MMErrorCode::InternalError,
                            message: "Failed to get latest deposit vault timestamp".to_string(),
                            timestamp: utc::now(),
                        }
                    }
                };
                Some(ProtocolMessage {
                    version: msg.version.clone(),
                    sequence: msg.sequence + 1,
                    payload: response,
                })
            }
            MMRequest::ValidateQuote {
                request_id,
                quote_id,
                quote_hash,
                user_destination_address,
                timestamp: _,
            } => {
                info!(
                    "Received quote validation request for quote {} from user {}",
                    quote_id, user_destination_address
                );

                // Verify the quote exists in our database
                let (accepted, rejection_reason) =
                    match self.quote_repository.get_quote(*quote_id).await {
                        Ok(quote) => {
                            let stored_hash = quote.hash();
                            info!(
                                "Found quote {} in database, hash: {}",
                                quote_id,
                                alloy::hex::encode(stored_hash)
                            );
                            // Verify the hash matches
                            if stored_hash != *quote_hash {
                                let rejection_reason = format!(
                                    "Quote {} hash mismatch! Expected: {}, Got: {}",
                                    quote_id,
                                    alloy::hex::encode(stored_hash),
                                    alloy::hex::encode(quote_hash),
                                );
                                warn!(rejection_reason);
                                (false, Some(rejection_reason))
                            } else {
                                // Make sure the quote is not expired
                                let current_time = utc::now();
                                let quote_expiration = quote.expires_at;
                                if current_time > quote_expiration {
                                    let rejection_reason = format!(
                                        "Swap was requested with quote {} that is expired!",
                                        quote_id
                                    );
                                    warn!(rejection_reason);
                                    (false, Some(rejection_reason))
                                } else {
                                    (true, None)
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to retrieve quote {} from database: {}", quote_id, e);
                            (false, Some("Quote not found in database".to_string()))
                        }
                    };

                info!(
                    "Quote {} validation result: accepted={}, reason={:?}",
                    quote_id, accepted, &rejection_reason
                );

                let response = MMResponse::QuoteValidated {
                    request_id: *request_id,
                    quote_id: *quote_id,
                    accepted,
                    rejection_reason: rejection_reason.clone(),
                    timestamp: utc::now(),
                };

                Some(ProtocolMessage {
                    version: msg.version.clone(),
                    sequence: msg.sequence + 1,
                    payload: response,
                })
            }

            MMRequest::UserDeposited {
                swap_id,
                quote_id,
                user_tx_hash,
                ..
            } => {
                info!(
                    message = "ACK User Deposit",
                    swap_id = %swap_id,
                    user_tx_hash = user_tx_hash
                );

                // Lock up funds for confirmed user deposits
                match self.quote_repository.get_quote(*quote_id).await {
                    Ok(quote) => {
                        let locked = LockedLiquidity {
                            from: quote.from.currency.clone(),
                            to: quote.to.currency.clone(),
                            amount: quote.to.amount,
                            created_at: utc::now(),
                        };
                        self.liquidity_lock_manager.lock(*swap_id, locked).await;
                        info!(
                            message = "Locked liquidity for swap",
                            swap_id = %swap_id,
                            quote_id = %quote_id,
                            amount = %quote.to.amount
                        );
                    }
                    Err(e) => {
                        warn!(
                            message = "Failed to retrieve quote for liquidity locking",
                            swap_id = %swap_id,
                            quote_id = %quote_id,
                            error = %e
                        );
                    }
                }

                None // For now, we don't respond to this
            }

            MMRequest::UserDepositConfirmed {
                request_id,
                swap_id,
                quote_id,
                user_destination_address,
                mm_nonce,
                expected_lot,
                user_deposit_confirmed_at,
                ..
            } => {
                info!(
                    message = "Making payment",
                    swap_id = swap_id.to_string(),
                    quote_id = quote_id.to_string(),
                );

                let response = self
                    .payment_manager
                    .queue_payment(
                        request_id,
                        swap_id,
                        quote_id,
                        user_destination_address,
                        *user_deposit_confirmed_at,
                        mm_nonce,
                        expected_lot,
                    )
                    .await;

                // Unlock liquidity if payment was queued successfully
                if matches!(response, MMResponse::PaymentQueued { .. }) {
                    if let Some(locked) = self.liquidity_lock_manager.unlock(*swap_id).await {
                        info!(
                            message = "Unlocked liquidity for swap",
                            swap_id = %swap_id,
                            amount = %locked.amount
                        );
                    }
                }

                info!(
                    message = "Payment manager response",
                    response = ?response,
                );

                Some(ProtocolMessage {
                    version: msg.version.clone(),
                    sequence: msg.sequence + 1,
                    payload: response,
                })
            }

            MMRequest::SwapComplete {
                request_id,
                swap_id,
                user_deposit_private_key,
                lot,
                user_deposit_tx_hash,
                swap_settlement_timestamp,
                ..
            } => {
                tracing::info!(message = "Swap complete, received user's private key", swap_id = %swap_id, user_deposit_tx_hash = %user_deposit_tx_hash);

                match self
                    .deposit_repository
                    .store_deposit(&Deposit {
                        private_key: user_deposit_private_key.to_string(),
                        holdings: lot.clone(),
                        funding_tx_hash: user_deposit_tx_hash.to_string(),
                    }, *swap_settlement_timestamp, *swap_id)
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        error!(
                            message = format!("Failed to store deposit: {e}"),
                            swap_id = %swap_id,
                            user_deposit_tx_hash = %user_deposit_tx_hash
                        );
                    }
                }

                let response = MMResponse::SwapCompleteAck {
                    request_id: *request_id,
                    swap_id: *swap_id,
                    timestamp: utc::now(),
                };

                Some(ProtocolMessage {
                    version: msg.version.clone(),
                    sequence: msg.sequence + 1,
                    payload: response,
                })
            }
        }
    }
}

#[async_trait]
impl MessageHandler for OTCMessageHandler {
    async fn handle_text(&self, text: &str) -> Option<String> {
        match serde_json::from_str::<ProtocolMessage<MMRequest>>(text) {
            Ok(msg) => {
                if let Some(response) = self.handle_request(&msg).await {
                    match serde_json::to_string(&response) {
                        Ok(json) => Some(json),
                        Err(e) => {
                            error!("Failed to serialize OTC response: {}", e);
                            None
                        }
                    }
                } else {
                    None
                }
            }
            Err(e) => {
                error!("Failed to parse OTC message: {}", e);
                None
            }
        }
    }
}
