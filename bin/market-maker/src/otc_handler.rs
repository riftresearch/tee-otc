use crate::deposit_key_storage::{Deposit, DepositKeyStorage, DepositKeyStorageTrait};
use crate::payment_manager::PaymentManager;
use crate::quote_storage::QuoteStorage;
use crate::strategy::ValidationStrategy;
use crate::{config::Config, wallet::WalletManager};
use alloy::primitives::U256;
use blockchain_utils::FeeCalcFromLot;
use otc_chains::traits::MarketMakerPaymentValidation;
use otc_protocols::mm::{MMErrorCode, MMRequest, MMResponse, MMStatus, ProtocolMessage};
use std::sync::Arc;
use tracing::{error, info, warn};

pub struct OTCMessageHandler {
    config: Config,
    strategy: ValidationStrategy,
    quote_storage: Arc<QuoteStorage>,
    deposit_key_storage: Arc<DepositKeyStorage>,
    payment_manager: Arc<PaymentManager>,
}

impl OTCMessageHandler {
    pub fn new(
        config: Config,
        quote_storage: Arc<QuoteStorage>,
        deposit_key_storage: Arc<DepositKeyStorage>,
        payment_manager: Arc<PaymentManager>,
    ) -> Self {
        let strategy = ValidationStrategy::new();
        Self {
            config,
            strategy,
            quote_storage,
            deposit_key_storage,
            payment_manager,
        }
    }

    pub async fn handle_request(
        &self,
        msg: &ProtocolMessage<MMRequest>,
    ) -> Option<ProtocolMessage<MMResponse>> {
        match &msg.payload {
            MMRequest::ValidateQuote {
                request_id,
                quote_id,
                quote_hash,
                user_destination_address,
                timestamp,
            } => {
                info!(
                    "Received quote validation request for quote {} from user {}",
                    quote_id, user_destination_address
                );

                // Verify the quote exists in our database
                let quote_exists = match self.quote_storage.get_quote(*quote_id).await {
                    Ok(quote) => {
                        info!(
                            "Found quote {} in database, hash: {:?}",
                            quote_id,
                            quote.hash()
                        );
                        // Verify the hash matches
                        if quote.hash() != *quote_hash {
                            warn!(
                                "Quote {} hash mismatch! Expected: {:?}, Got: {:?}",
                                quote_id,
                                quote.hash(),
                                quote_hash
                            );
                        }
                        true
                    }
                    Err(e) => {
                        error!("Failed to retrieve quote {} from database: {}", quote_id, e);
                        false
                    }
                };

                let (accepted, rejection_reason) = if quote_exists {
                    self.strategy
                        .validate_quote(quote_id, quote_hash, user_destination_address)
                } else {
                    (false, Some("Quote not found in database".to_string()))
                };

                info!(
                    "Quote {} validation result: accepted={}, reason={:?}",
                    quote_id, accepted, &rejection_reason
                );

                let response = MMResponse::QuoteValidated {
                    request_id: *request_id,
                    quote_id: *quote_id,
                    accepted,
                    rejection_reason,
                    timestamp: utc::now(),
                };

                Some(ProtocolMessage {
                    version: msg.version.clone(),
                    sequence: msg.sequence + 1,
                    payload: response,
                })
            }

            MMRequest::UserDeposited {
                request_id,
                swap_id,
                quote_id,
                deposit_address,
                user_tx_hash,
                ..
            } => {
                info!(
                    "User deposited for swap {}: to address {}",
                    swap_id, deposit_address
                );
                info!("Quote ID: {}", quote_id);
                info!("User tx hash: {}", user_tx_hash);

                warn!(
                    "TODO: implement locking up funds for this user {}",
                    deposit_address
                );

                None // For now, we don't respond to this
            }

            MMRequest::UserDepositConfirmed {
                request_id,
                swap_id,
                quote_id,
                user_destination_address,
                mm_nonce,
                expected_lot,
                ..
            } => {
                info!(
                    message = "User deposit confirmed for swap {swap_id}: MM should send {expected_lot:?} to {user_destination_address}",
                    quote_id = quote_id.to_string(),
                );

                let response = self
                    .payment_manager
                    .make_payment(
                        request_id,
                        swap_id,
                        quote_id,
                        user_destination_address,
                        mm_nonce,
                        expected_lot,
                    )
                    .await;

                // TODO: Implement payment manager
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
                ..
            } => {
                tracing::info!(message = "Swap complete, received user's private key", swap_id = %swap_id, user_deposit_tx_hash = %user_deposit_tx_hash);

                match self
                    .deposit_key_storage
                    .store_deposit(&Deposit {
                        private_key: user_deposit_private_key.to_string(),
                        holdings: lot.clone(),
                        funding_tx_hash: user_deposit_tx_hash.to_string(),
                    })
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

            MMRequest::Ping { request_id, .. } => {
                let response = MMResponse::Pong {
                    request_id: *request_id,
                    status: MMStatus::Active,
                    version: env!("CARGO_PKG_VERSION").to_string(),
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
