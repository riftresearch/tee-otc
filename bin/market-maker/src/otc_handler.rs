use crate::quote_storage::QuoteStorage;
use crate::strategy::ValidationStrategy;
use crate::{config::Config, wallet::WalletManager};
use alloy::primitives::U256;
use chrono::Utc;
use blockchain_utils::FeeCalcFromLot;
use otc_chains::traits::MarketMakerPaymentValidation;
use otc_protocols::mm::{MMErrorCode, MMRequest, MMResponse, MMStatus, ProtocolMessage};
use std::sync::Arc;
use tracing::{error, info, warn};

pub struct OTCMessageHandler {
    config: Config,
    strategy: ValidationStrategy,
    wallet_manager: WalletManager,
    quote_storage: Arc<QuoteStorage>,
}

impl OTCMessageHandler {
    pub fn new(
        config: Config,
        wallet_manager: WalletManager,
        quote_storage: Arc<QuoteStorage>,
    ) -> Self {
        let strategy = ValidationStrategy::new();
        Self {
            config,
            strategy,
            wallet_manager,
            quote_storage,
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
                    timestamp: Utc::now(),
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

                // TODO: We should have additional safety checks here to ensure the user's deposit is valid
                // instead of trusting the TEE
                let wallet = self.wallet_manager.get(expected_lot.currency.chain);
                let response: MMResponse = {
                    if let Some(wallet) = wallet {
                        let tx_result = wallet
                            .create_payment(
                                expected_lot,
                                user_destination_address,
                                Some(MarketMakerPaymentValidation {
                                    fee_amount: U256::from(expected_lot.compute_protocol_fee()),
                                    embedded_nonce: *mm_nonce,
                                }),
                            )
                            .await;

                        match tx_result {
                            Ok(txid) => MMResponse::DepositInitiated {
                                request_id: *request_id,
                                swap_id: *swap_id,
                                tx_hash: txid,
                                amount_sent: expected_lot.amount,
                                timestamp: Utc::now(),
                            },
                            Err(e) => MMResponse::Error {
                                request_id: *request_id,
                                error_code: MMErrorCode::InternalError,
                                message: e.to_string(),
                                timestamp: Utc::now(),
                            },
                        }
                    } else {
                        MMResponse::Error {
                            request_id: *request_id,
                            error_code: MMErrorCode::UnsupportedChain,
                            message: "No wallet found for chain".to_string(),
                            timestamp: Utc::now(),
                        }
                    }
                };

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
                chain,
                user_withdrawal_tx,
                ..
            } => {
                info!("Swap {} complete, received user's private key", swap_id);
                info!("User withdrawal tx: {}", user_withdrawal_tx);

                // TODO: Implement claiming logic
                warn!("TODO: Implement claiming from user's wallet");

                let response = MMResponse::SwapCompleteAck {
                    request_id: *request_id,
                    swap_id: *swap_id,
                    timestamp: Utc::now(),
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
                    timestamp: Utc::now(),
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
