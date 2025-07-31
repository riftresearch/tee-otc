use crate::strategy::ValidationStrategy;
use crate::{config::Config, wallet::WalletManager};
use chrono::Utc;
use otc_mm_protocol::{MMRequest, MMResponse, MMStatus, ProtocolMessage};
use tracing::{info, warn};

pub struct MessageHandler {
    config: Config,
    strategy: ValidationStrategy,
    wallet_manager: WalletManager,
}

impl MessageHandler {
    pub fn new(config: Config, wallet_manager: WalletManager) -> Self {
        let strategy = ValidationStrategy::new(config.auto_accept);
        Self {
            config,
            strategy,
            wallet_manager,
        }
    }

    pub fn handle_request(
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

                let (accepted, rejection_reason) =
                    self.strategy
                        .validate_quote(quote_id, quote_hash, user_destination_address);

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

                // TODO: Implement actual deposit logic
                // For now, just acknowledge
                warn!("TODO: Implement actual deposit to {}", deposit_address);

                // In a real implementation, we would:
                // 1. Verify the user's deposit on-chain
                // 2. Initiate our deposit to the provided address
                // 3. Send DepositInitiated response with our tx hash

                None // For now, we don't respond to this
            }

            MMRequest::UserDepositConfirmed {
                request_id,
                swap_id,
                quote_id,
                user_destination_address,
                mm_nonce,
                expected_amount,
                expected_chain,
                expected_token,
                ..
            } => {
                info!(
                    "User deposit confirmed for swap {}: MM should send {} {} on {} to {}",
                    swap_id,
                    expected_amount,
                    expected_token,
                    expected_chain,
                    user_destination_address
                );
                info!("Quote ID: {}", quote_id);
                info!("MM nonce to embed: {:?}", alloy::hex::encode(mm_nonce));

                // TODO: Implement actual payment with embedded nonce
                warn!(
                    "TODO: Send {} {} on {} to {} with embedded nonce",
                    expected_amount, expected_token, expected_chain, user_destination_address
                );

                // In a real implementation, we would:
                // 1. Prepare the transaction to user_destination_address
                // 2. Embed the mm_nonce in the transaction (method depends on chain)
                // 3. Send the transaction
                // 4. Respond with DepositInitiated containing our tx hash

                // For now, simulate sending after a short delay
                if self.config.auto_accept {
                    let response = MMResponse::DepositInitiated {
                        request_id: *request_id,
                        swap_id: *swap_id,
                        tx_hash: format!("0xmm_tx_{}", alloy::hex::encode(&mm_nonce[..8])), // Simulated tx hash
                        amount_sent: *expected_amount,
                        timestamp: Utc::now(),
                    };

                    Some(ProtocolMessage {
                        version: msg.version.clone(),
                        sequence: msg.sequence + 1,
                        payload: response,
                    })
                } else {
                    None
                }
            }

            MMRequest::SwapComplete {
                request_id,
                swap_id,

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
