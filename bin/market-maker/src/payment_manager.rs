// Middleware that prevents double-sending payments to the same swap request

use std::sync::Arc;

use alloy::primitives::U256;
use blockchain_utils::FeeCalcFromLot;
use otc_chains::traits::MarketMakerPaymentValidation;
use otc_models::Lot;
use otc_protocols::mm::{MMErrorCode, MMResponse};
use tracing::info;
use uuid::Uuid;

use crate::{payment_storage::PaymentStorage, wallet::WalletManager};

pub struct PaymentManager {
    wallet_manager: Arc<WalletManager>,
    payment_storage: Arc<PaymentStorage>,
}

impl PaymentManager {
    pub fn new(wallet_manager: Arc<WalletManager>, payment_storage: Arc<PaymentStorage>) -> Self {
        Self {
            wallet_manager,
            payment_storage,
        }
    }

    pub async fn make_payment(
        &self,
        request_id: &Uuid,
        swap_id: &Uuid,
        quote_id: &Uuid,
        user_destination_address: &String,
        mm_nonce: &[u8; 16],
        expected_lot: &Lot,
    ) -> MMResponse {
        let wallet = self.wallet_manager.get(expected_lot.currency.chain);
        if let Some(wallet) = wallet {
            match self.payment_storage.has_payment_been_made(*swap_id).await {
                Ok(Some(txid)) => {
                    return MMResponse::Error {
                        request_id: *request_id,
                        error_code: MMErrorCode::InternalError,
                        message: format!("Payment already made for swap {swap_id}, txid: {txid:?}"),
                        timestamp: utc::now(),
                    };
                }
                Err(e) => {
                    return MMResponse::Error {
                        request_id: *request_id,
                        error_code: MMErrorCode::InternalError,
                        message: format!("Failed to check if payment has been made for swap: {e}"),
                        timestamp: utc::now(),
                    };
                }
                Ok(None) => {
                    info!("Creating payment for swap {swap_id}");
                }
            }
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
            info!("Payment created for swap {swap_id} {tx_result:?}");
            match tx_result {
                Ok(txid) => {
                    self.payment_storage
                        .set_payment(*swap_id, txid.clone())
                        .await
                        .expect("Failed to set payment");
                    MMResponse::DepositInitiated {
                        request_id: *request_id,
                        swap_id: *swap_id,
                        tx_hash: txid,
                        amount_sent: expected_lot.amount,
                        timestamp: utc::now(),
                    }
                }
                Err(e) => MMResponse::Error {
                    request_id: *request_id,
                    error_code: MMErrorCode::InternalError,
                    message: e.to_string(),
                    timestamp: utc::now(),
                },
            }
        } else {
            MMResponse::Error {
                request_id: *request_id,
                error_code: MMErrorCode::UnsupportedChain,
                message: "No wallet found for chain".to_string(),
                timestamp: utc::now(),
            }
        }
    }
}
