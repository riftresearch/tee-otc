use alloy::primitives::{Address, U256};
use chrono::{DateTime, Utc};
use otc_models::{ChainType, Quote, RefundSwapReason};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefundPayload {
    pub swap_id: Uuid,
    pub refund_recipient: String,
    pub refund_transaction_fee: U256,
}

/// Request to refund a swap
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RefundSwapRequest {
    /// The "message" part of the EIP712 typed data
    pub payload: RefundPayload,

    /// The signature of the full EIP712 typed data
    pub signature: Vec<u8>,
}

/// Response after refunding a swap
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefundSwapResponse {
    /// The refunded swap ID
    pub swap_id: Uuid,
    /// The reason the refund was allowed
    pub reason: RefundSwapReason,
    /// The "ready to be broadcast" signed transaction data as a hex string
    pub tx_data: String,
    /// The chain the transaction needs to be broadcast on
    pub tx_chain: ChainType,
}

/// Request to create a new swap from a quote
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CreateSwapRequest {
    /// The quote ID to create a swap from
    pub quote: Quote,

    /// User's destination address for receiving funds
    pub user_destination_address: String,

    /// User's EVM account that is authorized to control the swap
    pub user_evm_account_address: Address,
}

/// Response after successfully creating a swap
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSwapResponse {
    /// The newly created swap ID
    pub swap_id: Uuid,

    /// Deposit address for the user to send funds to
    pub deposit_address: String,

    /// Chain type for the deposit (Bitcoin/Ethereum)
    pub deposit_chain: String,

    /// Expected amount to deposit (matches quote.from.amount)
    pub expected_amount: U256,

    /// Number of decimals for the amount
    pub decimals: u8,

    /// Token type (Native or token address)
    pub token: String,

    /// When the swap expires (based on quote expiry)
    pub expires_at: DateTime<Utc>,

    /// Current swap status
    pub status: String,
}

/// Response for GET /swaps/:id
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapResponse {
    pub id: Uuid,
    pub quote_id: Uuid,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    /// User's deposit information
    pub user_deposit: DepositInfoResponse,

    /// Market maker's deposit information  
    pub mm_deposit: DepositInfoResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepositInfoResponse {
    pub address: String,
    pub chain: String,
    pub expected_amount: U256,
    pub decimals: u8,
    pub token: String,

    /// Actual deposit info if detected
    pub deposit_tx: Option<String>,
    pub deposit_amount: Option<U256>,
    pub deposit_detected_at: Option<DateTime<Utc>>,
}
