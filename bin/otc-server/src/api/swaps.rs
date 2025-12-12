use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use otc_models::{ChainType, Metadata, Quote, RefundSwapReason};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockHashResponse {
    pub block_hash: String,
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

    /// User's refund address for receiving their "from" currency if the swap fails
    pub refund_address: String,

    /// Optional metadata describing the swap source
    #[serde(default)]
    pub metadata: Option<Metadata>,
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

    /// Minimum deposit amount (from quote bounds)
    pub min_input: U256,

    /// Maximum deposit amount (from quote bounds)
    pub max_input: U256,

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

/// Deposit information for rate-based swaps.
/// For user deposits: shows min/max bounds.
/// For MM deposits: shows expected output computed from actual user deposit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepositInfoResponse {
    pub address: String,
    pub chain: String,
    /// Minimum input (for user deposits) - None for MM deposits
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_input: Option<U256>,
    /// Maximum input (for user deposits) - None for MM deposits
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_input: Option<U256>,
    /// Expected output (for MM deposits, computed from realized swap) - None for user deposits
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_output: Option<U256>,
    pub decimals: u8,
    pub token: String,

    /// Actual deposit info if detected
    pub deposit_tx: Option<String>,
    pub deposit_amount: Option<U256>,
    pub deposit_detected_at: Option<DateTime<Utc>>,
}
