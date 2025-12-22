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
