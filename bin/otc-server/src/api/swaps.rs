use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Request to create a new swap from a quote
#[derive(Debug, Clone, Deserialize)]
pub struct CreateSwapRequest {
    /// The quote ID to create a swap from
    pub quote_id: Uuid,
    
    /// Market maker identifier (must match the quote)
    pub market_maker_identifier: String,
    
    /// User's destination address for receiving funds
    pub user_destination_address: String,
    
    /// User's refund address in case swap needs to be reversed
    pub user_refund_address: String,
}

/// Response after successfully creating a swap
#[derive(Debug, Clone, Serialize)]
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
#[derive(Debug, Clone, Serialize)]
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
    
    /// Settlement transaction (if completed)
    pub settlement_tx: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
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