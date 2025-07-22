use crate::SwapStatus;
use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Swap {
    pub id: Uuid,
    pub quote_id: Uuid,
    pub market_maker: String,
    
    // Salts for deterministic wallet generation
    pub user_deposit_salt: [u8; 32],
    pub mm_deposit_salt: [u8; 32],
    
    // User's addresses
    pub user_destination_address: String,
    pub user_refund_address: String,
    
    // Status tracking
    pub status: SwapStatus,
    pub user_deposit_status: Option<DepositInfo>,
    pub mm_deposit_status: Option<DepositInfo>,
    
    // Only one withdrawal tx (MM deposit -> user destination)
    pub user_withdrawal_tx: Option<String>,
    
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepositInfo {
    pub tx_hash: String,
    pub amount: U256,
    pub detected_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementInfo {
    pub tx_hash: String,
    pub fee_rate: String, // gwei or sats/vB for example
    pub settled_at: DateTime<Utc>,
}