use crate::{Quote, SwapStatus};
use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Swap {
    pub id: Uuid,
    pub market_maker_id: Uuid,

    pub quote: Quote,
    
    // Salt for deterministic wallet generation when combined with the TEE master key
    pub user_deposit_salt: [u8; 32],
    pub user_deposit_address: String, // cached for convenience, can be derived from the salt and master key

    // Nonce for the market maker to embed in their payment address
    pub mm_nonce: [u8; 16],
    
    // User's addresses
    pub user_destination_address: String,
    pub user_refund_address: String,
    
    // Core status
    pub status: SwapStatus,
    
    // Deposit tracking (JSONB in database)
    pub user_deposit_status: Option<UserDepositStatus>,
    pub mm_deposit_status: Option<MMDepositStatus>,
    
    // Settlement tracking
    pub settlement_status: Option<SettlementStatus>,
    
    // Failure/timeout tracking
    pub failure_reason: Option<String>,
    pub failure_at: Option<DateTime<Utc>>,
    
    // MM coordination
    pub mm_notified_at: Option<DateTime<Utc>>,
    pub mm_private_key_sent_at: Option<DateTime<Utc>>,
    
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// JSONB types for rich deposit/settlement data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserDepositStatus {
    pub tx_hash: String,
    pub amount: U256,
    pub detected_at: DateTime<Utc>,
    pub confirmations: u64,
    pub last_checked: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MMDepositStatus {
    pub tx_hash: String,
    pub amount: U256,
    pub detected_at: DateTime<Utc>,
    pub confirmations: u64,
    pub last_checked: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementStatus {
    pub tx_hash: String,
    pub broadcast_at: DateTime<Utc>,
    pub confirmations: u64,
    pub completed_at: Option<DateTime<Utc>>,
    pub fee: Option<U256>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferInfo {
    pub tx_hash: String,
    pub amount: U256,
    pub detected_at: DateTime<Utc>,
    pub confirmations: u64,
}