use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use otc_models::Lot;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Response from OTC server confirming connection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Connected {
    pub session_id: Uuid,
    pub server_version: String,
    pub timestamp: DateTime<Utc>,
}

/// Messages sent from OTC server to Market Maker
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MMRequest {
    /// Ask MM if they will fill a specific quote
    ValidateQuote {
        request_id: Uuid,
        quote_id: Uuid,
        quote_hash: [u8; 32],
        user_destination_address: String,
        timestamp: DateTime<Utc>,
    },

    /// Notify MM that user has deposited and provide deposit address
    UserDeposited {
        request_id: Uuid,
        swap_id: Uuid,
        quote_id: Uuid,
        /// MM's deposit address
        deposit_address: String,
        /// Proof that user is real - their deposit tx hash
        user_tx_hash: String,
        /// Actual amount the user deposited (for accurate liquidity locking)
        deposit_amount: U256,
        timestamp: DateTime<Utc>,
    },

    /// Notify MM that user's deposit is confirmed and MM should send payment
    UserDepositConfirmed {
        request_id: Uuid,
        swap_id: Uuid,
        quote_id: Uuid,
        /// User's destination address where MM should send funds
        user_destination_address: String,
        /// The nonce MM must embed in their transaction
        mm_nonce: [u8; 16],
        /// Expected payment details
        expected_lot: Lot,
        /// Protocol fee for this swap (from realized amounts)
        protocol_fee: U256,
        user_deposit_confirmed_at: DateTime<Utc>,
        timestamp: DateTime<Utc>,
    },

    /// Notify MM that swap is complete and provide user's private key
    SwapComplete {
        request_id: Uuid,
        swap_id: Uuid,
        /// Private key for user's deposit wallet
        user_deposit_private_key: String,
        lot: Lot,
        /// Final settlement details
        user_deposit_tx_hash: String,
        swap_settlement_timestamp: DateTime<Utc>,
        timestamp: DateTime<Utc>,
    },

    /// Ask the MM to tell us the swap_settlement_timestamp of their newest deposit vault
    LatestDepositVaultTimestamp { 
        request_id: Uuid,
    },

    /// Ask the MM for any batches they have that are newer than the newest batch we've seen
    NewBatches { 
        request_id: Uuid,
        newest_batch_timestamp: Option<DateTime<Utc>>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkBatch { 
    pub tx_hash: String,
    pub swap_ids: Vec<Uuid>,
    pub batch_nonce_digest: [u8; 32],
}

/// Messages sent from Market Maker to OTC server
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MMResponse {
    /// Response to `ValidateQuote`
    QuoteValidated {
        request_id: Uuid,
        quote_id: Uuid,
        /// Whether MM will fill this quote
        accepted: bool,
        /// Optional reason if rejected
        rejection_reason: Option<String>,
        timestamp: DateTime<Utc>,
    },
    
    /// Response to `LatestDepositVaultTimestamp`
    /// Will give us the swap_settlement_timestamp of their newest deposit vault
    LatestDepositVaultTimestampResponse {
        request_id: Uuid,
        swap_settlement_timestamp: Option<DateTime<Utc>>,
        timestamp: DateTime<Utc>,
    },

    /// Response to payment request - payment has been queued for batching
    PaymentQueued {
        request_id: Uuid,
        swap_id: Uuid,
        timestamp: DateTime<Utc>,
    },

    /// Notification that MM has sent a batch payment
    Batches {
        request_id: Uuid,
        batches: Vec<NetworkBatch>,
    },

    /// Acknowledgment of `SwapComplete`
    SwapCompleteAck {
        request_id: Uuid,
        swap_id: Uuid,
        timestamp: DateTime<Utc>,
    },

    /// Error response for any request
    Error {
        request_id: Uuid,
        error_code: MMErrorCode,
        message: String,
        timestamp: DateTime<Utc>,
    },
}

/// Market Maker operational status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MMStatus {
    /// Fully operational and accepting quotes
    Active,
    /// Operational but not accepting new quotes
    Paused,
    /// Undergoing maintenance
    Maintenance,
    /// Experiencing issues
    Degraded,
}

/// Standard error codes for MM protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MMErrorCode {
    /// Quote not found in MM's system
    QuoteNotFound,
    /// Quote has expired
    QuoteExpired,
    /// Insufficient liquidity
    InsufficientLiquidity,
    /// Invalid request format
    InvalidRequest,
    /// Internal MM error
    InternalError,
    /// Rate limit exceeded
    RateLimited,
    /// Unsupported chain
    UnsupportedChain,
    /// Invalid deposit amount
    InvalidAmount,
}

/// Wrapper for protocol messages with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolMessage<T> {
    /// Protocol version
    pub version: String,
    /// Message sequence number for ordering
    pub sequence: u64,
    /// The actual message
    pub payload: T,
}
