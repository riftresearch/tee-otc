use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use otc_models::ChainType;
use uuid::Uuid;

/// Messages sent from OTC server to Market Maker
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MMRequest {
    /// Ask MM if they will fill a specific quote
    ValidateQuote {
        request_id: Uuid,
        quote_id: Uuid,
        user_id: Uuid,
        timestamp: DateTime<Utc>,
    },
    
    /// Notify MM that user has deposited and provide deposit address
    UserDeposited {
        request_id: Uuid,
        swap_id: Uuid,
        quote_id: Uuid,
        /// MM's deposit address
        deposit_address: String,
        /// Chain for the deposit
        deposit_chain: ChainType,
        /// Expected amount to deposit
        deposit_amount: U256,
        /// Proof that user is real - their deposit tx hash
        user_tx_hash: String,
        /// Deadline for MM to deposit
        deposit_deadline: DateTime<Utc>,
        timestamp: DateTime<Utc>,
    },
    
    /// Notify MM that swap is complete and provide user's private key
    SwapComplete {
        request_id: Uuid,
        swap_id: Uuid,
        /// Private key for user's deposit wallet
        user_deposit_private_key: String,
        /// Final settlement details
        user_withdrawal_tx: String,
        timestamp: DateTime<Utc>,
    },
    
    /// Request MM status/health check
    Ping {
        request_id: Uuid,
        timestamp: DateTime<Utc>,
    },
}

/// Messages sent from Market Maker to OTC server
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MMResponse {
    /// Response to ValidateQuote
    QuoteValidated {
        request_id: Uuid,
        quote_id: Uuid,
        /// Whether MM will fill this quote
        accepted: bool,
        /// Optional reason if rejected
        rejection_reason: Option<String>,
        /// Optional updated destination address
        mm_destination_address: Option<String>,
        timestamp: DateTime<Utc>,
    },
    
    /// Response to UserDeposited - MM has initiated deposit
    DepositInitiated {
        request_id: Uuid,
        swap_id: Uuid,
        /// Transaction hash of MM's deposit
        tx_hash: String,
        /// Actual amount sent (in case of rounding)
        amount_sent: U256,
        timestamp: DateTime<Utc>,
    },
    
    /// Acknowledgment of SwapComplete
    SwapCompleteAck {
        request_id: Uuid,
        swap_id: Uuid,
        timestamp: DateTime<Utc>,
    },
    
    /// Response to Ping
    Pong {
        request_id: Uuid,
        /// MM's current status
        status: MMStatus,
        /// Software version
        version: String,
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