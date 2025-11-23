use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use otc_models::{Currency, Quote, QuoteRequest};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use otc_models::FeeSchedule;

/// Protocol wrapper for RFQ messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolMessage<T> {
    pub version: String,
    pub sequence: u64,
    pub payload: T,
}

/// Response from RFQ server confirming connection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Connected {
    pub session_id: Uuid,
    pub server_version: String,
    pub timestamp: DateTime<Utc>,
}

/// Represents maximum liquidity available for a specific trading pair
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingPairLiquidity {
    pub from: Currency,
    pub to: Currency,
    /// Maximum from_amount the MM can handle
    pub max_amount: U256,
}

/// Messages sent from RFQ server to Market Maker
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RFQRequest {
    /// Broadcast to all MMs when user requests quotes
    QuoteRequested {
        request_id: Uuid,
        request: QuoteRequest,
        timestamp: DateTime<Utc>,
    },

    /// Notify winning MM their quote was selected
    QuoteSelected {
        request_id: Uuid,
        quote_id: Uuid,
        timestamp: DateTime<Utc>,
    },

    /// Request for maximum swap sizes from MM
    LiquidityRequest {
        request_id: Uuid,
        timestamp: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum RFQResult<T> {
    Success(T),
    /// Something the user couldn't do anything about (not relevant to show the user)
    MakerUnavailable(String),
    /// Something the user could do something about (relevant to show the user)
    InvalidRequest(String),
    /// Market maker doesn't support this trading pair/chain (silently ignored by RFQ server)
    Unsupported(String),
}

/// Messages sent from Market Maker to RFQ server
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RFQResponse {
    /// MM's response with their quote (or None if they can't quote)
    QuoteResponse {
        request_id: Uuid,
        quote: RFQResult<Quote>,
        timestamp: DateTime<Utc>,
    },

    /// Error response
    Error {
        request_id: Uuid,
        error_code: RFQErrorCode,
        message: String,
        timestamp: DateTime<Utc>,
    },

    /// MM's response with liquidity information
    LiquidityResponse {
        request_id: Uuid,
        liquidity: Vec<TradingPairLiquidity>,
        timestamp: DateTime<Utc>,
    },
}

/// Standard error codes for RFQ protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RFQErrorCode {
    /// Cannot provide quote for this pair
    PairNotSupported,
    /// Insufficient liquidity
    InsufficientLiquidity,
    /// Invalid request format
    InvalidRequest,
    /// Internal MM error
    InternalError,
    /// Request timeout
    Timeout,
}
