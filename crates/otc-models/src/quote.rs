use crate::ChainType;
use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type", content = "data")]
pub enum TokenIdentifier {
    Native,
    Address(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Currency { 
    pub chain: ChainType,
    pub token: TokenIdentifier,
    pub amount: U256,
    pub decimals: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quote {
    pub id: Uuid,
    
    // Conversion details
    pub from: Currency,  // Amount user will send
    pub to: Currency,    // Amount user will receive
    
    pub market_maker_identifier: String,
    
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}