use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChainType {
    Bitcoin,
    Ethereum,
    Base,
}

impl ChainType {
    pub fn to_db_string(&self) -> &'static str {
        match self {
            ChainType::Bitcoin => "bitcoin",
            ChainType::Ethereum => "ethereum",
            ChainType::Base => "base",
        }
    }
    pub fn from_db_string(s: &str) -> Option<ChainType> {
        match s {
            "bitcoin" => Some(ChainType::Bitcoin),
            "ethereum" => Some(ChainType::Ethereum),
            "base" => Some(ChainType::Base),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingTxStatus {
    pub current_height: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfirmedTxStatus {
    pub confirmations: u64,
    pub current_height: u64,
    pub inclusion_height: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxStatus {
    NotFound,
    Pending(PendingTxStatus),
    Confirmed(ConfirmedTxStatus),
}
