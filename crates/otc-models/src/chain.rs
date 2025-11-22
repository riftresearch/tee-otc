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

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum TxStatus {
    NotFound,
    Confirmed(u64), // 0 = in mempool, 1+ = confirmed blocks
}
