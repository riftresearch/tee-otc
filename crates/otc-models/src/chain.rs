use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChainType {
    Bitcoin,
    Ethereum,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum TxStatus {
    NotFound,
    Confirmed(u64), // 0 = in mempool, 1+ = confirmed blocks
}