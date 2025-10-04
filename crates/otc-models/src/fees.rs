use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeeSchedule {
    pub network_fee_sats: u64,
    pub liquidity_fee_sats: u64,
    pub protocol_fee_sats: u64,
}
