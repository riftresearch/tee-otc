use serde::{Deserialize, Serialize};

use crate::constants::MIN_PROTOCOL_FEE_SATS;

/// Rate parameters for computing swap fees.
/// All rate fields are in basis points (bps) unless otherwise noted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwapRates {
    /// MM liquidity fee spread in bps (e.g., 13 = 0.13%)
    pub liquidity_fee_bps: u64,
    /// Protocol fee spread in bps (e.g., 10 = 0.10%)
    pub protocol_fee_bps: u64,
    /// Fixed gas/network fee in sats (absolute amount, not a rate)
    pub network_fee_sats: u64,
    /// Minimum protocol fee in sats (floor for small swaps to avoid dust outputs)
    #[serde(default = "default_min_protocol_fee")]
    pub min_protocol_fee_sats: u64,
}

fn default_min_protocol_fee() -> u64 {
    MIN_PROTOCOL_FEE_SATS
}

impl SwapRates {
    /// Creates a new SwapRates with the given parameters and default min protocol fee.
    pub fn new(liquidity_fee_bps: u64, protocol_fee_bps: u64, network_fee_sats: u64) -> Self {
        Self {
            liquidity_fee_bps,
            protocol_fee_bps,
            network_fee_sats,
            min_protocol_fee_sats: MIN_PROTOCOL_FEE_SATS,
        }
    }

    /// Creates a new SwapRates with all parameters including custom min protocol fee.
    pub fn with_min_fee(
        liquidity_fee_bps: u64,
        protocol_fee_bps: u64,
        network_fee_sats: u64,
        min_protocol_fee_sats: u64,
    ) -> Self {
        Self {
            liquidity_fee_bps,
            protocol_fee_bps,
            network_fee_sats,
            min_protocol_fee_sats,
        }
    }
}
