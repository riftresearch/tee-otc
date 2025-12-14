use otc_models::{compute_protocol_fee, inverse_compute_input, Lot, RealizedSwap, SwapRates};

#[cfg(test)]
use otc_models::{compute_fees, SwapMode};

pub const PROTOCOL_FEE_BPS: u64 = 10;

/// Compute realized swap amounts from an input amount and rates.
/// This is a convenience re-export of RealizedSwap::compute.
/// Returns None if the output would be below the minimum viable threshold.
pub fn compute_realized_swap(input: u64, rates: &SwapRates) -> Option<RealizedSwap> {
    RealizedSwap::compute(input, rates)
}

/// Compute the protocol fee for a given amount in sats using ceiling division.
pub fn compute_protocol_fee_sats(sats: u64) -> u64 {
    compute_protocol_fee(sats, PROTOCOL_FEE_BPS)
}

/// Given an amount after protocol fee deduction, compute what the original amount was.
pub fn inverse_compute_protocol_fee(g: u64) -> u64 {
    // Create rates with only protocol fee to use the consolidated inverse function
    let rates = SwapRates::new(0, PROTOCOL_FEE_BPS, 0);
    inverse_compute_input(g, &rates)
}

pub trait FeeCalcFromLot {
    fn compute_protocol_fee(&self) -> u64;
}

impl FeeCalcFromLot for Lot {
    fn compute_protocol_fee(&self) -> u64 {
        let amount = self.amount.to::<u64>();
        let original = inverse_compute_protocol_fee(amount);
        original.saturating_sub(amount)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_fee_inversion() {
        let amount_sats = [300, 512, 262143, 400_001, 1_010_011];

        for amount_sats in amount_sats {
            let fee_sats = compute_protocol_fee_sats(amount_sats);
            let amount_after_fee = amount_sats.saturating_sub(fee_sats);
            let amount_before_fee = inverse_compute_protocol_fee(amount_after_fee);
            println!("amount_sats: {amount_sats}");
            println!("fee_sats: {fee_sats}");
            println!("amount_after_fee: {amount_after_fee}");
            println!("amount_before_fee: {amount_before_fee}");
            // With ceiling-based fees, the inverse should give us at least the original
            assert!(
                amount_before_fee >= amount_sats,
                "Inverse should recover at least original: {} >= {}",
                amount_before_fee,
                amount_sats
            );
        }
    }

    #[test]
    fn test_compute_realized_swap() {
        let rates = SwapRates::new(13, 10, 1000); // 0.13% liquidity, 0.10% protocol, 1000 sats network
        let input = 1_000_000u64;

        let realized = compute_realized_swap(input, &rates).expect("should produce valid output");

        // Verify the computation matches RealizedSwap::compute
        let direct = RealizedSwap::compute(input, &rates).expect("should produce valid output");
        assert_eq!(realized, direct);
    }

    #[test]
    fn test_compute_realized_swap_rejects_dust_output() {
        let rates = SwapRates::new(13, 10, 1000); // 0.13% liquidity, 0.10% protocol, 1000 sats network

        // Very small input that would result in dust output
        let tiny_input = 1_500u64;
        let realized = compute_realized_swap(tiny_input, &rates);
        assert!(realized.is_none(), "Should reject inputs that result in dust output");
    }

    #[test]
    fn test_compute_fees_directly() {
        let rates = SwapRates::new(13, 10, 1000);
        let input = 1_000_000u64;

        let breakdown = compute_fees(SwapMode::ExactInput(input), &rates).unwrap();
        assert_eq!(breakdown.liquidity_fee, 1300);
        assert_eq!(breakdown.protocol_fee, 1000);
        assert_eq!(breakdown.network_fee, 1000);
        assert_eq!(breakdown.output, 996_700);
    }
}
