use otc_models::{Lot, RealizedSwap, SwapRates, MIN_PROTOCOL_FEE_SATS};

pub const PROTOCOL_FEE_BPS: u64 = 10;

/// Compute realized swap amounts from an input amount and rates.
/// This is a convenience re-export of RealizedSwap::compute.
/// Returns None if the output would be below the minimum viable threshold.
pub fn compute_realized_swap(input: u64, rates: &SwapRates) -> Option<RealizedSwap> {
    RealizedSwap::compute(input, rates)
}

/// Compute the protocol fee for a given amount in sats.
/// Returns at least MIN_PROTOCOL_FEE_SATS.
pub fn compute_protocol_fee_sats(sats: u64) -> u64 {
    let fee = sats.saturating_mul(PROTOCOL_FEE_BPS) / 10_000;
    if fee < MIN_PROTOCOL_FEE_SATS {
        MIN_PROTOCOL_FEE_SATS
    } else {
        fee
    }
}

/// Given an amount after protocol fee deduction, compute what the original amount was.
pub fn inverse_compute_protocol_fee(g: u64) -> u64 {
    let threshold = MIN_PROTOCOL_FEE_SATS
        .saturating_mul(10_000)
        .saturating_div(PROTOCOL_FEE_BPS);

    let max_g_for_min_fee = threshold.saturating_sub(MIN_PROTOCOL_FEE_SATS);

    if g < max_g_for_min_fee {
        g.saturating_add(MIN_PROTOCOL_FEE_SATS)
    } else {
        g.saturating_mul(10_000)
            .saturating_div(10_000 - PROTOCOL_FEE_BPS)
    }
}

pub trait FeeCalcFromLot {
    fn compute_protocol_fee(&self) -> u64;
}

impl FeeCalcFromLot for Lot {
    fn compute_protocol_fee(&self) -> u64 {
        inverse_compute_protocol_fee(self.amount.to::<u64>()) - self.amount.to::<u64>()
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
            assert_eq!(amount_sats, amount_before_fee, "Fee computation is correct");
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
        let tiny_input = 1_500u64; // After fees, this would be below MIN_VIABLE_OUTPUT_SATS
        let realized = compute_realized_swap(tiny_input, &rates);
        assert!(realized.is_none(), "Should reject inputs that result in dust output");
    }
}
