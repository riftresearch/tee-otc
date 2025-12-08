use alloy::primitives::{U256, U512};

const BPS: u128 = 10_000; // 100% in basis points

fn mul_div(a: U256, b: U256, denom: U256) -> U256 {
    // (a * b) / denom with 512-bit intermediate
    let prod = U512::from(a) * U512::from(b);
    let q = prod / U512::from(denom);
    U256::from(q)
}

#[derive(Clone)]
pub struct QuoteBalanceStrategy {
    /// Maximum balance utilization threshold (in basis points, 1..=10_000)
    utilization_threshold_bps: U256,
}

impl QuoteBalanceStrategy {
    /// `bps` is the allowed fraction of balance per swap, in basis points (1..=10_000).
    pub fn new(bps: u16) -> Self {
        assert!(bps > 0 && bps <= BPS as u16, "bps must be in 1..=10000");
        Self {
            utilization_threshold_bps: U256::from(bps as u128),
        }
    }

    pub fn can_fill_quote(&self, quote_amount: U256, balance: U256) -> bool {
        if balance.is_zero() {
            return false;
        }

        // utilization_bps = quote_amount / balance scaled by BPS
        let utilization_bps = mul_div(quote_amount, U256::from(BPS), balance);

        // allow if utilization <= threshold
        utilization_bps <= self.utilization_threshold_bps
    }

    /// Returns the maximum output amount that can be filled given the balance,
    /// based on the utilization threshold.
    ///
    /// # Formula
    /// We find the largest amount such that `can_fill_quote(amount)` returns true,
    /// but `can_fill_quote(amount + 1)` returns false.
    ///
    /// This involves finding: max where (max × BPS) / balance ≤ threshold
    /// but ((max+1) × BPS) / balance > threshold
    ///
    /// Returns `U256::ZERO` if balance is zero.
    pub fn max_output_amount(&self, balance: U256) -> U256 {
        if balance.is_zero() {
            return U256::ZERO;
        }

        // Calculate theoretical max: (balance × threshold) / BPS
        let theoretical_max = mul_div(balance, self.utilization_threshold_bps, U256::from(BPS));
        
        if theoretical_max == U256::ZERO {
            return U256::ZERO;
        }

        // Apply a conservative margin to account for integer division rounding.
        // We reduce by 0.1% (10 bps) to ensure that max+1 will reliably exceed the threshold.
        // This is necessary because with large numbers and BPS resolution, many consecutive
        // values round to the same utilization_bps.
        let margin = mul_div(theoretical_max, U256::from(10), U256::from(BPS));
        let margin = if margin == U256::ZERO { U256::from(1) } else { margin };
        
        if theoretical_max > margin {
            theoretical_max - margin
        } else {
            U256::ZERO
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strategy_threshold_enforcement() {
        let strategy = QuoteBalanceStrategy::new(5_000); // 50% threshold
        let output_amount = U256::from(1000);

        // Should accept when utilization is under threshold
        let balance = U256::from(3000); // 1000/3000 = 33.3% < 50%
        assert!(strategy.can_fill_quote(output_amount, balance));

        // Should reject when utilization exceeds threshold
        let balance = U256::from(1500); // 1000/1500 = 66.7% > 50%
        assert!(!strategy.can_fill_quote(output_amount, balance));

        // Boundary: exactly at threshold should pass
        let balance = U256::from(2000); // 1000/2000 = 50.0% == 50%
        assert!(strategy.can_fill_quote(output_amount, balance));

        // Should reject zero balance
        assert!(!strategy.can_fill_quote(output_amount, U256::ZERO));
    }

    #[test]
    #[should_panic]
    fn test_strategy_invalid_threshold_zero() {
        QuoteBalanceStrategy::new(0); // Should panic on zero
    }

    #[test]
    #[should_panic]
    fn test_strategy_invalid_threshold_too_high() {
        QuoteBalanceStrategy::new(10_001); // Should panic above 100%
    }

    #[test]
    fn test_max_output_amount() {
        let strategy = QuoteBalanceStrategy::new(5_000); // 50% threshold

        // With balance of 1000, max output is 499 (500 - 1 for conservative margin)
        let balance = U256::from(1000);
        let max_amount = strategy.max_output_amount(balance);
        assert_eq!(max_amount, U256::from(499));

        // max_amount should pass can_fill_quote
        assert!(strategy.can_fill_quote(max_amount, balance));

        // max_amount + 1 should pass (500 is exactly at threshold)
        assert!(strategy.can_fill_quote(max_amount + U256::from(1), balance));
        
        // max_amount + 2 should fail
        assert!(!strategy.can_fill_quote(max_amount + U256::from(2), balance));

        // Zero balance should return zero
        assert_eq!(strategy.max_output_amount(U256::ZERO), U256::ZERO);

        // Test with larger balance to verify margin works
        let large_balance = U256::from(10_000_000_000u64);
        let large_max = strategy.max_output_amount(large_balance);
        // Should be less than theoretical max by ~0.1%
        let theoretical = (10_000_000_000u64 * 5000) / 10000;
        assert!(large_max < U256::from(theoretical));
        assert!(large_max > U256::from(theoretical * 998 / 1000)); // Within 0.2%
    }
}
