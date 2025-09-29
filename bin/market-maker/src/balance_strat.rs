use alloy::primitives::{U256, U512};
use otc_models::Quote;

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

    pub fn can_fill_quote(&self, quote: &Quote, balance: U256) -> bool {
        if balance.is_zero() {
            return false;
        }

        // utilization_bps = quote_amount / balance scaled by BPS
        let quote_amount = quote.to.amount; // U256
        let utilization_bps = mul_div(quote_amount, U256::from(BPS), balance);

        // allow if utilization <= threshold
        utilization_bps <= self.utilization_threshold_bps
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otc_models::{ChainType, Currency, Lot, TokenIdentifier};
    use uuid::Uuid;

    fn create_test_quote(amount: u64) -> Quote {
        let currency = Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        };

        Quote {
            id: Uuid::new_v4(),
            market_maker_id: Uuid::new_v4(),
            from: Lot {
                currency: currency.clone(),
                amount: U256::from(amount / 2),
            },
            to: Lot {
                currency,
                amount: U256::from(amount),
            },
            expires_at: utc::now(),
            created_at: utc::now(),
        }
    }

    #[test]
    fn test_strategy_threshold_enforcement() {
        let strategy = QuoteBalanceStrategy::new(5_000); // 50% threshold
        let quote = create_test_quote(1000);

        // Should accept when utilization is under threshold
        let balance = U256::from(3000); // 1000/3000 = 33.3% < 50%
        assert!(strategy.can_fill_quote(&quote, balance));

        // Should reject when utilization exceeds threshold
        let balance = U256::from(1500); // 1000/1500 = 66.7% > 50%
        assert!(!strategy.can_fill_quote(&quote, balance));

        // Boundary: exactly at threshold should pass
        let balance = U256::from(2000); // 1000/2000 = 50.0% == 50%
        assert!(strategy.can_fill_quote(&quote, balance));

        // Should reject zero balance
        assert!(!strategy.can_fill_quote(&quote, U256::ZERO));
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
}
