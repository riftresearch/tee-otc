use otc_models::Lot;

pub const PROTOCOL_FEE_BPS: u64 = 10;
pub const MIN_PROTOCOL_FEE_SATS: u64 = 300;

pub fn compute_protocol_fee_sats(sats: u64) -> u64 {
    let fee = sats.saturating_mul(PROTOCOL_FEE_BPS) / 10_000;
    if fee < MIN_PROTOCOL_FEE_SATS {
        MIN_PROTOCOL_FEE_SATS
    } else {
        fee
    }
}

/// Given an amount, compute what the original amount was before the protocol fee was removed.
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
            // f = comp(a)
            // g = f - a
            // a = inv(g)
            println!("amount_sats: {amount_sats}");
            println!("fee_sats: {fee_sats}");
            println!("amount_after_fee: {amount_after_fee}");
            println!("amount_before_fee: {amount_before_fee}");
            assert_eq!(amount_sats, amount_before_fee, "Fee computation is correct");
        }
    }
}
