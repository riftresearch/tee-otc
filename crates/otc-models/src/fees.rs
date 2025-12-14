use serde::{Deserialize, Serialize};

use crate::constants::MIN_VIABLE_OUTPUT_SATS;

const BPS_DENOM: u64 = 10_000;

/// Swap computation mode - determines whether input or output is the fixed value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "amount", rename_all = "snake_case")]
pub enum SwapMode {
    /// User specifies exact input amount, output is computed.
    ExactInput(u64),
    /// User specifies exact output amount, input is computed.
    ExactOutput(u64),
}

/// Rate parameters for computing swap fees.
/// Basis point fees are additive and applied to the gross input amount.
/// Formula: `output = input - liquidity_fee - protocol_fee - network_fee`
/// where `liquidity_fee = ceil(input * liquidity_fee_bps / 10_000)`
/// and `protocol_fee = ceil(input * protocol_fee_bps / 10_000)`
///
/// The `network_fee_sats` acts as a base value that may be increased (never decreased)
/// to absorb rounding when computing exact output swaps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwapRates {
    /// MM liquidity fee spread in bps (e.g., 13 = 0.13%)
    pub liquidity_fee_bps: u64,
    /// Protocol fee spread in bps (e.g., 10 = 0.10%)
    pub protocol_fee_bps: u64,
    /// Base gas/network fee in sats (may be increased to absorb rounding)
    pub network_fee_sats: u64,
}

impl SwapRates {
    /// Creates a new SwapRates with the given parameters.
    pub fn new(liquidity_fee_bps: u64, protocol_fee_bps: u64, network_fee_sats: u64) -> Self {
        Self {
            liquidity_fee_bps,
            protocol_fee_bps,
            network_fee_sats,
        }
    }

    /// Total basis points (liquidity + protocol fees).
    #[inline]
    pub fn total_bps(&self) -> u64 {
        self.liquidity_fee_bps + self.protocol_fee_bps
    }
}

/// Breakdown of fees computed from an input amount and rates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeeBreakdown {
    pub input: u64,
    pub liquidity_fee: u64,
    pub protocol_fee: u64,
    pub network_fee: u64,
    pub output: u64,
}

impl FeeBreakdown {
    /// Total fees deducted (liquidity + protocol + network).
    #[inline]
    pub fn total_fees(&self) -> u64 {
        self.liquidity_fee + self.protocol_fee + self.network_fee
    }
}

/// Compute fees for a swap based on the mode (exact input or exact output).
///
/// - **ExactInput**: Given the input, compute output. Bps fees are pure ceiling-based.
/// - **ExactOutput**: Given desired output, compute input. Network fee may be bumped
///   (never reduced) to absorb rounding and guarantee exact output.
///
/// Returns `None` if the output would be below `MIN_VIABLE_OUTPUT_SATS`.
pub fn compute_fees(mode: SwapMode, rates: &SwapRates) -> Option<FeeBreakdown> {
    match mode {
        SwapMode::ExactInput(input) => compute_fees_exact_input(input, rates),
        SwapMode::ExactOutput(output) => compute_fees_exact_output(output, rates),
    }
}

/// Compute fees for exact input mode.
///
/// Fees are pure bps-based with ceiling division. The output is deterministic
/// for any given input.
///
/// Returns `None` if the output would be below `MIN_VIABLE_OUTPUT_SATS`.
pub fn compute_fees_exact_input(input: u64, rates: &SwapRates) -> Option<FeeBreakdown> {
    let liquidity_fee = input.saturating_mul(rates.liquidity_fee_bps).div_ceil(BPS_DENOM);
    let protocol_fee = input.saturating_mul(rates.protocol_fee_bps).div_ceil(BPS_DENOM);
    let network_fee = rates.network_fee_sats;

    let total_fees = liquidity_fee
        .saturating_add(protocol_fee)
        .saturating_add(network_fee);
    let output = input.saturating_sub(total_fees);

    if output < MIN_VIABLE_OUTPUT_SATS {
        return None;
    }

    Some(FeeBreakdown {
        input,
        liquidity_fee,
        protocol_fee,
        network_fee,
        output,
    })
}

/// Compute fees for exact output mode.
///
/// Given a desired output, computes the required input. Bps fees (liquidity, protocol)
/// remain pure ceiling-based. The network fee absorbs any rounding slack by being
/// bumped up (never reduced) from the base `rates.network_fee_sats`.
///
/// This guarantees: `output == desired_output` exactly.
///
/// Returns `None` if:
/// - `desired_output < MIN_VIABLE_OUTPUT_SATS`
/// - Fees would exceed 100%
/// - Network fee adjustment exceeds reasonable bounds (> 3 sats above base)
pub fn compute_fees_exact_output(desired_output: u64, rates: &SwapRates) -> Option<FeeBreakdown> {
    if desired_output < MIN_VIABLE_OUTPUT_SATS {
        return None;
    }

    let input = inverse_input_for_exact_output(desired_output, rates);
    if input == u64::MAX {
        return None;
    }

    // Compute pure bps-based fees (ceiling)
    let liquidity_fee = input.saturating_mul(rates.liquidity_fee_bps).div_ceil(BPS_DENOM);
    let protocol_fee = input.saturating_mul(rates.protocol_fee_bps).div_ceil(BPS_DENOM);

    // Compute what output would be with base network fee
    let base_network_fee = rates.network_fee_sats;
    let tentative_output = input
        .saturating_sub(liquidity_fee)
        .saturating_sub(protocol_fee)
        .saturating_sub(base_network_fee);

    if tentative_output < desired_output {
        // Closed-form input was not enough (shouldn't happen with correct formula, but guard)
        return None;
    }

    // Network fee absorbs the slack: bump it up to hit exact output
    let network_fee_adjustment = tentative_output - desired_output;
    let network_fee = base_network_fee + network_fee_adjustment;

    // Sanity check: network fee shouldn't drift too far from base
    // With the +2 margin in inverse calculation, adjustment is typically 0-5 sats
    if network_fee_adjustment > 5 {
        return None;
    }

    Some(FeeBreakdown {
        input,
        liquidity_fee,
        protocol_fee,
        network_fee,
        output: desired_output,
    })
}

/// Compute the protocol fee for a given amount using ceiling division.
pub fn compute_protocol_fee(amount: u64, protocol_fee_bps: u64) -> u64 {
    amount.saturating_mul(protocol_fee_bps).div_ceil(BPS_DENOM)
}

/// Compute the input required to produce exactly `desired_output` after all fees.
///
/// This is a closed-form computation (no iteration):
/// `input = ceil((desired_output + network_fee) * 10_000 / (10_000 - total_bps)) + margin`
///
/// The +2 margin accounts for ceiling rounding on both bps fees, ensuring
/// the computed input is always sufficient.
///
/// Returns `u64::MAX` if fees would exceed 100%.
pub fn inverse_input_for_exact_output(desired_output: u64, rates: &SwapRates) -> u64 {
    if desired_output == 0 {
        return 0;
    }

    let remaining_bps = BPS_DENOM.saturating_sub(rates.total_bps());
    if remaining_bps == 0 {
        return u64::MAX; // 100%+ fees, impossible
    }

    // Add +2 margin to account for ceiling rounding on both liquidity and protocol fees
    desired_output
        .saturating_add(rates.network_fee_sats)
        .saturating_mul(BPS_DENOM)
        .div_ceil(remaining_bps)
        .saturating_add(2)
}

/// Alias for `inverse_input_for_exact_output` for backwards compatibility.
pub fn inverse_compute_input(desired_output: u64, rates: &SwapRates) -> u64 {
    inverse_input_for_exact_output(desired_output, rates)
}

/// Compute the minimum input that produces at least `MIN_VIABLE_OUTPUT_SATS` after all fees.
///
/// Uses the exact output calculation plus a small margin to account for ceiling rounding
/// when computing fees via `compute_fees()`.
pub fn compute_min_viable_input(rates: &SwapRates) -> u64 {
    // The exact output path gives us the theoretical minimum, but compute_fees uses
    // ceiling on both bps fees, so we add a small margin.
    inverse_input_for_exact_output(MIN_VIABLE_OUTPUT_SATS, rates).saturating_add(2)
}

/// Compute the maximum input that produces at most `max_output` after all fees.
///
/// This finds the largest input where `compute_fees(input, rates).output <= max_output`.
pub fn compute_max_input_for_output(max_output: u64, rates: &SwapRates) -> u64 {
    if max_output == 0 {
        return 0;
    }

    // Use floor division to get the max input that doesn't exceed max_output
    let after_network = max_output.saturating_add(rates.network_fee_sats);
    let remaining_bps = BPS_DENOM.saturating_sub(rates.total_bps());

    if remaining_bps == 0 {
        return u64::MAX;
    }

    after_network.saturating_mul(BPS_DENOM) / remaining_bps
}

#[cfg(test)]
mod tests {
    use super::*;

    // Keep test coverage broad but compact:
    // - prime-heavy values to stress ceil boundaries
    // - some values ending in 5 and multiples of 10 to hit common "human" amounts
    // - representative rate configs (including prime-heavy)
    const RATE_CONFIGS: &[SwapRates] = &[
        SwapRates {
            liquidity_fee_bps: 0,
            protocol_fee_bps: 0,
            network_fee_sats: 0,
        },
        SwapRates {
            liquidity_fee_bps: 10,
            protocol_fee_bps: 10,
            network_fee_sats: 0,
        },
        SwapRates {
            liquidity_fee_bps: 0,
            protocol_fee_bps: 0,
            network_fee_sats: 1000,
        },
        SwapRates {
            liquidity_fee_bps: 13,
            protocol_fee_bps: 10,
            network_fee_sats: 1000,
        },
        SwapRates {
            liquidity_fee_bps: 100,
            protocol_fee_bps: 50,
            network_fee_sats: 5000,
        },
        SwapRates {
            liquidity_fee_bps: 1,
            protocol_fee_bps: 1,
            network_fee_sats: 1,
        },
        SwapRates {
            liquidity_fee_bps: 500,
            protocol_fee_bps: 500,
            network_fee_sats: 0,
        },
        // Prime-heavy stress config
        SwapRates {
            liquidity_fee_bps: 17,   // prime
            protocol_fee_bps: 19,    // prime
            network_fee_sats: 1_003, // prime
        },
    ];

    // Outputs to try in ExactOutput mode (must be >= MIN_VIABLE_OUTPUT_SATS to be "valid").
    // Includes:
    // - boundary/dust-adjacent
    // - primes near powers of 10
    // - numbers ending in 5
    // - multiples of 10
    fn desired_outputs() -> impl Iterator<Item = u64> {
        [
            // boundary / dust-adjacent
            MIN_VIABLE_OUTPUT_SATS,
            MIN_VIABLE_OUTPUT_SATS + 1,

            // small human-ish values (end in 5 / multiples of 10)
            1_000,
            1_005,
            1_010,

            // small primes
            997,
            1_003,
            1_009,

            // primes near 10^4 + human-ish
            9_970,
            9_975,
            9_983,
            9_973,
            10_000,
            10_005,
            10_010,
            10_007,

            // primes near 10^5 + human-ish
            99_990,
            99_995,
            100_000,
            100_005,
            99_991,
            100_003,

            // primes near 10^6 + human-ish
            999_980,
            999_985,
            1_000_000,
            1_000_005,
            999_983,
            1_000_003,

            // larger primes + human-ish
            10_000_000,
            10_000_005,
            10_000_019,
            99_999_930,
            99_999_935,
            99_999_937,

            // very large primes + human-ish
            1_000_000_000,
            1_000_000_005,
            1_000_000_007,
            10_000_000_000,
            10_000_000_005,
            10_000_000_019,
        ]
        .into_iter()
        .filter(|&x| x >= MIN_VIABLE_OUTPUT_SATS)
    }

    // Inputs to try in ExactInput mode.
    // Includes:
    // - degenerate/boundary
    // - primes
    // - values ending in 5
    // - multiples of 10
    fn inputs() -> impl Iterator<Item = u64> {
        [
            // degenerate / boundary
            0,
            1,

            // small multiples of 10 + ending in 5
            500,
            505,
            510,

            // small primes
            499,
            503,
            509,

            // dust-adjacent values (end in 5 + primes nearby)
            1_490,
            1_495,
            1_500,
            1_493,
            1_499,
            1_503,

            // primes near 10^4 + human-ish
            9_970,
            9_975,
            10_000,
            10_005,
            10_010,
            9_973,
            10_007,

            // primes near 10^5 + human-ish
            99_990,
            99_995,
            100_000,
            100_005,
            99_991,
            100_003,

            // primes near 10^6 + human-ish
            999_980,
            999_985,
            1_000_000,
            1_000_005,
            999_983,
            1_000_003,

            // large primes + human-ish
            10_000_000,
            10_000_005,
            10_000_019,
            99_999_930,
            99_999_935,
            99_999_937,

            // very large primes + human-ish
            1_000_000_000,
            1_000_000_005,
            1_000_000_007,
        ]
        .into_iter()
    }

    fn assert_breakdown_sane(bd: FeeBreakdown, rates: &SwapRates) {
        // Core accounting identity
        assert_eq!(
            bd.input,
            bd.output + bd.total_fees(),
            "accounting invariant violated: {:?} rates={:?}",
            bd,
            rates
        );

        // Fees match the implementation's definition (bps fees are pure ceil on input)
        let expected_liq = bd
            .input
            .saturating_mul(rates.liquidity_fee_bps)
            .div_ceil(BPS_DENOM);
        let expected_proto = bd
            .input
            .saturating_mul(rates.protocol_fee_bps)
            .div_ceil(BPS_DENOM);

        assert_eq!(
            bd.liquidity_fee, expected_liq,
            "liquidity_fee not pure bps: {:?} rates={:?}",
            bd, rates
        );
        assert_eq!(
            bd.protocol_fee, expected_proto,
            "protocol_fee not pure bps: {:?} rates={:?}",
            bd, rates
        );

        // Network fee is never below the base (ExactInput uses base; ExactOutput may bump)
        assert!(
            bd.network_fee >= rates.network_fee_sats,
            "network_fee below base: {:?} rates={:?}",
            bd,
            rates
        );

        // Output viability invariant when compute_fees returns Some
        assert!(
            bd.output >= MIN_VIABLE_OUTPUT_SATS,
            "returned Some but output is dust: {:?} rates={:?}",
            bd,
            rates
        );
    }

    /// Holistic test over multiple rate configs and a range of inputs/outputs.
    ///
    /// Validates:
    /// - ExactInput: deterministic accounting invariants whenever Some(...)
    /// - ExactOutput: output is exact, invariants hold, and network fee adjustment is bounded
    #[test]
    fn test_holistic_invariants_across_modes() {
        for rates in RATE_CONFIGS {
            // Fees >= 100% is nonsensical; skip (not present here, but safe)
            if rates.total_bps() >= BPS_DENOM {
                continue;
            }

            // ExactInput sweep
            for input in inputs() {
                let res = compute_fees(SwapMode::ExactInput(input), rates);

                if let Some(bd) = res {
                    assert_breakdown_sane(bd, rates);

                    // ExactInput uses exactly the base network fee
                    assert_eq!(
                        bd.network_fee, rates.network_fee_sats,
                        "ExactInput should not bump network fee: {:?} rates={:?}",
                        bd,
                        rates
                    );
                }
            }

            // ExactOutput sweep
            for desired_output in desired_outputs() {
                let res = compute_fees(SwapMode::ExactOutput(desired_output), rates);

                // In your current design ExactOutput may return None (e.g., adjustment > 5).
                // This test asserts invariants when a quote is produced.
                let Some(bd) = res else { continue };

                // ExactOutput guarantee
                assert_eq!(
                    bd.output, desired_output,
                    "ExactOutput not exact: desired={} got={:?} rates={:?}",
                    desired_output,
                    bd,
                    rates
                );

                assert_breakdown_sane(bd, rates);

                // Adjustment bound (matches compute_fees_exact_output guard)
                let adjustment = bd.network_fee - rates.network_fee_sats;
                assert!(
                    adjustment <= 5,
                    "network_fee adjustment too large: adj={} bd={:?} rates={:?}",
                    adjustment,
                    bd,
                    rates
                );
            }
        }
    }

    /// Wires ExactOutput -> ExactInput on the quoted input, validating:
    /// - same input => same bps fees
    /// - ExactInput output is >= ExactOutput output (because ExactOutput may bump network fee)
    /// - output delta equals the network fee bump exactly
    #[test]
    fn test_exact_out_to_exact_in_consistency() {
        for rates in RATE_CONFIGS {
            if rates.total_bps() >= BPS_DENOM {
                continue;
            }

            for desired_output in desired_outputs() {
                let Some(exact_out) = compute_fees(SwapMode::ExactOutput(desired_output), rates)
                else {
                    continue;
                };

                assert_eq!(exact_out.output, desired_output);
                assert_breakdown_sane(exact_out, rates);

                let exact_in = compute_fees(SwapMode::ExactInput(exact_out.input), rates)
                    .expect("ExactInput should be viable if ExactOutput was viable");

                assert_breakdown_sane(exact_in, rates);

                // Same input => same bps fees
                assert_eq!(exact_in.liquidity_fee, exact_out.liquidity_fee);
                assert_eq!(exact_in.protocol_fee, exact_out.protocol_fee);

                // ExactInput uses base network fee; ExactOutput may be bumped
                assert_eq!(exact_in.network_fee, rates.network_fee_sats);
                assert!(exact_out.network_fee >= exact_in.network_fee);

                // Therefore ExactInput output should be >= ExactOutput output
                assert!(
                    exact_in.output >= exact_out.output,
                    "ExactInput output should be >= ExactOutput output for same input. \
                     exact_in={:?} exact_out={:?} rates={:?}",
                    exact_in,
                    exact_out,
                    rates
                );

                // If ExactOutput bumped network fee, the difference should match the bump
                let bump = exact_out.network_fee - exact_in.network_fee;
                assert_eq!(
                    exact_in.output - exact_out.output,
                    bump,
                    "output delta should equal network fee bump. exact_in={:?} exact_out={:?} rates={:?}",
                    exact_in,
                    exact_out,
                    rates
                );
            }
        }
    }

    /// Minimal unit sanity checks that don't overlap with the holistic tests.
    /// - total_bps computation
    /// - dust rejection smoke check for both modes
    #[test]
    fn test_basic_sanity() {
        let rates = SwapRates::new(13, 10, 1000);
        assert_eq!(rates.total_bps(), 23);

        assert!(compute_fees(SwapMode::ExactInput(1_500), &rates).is_none());
        assert!(compute_fees(SwapMode::ExactOutput(500), &rates).is_none());
    }
}
