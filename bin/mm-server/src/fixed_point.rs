use alloy::primitives::{U160, U256};
use snafu::prelude::*;
use std::fmt;

/// Fixed-point arithmetic for precise financial calculations
/// Uses 18 decimal places (1e18) as the standard precision
pub const PRECISION: u32 = 18;
pub const PRECISION_FACTOR: u128 = 1_000_000_000_000_000_000; // 10^18

#[derive(Debug, Snafu)]
pub enum FixedPointError {
    #[snafu(display("Overflow in fixed-point arithmetic"))]
    Overflow,
    
    #[snafu(display("Division by zero"))]
    DivisionByZero,
    
    #[snafu(display("Invalid conversion: value too large"))]
    InvalidConversion,
}

type Result<T> = std::result::Result<T, FixedPointError>;

/// Fixed-point conversion rates for high-precision calculations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixedConversionRates {
    /// cbBTC per ETH with 18 decimal precision
    pub cbbtc_per_eth_fixed: U256,
    /// ETH per cbBTC with 18 decimal precision
    pub eth_per_cbbtc_fixed: U256,
}

impl FixedConversionRates {
    /// Create new conversion rates with validation
    pub fn new(cbbtc_per_eth: U256, eth_per_cbbtc: U256) -> Result<Self> {
        // Validate that rates are reciprocals (within acceptable tolerance)
        // cbbtc_per_eth * eth_per_cbbtc should equal PRECISION_FACTOR^2
        let product = cbbtc_per_eth.saturating_mul(eth_per_cbbtc);
        let expected = U256::from(PRECISION_FACTOR).saturating_mul(U256::from(PRECISION_FACTOR));
        
        // Allow 0.01% tolerance for rounding
        let tolerance = expected / U256::from(10000);
        let diff = if product > expected {
            product - expected
        } else {
            expected - product
        };
        
        if diff > tolerance {
            return Err(FixedPointError::InvalidConversion);
        }
        
        Ok(Self {
            cbbtc_per_eth_fixed: cbbtc_per_eth,
            eth_per_cbbtc_fixed: eth_per_cbbtc,
        })
    }
    
    /// Convert ETH amount to cbBTC with proper decimal handling
    pub fn eth_to_cbbtc(&self, eth_amount: U256) -> Result<U256> {
        // ETH has 18 decimals, cbBTC has 8 decimals
        // eth_amount * cbbtc_per_eth_fixed / PRECISION_FACTOR / 10^10
        let cbbtc_raw = eth_amount
            .checked_mul(self.cbbtc_per_eth_fixed)
            .ok_or(FixedPointError::Overflow)?;
        
        let cbbtc_adjusted = cbbtc_raw
            .checked_div(U256::from(PRECISION_FACTOR))
            .ok_or(FixedPointError::DivisionByZero)?;
        
        // Adjust for decimal difference (18 - 8 = 10)
        let result = cbbtc_adjusted
            .checked_div(U256::from(10_000_000_000u64))
            .ok_or(FixedPointError::DivisionByZero)?;
        
        Ok(result)
    }
    
    /// Convert cbBTC amount to ETH with proper decimal handling
    pub fn cbbtc_to_eth(&self, cbbtc_amount: U256) -> Result<U256> {
        // cbBTC has 8 decimals, ETH has 18 decimals
        // cbbtc_amount * eth_per_cbbtc_fixed * 10^10 / PRECISION_FACTOR
        let cbbtc_adjusted = cbbtc_amount
            .checked_mul(U256::from(10_000_000_000u64))
            .ok_or(FixedPointError::Overflow)?;
        
        let eth_raw = cbbtc_adjusted
            .checked_mul(self.eth_per_cbbtc_fixed)
            .ok_or(FixedPointError::Overflow)?;
        
        let result = eth_raw
            .checked_div(U256::from(PRECISION_FACTOR))
            .ok_or(FixedPointError::DivisionByZero)?;
        
        Ok(result)
    }
    
    /// Apply spread in basis points (1 bp = 0.01%)
    pub fn apply_spread(&self, amount: U256, spread_bps: u32, is_buy: bool) -> Result<U256> {
        let spread_factor = U256::from(10000 + spread_bps);
        let base_factor = U256::from(10000);
        
        if is_buy {
            // When buying, we give less (subtract spread)
            let adjusted = amount
                .checked_mul(base_factor)
                .ok_or(FixedPointError::Overflow)?;
            adjusted
                .checked_div(spread_factor)
                .ok_or(FixedPointError::DivisionByZero)
        } else {
            // When selling, we ask for more (add spread)
            let adjusted = amount
                .checked_mul(spread_factor)
                .ok_or(FixedPointError::Overflow)?;
            adjusted
                .checked_div(base_factor)
                .ok_or(FixedPointError::DivisionByZero)
        }
    }
}

/// Fixed-point arithmetic operations
pub struct FixedPoint;

impl FixedPoint {
    /// Convert a Uniswap V3 sqrtPriceX96 to conversion rates
    pub fn sqrt_price_to_rates(sqrt_price_x96: U160, weth_is_token0: bool) -> Result<FixedConversionRates> {
        // Convert sqrtPriceX96 to a regular price
        // sqrtPriceX96 is sqrt(price) * 2^96
        let sqrt_price_u256 = U256::from(sqrt_price_x96);
        
        // Square the price: (sqrtPrice)^2
        let price_x192 = sqrt_price_u256
            .checked_mul(sqrt_price_u256)
            .ok_or(FixedPointError::Overflow)?;
        
        // Divide by 2^192 to get the actual price ratio
        // We'll do this in steps to avoid overflow
        let price_x96: U256 = price_x192 >> 96;
        
        // Convert to fixed-point with 18 decimals
        // price_x96 / 2^96 * 10^18
        let precision_u256 = U256::from(PRECISION_FACTOR);
        let two_pow_96 = U256::from(1u128) << 96;
        
        let price_fixed = price_x96
            .checked_mul(precision_u256)
            .ok_or(FixedPointError::Overflow)?
            .checked_div(two_pow_96)
            .ok_or(FixedPointError::DivisionByZero)?;
        
        // Adjust for decimal differences (WETH: 18, cbBTC: 8)
        // Difference: 10 decimals = 10^10
        let decimal_adjustment = U256::from(10_000_000_000u64);
        
        let (cbbtc_per_eth_fixed, eth_per_cbbtc_fixed) = if weth_is_token0 {
            // price is token1/token0 = cbBTC/WETH
            // Adjust for decimals: multiply by 10^10
            let cbbtc_per_eth = price_fixed
                .checked_mul(decimal_adjustment)
                .ok_or(FixedPointError::Overflow)?;
            
            // Calculate reciprocal: 1 / cbbtc_per_eth
            let eth_per_cbbtc = precision_u256
                .checked_mul(precision_u256)
                .ok_or(FixedPointError::Overflow)?
                .checked_div(cbbtc_per_eth)
                .ok_or(FixedPointError::DivisionByZero)?;
            
            (cbbtc_per_eth, eth_per_cbbtc)
        } else {
            // price is token1/token0 = WETH/cbBTC
            // This is already eth_per_cbbtc, but needs decimal adjustment
            let eth_per_cbbtc = price_fixed
                .checked_div(decimal_adjustment)
                .ok_or(FixedPointError::DivisionByZero)?;
            
            // Calculate reciprocal
            let cbbtc_per_eth = precision_u256
                .checked_mul(precision_u256)
                .ok_or(FixedPointError::Overflow)?
                .checked_div(eth_per_cbbtc)
                .ok_or(FixedPointError::DivisionByZero)?;
            
            (cbbtc_per_eth, eth_per_cbbtc)
        };
        
        FixedConversionRates::new(cbbtc_per_eth_fixed, eth_per_cbbtc_fixed)
    }
    
    /// Convert float rates to fixed-point representation
    pub fn from_float_rates(cbbtc_per_eth: f64, eth_per_cbbtc: f64) -> Result<FixedConversionRates> {
        if cbbtc_per_eth <= 0.0 || eth_per_cbbtc <= 0.0 {
            return Err(FixedPointError::InvalidConversion);
        }
        
        let cbbtc_per_eth_fixed = U256::from((cbbtc_per_eth * PRECISION_FACTOR as f64) as u128);
        let eth_per_cbbtc_fixed = U256::from((eth_per_cbbtc * PRECISION_FACTOR as f64) as u128);
        
        FixedConversionRates::new(cbbtc_per_eth_fixed, eth_per_cbbtc_fixed)
    }
}

impl fmt::Display for FixedConversionRates {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cbbtc_per_eth = self.cbbtc_per_eth_fixed.to::<u128>() as f64 / PRECISION_FACTOR as f64;
        let eth_per_cbbtc = self.eth_per_cbbtc_fixed.to::<u128>() as f64 / PRECISION_FACTOR as f64;
        
        write!(
            f,
            "1 ETH = {:.8} cbBTC, 1 cbBTC = {:.8} ETH",
            cbbtc_per_eth,
            eth_per_cbbtc
        )
    }
}