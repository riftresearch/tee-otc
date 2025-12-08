use crate::error::{OtcServerError, OtcServerResult};
use alloy::primitives::U256;
use otc_models::{
    ChainType, Currency, LatestRefund, Lot, MMDepositStatus, Metadata, RealizedSwap,
    SettlementStatus, SwapRates, TokenIdentifier, UserDepositStatus,
};
use serde_json;

pub fn token_identifier_to_json(token: &TokenIdentifier) -> OtcServerResult<serde_json::Value> {
    serde_json::to_value(token).map_err(|e| OtcServerError::InvalidData {
        message: format!("Failed to serialize token identifier: {e}"),
    })
}

pub fn token_identifier_from_json(value: serde_json::Value) -> OtcServerResult<TokenIdentifier> {
    serde_json::from_value(value).map_err(|e| OtcServerError::InvalidData {
        message: format!("Failed to deserialize token identifier: {e}"),
    })
}

#[must_use]
pub fn u256_to_db(value: &U256) -> String {
    value.to_string()
}

pub fn u256_from_db(s: &str) -> OtcServerResult<U256> {
    U256::from_str_radix(s, 10).map_err(|_| OtcServerError::InvalidData {
        message: format!("Invalid U256 value: {s}"),
    })
}

/// Convert Currency to database fields (chain, token JSON, decimals)
pub fn currency_to_db(currency: &Currency) -> OtcServerResult<(String, serde_json::Value, u8)> {
    let chain = currency.chain.to_db_string();
    let token = token_identifier_to_json(&currency.token)?;
    let decimals = currency.decimals;
    Ok((chain.to_string(), token, decimals))
}

/// Convert database fields back to Currency
pub fn currency_from_db(
    chain: String,
    token: serde_json::Value,
    decimals: u8,
) -> OtcServerResult<Currency> {
    Ok(Currency {
        chain: ChainType::from_db_string(&chain).ok_or(OtcServerError::InvalidData {
            message: format!("Invalid chain type: {chain}"),
        })?,
        token: token_identifier_from_json(token)?,
        decimals,
    })
}

pub fn lot_to_db(lot: &Lot) -> OtcServerResult<(String, serde_json::Value, String, u8)> {
    let chain = lot.currency.chain.to_db_string();
    let token = token_identifier_to_json(&lot.currency.token)?;
    let amount = u256_to_db(&lot.amount);
    let decimals = lot.currency.decimals;
    Ok((chain.to_string(), token, amount, decimals))
}

pub fn lot_from_db(
    chain: String,
    token: serde_json::Value,
    amount: String,
    decimals: u8,
) -> OtcServerResult<Lot> {
    Ok(Lot {
        currency: Currency {
            chain: ChainType::from_db_string(&chain).ok_or(OtcServerError::InvalidData {
                message: format!("Invalid chain type: {chain}"),
            })?,
            token: token_identifier_from_json(token)?,
            decimals,
        },
        amount: u256_from_db(&amount)?,
    })
}

pub fn swap_rates_from_db(
    liquidity_fee_bps: i64,
    protocol_fee_bps: i64,
    network_fee_sats: i64,
) -> SwapRates {
    SwapRates::new(
        liquidity_fee_bps as u64,
        protocol_fee_bps as u64,
        network_fee_sats as u64,
    )
}

pub fn user_deposit_status_to_json(
    status: &UserDepositStatus,
) -> OtcServerResult<serde_json::Value> {
    serde_json::to_value(status).map_err(|e| OtcServerError::InvalidData {
        message: format!("Failed to serialize user deposit status: {e}"),
    })
}

pub fn user_deposit_status_from_json(
    value: serde_json::Value,
) -> OtcServerResult<UserDepositStatus> {
    serde_json::from_value(value).map_err(|e| OtcServerError::InvalidData {
        message: format!("Failed to deserialize user deposit status: {e}"),
    })
}

pub fn mm_deposit_status_to_json(status: &MMDepositStatus) -> OtcServerResult<serde_json::Value> {
    serde_json::to_value(status).map_err(|e| OtcServerError::InvalidData {
        message: format!("Failed to serialize MM deposit status: {e}"),
    })
}

pub fn mm_deposit_status_from_json(value: serde_json::Value) -> OtcServerResult<MMDepositStatus> {
    serde_json::from_value(value).map_err(|e| OtcServerError::InvalidData {
        message: format!("Failed to deserialize MM deposit status: {e}"),
    })
}

pub fn settlement_status_to_json(status: &SettlementStatus) -> OtcServerResult<serde_json::Value> {
    serde_json::to_value(status).map_err(|e| OtcServerError::InvalidData {
        message: format!("Failed to serialize settlement status: {e}"),
    })
}

pub fn settlement_status_from_json(value: serde_json::Value) -> OtcServerResult<SettlementStatus> {
    serde_json::from_value(value).map_err(|e| OtcServerError::InvalidData {
        message: format!("Failed to deserialize settlement status: {e}"),
    })
}

pub fn latest_refund_to_json(status: &LatestRefund) -> OtcServerResult<serde_json::Value> {
    serde_json::to_value(status).map_err(|e| OtcServerError::InvalidData {
        message: format!("Failed to serialize latest refund: {e}"),
    })
}

pub fn latest_refund_from_json(value: serde_json::Value) -> OtcServerResult<LatestRefund> {
    serde_json::from_value(value).map_err(|e| OtcServerError::InvalidData {
        message: format!("Failed to deserialize latest refund: {e}"),
    })
}

pub fn metadata_to_json(metadata: &Metadata) -> OtcServerResult<serde_json::Value> {
    serde_json::to_value(metadata).map_err(|e| OtcServerError::InvalidData {
        message: format!("Failed to serialize metadata: {e}"),
    })
}

pub fn metadata_from_json(value: serde_json::Value) -> OtcServerResult<Metadata> {
    serde_json::from_value(value).map_err(|e| OtcServerError::InvalidData {
        message: format!("Failed to deserialize metadata: {e}"),
    })
}

pub fn realized_swap_to_json(realized: &RealizedSwap) -> OtcServerResult<serde_json::Value> {
    serde_json::to_value(realized).map_err(|e| OtcServerError::InvalidData {
        message: format!("Failed to serialize realized swap: {e}"),
    })
}

pub fn realized_swap_from_json(value: serde_json::Value) -> OtcServerResult<RealizedSwap> {
    serde_json::from_value(value).map_err(|e| OtcServerError::InvalidData {
        message: format!("Failed to deserialize realized swap: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chain_type_conversion() {
        assert_eq!(ChainType::Bitcoin.to_db_string(), "bitcoin");
        assert_eq!(ChainType::Ethereum.to_db_string(), "ethereum");
        assert_eq!(ChainType::Base.to_db_string(), "base");

        assert_eq!(
            ChainType::from_db_string("bitcoin").unwrap(),
            ChainType::Bitcoin
        );
        assert_eq!(
            ChainType::from_db_string("ethereum").unwrap(),
            ChainType::Ethereum
        );
        assert_eq!(ChainType::from_db_string("base").unwrap(), ChainType::Base);
        assert!(ChainType::from_db_string("invalid").is_none());
    }

    #[test]
    fn test_currency_conversion() {
        let currency = Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        };
        let (chain, token, decimals) = currency_to_db(&currency).unwrap();
        assert_eq!(chain, "bitcoin");
        assert_eq!(token, serde_json::json!({ "type": "Native" }));
        assert_eq!(decimals, 8);

        let currency2 = currency_from_db(chain, token, decimals).unwrap();
        assert_eq!(currency2.chain, currency.chain);
        assert_eq!(currency2.token, currency.token);
        assert_eq!(currency2.decimals, currency.decimals);
    }

    #[test]
    fn test_lot_conversion() {
        let lot = Lot {
            currency: Currency {
                chain: ChainType::Bitcoin,
                token: TokenIdentifier::Native,
                decimals: 8,
            },
            amount: U256::from(100),
        };
        let (chain, token, amount, decimals) = lot_to_db(&lot).unwrap();
        assert_eq!(chain, "bitcoin");
        assert_eq!(token, serde_json::json!({ "type": "Native" }));
        assert_eq!(amount, "100");
        assert_eq!(decimals, 8);

        let lot2 = lot_from_db(chain, token, amount, decimals).unwrap();
        assert_eq!(lot2.currency.chain, lot.currency.chain);
        assert_eq!(lot2.currency.token, lot.currency.token);
        assert_eq!(lot2.amount, lot.amount);
    }

    #[test]
    fn test_swap_rates_conversion() {
        let rates = swap_rates_from_db(13, 10, 1000);
        assert_eq!(rates.liquidity_fee_bps, 13);
        assert_eq!(rates.protocol_fee_bps, 10);
        assert_eq!(rates.network_fee_sats, 1000);
    }
}
