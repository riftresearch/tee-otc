use crate::error::{OtcServerError, OtcServerResult};
use alloy::primitives::U256;
use otc_models::{
    ChainType, Currency, FeeSchedule, LatestRefund, Lot, MMDepositStatus, Metadata,
    SettlementStatus, TokenIdentifier, UserDepositStatus,
};
use serde_json;

#[must_use]
pub fn chain_type_to_db(chain: &ChainType) -> &'static str {
    match chain {
        ChainType::Bitcoin => "bitcoin",
        ChainType::Ethereum => "ethereum",
    }
}

pub fn chain_type_from_db(s: &str) -> OtcServerResult<ChainType> {
    match s {
        "bitcoin" => Ok(ChainType::Bitcoin),
        "ethereum" => Ok(ChainType::Ethereum),
        _ => Err(OtcServerError::InvalidData {
            message: format!("Invalid chain type: {s}"),
        }),
    }
}

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

pub fn lot_to_db(lot: &Lot) -> OtcServerResult<(String, serde_json::Value, String, u8)> {
    let chain = chain_type_to_db(&lot.currency.chain).to_string();
    let token = token_identifier_to_json(&lot.currency.token)?;
    let amount = u256_to_db(&lot.amount);
    let decimals = lot.currency.decimals;
    Ok((chain, token, amount, decimals))
}

pub fn lot_from_db(
    chain: String,
    token: serde_json::Value,
    amount: String,
    decimals: u8,
) -> OtcServerResult<Lot> {
    Ok(Lot {
        currency: Currency {
            chain: chain_type_from_db(&chain)?,
            token: token_identifier_from_json(token)?,
            decimals,
        },
        amount: u256_from_db(&amount)?,
    })
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

pub fn fee_schedule_to_json(schedule: &FeeSchedule) -> OtcServerResult<serde_json::Value> {
    serde_json::to_value(schedule).map_err(|e| OtcServerError::InvalidData {
        message: format!("Failed to serialize fee schedule: {e}"),
    })
}

pub fn fee_schedule_from_json(value: serde_json::Value) -> OtcServerResult<FeeSchedule> {
    serde_json::from_value(value).map_err(|e| OtcServerError::InvalidData {
        message: format!("Failed to deserialize fee schedule: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chain_type_conversion() {
        assert_eq!(chain_type_to_db(&ChainType::Bitcoin), "bitcoin");
        assert_eq!(chain_type_to_db(&ChainType::Ethereum), "ethereum");

        assert_eq!(chain_type_from_db("bitcoin").unwrap(), ChainType::Bitcoin);
        assert_eq!(chain_type_from_db("ethereum").unwrap(), ChainType::Ethereum);
        assert!(chain_type_from_db("invalid").is_err());
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
}
