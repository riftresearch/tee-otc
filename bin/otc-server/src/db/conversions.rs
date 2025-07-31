use alloy::primitives::U256;
use otc_models::{ChainType, TokenIdentifier, Currency, UserDepositStatus, MMDepositStatus, SettlementStatus};
use serde_json;
use crate::error::{OtcServerError, OtcServerResult};

#[must_use] pub fn chain_type_to_db(chain: &ChainType) -> &'static str {
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

#[must_use] pub fn u256_to_db(value: &U256) -> String {
    value.to_string()
}

pub fn u256_from_db(s: &str) -> OtcServerResult<U256> {
    U256::from_str_radix(s, 10).map_err(|_| OtcServerError::InvalidData {
        message: format!("Invalid U256 value: {s}"),
    })
}

pub fn currency_to_db(currency: &Currency) -> OtcServerResult<(String, serde_json::Value, String, u8)> {
    let chain = chain_type_to_db(&currency.chain).to_string();
    let token = token_identifier_to_json(&currency.token)?;
    let amount = u256_to_db(&currency.amount);
    let decimals = currency.decimals;
    Ok((chain, token, amount, decimals))
}

pub fn currency_from_db(chain: String, token: serde_json::Value, amount: String, decimals: u8) -> OtcServerResult<Currency> {
    Ok(Currency {
        chain: chain_type_from_db(&chain)?,
        token: token_identifier_from_json(token)?,
        amount: u256_from_db(&amount)?,
        decimals,
    })
}

pub fn user_deposit_status_to_json(status: &UserDepositStatus) -> OtcServerResult<serde_json::Value> {
    serde_json::to_value(status).map_err(|e| OtcServerError::InvalidData {
        message: format!("Failed to serialize user deposit status: {e}"),
    })
}

pub fn user_deposit_status_from_json(value: serde_json::Value) -> OtcServerResult<UserDepositStatus> {
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
    fn test_currency_conversion() {
        let currency = Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            amount: U256::from(100),
            decimals: 8,
        };
        let (chain, token, amount, decimals) = currency_to_db(&currency).unwrap();
        assert_eq!(chain, "bitcoin");
        assert_eq!(token, serde_json::json!({ "type": "Native" }));
        assert_eq!(amount, "100");
        assert_eq!(decimals, 8);

        let currency2 = currency_from_db(chain, token, amount, decimals).unwrap();
        assert_eq!(currency2.chain, currency.chain);
        assert_eq!(currency2.token, currency.token);
        assert_eq!(currency2.amount, currency.amount); 
    }
}