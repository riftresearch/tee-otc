use alloy::primitives::U256;
use otc_models::{ChainType, SwapStatus, TokenIdentifier, Currency};
use serde_json;
use super::{DbError, DbResult};

pub fn chain_type_to_db(chain: &ChainType) -> &'static str {
    match chain {
        ChainType::Bitcoin => "bitcoin",
        ChainType::Ethereum => "ethereum",
    }
}

pub fn chain_type_from_db(s: &str) -> DbResult<ChainType> {
    match s {
        "bitcoin" => Ok(ChainType::Bitcoin),
        "ethereum" => Ok(ChainType::Ethereum),
        _ => Err(DbError::InvalidData {
            message: format!("Invalid chain type: {}", s),
        }),
    }
}

pub fn swap_status_to_db(status: &SwapStatus) -> &'static str {
    match status {
        SwapStatus::QuoteValidation => "quote_validation",
        SwapStatus::QuoteRejected => "quote_rejected",
        SwapStatus::WaitingUserDeposit => "waiting_user_deposit",
        SwapStatus::WaitingMMDeposit => "waiting_mm_deposit",
        SwapStatus::WaitingConfirmations => "waiting_confirmations",
        SwapStatus::Settling => "settling",
        SwapStatus::Completed => "completed",
        SwapStatus::Refunding => "refunding",
    }
}

pub fn swap_status_from_db(s: &str) -> DbResult<SwapStatus> {
    match s {
        "quote_validation" => Ok(SwapStatus::QuoteValidation),
        "quote_rejected" => Ok(SwapStatus::QuoteRejected),
        "waiting_user_deposit" => Ok(SwapStatus::WaitingUserDeposit),
        "waiting_mm_deposit" => Ok(SwapStatus::WaitingMMDeposit),
        "waiting_confirmations" => Ok(SwapStatus::WaitingConfirmations),
        "settling" => Ok(SwapStatus::Settling),
        "completed" => Ok(SwapStatus::Completed),
        "refunding" => Ok(SwapStatus::Refunding),
        _ => Err(DbError::InvalidData {
            message: format!("Invalid swap status: {}", s),
        }),
    }
}

pub fn token_identifier_to_json(token: &TokenIdentifier) -> DbResult<serde_json::Value> {
    serde_json::to_value(token).map_err(|e| DbError::InvalidData {
        message: format!("Failed to serialize token identifier: {}", e),
    })
}

pub fn token_identifier_from_json(value: serde_json::Value) -> DbResult<TokenIdentifier> {
    serde_json::from_value(value).map_err(|e| DbError::InvalidData {
        message: format!("Failed to deserialize token identifier: {}", e),
    })
}

pub fn u256_to_db(value: &U256) -> String {
    value.to_string()
}

pub fn u256_from_db(s: &str) -> DbResult<U256> {
    U256::from_str_radix(s, 10).map_err(|_| DbError::InvalidData {
        message: format!("Invalid U256 value: {}", s),
    })
}

pub fn currency_to_db(currency: &Currency) -> DbResult<(String, serde_json::Value, String, u8)> {
    let chain = chain_type_to_db(&currency.chain).to_string();
    let token = token_identifier_to_json(&currency.token)?;
    let amount = u256_to_db(&currency.amount);
    let decimals = currency.decimals;
    Ok((chain, token, amount, decimals))
}

pub fn currency_from_db(chain: String, token: serde_json::Value, amount: String, decimals: u8) -> DbResult<Currency> {
    Ok(Currency {
        chain: chain_type_from_db(&chain)?,
        token: token_identifier_from_json(token)?,
        amount: u256_from_db(&amount)?,
        decimals: decimals,
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
    fn test_swap_status_conversion() {
        assert_eq!(swap_status_to_db(&SwapStatus::Completed), "completed");
        assert_eq!(swap_status_from_db("completed").unwrap(), SwapStatus::Completed);
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