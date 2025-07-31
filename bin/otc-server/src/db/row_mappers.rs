use chrono::{DateTime, Utc};
use otc_models::{Quote, Swap, SwapStatus};
use sqlx::postgres::PgRow;
use sqlx::Row;
use uuid::Uuid;

use super::conversions::{currency_from_db, user_deposit_status_from_json, mm_deposit_status_from_json, settlement_status_from_json};
use crate::error::{OtcServerError, OtcServerResult};

pub trait FromRow<'r>: Sized {
    fn from_row(row: &'r PgRow) -> OtcServerResult<Self>;
}

impl<'r> FromRow<'r> for Quote {
    fn from_row(row: &'r PgRow) -> OtcServerResult<Self> {
        let id: Uuid = row.try_get("id")?;
        let from_chain: String = row.try_get("from_chain")?;
        let from_token: serde_json::Value = row.try_get("from_token")?;
        let from_amount: String = row.try_get("from_amount")?;
        let from_decimals: i16 = row.try_get("from_decimals")?;
        let to_chain: String = row.try_get("to_chain")?;
        let to_token: serde_json::Value = row.try_get("to_token")?;
        let to_amount: String = row.try_get("to_amount")?;
        let to_decimals: i16 = row.try_get("to_decimals")?;
        let market_maker_id: Uuid = row.try_get("market_maker_id")?;
        let expires_at: DateTime<Utc> = row.try_get("expires_at")?;
        let created_at: DateTime<Utc> = row.try_get("created_at")?;
        
        let from = currency_from_db(from_chain, from_token, from_amount, from_decimals as u8)?;
        let to = currency_from_db(to_chain, to_token, to_amount, to_decimals as u8)?;
        
        Ok(Quote {
            id,
            from,
            to,
            market_maker_id,
            expires_at,
            created_at,
        })
    }
}

impl<'r> FromRow<'r> for Swap {
    fn from_row(row: &'r PgRow) -> OtcServerResult<Self> {
        let id: Uuid = row.try_get("id")?;
        let market_maker_id: Uuid = row.try_get("market_maker_id")?;
        
        // Get salt as Vec<u8> from database and convert to [u8; 32]
        let user_deposit_salt_vec: Vec<u8> = row.try_get("user_deposit_salt")?;
        let mut user_deposit_salt = [0u8; 32];
        
        if user_deposit_salt_vec.len() != 32 {
            return Err(OtcServerError::InvalidData {
                message: "user_deposit_salt must be exactly 32 bytes".to_string(),
            });
        }
        user_deposit_salt.copy_from_slice(&user_deposit_salt_vec);
        
        // Get mm_nonce as Vec<u8> from database and convert to [u8; 16]
        let mm_nonce_vec: Vec<u8> = row.try_get("mm_nonce")?;
        let mut mm_nonce = [0u8; 16];
        
        if mm_nonce_vec.len() != 16 {
            return Err(OtcServerError::InvalidData {
                message: "mm_nonce must be exactly 16 bytes".to_string(),
            });
        }
        mm_nonce.copy_from_slice(&mm_nonce_vec);
        
        // Get the embedded quote fields
        let quote_id: Uuid = row.try_get("quote_id")?;
        let from_chain: String = row.try_get("from_chain")?;
        let from_token: serde_json::Value = row.try_get("from_token")?;
        let from_amount: String = row.try_get("from_amount")?;
        let from_decimals: i16 = row.try_get("from_decimals")?;
        let to_chain: String = row.try_get("to_chain")?;
        let to_token: serde_json::Value = row.try_get("to_token")?;
        let to_amount: String = row.try_get("to_amount")?;
        let to_decimals: i16 = row.try_get("to_decimals")?;
        let quote_market_maker_id: Uuid = row.try_get("quote_market_maker_id")?;
        let expires_at: DateTime<Utc> = row.try_get("expires_at")?;
        let quote_created_at: DateTime<Utc> = row.try_get("quote_created_at")?;
        
        let from = currency_from_db(from_chain, from_token, from_amount, from_decimals as u8)?;
        let to = currency_from_db(to_chain, to_token, to_amount, to_decimals as u8)?;
        
        let quote = Quote {
            id: quote_id,
            from,
            to,
            market_maker_id: quote_market_maker_id,
            expires_at,
            created_at: quote_created_at,
        };
        
        let user_deposit_address: String = row.try_get("user_deposit_address")?;
        let user_destination_address: String = row.try_get("user_destination_address")?;
        let user_refund_address: String = row.try_get("user_refund_address")?;
        let status: SwapStatus = row.try_get("status")?;
        
        // Handle JSONB fields
        let user_deposit_json: Option<serde_json::Value> = row.try_get("user_deposit_status")?;
        let user_deposit_status = match user_deposit_json {
            Some(json) => Some(user_deposit_status_from_json(json)?),
            None => None,
        };
        
        let mm_deposit_json: Option<serde_json::Value> = row.try_get("mm_deposit_status")?;
        let mm_deposit_status = match mm_deposit_json {
            Some(json) => Some(mm_deposit_status_from_json(json)?),
            None => None,
        };
        
        let settlement_json: Option<serde_json::Value> = row.try_get("settlement_status")?;
        let settlement_status = match settlement_json {
            Some(json) => Some(settlement_status_from_json(json)?),
            None => None,
        };
        
        let failure_reason: Option<String> = row.try_get("failure_reason")?;
        let failure_at: Option<DateTime<Utc>> = row.try_get("failure_at")?;
        let mm_notified_at: Option<DateTime<Utc>> = row.try_get("mm_notified_at")?;
        let mm_private_key_sent_at: Option<DateTime<Utc>> = row.try_get("mm_private_key_sent_at")?;
        let created_at: DateTime<Utc> = row.try_get("created_at")?;
        let updated_at: DateTime<Utc> = row.try_get("updated_at")?;
        
        Ok(Swap {
            id,
            market_maker_id,
            quote,
            user_deposit_salt,
            user_deposit_address,
            mm_nonce,
            user_destination_address,
            user_refund_address,
            status,
            user_deposit_status,
            mm_deposit_status,
            settlement_status,
            failure_reason,
            failure_at,
            mm_notified_at,
            mm_private_key_sent_at,
            created_at,
            updated_at,
        })
    }
}

