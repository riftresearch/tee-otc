use chrono::{DateTime, Utc};
use otc_models::{Quote, Swap};
use sqlx::postgres::PgRow;
use sqlx::Row;
use uuid::Uuid;

use super::conversions::{currency_from_db, swap_status_from_db, u256_from_db};
use super::DbResult;

pub trait FromRow<'r>: Sized {
    fn from_row(row: &'r PgRow) -> DbResult<Self>;
}

impl<'r> FromRow<'r> for Quote {
    fn from_row(row: &'r PgRow) -> DbResult<Self> {
        let id: Uuid = row.try_get("id")?;
        let from_chain: String = row.try_get("from_chain")?;
        let from_token: serde_json::Value = row.try_get("from_token")?;
        let from_amount: String = row.try_get("from_amount")?;
        let from_decimals: i16 = row.try_get("from_decimals")?;
        let to_chain: String = row.try_get("to_chain")?;
        let to_token: serde_json::Value = row.try_get("to_token")?;
        let to_amount: String = row.try_get("to_amount")?;
        let to_decimals: i16 = row.try_get("to_decimals")?;
        let market_maker_identifier: String = row.try_get("market_maker_identifier")?;
        let expires_at: DateTime<Utc> = row.try_get("expires_at")?;
        let created_at: DateTime<Utc> = row.try_get("created_at")?;
        
        let from = currency_from_db(from_chain, from_token, from_amount, from_decimals as u8)?;
        let to = currency_from_db(to_chain, to_token, to_amount, to_decimals as u8)?;
        
        Ok(Quote {
            id,
            from,
            to,
            market_maker_identifier,
            expires_at,
            created_at,
        })
    }
}

impl<'r> FromRow<'r> for Swap {
    fn from_row(row: &'r PgRow) -> DbResult<Self> {
        let id: Uuid = row.try_get("id")?;
        let quote_id: Uuid = row.try_get("quote_id")?;
        let market_maker: String = row.try_get("market_maker")?;
        
        // Get salts as Vec<u8> from database and convert to [u8; 32]
        let user_deposit_salt_vec: Vec<u8> = row.try_get("user_deposit_salt")?;
        let mm_deposit_salt_vec: Vec<u8> = row.try_get("mm_deposit_salt")?;
        
        let mut user_deposit_salt = [0u8; 32];
        let mut mm_deposit_salt = [0u8; 32];
        
        if user_deposit_salt_vec.len() != 32 {
            return Err(super::DbError::InvalidData {
                message: "user_deposit_salt must be exactly 32 bytes".to_string(),
            });
        }
        if mm_deposit_salt_vec.len() != 32 {
            return Err(super::DbError::InvalidData {
                message: "mm_deposit_salt must be exactly 32 bytes".to_string(),
            });
        }
        
        user_deposit_salt.copy_from_slice(&user_deposit_salt_vec);
        mm_deposit_salt.copy_from_slice(&mm_deposit_salt_vec);
        
        let user_destination_address: String = row.try_get("user_destination_address")?;
        let user_refund_address: String = row.try_get("user_refund_address")?;
        let status: String = row.try_get("status")?;
        
        let user_deposit_status = map_optional_deposit_info(
            row.try_get("user_deposit_tx_hash")?,
            row.try_get("user_deposit_amount")?,
            row.try_get("user_deposit_detected_at")?,
        )?;
        
        let mm_deposit_status = map_optional_deposit_info(
            row.try_get("mm_deposit_tx_hash")?,
            row.try_get("mm_deposit_amount")?,
            row.try_get("mm_deposit_detected_at")?,
        )?;
        
        let user_withdrawal_tx: Option<String> = row.try_get("user_withdrawal_tx")?;
        let created_at: DateTime<Utc> = row.try_get("created_at")?;
        let updated_at: DateTime<Utc> = row.try_get("updated_at")?;
        
        Ok(Swap {
            id,
            quote_id,
            market_maker,
            user_deposit_salt,
            mm_deposit_salt,
            user_destination_address,
            user_refund_address,
            status: swap_status_from_db(&status)?,
            user_deposit_status,
            mm_deposit_status,
            user_withdrawal_tx,
            created_at,
            updated_at,
        })
    }
}

pub fn map_optional_deposit_info(
    tx_hash: Option<String>,
    amount: Option<String>,
    detected_at: Option<DateTime<Utc>>,
) -> DbResult<Option<otc_models::DepositInfo>> {
    match (tx_hash, amount, detected_at) {
        (Some(tx_hash), Some(amount), Some(detected_at)) => {
            let amount = u256_from_db(&amount)?;
            Ok(Some(otc_models::DepositInfo {
                tx_hash,
                amount,
                detected_at,
            }))
        }
        _ => Ok(None),
    }
}