use std::str::FromStr;

use alloy::primitives::Address;
use chrono::{DateTime, Utc};
use otc_models::{Quote, Swap, SwapStatus};
use sqlx::postgres::PgRow;
use sqlx::Row;
use uuid::Uuid;

use super::conversions::{
    currency_from_db, latest_refund_from_json, metadata_from_json, mm_deposit_status_from_json,
    realized_swap_from_json, settlement_status_from_json, swap_rates_from_db,
    u256_from_db, user_deposit_status_from_json,
};
use crate::error::{OtcServerError, OtcServerResult};

pub trait FromRow<'r>: Sized {
    fn from_row(row: &'r PgRow) -> OtcServerResult<Self>;
}

impl<'r> FromRow<'r> for Quote {
    fn from_row(row: &'r PgRow) -> OtcServerResult<Self> {
        let id: Uuid = row.try_get("id")?;
        let from_chain: String = row.try_get("from_chain")?;
        let from_token: serde_json::Value = row.try_get("from_token")?;
        let from_decimals: i16 = row.try_get("from_decimals")?;
        let to_chain: String = row.try_get("to_chain")?;
        let to_token: serde_json::Value = row.try_get("to_token")?;
        let to_decimals: i16 = row.try_get("to_decimals")?;

        // Rate parameters
        let liquidity_fee_bps: i64 = row.try_get("liquidity_fee_bps")?;
        let protocol_fee_bps: i64 = row.try_get("protocol_fee_bps")?;
        let network_fee_sats: i64 = row.try_get("network_fee_sats")?;
        let rates = swap_rates_from_db(liquidity_fee_bps, protocol_fee_bps, network_fee_sats);

        // Input bounds
        let min_input_str: String = row.try_get("min_input")?;
        let max_input_str: String = row.try_get("max_input")?;
        let min_input = u256_from_db(&min_input_str)?;
        let max_input = u256_from_db(&max_input_str)?;

        let market_maker_id: Uuid = row.try_get("market_maker_id")?;
        let expires_at: DateTime<Utc> = row.try_get("expires_at")?;
        let created_at: DateTime<Utc> = row.try_get("created_at")?;

        let from_currency = currency_from_db(from_chain, from_token, from_decimals as u8)?;
        let to_currency = currency_from_db(to_chain, to_token, to_decimals as u8)?;

        Ok(Quote {
            id,
            market_maker_id,
            from_currency,
            to_currency,
            rates,
            min_input,
            max_input,
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
        let from_decimals: i16 = row.try_get("from_decimals")?;
        let to_chain: String = row.try_get("to_chain")?;
        let to_token: serde_json::Value = row.try_get("to_token")?;
        let to_decimals: i16 = row.try_get("to_decimals")?;

        // Rate parameters from quote
        let liquidity_fee_bps: i64 = row.try_get("liquidity_fee_bps")?;
        let protocol_fee_bps: i64 = row.try_get("protocol_fee_bps")?;
        let network_fee_sats: i64 = row.try_get("network_fee_sats")?;
        let rates = swap_rates_from_db(liquidity_fee_bps, protocol_fee_bps, network_fee_sats);

        // Input bounds from quote
        let min_input_str: String = row.try_get("min_input")?;
        let max_input_str: String = row.try_get("max_input")?;
        let min_input = u256_from_db(&min_input_str)?;
        let max_input = u256_from_db(&max_input_str)?;

        let quote_market_maker_id: Uuid = row.try_get("quote_market_maker_id")?;
        let expires_at: DateTime<Utc> = row.try_get("expires_at")?;
        let quote_created_at: DateTime<Utc> = row.try_get("quote_created_at")?;

        let from_currency = currency_from_db(from_chain, from_token, from_decimals as u8)?;
        let to_currency = currency_from_db(to_chain, to_token, to_decimals as u8)?;

        let quote = Quote {
            id: quote_id,
            market_maker_id: quote_market_maker_id,
            from_currency,
            to_currency,
            rates,
            min_input,
            max_input,
            expires_at,
            created_at: quote_created_at,
        };

        let user_deposit_address: String = row.try_get("user_deposit_address")?;
        let user_destination_address: String = row.try_get("user_destination_address")?;
        let status: SwapStatus = row.try_get("status")?;
        let metadata_json: serde_json::Value = row.try_get("metadata")?;
        let metadata = metadata_from_json(metadata_json)?;

        // Handle realized swap JSONB field
        let realized_json: Option<serde_json::Value> = row.try_get("realized_swap")?;
        let realized = match realized_json {
            Some(json) => Some(realized_swap_from_json(json)?),
            None => None,
        };

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

        let latest_refund_json: Option<serde_json::Value> = row.try_get("latest_refund")?;
        let latest_refund = match latest_refund_json {
            Some(json) => Some(latest_refund_from_json(json)?),
            None => None,
        };

        let failure_reason: Option<String> = row.try_get("failure_reason")?;
        let failure_at: Option<DateTime<Utc>> = row.try_get("failure_at")?;
        let mm_notified_at: Option<DateTime<Utc>> = row.try_get("mm_notified_at")?;
        let mm_private_key_sent_at: Option<DateTime<Utc>> =
            row.try_get("mm_private_key_sent_at")?;
        let created_at: DateTime<Utc> = row.try_get("created_at")?;
        let updated_at: DateTime<Utc> = row.try_get("updated_at")?;

        let refund_address: String = row.try_get("refund_address")?;

        Ok(Swap {
            id,
            market_maker_id,
            quote,
            metadata,
            realized,
            user_deposit_salt,
            user_deposit_address,
            mm_nonce,
            user_destination_address,
            refund_address,
            status,
            user_deposit_status,
            mm_deposit_status,
            settlement_status,
            latest_refund,
            failure_reason,
            failure_at,
            mm_notified_at,
            mm_private_key_sent_at,
            created_at,
            updated_at,
        })
    }
}
