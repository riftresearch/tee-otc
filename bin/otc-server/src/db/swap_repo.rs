use std::collections::HashMap;

use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use otc_models::{
    ChainType, LatestRefund, Lot, MMDepositStatus, RealizedSwap, SettlementStatus, Swap,
    SwapStatus, UserDepositStatus,
};
use sqlx::postgres::PgPool;
use sqlx::Row;
use uuid::Uuid;

use super::conversions::{
    currency_from_db, latest_refund_to_json, metadata_to_json, mm_deposit_status_to_json,
    realized_swap_to_json, settlement_status_to_json, user_deposit_status_from_json,
    user_deposit_status_to_json,
};
use super::row_mappers::FromRow;
use crate::db::quote_repo::QuoteRepository;
use crate::error::{OtcServerError, OtcServerResult};

pub const SWAP_VOLUME_TOTAL_METRIC: &str = "otc_swap_volume_total";
pub const SWAP_FEES_TOTAL_METRIC: &str = "otc_swap_fees_total";

#[derive(Debug, Clone)]
pub struct PendingMMDepositSwap {
    pub swap_id: Uuid,
    pub quote_id: Uuid,
    pub user_destination_address: String,
    pub mm_nonce: [u8; 16],
    pub expected_lot: Lot,
    pub protocol_fee: U256,
    pub user_deposit_confirmed_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct SettledSwapNotification {
    pub swap_id: Uuid,
    pub user_deposit_salt: [u8; 32],
    pub user_deposit_tx_hash: String,
    pub lot: Lot,
    pub deposit_chain: ChainType,
    pub swap_settlement_timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ForceSwapNotification {
    pub swap_id: Uuid,
    pub market_maker_id: Uuid,
    pub user_deposit_salt: [u8; 32],
    pub user_deposit_tx_hash: String,
    pub lot: Lot,
}

#[derive(Clone)]
pub struct SwapRepository {
    pool: PgPool,
    quote_repo: QuoteRepository,
}

impl SwapRepository {
    #[must_use]
    pub fn new(pool: PgPool, quote_repo: QuoteRepository) -> Self {
        Self { pool, quote_repo }
    }

    pub async fn create(&self, swap: &Swap) -> OtcServerResult<()> {
        let user_deposit_json = match &swap.user_deposit_status {
            Some(status) => Some(user_deposit_status_to_json(status)?),
            None => None,
        };
        let mm_deposit_json = match &swap.mm_deposit_status {
            Some(status) => Some(mm_deposit_status_to_json(status)?),
            None => None,
        };
        let settlement_json = match &swap.settlement_status {
            Some(status) => Some(settlement_status_to_json(status)?),
            None => None,
        };
        let latest_refund_json = match &swap.latest_refund {
            Some(status) => Some(latest_refund_to_json(status)?),
            None => None,
        };
        let metadata_json = metadata_to_json(&swap.metadata)?;

        self.quote_repo.create(&swap.quote).await?;

        sqlx::query(
            r"
            INSERT INTO swaps (
                id, quote_id, market_maker_id,
                metadata,
                user_deposit_salt, user_deposit_address, mm_nonce,
                user_destination_address, refund_address,
                status,
                user_deposit_status, mm_deposit_status, settlement_status,
                latest_refund,
                failure_reason, failure_at,
                mm_notified_at, mm_private_key_sent_at,
                created_at, updated_at
            )
            VALUES (
                $1, $2, $3, $4,
                $5, $6, $7,
                $8, $9,
                $10, $11, $12, $13, $14,
                $15, $16,
                $17, $18, $19, $20
            )
            ",
        )
        .bind(swap.id)
        .bind(swap.quote.id)
        .bind(swap.market_maker_id)
        .bind(metadata_json)
        .bind(&swap.user_deposit_salt[..])
        .bind(&swap.user_deposit_address)
        .bind(&swap.mm_nonce[..])
        .bind(&swap.user_destination_address)
        .bind(swap.refund_address.to_string())
        .bind(swap.status)
        .bind(user_deposit_json)
        .bind(mm_deposit_json)
        .bind(settlement_json)
        .bind(latest_refund_json)
        .bind(&swap.failure_reason)
        .bind(swap.failure_at)
        .bind(swap.mm_notified_at)
        .bind(swap.mm_private_key_sent_at)
        .bind(swap.created_at)
        .bind(swap.updated_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get(&self, id: Uuid) -> OtcServerResult<Swap> {
        let row = sqlx::query(
            r"
            SELECT 
                s.id, s.quote_id, s.market_maker_id,
                s.metadata, s.realized_swap,
                s.user_deposit_salt, s.user_deposit_address, s.mm_nonce,
                s.user_destination_address, s.refund_address,
                s.status,
                s.user_deposit_status, s.mm_deposit_status, s.settlement_status,
                s.latest_refund,
                s.failure_reason, s.failure_at,
                s.mm_notified_at, s.mm_private_key_sent_at,
                s.created_at, s.updated_at,
                -- Quote fields
                q.id as quote_id,
                q.from_chain, q.from_token, q.from_decimals,
                q.to_chain, q.to_token, q.to_decimals,
                q.liquidity_fee_bps, q.protocol_fee_bps, q.network_fee_sats,
                q.min_input, q.max_input,
                q.market_maker_id as quote_market_maker_id, q.expires_at, q.created_at as quote_created_at
            FROM swaps s
            JOIN quotes q ON s.quote_id = q.id
            WHERE s.id = $1
            ",
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await?;

        Swap::from_row(&row)
    }

    pub async fn get_swaps(&self, ids: &[Uuid]) -> OtcServerResult<Vec<Swap>> {
        use std::collections::HashMap;

        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let rows = sqlx::query(
            r"
            SELECT 
                s.id, s.quote_id, s.market_maker_id,
                s.metadata, s.realized_swap,
                s.user_deposit_salt, s.user_deposit_address, s.mm_nonce,
                s.user_destination_address, s.refund_address,
                s.status,
                s.user_deposit_status, s.mm_deposit_status, s.settlement_status,
                s.latest_refund,
                s.failure_reason, s.failure_at,
                s.mm_notified_at, s.mm_private_key_sent_at,
                s.created_at, s.updated_at,
                -- Quote fields
                q.id as quote_id,
                q.from_chain, q.from_token, q.from_decimals,
                q.to_chain, q.to_token, q.to_decimals,
                q.liquidity_fee_bps, q.protocol_fee_bps, q.network_fee_sats,
                q.min_input, q.max_input,
                q.market_maker_id as quote_market_maker_id, q.expires_at, q.created_at as quote_created_at
            FROM swaps s
            JOIN quotes q ON s.quote_id = q.id
            WHERE s.id = ANY($1)
            ",
        )
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        // Build a map of id -> Swap for efficient lookup
        let mut swap_map: HashMap<Uuid, Swap> = HashMap::with_capacity(rows.len());
        for row in rows {
            let swap = Swap::from_row(&row)?;
            swap_map.insert(swap.id, swap);
        }

        // Collect swaps in the same order as the input IDs, returning an error if any ID is missing
        let mut result = Vec::with_capacity(ids.len());
        for &id in ids {
            match swap_map.remove(&id) {
                Some(swap) => result.push(swap),
                None => {
                    return Err(OtcServerError::InvalidData {
                        message: format!("Swap with id {} not found", id),
                    })
                }
            }
        }

        Ok(result)
    }

    pub async fn update_status(&self, id: Uuid, status: SwapStatus) -> OtcServerResult<()> {
        let now = utc::now();

        sqlx::query(
            r"
            UPDATE swaps
            SET status = $2, updated_at = $3
            WHERE id = $1
            ",
        )
        .bind(id)
        .bind(status)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn update_user_deposit(
        &self,
        id: Uuid,
        status: &UserDepositStatus,
    ) -> OtcServerResult<()> {
        let status_json = user_deposit_status_to_json(status)?;
        let now = utc::now();

        sqlx::query(
            r"
            UPDATE swaps
            SET 
                user_deposit_status = $2,
                updated_at = $3
            WHERE id = $1
            ",
        )
        .bind(id)
        .bind(status_json)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn update_mm_deposit(
        &self,
        id: Uuid,
        status: &MMDepositStatus,
    ) -> OtcServerResult<()> {
        let status_json = mm_deposit_status_to_json(status)?;
        let now = utc::now();

        sqlx::query(
            r"
            UPDATE swaps
            SET 
                mm_deposit_status = $2,
                updated_at = $3
            WHERE id = $1
            ",
        )
        .bind(id)
        .bind(status_json)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn update_latest_refund(
        &self,
        id: Uuid,
        refund: &LatestRefund,
    ) -> OtcServerResult<()> {
        let refund_json = latest_refund_to_json(refund)?;
        let now = utc::now();

        sqlx::query(
            r#"
            UPDATE swaps
            SET
                latest_refund = $2,
                updated_at = $3
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(refund_json)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn update_settlement(
        &self,
        id: Uuid,
        status: &SettlementStatus,
    ) -> OtcServerResult<()> {
        let status_json = settlement_status_to_json(status)?;
        let now = utc::now();

        sqlx::query(
            r"
            UPDATE swaps
            SET 
                settlement_status = $2,
                updated_at = $3
            WHERE id = $1
            ",
        )
        .bind(id)
        .bind(status_json)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_active_swaps(&self) -> OtcServerResult<Vec<Swap>> {
        let cutoff_time = utc::now() - chrono::Duration::hours(24);

        let rows = sqlx::query(
            r"
            SELECT 
                s.id, s.quote_id, s.market_maker_id,
                s.metadata, s.realized_swap,
                s.user_deposit_salt, s.user_deposit_address, s.mm_nonce,
                s.user_destination_address, s.refund_address,
                s.status,
                s.user_deposit_status, s.mm_deposit_status, s.settlement_status,
                s.latest_refund,
                s.failure_reason, s.failure_at,
                s.mm_notified_at, s.mm_private_key_sent_at,
                s.created_at, s.updated_at,
                -- Quote fields
                q.id as quote_id,
                q.from_chain, q.from_token, q.from_decimals,
                q.to_chain, q.to_token, q.to_decimals,
                q.liquidity_fee_bps, q.protocol_fee_bps, q.network_fee_sats,
                q.min_input, q.max_input,
                q.market_maker_id as quote_market_maker_id, q.expires_at, q.created_at as quote_created_at
            FROM swaps s
            JOIN quotes q ON s.quote_id = q.id
            WHERE s.status NOT IN ('settled', 'failed', 'refunding_user')
              AND NOT (s.updated_at < $1)
            ORDER BY s.created_at DESC
            ",
        )
        .bind(cutoff_time)
        .fetch_all(&self.pool)
        .await?;

        let mut swaps = Vec::new();
        for row in rows {
            swaps.push(Swap::from_row(&row)?);
        }

        Ok(swaps)
    }

    pub async fn get_waiting_mm_deposit_swaps(
        &self,
        market_maker_id: Uuid,
    ) -> OtcServerResult<Vec<PendingMMDepositSwap>> {
        let rows = sqlx::query(
            r#"
            SELECT
                s.id AS swap_id,
                s.quote_id,
                s.user_destination_address,
                s.mm_nonce,
                s.user_deposit_status,
                s.realized_swap,
                q.to_chain,
                q.to_token,
                q.to_decimals
            FROM swaps s
            JOIN quotes q ON s.quote_id = q.id
            WHERE s.market_maker_id = $1
              AND s.status = $2
              AND s.user_deposit_status IS NOT NULL
              AND s.realized_swap IS NOT NULL
            "#,
        )
        .bind(market_maker_id)
        .bind(SwapStatus::WaitingMMDepositInitiated)
        .fetch_all(&self.pool)
        .await?;

        let mut swaps = Vec::with_capacity(rows.len());
        for row in rows {
            let mm_nonce_vec: Vec<u8> = row.try_get("mm_nonce")?;
            if mm_nonce_vec.len() != 16 {
                return Err(OtcServerError::InvalidData {
                    message: "mm_nonce must be exactly 16 bytes".to_string(),
                });
            }
            let mut mm_nonce = [0u8; 16];
            mm_nonce.copy_from_slice(&mm_nonce_vec);

            // Get realized swap to extract mm_output
            let realized_json: serde_json::Value = row.try_get("realized_swap")?;
            let realized: RealizedSwap = serde_json::from_value(realized_json)
                .map_err(|e| OtcServerError::InvalidData {
                    message: format!("Failed to deserialize realized_swap: {e}"),
                })?;

            // Build expected_lot from to_currency and realized.mm_output
            let to_currency = currency_from_db(
                row.try_get::<String, _>("to_chain")?,
                row.try_get::<serde_json::Value, _>("to_token")?,
                row.try_get::<i16, _>("to_decimals")? as u8,
            )?;

            let expected_lot = Lot {
                currency: to_currency,
                amount: realized.mm_output,
            };

            let deposit_status_json: serde_json::Value = row.try_get("user_deposit_status")?;
            let deposit_status = user_deposit_status_from_json(deposit_status_json)?;
            let user_deposit_confirmed_at =
                deposit_status
                    .confirmed_at
                    .ok_or_else(|| OtcServerError::InvalidData {
                        message: "user_deposit_status.confirmed_at missing".to_string(),
                    })?;

            swaps.push(PendingMMDepositSwap {
                swap_id: row.try_get("swap_id")?,
                quote_id: row.try_get("quote_id")?,
                user_destination_address: row.try_get("user_destination_address")?,
                mm_nonce,
                expected_lot,
                protocol_fee: realized.protocol_fee,
                user_deposit_confirmed_at,
            });
        }

        Ok(swaps)
    }

    pub async fn get_settled_swaps_for_market_maker(
        &self,
        market_maker_id: Uuid,
        last_seen_settlement_timestamp: Option<DateTime<Utc>>,
    ) -> OtcServerResult<Vec<SettledSwapNotification>> {
        let rows = sqlx::query(
            r#"
            SELECT
                s.id AS swap_id,
                s.user_deposit_salt,
                s.user_deposit_status,
                s.updated_at,
                q.from_chain,
                q.from_token,
                q.from_decimals
            FROM swaps s
            JOIN quotes q ON s.quote_id = q.id
            WHERE s.market_maker_id = $1
              AND s.status = $2
              AND s.user_deposit_status IS NOT NULL
              AND s.updated_at >= $3
            "#,
        )
        .bind(market_maker_id)
        .bind(SwapStatus::Settled)
        .bind(last_seen_settlement_timestamp.unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap()))
        .fetch_all(&self.pool)
        .await?;

        let mut swaps = Vec::with_capacity(rows.len());
        for row in rows {
            let salt_vec: Vec<u8> = row.try_get("user_deposit_salt")?;
            if salt_vec.len() != 32 {
                return Err(OtcServerError::InvalidData {
                    message: "user_deposit_salt must be exactly 32 bytes".to_string(),
                });
            }
            let mut user_deposit_salt = [0u8; 32];
            user_deposit_salt.copy_from_slice(&salt_vec);

            let from_chain: String = row.try_get("from_chain")?;
            let deposit_chain = ChainType::from_db_string(&from_chain).ok_or(OtcServerError::InvalidData {
                message: format!("Invalid chain type: {from_chain}"),
            })?;

            let deposit_status_json: serde_json::Value = row.try_get("user_deposit_status")?;
            let deposit_status = user_deposit_status_from_json(deposit_status_json)?;

            let swap_settlement_timestamp: DateTime<Utc> = row.try_get("updated_at")?;

            // Build the lot from currency and actual deposit amount
            let from_currency = currency_from_db(
                row.try_get::<String, _>("from_chain")?,
                row.try_get::<serde_json::Value, _>("from_token")?,
                row.try_get::<i16, _>("from_decimals")? as u8,
            )?;
            let actual_deposit_lot = Lot {
                currency: from_currency,
                amount: deposit_status.amount,
            };

            swaps.push(SettledSwapNotification {
                swap_id: row.try_get("swap_id")?,
                user_deposit_salt,
                user_deposit_tx_hash: deposit_status.tx_hash,
                lot: actual_deposit_lot,
                deposit_chain,
                swap_settlement_timestamp,
            });
        }

        Ok(swaps)
    }

    pub async fn get_swaps_waiting_mm_for_force_complete(
        &self,
    ) -> OtcServerResult<Vec<ForceSwapNotification>> {
        let rows = sqlx::query(
            r#"
            SELECT
                s.id AS swap_id,
                s.market_maker_id,
                s.user_deposit_salt,
                s.user_deposit_status,
                q.from_chain,
                q.from_token,
                q.from_decimals
            FROM swaps s
            JOIN quotes q ON s.quote_id = q.id
            WHERE (s.status = $1 OR s.status = $2)
              AND s.user_deposit_status IS NOT NULL
            "#,
        )
        .bind(SwapStatus::WaitingMMDepositInitiated)
        .bind(SwapStatus::WaitingMMDepositConfirmed)
        .fetch_all(&self.pool)
        .await?;

        let mut swaps = Vec::with_capacity(rows.len());
        for row in rows {
            let salt_vec: Vec<u8> = row.try_get("user_deposit_salt")?;
            if salt_vec.len() != 32 {
                return Err(OtcServerError::InvalidData {
                    message: "user_deposit_salt must be exactly 32 bytes".to_string(),
                });
            }
            let mut user_deposit_salt = [0u8; 32];
            user_deposit_salt.copy_from_slice(&salt_vec);

            let user_deposit_status_json: serde_json::Value = row.try_get("user_deposit_status")?;
            let user_deposit_status = user_deposit_status_from_json(user_deposit_status_json)?;

            let from_currency = currency_from_db(
                row.try_get::<String, _>("from_chain")?,
                row.try_get::<serde_json::Value, _>("from_token")?,
                row.try_get::<i16, _>("from_decimals")? as u8,
            )?;
            let lot = Lot {
                currency: from_currency,
                amount: user_deposit_status.amount,
            };

            swaps.push(ForceSwapNotification {
                swap_id: row.try_get("swap_id")?,
                market_maker_id: row.try_get("market_maker_id")?,
                user_deposit_salt,
                user_deposit_tx_hash: user_deposit_status.tx_hash,
                lot,
            });
        }

        Ok(swaps)
    }

    /// Update entire swap record
    pub async fn update(&self, swap: &Swap, expected_status: Option<SwapStatus>) -> OtcServerResult<()> {
        let user_deposit_json = swap
            .user_deposit_status
            .as_ref()
            .map(user_deposit_status_to_json)
            .transpose()?;
        let mm_deposit_json = swap
            .mm_deposit_status
            .as_ref()
            .map(mm_deposit_status_to_json)
            .transpose()?;
        let settlement_json = swap
            .settlement_status
            .as_ref()
            .map(settlement_status_to_json)
            .transpose()?;
        let latest_refund_json = swap
            .latest_refund
            .as_ref()
            .map(latest_refund_to_json)
            .transpose()?;
        let realized_json = swap
            .realized
            .as_ref()
            .map(realized_swap_to_json)
            .transpose()?;
        
        let result = match expected_status {
            Some(expected) => {
                sqlx::query(
                    r"
                    UPDATE swaps
                    SET 
                        status = $2,
                        user_deposit_status = $3,
                        mm_deposit_status = $4,
                        settlement_status = $5,
                        latest_refund = $6,
                        realized_swap = $7,
                        failure_reason = $8,
                        failure_at = $9,
                        mm_notified_at = $10,
                        mm_private_key_sent_at = $11,
                        updated_at = $12
                    WHERE id = $1
                      AND status = $13
                    ",
                )
                .bind(swap.id)
                .bind(swap.status)
                .bind(user_deposit_json)
                .bind(mm_deposit_json)
                .bind(settlement_json)
                .bind(latest_refund_json)
                .bind(realized_json)
                .bind(&swap.failure_reason)
                .bind(swap.failure_at)
                .bind(swap.mm_notified_at)
                .bind(swap.mm_private_key_sent_at)
                .bind(swap.updated_at)
                .bind(expected)
                .execute(&self.pool)
                .await?
            }
            None => {
                sqlx::query(
                    r"
                    UPDATE swaps
                    SET 
                        status = $2,
                        user_deposit_status = $3,
                        mm_deposit_status = $4,
                        settlement_status = $5,
                        latest_refund = $6,
                        realized_swap = $7,
                        failure_reason = $8,
                        failure_at = $9,
                        mm_notified_at = $10,
                        mm_private_key_sent_at = $11,
                        updated_at = $12
                    WHERE id = $1
                    ",
                )
                .bind(swap.id)
                .bind(swap.status)
                .bind(user_deposit_json)
                .bind(mm_deposit_json)
                .bind(settlement_json)
                .bind(latest_refund_json)
                .bind(realized_json)
                .bind(&swap.failure_reason)
                .bind(swap.failure_at)
                .bind(swap.mm_notified_at)
                .bind(swap.mm_private_key_sent_at)
                .bind(swap.updated_at)
                .execute(&self.pool)
                .await?
            }
        };

        // Check if the update actually modified a row
        if result.rows_affected() == 0 {
            return Err(match expected_status {
                Some(expected) => OtcServerError::InvalidState {
                    message: format!(
                        "Update failed: swap {} status was not {:?} (possible concurrent modification)",
                        swap.id, expected
                    ),
                },
                None => OtcServerError::NotFound,
            });
        }

        Ok(())
    }

    pub async fn get_swaps_by_market_maker(&self, mm_id: Uuid) -> OtcServerResult<Vec<Swap>> {
        let rows = sqlx::query(
            r"
            SELECT 
                s.id, s.quote_id, s.market_maker_id,
                s.metadata, s.realized_swap,
                s.user_deposit_salt, s.user_deposit_address, s.mm_nonce,
                s.user_destination_address, s.refund_address,
                s.status,
                s.user_deposit_status, s.mm_deposit_status, s.settlement_status,
                s.latest_refund,
                s.failure_reason, s.failure_at,
                s.mm_notified_at, s.mm_private_key_sent_at,
                s.created_at, s.updated_at,
                -- Quote fields
                q.id as quote_id,
                q.from_chain, q.from_token, q.from_decimals,
                q.to_chain, q.to_token, q.to_decimals,
                q.liquidity_fee_bps, q.protocol_fee_bps, q.network_fee_sats,
                q.min_input, q.max_input,
                q.market_maker_id as quote_market_maker_id, q.expires_at, q.created_at as quote_created_at
            FROM swaps s
            JOIN quotes q ON s.quote_id = q.id
            WHERE s.market_maker_id = $1
            ORDER BY s.created_at DESC
            ",
        )
        .bind(mm_id)
        .fetch_all(&self.pool)
        .await?;

        let mut swaps = Vec::new();
        for row in rows {
            swaps.push(Swap::from_row(&row)?);
        }

        Ok(swaps)
    }

    /// Alias for `get_active_swaps` for consistency with monitoring service
    pub async fn get_active(&self) -> OtcServerResult<Vec<Swap>> {
        self.get_active_swaps().await
    }

    /// Update swap when user deposit is detected
    pub async fn user_deposit_detected(
        &self,
        swap_id: Uuid,
        deposit_status: UserDepositStatus,
        realized: Option<RealizedSwap>,
    ) -> OtcServerResult<()> {
        // First get the swap
        let mut swap = self.get(swap_id).await?;

        // Save the original status for optimistic concurrency check
        let original_status = swap.status;

        // Apply the state transition
        swap.user_deposit_detected(
            deposit_status.tx_hash.clone(),
            deposit_status.amount,
            deposit_status.confirmations,
            realized,
        )
        .map_err(|e| OtcServerError::InvalidState {
            message: format!("State transition failed: {e}"),
        })?;

        // Update the database, allowing update from either WaitingUserDepositInitiated 
        // or WaitingUserDepositConfirmed (for transaction replacement scenarios)
        let expected_status = match original_status {
            SwapStatus::WaitingUserDepositInitiated | SwapStatus::WaitingUserDepositConfirmed => {
                Some(original_status)
            }
            _ => {
                return Err(OtcServerError::InvalidState {
                    message: format!(
                        "Cannot detect user deposit from status {:?}",
                        original_status
                    ),
                });
            }
        };
        self.update(&swap, expected_status).await?;
        Ok(())
    }

    /// Atomically transition a swap to RefundingUser status from an expected status.
    /// This prevents race conditions where the swap could be progressing through states
    /// while a refund is being processed.
    pub async fn mark_swap_as_refunding_user(&self, swap_id: Uuid, expected_status: SwapStatus) -> OtcServerResult<()> {
        let mut swap = self.get(swap_id).await?;
        swap.status = SwapStatus::RefundingUser;
        // Atomic check will prevent concurrent updates from other threads
        self.update(&swap, Some(expected_status)).await?;
        Ok(())
    }

    /// Update multiple swaps when MM deposits are detected in a batch
    pub async fn batch_mm_deposit_detected(
        &self,
        deposit_status_map: HashMap<Uuid, MMDepositStatus>,
    ) -> OtcServerResult<()> {
        if deposit_status_map.is_empty() {
            return Ok(());
        }

        let swap_ids: Vec<Uuid> = deposit_status_map.keys().copied().collect();

        // Get all swaps
        let mut swaps = self.get_swaps(&swap_ids).await?;

        // Store original status for optimistic locking
        let original_statuses: HashMap<Uuid, SwapStatus> = 
            swaps.iter().map(|s| (s.id, s.status)).collect();

        // Apply state transitions to each swap
        for swap in &mut swaps {
            let deposit_status =
                deposit_status_map
                    .get(&swap.id)
                    .ok_or_else(|| OtcServerError::InvalidData {
                        message: format!("Missing deposit status for swap {}", swap.id),
                    })?;

            swap.mm_deposit_detected(
                deposit_status.tx_hash.clone(),
                deposit_status.amount,
                deposit_status.confirmations,
            )
            .map_err(|e| OtcServerError::InvalidState {
                message: format!("State transition failed for swap {}: {}", swap.id, e),
            })?;
        }

        // Update all swaps in a transaction for atomicity
        let mut tx = self.pool.begin().await?;

        for swap in &swaps {
            let user_deposit_json = swap
                .user_deposit_status
                .as_ref()
                .map(user_deposit_status_to_json)
                .transpose()?;
            let mm_deposit_json = swap
                .mm_deposit_status
                .as_ref()
                .map(mm_deposit_status_to_json)
                .transpose()?;
            let settlement_json = swap
                .settlement_status
                .as_ref()
                .map(settlement_status_to_json)
                .transpose()?;
            let latest_refund_json = swap
                .latest_refund
                .as_ref()
                .map(latest_refund_to_json)
                .transpose()?;

            let original_status = original_statuses.get(&swap.id).ok_or_else(|| {
                OtcServerError::InvalidState {
                    message: format!("Missing original status for swap {}", swap.id),
                }
            })?;

            let result = sqlx::query(
                r"
                UPDATE swaps
                SET 
                    status = $2,
                    user_deposit_status = $3,
                    mm_deposit_status = $4,
                    settlement_status = $5,
                    latest_refund = $6,
                    failure_reason = $7,
                    failure_at = $8,
                    mm_notified_at = $9,
                    mm_private_key_sent_at = $10,
                    updated_at = $11
                WHERE id = $1
                  AND status = $12
                ",
            )
            .bind(swap.id)
            .bind(swap.status)
            .bind(user_deposit_json)
            .bind(mm_deposit_json)
            .bind(settlement_json)
            .bind(latest_refund_json)
            .bind(&swap.failure_reason)
            .bind(swap.failure_at)
            .bind(swap.mm_notified_at)
            .bind(swap.mm_private_key_sent_at)
            .bind(swap.updated_at)
            .bind(original_status)
            .execute(&mut *tx)
            .await?;

            // Check if update succeeded
            if result.rows_affected() == 0 {
                return Err(OtcServerError::InvalidState {
                    message: format!(
                        "Batch update failed: swap {} status was not {:?} (possible concurrent modification)",
                        swap.id, original_status
                    ),
                });
            }
        }

        tx.commit().await?;

        Ok(())
    }

    /// Update MM deposit confirmations for multiple swaps in a batch
    pub async fn batch_update_mm_confirmations(
        &self,
        swap_ids: &[Uuid],
        confirmations: u32,
    ) -> OtcServerResult<()> {
        if swap_ids.is_empty() {
            return Ok(());
        }

        // Get all swaps
        let mut swaps = self.get_swaps(swap_ids).await?;

        // Apply state transitions to each swap
        for swap in &mut swaps {
            swap.update_confirmations(None, Some(confirmations as u64))
                .map_err(|e| OtcServerError::InvalidState {
                    message: format!("State transition failed for swap {}: {}", swap.id, e),
                })?;
        }

        // Update all swaps in a transaction for atomicity
        let mut tx = self.pool.begin().await?;

        for swap in &swaps {
            let user_deposit_json = swap
                .user_deposit_status
                .as_ref()
                .map(user_deposit_status_to_json)
                .transpose()?;
            let mm_deposit_json = swap
                .mm_deposit_status
                .as_ref()
                .map(mm_deposit_status_to_json)
                .transpose()?;
            let settlement_json = swap
                .settlement_status
                .as_ref()
                .map(settlement_status_to_json)
                .transpose()?;
            let latest_refund_json = swap
                .latest_refund
                .as_ref()
                .map(latest_refund_to_json)
                .transpose()?;

            sqlx::query(
                r"
                UPDATE swaps
                SET 
                    status = $2,
                    user_deposit_status = $3,
                    mm_deposit_status = $4,
                    settlement_status = $5,
                    latest_refund = $6,
                    failure_reason = $7,
                    failure_at = $8,
                    mm_notified_at = $9,
                    mm_private_key_sent_at = $10,
                    updated_at = $11
                WHERE id = $1
                ",
            )
            .bind(swap.id)
            .bind(swap.status)
            .bind(user_deposit_json)
            .bind(mm_deposit_json)
            .bind(settlement_json)
            .bind(latest_refund_json)
            .bind(&swap.failure_reason)
            .bind(swap.failure_at)
            .bind(swap.mm_notified_at)
            .bind(swap.mm_private_key_sent_at)
            .bind(swap.updated_at)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        Ok(())
    }

    /// Mark multiple swaps as MM deposit confirmed (settled) in a batch
    pub async fn batch_mm_deposit_confirmed(&self, swap_ids: &[Uuid]) -> OtcServerResult<DateTime<Utc>> {
        if swap_ids.is_empty() {
            return Ok(utc::now());
        }

        // Get all swaps
        let mut swaps = self.get_swaps(swap_ids).await?;

        // Store original status for optimistic locking
        let original_statuses: HashMap<Uuid, SwapStatus> = 
            swaps.iter().map(|s| (s.id, s.status)).collect();

        let swap_settlement_timestamp = utc::now();

        // Apply state transitions to each swap
        for swap in &mut swaps {
            swap.mm_deposit_confirmed(&swap_settlement_timestamp)
                .map_err(|e| OtcServerError::InvalidState {
                    message: format!("State transition failed for swap {}: {}", swap.id, e),
                })?;
        }

        // Update all swaps in a transaction for atomicity
        let mut tx = self.pool.begin().await?;

        for swap in &swaps {
            let user_deposit_json = swap
                .user_deposit_status
                .as_ref()
                .map(user_deposit_status_to_json)
                .transpose()?;
            let mm_deposit_json = swap
                .mm_deposit_status
                .as_ref()
                .map(mm_deposit_status_to_json)
                .transpose()?;
            let settlement_json = swap
                .settlement_status
                .as_ref()
                .map(settlement_status_to_json)
                .transpose()?;
            let latest_refund_json = swap
                .latest_refund
                .as_ref()
                .map(latest_refund_to_json)
                .transpose()?;

            let original_status = original_statuses.get(&swap.id).ok_or_else(|| {
                OtcServerError::InvalidState {
                    message: format!("Missing original status for swap {}", swap.id),
                }
            })?;

            let result = sqlx::query(
                r"
                UPDATE swaps
                SET 
                    status = $2,
                    user_deposit_status = $3,
                    mm_deposit_status = $4,
                    settlement_status = $5,
                    latest_refund = $6,
                    failure_reason = $7,
                    failure_at = $8,
                    mm_notified_at = $9,
                    mm_private_key_sent_at = $10,
                    updated_at = $11
                WHERE id = $1
                  AND status = $12
                ",
            )
            .bind(swap.id)
            .bind(swap.status)
            .bind(user_deposit_json)
            .bind(mm_deposit_json)
            .bind(settlement_json)
            .bind(latest_refund_json)
            .bind(&swap.failure_reason)
            .bind(swap.failure_at)
            .bind(swap.mm_notified_at)
            .bind(swap.mm_private_key_sent_at)
            .bind(swap.updated_at)
            .bind(original_status)
            .execute(&mut *tx)
            .await?;

            // Check if update succeeded
            if result.rows_affected() == 0 {
                return Err(OtcServerError::InvalidState {
                    message: format!(
                        "Batch settlement failed: swap {} status was not {:?} (possible concurrent modification)",
                        swap.id, original_status
                    ),
                });
            }
        }

        tx.commit().await?;

        Ok(swap_settlement_timestamp)
    }

    /// Update user deposit confirmations
    pub async fn update_user_confirmations(
        &self,
        swap_id: Uuid,
        confirmations: u32,
    ) -> OtcServerResult<()> {
        let mut swap = self.get(swap_id).await?;
        swap.update_confirmations(Some(confirmations as u64), None)
            .map_err(|e| OtcServerError::InvalidState {
                message: format!("State transition failed: {e}"),
            })?;
        self.update(&swap, None).await?;
        Ok(())
    }

    /// Update swap when user deposit is confirmed
    pub async fn user_deposit_confirmed(&self, swap_id: Uuid) -> OtcServerResult<DateTime<Utc>> {
        let mut swap = self.get(swap_id).await?;
        swap.user_deposit_confirmed()
            .map_err(|e| OtcServerError::InvalidState {
                message: format!("State transition failed: {e}"),
            })?;
        // only transition to WaitingMMDepositInitiated if the swap is in WaitingUserDepositConfirmed (prevent refund race condition @ pg level)
        self.update(&swap, Some(SwapStatus::WaitingUserDepositConfirmed)).await?;
        Ok(swap
            .user_deposit_status
            .as_ref()
            .unwrap()
            .confirmed_at
            .unwrap())
    }

    /// Mark private key as sent to MM
    pub async fn mark_private_key_sent(&self, swap_id: Uuid) -> OtcServerResult<()> {
        let mut swap = self.get(swap_id).await?;
        swap.mark_private_key_sent()
            .map_err(|e| OtcServerError::InvalidState {
                message: format!("State transition failed: {e}"),
            })?;
        self.update(&swap, None).await?;
        Ok(())
    }

    /// Mark swap as failed
    pub async fn mark_failed(&self, swap_id: Uuid, reason: &str) -> OtcServerResult<()> {
        let mut swap = self.get(swap_id).await?;
        swap.mark_failed(reason.to_string())
            .map_err(|e| OtcServerError::InvalidState {
                message: format!("State transition failed: {e}"),
            })?;
        self.update(&swap, None).await?;
        Ok(())
    }

    /// Initiate user refund
    pub async fn initiate_user_refund(&self, swap_id: Uuid, reason: &str) -> OtcServerResult<()> {
        let mut swap = self.get(swap_id).await?;
        swap.initiate_user_refund(reason.to_string()).map_err(|e| {
            OtcServerError::InvalidState {
                message: format!("State transition failed: {e}"),
            }
        })?;
        self.update(&swap, None).await?;
        Ok(())
    }

    /// Query settled volume totals by market from aggregate table
    pub async fn get_settled_volume_totals(&self) -> OtcServerResult<Vec<(String, i64)>> {
        let rows = sqlx::query("SELECT market, total FROM settled_volume_totals_market")
            .fetch_all(&self.pool)
            .await?;

        Ok(rows
            .iter()
            .map(|row| {
                let market: String = row.get("market");
                let total: i64 = row.get("total");
                (market, total)
            })
            .collect())
    }

    /// Query settled protocol fee totals by market from aggregate table
    pub async fn get_settled_fee_totals(&self) -> OtcServerResult<Vec<(String, i64)>> {
        let rows = sqlx::query("SELECT market, total FROM settled_protocol_fee_totals_market")
            .fetch_all(&self.pool)
            .await?;

        Ok(rows
            .iter()
            .map(|row| {
                let market: String = row.get("market");
                let total: i64 = row.get("total");
                (market, total)
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use crate::db::Database;
    use alloy::primitives::U256;
    use chrono::Duration;
    use otc_models::{
        ChainType, Currency, LatestRefund, MMDepositStatus, Metadata, Quote, SettlementStatus,
        Swap, SwapRates, SwapStatus, TokenIdentifier, UserDepositStatus,
    };
    use uuid::Uuid;

    fn create_test_quote() -> Quote {
        Quote {
            id: Uuid::new_v4(),
            market_maker_id: Uuid::new_v4(),
            from_currency: Currency {
                chain: ChainType::Bitcoin,
                token: TokenIdentifier::Native,
                decimals: 8,
            },
            to_currency: Currency {
                chain: ChainType::Ethereum,
                token: TokenIdentifier::Native,
                decimals: 18,
            },
            rates: SwapRates::new(13, 10, 1000),
            min_input: U256::from(10_000u64),
            max_input: U256::from(100_000_000u64),
            expires_at: utc::now() + Duration::hours(1),
            created_at: utc::now(),
        }
    }

    fn create_test_swap(quote: Quote) -> Swap {
        let mut user_salt = [0u8; 32];
        let mut mm_nonce = [0u8; 16];
        getrandom::getrandom(&mut user_salt).unwrap();
        getrandom::getrandom(&mut mm_nonce).unwrap();

        Swap {
            id: Uuid::new_v4(),
            market_maker_id: quote.market_maker_id,
            quote,
            metadata: Metadata {
                affiliate: Some("affiliate_123".to_string()),
                start_asset: Some("btc".to_string()),
            },
            realized: None,
            user_deposit_salt: user_salt,
            user_deposit_address: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh".to_string(),
            mm_nonce,
            user_destination_address: "0x1234567890123456789012345678901234567890".to_string(),
            refund_address: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx124".to_string(),
            status: SwapStatus::WaitingUserDepositInitiated,
            user_deposit_status: None,
            mm_deposit_status: None,
            settlement_status: None,
            latest_refund: None,
            failure_reason: None,
            failure_at: None,
            mm_notified_at: None,
            mm_private_key_sent_at: None,
            created_at: utc::now(),
            updated_at: utc::now(),
        }
    }

    #[sqlx::test]
    async fn test_swap_round_trip(pool: sqlx::PgPool) -> sqlx::Result<()> {
        // Database will auto-initialize with schema
        let db = Database::from_pool(pool.clone()).await.unwrap();
        let swap_repo = db.swaps();

        // First create a quote that the swap will reference
        let quote = create_test_quote();
        let original_swap = create_test_swap(quote.clone());

        // Store the swap
        swap_repo.create(&original_swap).await.unwrap();

        // Retrieve the swap
        let retrieved_swap = swap_repo.get(original_swap.id).await.unwrap();

        // Validate data
        assert_eq!(retrieved_swap.id, original_swap.id);
        assert_eq!(retrieved_swap.quote.id, original_swap.quote.id);
        assert_eq!(
            retrieved_swap.market_maker_id,
            original_swap.market_maker_id
        );
        assert_eq!(
            retrieved_swap.user_deposit_salt,
            original_swap.user_deposit_salt
        );
        assert_eq!(
            retrieved_swap.user_deposit_address,
            original_swap.user_deposit_address
        );
        assert_eq!(
            retrieved_swap.metadata.affiliate,
            original_swap.metadata.affiliate
        );
        assert_eq!(
            retrieved_swap.metadata.start_asset,
            original_swap.metadata.start_asset
        );
        assert_eq!(retrieved_swap.mm_nonce, original_swap.mm_nonce);
        assert_eq!(
            retrieved_swap.user_destination_address,
            original_swap.user_destination_address
        );
        assert_eq!(
            retrieved_swap.refund_address,
            original_swap.refund_address
        );
        assert_eq!(retrieved_swap.status, original_swap.status);

        Ok(())
    }

    #[sqlx::test]
    async fn test_swap_with_deposits(pool: sqlx::PgPool) -> sqlx::Result<()> {
        // Database will auto-initialize with schema
        let db = Database::from_pool(pool.clone()).await.unwrap();
        let swap_repo = db.swaps();

        // Create quote using helper
        let quote = create_test_quote();
        let now = utc::now();

        // Create swap with deposit info
        let mut original_swap = create_test_swap(quote.clone());
        original_swap.status = SwapStatus::WaitingMMDepositConfirmed;
        original_swap.user_deposit_status = Some(UserDepositStatus {
            tx_hash: "7d865e959b2466918c9863afca942d0fb89d7c9ac0c99bafc3749504ded97730"
                .to_string(),
            amount: U256::from(2000000u64),
            deposit_detected_at: now,
            confirmed_at: None,
            confirmations: 6,
            last_checked: now,
        });
        original_swap.mm_deposit_status = Some(MMDepositStatus {
            tx_hash: "0x88df016429689c079f3b2f6ad39fa052532c56b6a39df8e3c84c03b8346cfc63"
                .to_string(),
            amount: U256::from(1000000000000000000u64),
            deposit_detected_at: now + Duration::minutes(5),
            confirmations: 12,
            last_checked: now + Duration::minutes(5),
        });
        original_swap.updated_at = now + Duration::minutes(5);

        // Store and retrieve
        swap_repo.create(&original_swap).await.unwrap();
        let retrieved_swap = swap_repo.get(original_swap.id).await.unwrap();

        // Validate deposit info
        assert!(retrieved_swap.user_deposit_status.is_some());
        let user_deposit = retrieved_swap.user_deposit_status.unwrap();
        let original_user_deposit = original_swap.user_deposit_status.unwrap();
        assert_eq!(user_deposit.tx_hash, original_user_deposit.tx_hash);
        assert_eq!(user_deposit.amount, original_user_deposit.amount);
        assert!(
            (user_deposit.deposit_detected_at - original_user_deposit.deposit_detected_at)
                .num_seconds()
                .abs()
                < 1
        );

        assert!(retrieved_swap.mm_deposit_status.is_some());
        let mm_deposit = retrieved_swap.mm_deposit_status.unwrap();
        let original_mm_deposit = original_swap.mm_deposit_status.unwrap();
        assert_eq!(mm_deposit.tx_hash, original_mm_deposit.tx_hash);
        assert_eq!(mm_deposit.amount, original_mm_deposit.amount);
        assert!(
            (mm_deposit.deposit_detected_at - original_mm_deposit.deposit_detected_at)
                .num_seconds()
                .abs()
                < 1
        );

        Ok(())
    }

    #[sqlx::test]
    async fn test_swap_status_updates(pool: sqlx::PgPool) -> sqlx::Result<()> {
        // Database will auto-initialize with schema
        let db = Database::from_pool(pool.clone()).await.unwrap();
        let swap_repo = db.swaps();

        // Create quote and swap using helpers
        let quote = create_test_quote();
        let swap = create_test_swap(quote);

        swap_repo.create(&swap).await.unwrap();

        // Update status
        swap_repo
            .update_status(swap.id, SwapStatus::WaitingUserDepositConfirmed)
            .await
            .unwrap();

        let updated = swap_repo.get(swap.id).await.unwrap();
        assert_eq!(updated.status, SwapStatus::WaitingUserDepositConfirmed);

        // Update user deposit
        let deposit_amount = U256::from(1000000u64);
        let user_deposit = UserDepositStatus {
            tx_hash: "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890".to_string(),
            amount: deposit_amount,
            deposit_detected_at: utc::now(),
            confirmed_at: None,
            confirmations: 0,
            last_checked: utc::now(),
        };
        swap_repo
            .update_user_deposit(swap.id, &user_deposit)
            .await
            .unwrap();

        let updated = swap_repo.get(swap.id).await.unwrap();
        assert!(updated.user_deposit_status.is_some());
        let deposit = updated.user_deposit_status.unwrap();
        assert_eq!(deposit.amount, deposit_amount);

        // Update settlement status
        let settlement_status = SettlementStatus {
            tx_hash: "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                .to_string(),
            broadcast_at: utc::now(),
            confirmations: 0,
            completed_at: None,
            fee: None,
        };
        swap_repo
            .update_settlement(swap.id, &settlement_status)
            .await
            .unwrap();

        let updated = swap_repo.get(swap.id).await.unwrap();
        assert!(updated.settlement_status.is_some());
        assert_eq!(
            updated.settlement_status.unwrap().tx_hash,
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );

        Ok(())
    }

    #[sqlx::test]
    async fn test_update_latest_refund(pool: sqlx::PgPool) -> sqlx::Result<()> {
        let db = Database::from_pool(pool.clone()).await.unwrap();
        let swaps = db.swaps();

        let quote = create_test_quote();
        let swap = create_test_swap(quote);

        swaps.create(&swap).await.unwrap();

        let refund_record = LatestRefund {
            timestamp: utc::now(),
            recipient_address: "1RefundRecipientAddress".to_string(),
        };

        swaps
            .update_latest_refund(swap.id, &refund_record)
            .await
            .unwrap();

        let refreshed_swap = swaps.get(swap.id).await.unwrap();
        let latest_refund = refreshed_swap.latest_refund.expect("latest_refund set");
        assert_eq!(
            latest_refund.recipient_address,
            refund_record.recipient_address
        );
        assert_eq!(latest_refund.timestamp, refund_record.timestamp);

        Ok(())
    }
}
