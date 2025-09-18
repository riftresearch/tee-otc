use otc_models::{
    ChainType, Lot, MMDepositStatus, SettlementStatus, Swap, SwapStatus, UserDepositStatus,
};
use sqlx::postgres::PgPool;
use sqlx::Row;
use uuid::Uuid;

use super::conversions::{
    chain_type_from_db, lot_from_db, mm_deposit_status_to_json, settlement_status_to_json,
    user_deposit_status_from_json, user_deposit_status_to_json,
};
use super::row_mappers::FromRow;
use crate::db::quote_repo::QuoteRepository;
use crate::error::{OtcServerError, OtcServerResult};

#[derive(Debug, Clone)]
pub struct PendingMMDepositSwap {
    pub swap_id: Uuid,
    pub quote_id: Uuid,
    pub user_destination_address: String,
    pub mm_nonce: [u8; 16],
    pub expected_lot: Lot,
}

#[derive(Debug, Clone)]
pub struct SettledSwapNotification {
    pub swap_id: Uuid,
    pub user_deposit_salt: [u8; 32],
    pub user_deposit_tx_hash: String,
    pub lot: Lot,
    pub deposit_chain: ChainType,
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

        self.quote_repo.create(&swap.quote).await?;

        sqlx::query(
            r"
            INSERT INTO swaps (
                id, quote_id, market_maker_id,
                user_deposit_salt, user_deposit_address, mm_nonce,
                user_destination_address, user_evm_account_address,
                status,
                user_deposit_status, mm_deposit_status, settlement_status,
                failure_reason, failure_at,
                mm_notified_at, mm_private_key_sent_at,
                created_at, updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
            )
            ",
        )
        .bind(swap.id)
        .bind(swap.quote.id)
        .bind(swap.market_maker_id)
        .bind(&swap.user_deposit_salt[..])
        .bind(&swap.user_deposit_address)
        .bind(&swap.mm_nonce[..])
        .bind(&swap.user_destination_address)
        .bind(swap.user_evm_account_address.to_string())
        .bind(swap.status)
        .bind(user_deposit_json)
        .bind(mm_deposit_json)
        .bind(settlement_json)
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
                s.user_deposit_salt, s.user_deposit_address, s.mm_nonce,
                s.user_destination_address, s.user_evm_account_address,
                s.status,
                s.user_deposit_status, s.mm_deposit_status, s.settlement_status,
                s.failure_reason, s.failure_at,
                s.mm_notified_at, s.mm_private_key_sent_at,
                s.created_at, s.updated_at,
                -- Quote fields
                q.id as quote_id, q.from_chain, q.from_token, q.from_amount, q.from_decimals,
                q.to_chain, q.to_token, q.to_amount, q.to_decimals,
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

    pub async fn update_status(&self, id: Uuid, status: SwapStatus) -> OtcServerResult<()> {
        sqlx::query(
            r"
            UPDATE swaps
            SET status = $2, updated_at = NOW()
            WHERE id = $1
            ",
        )
        .bind(id)
        .bind(status)
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

        sqlx::query(
            r"
            UPDATE swaps
            SET 
                user_deposit_status = $2,
                updated_at = NOW()
            WHERE id = $1
            ",
        )
        .bind(id)
        .bind(status_json)
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

        sqlx::query(
            r"
            UPDATE swaps
            SET 
                mm_deposit_status = $2,
                updated_at = NOW()
            WHERE id = $1
            ",
        )
        .bind(id)
        .bind(status_json)
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

        sqlx::query(
            r"
            UPDATE swaps
            SET 
                settlement_status = $2,
                updated_at = NOW()
            WHERE id = $1
            ",
        )
        .bind(id)
        .bind(status_json)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_active_swaps(&self) -> OtcServerResult<Vec<Swap>> {
        let rows = sqlx::query(
            r"
            SELECT 
                s.id, s.quote_id, s.market_maker_id,
                s.user_deposit_salt, s.user_deposit_address, s.mm_nonce,
                s.user_destination_address, s.user_evm_account_address,
                s.status,
                s.user_deposit_status, s.mm_deposit_status, s.settlement_status,
                s.failure_reason, s.failure_at,
                s.mm_notified_at, s.mm_private_key_sent_at,
                s.created_at, s.updated_at,
                -- Quote fields
                q.id as quote_id, q.from_chain, q.from_token, q.from_amount, q.from_decimals,
                q.to_chain, q.to_token, q.to_amount, q.to_decimals,
                q.market_maker_id as quote_market_maker_id, q.expires_at, q.created_at as quote_created_at
            FROM swaps s
            JOIN quotes q ON s.quote_id = q.id
            WHERE s.status NOT IN ('settled', 'failed')
            ORDER BY s.created_at DESC
            ",
        )
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
                q.to_chain,
                q.to_token,
                q.to_amount,
                q.to_decimals
            FROM swaps s
            JOIN quotes q ON s.quote_id = q.id
            WHERE s.market_maker_id = $1
              AND s.status = $2
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

            let expected_lot = lot_from_db(
                row.try_get::<String, _>("to_chain")?,
                row.try_get::<serde_json::Value, _>("to_token")?,
                row.try_get::<String, _>("to_amount")?,
                row.try_get::<i16, _>("to_decimals")? as u8,
            )?;

            swaps.push(PendingMMDepositSwap {
                swap_id: row.try_get("swap_id")?,
                quote_id: row.try_get("quote_id")?,
                user_destination_address: row.try_get("user_destination_address")?,
                mm_nonce,
                expected_lot,
            });
        }

        Ok(swaps)
    }

    pub async fn get_settled_swaps_for_market_maker(
        &self,
        market_maker_id: Uuid,
    ) -> OtcServerResult<Vec<SettledSwapNotification>> {
        let rows = sqlx::query(
            r#"
            SELECT
                s.id AS swap_id,
                s.user_deposit_salt,
                s.user_deposit_status,
                q.from_chain,
                q.to_chain,
                q.to_token,
                q.to_amount,
                q.to_decimals
            FROM swaps s
            JOIN quotes q ON s.quote_id = q.id
            WHERE s.market_maker_id = $1
              AND s.status = $2
              AND s.user_deposit_status IS NOT NULL
            "#,
        )
        .bind(market_maker_id)
        .bind(SwapStatus::Settled)
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
            let deposit_chain = chain_type_from_db(&from_chain)?;

            let deposit_status_json: serde_json::Value = row.try_get("user_deposit_status")?;
            let deposit_status = user_deposit_status_from_json(deposit_status_json)?;

            let lot = lot_from_db(
                row.try_get::<String, _>("to_chain")?,
                row.try_get::<serde_json::Value, _>("to_token")?,
                row.try_get::<String, _>("to_amount")?,
                row.try_get::<i16, _>("to_decimals")? as u8,
            )?;

            swaps.push(SettledSwapNotification {
                swap_id: row.try_get("swap_id")?,
                user_deposit_salt,
                user_deposit_tx_hash: deposit_status.tx_hash,
                lot,
                deposit_chain,
            });
        }

        Ok(swaps)
    }

    /// Update entire swap record
    pub async fn update(&self, swap: &Swap) -> OtcServerResult<()> {
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

        sqlx::query(
            r"
            UPDATE swaps
            SET 
                status = $2,
                user_deposit_status = $3,
                mm_deposit_status = $4,
                settlement_status = $5,
                failure_reason = $6,
                failure_at = $7,
                mm_notified_at = $8,
                mm_private_key_sent_at = $9,
                updated_at = $10
            WHERE id = $1
            ",
        )
        .bind(swap.id)
        .bind(swap.status)
        .bind(user_deposit_json)
        .bind(mm_deposit_json)
        .bind(settlement_json)
        .bind(&swap.failure_reason)
        .bind(swap.failure_at)
        .bind(swap.mm_notified_at)
        .bind(swap.mm_private_key_sent_at)
        .bind(swap.updated_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_swaps_by_market_maker(&self, mm_id: Uuid) -> OtcServerResult<Vec<Swap>> {
        let rows = sqlx::query(
            r"
            SELECT 
                s.id, s.quote_id, s.market_maker_id,
                s.user_deposit_salt, s.user_deposit_address, s.mm_nonce,
                s.user_destination_address, s.user_evm_account_address,
                s.status,
                s.user_deposit_status, s.mm_deposit_status, s.settlement_status,
                s.failure_reason, s.failure_at,
                s.mm_notified_at, s.mm_private_key_sent_at,
                s.created_at, s.updated_at,
                -- Quote fields
                q.id as quote_id, q.from_chain, q.from_token, q.from_amount, q.from_decimals,
                q.to_chain, q.to_token, q.to_amount, q.to_decimals,
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
    ) -> OtcServerResult<()> {
        // First get the swap
        let mut swap = self.get(swap_id).await?;

        // Apply the state transition
        swap.user_deposit_detected(
            deposit_status.tx_hash.clone(),
            deposit_status.amount,
            deposit_status.confirmations,
        )
        .map_err(|e| OtcServerError::InvalidState {
            message: format!("State transition failed: {e}"),
        })?;

        // Update the database
        self.update(&swap).await?;
        Ok(())
    }

    /// Update swap when MM deposit is detected
    pub async fn mm_deposit_detected(
        &self,
        swap_id: Uuid,
        deposit_status: MMDepositStatus,
    ) -> OtcServerResult<()> {
        // First get the swap
        let mut swap = self.get(swap_id).await?;

        // Apply the state transition
        swap.mm_deposit_detected(
            deposit_status.tx_hash.clone(),
            deposit_status.amount,
            deposit_status.confirmations,
        )
        .map_err(|e| OtcServerError::InvalidState {
            message: format!("State transition failed: {e}"),
        })?;

        // Update the database
        self.update(&swap).await?;
        Ok(())
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
        self.update(&swap).await?;
        Ok(())
    }

    /// Update MM deposit confirmations
    pub async fn update_mm_confirmations(
        &self,
        swap_id: Uuid,
        confirmations: u32,
    ) -> OtcServerResult<()> {
        let mut swap = self.get(swap_id).await?;
        swap.update_confirmations(None, Some(confirmations as u64))
            .map_err(|e| OtcServerError::InvalidState {
                message: format!("State transition failed: {e}"),
            })?;
        self.update(&swap).await?;
        Ok(())
    }

    /// Update swap when user deposit is confirmed
    pub async fn user_deposit_confirmed(&self, swap_id: Uuid) -> OtcServerResult<()> {
        let mut swap = self.get(swap_id).await?;
        swap.user_deposit_confirmed()
            .map_err(|e| OtcServerError::InvalidState {
                message: format!("State transition failed: {e}"),
            })?;
        self.update(&swap).await?;
        Ok(())
    }

    /// Update swap when MM deposit is confirmed
    pub async fn mm_deposit_confirmed(&self, swap_id: Uuid) -> OtcServerResult<()> {
        let mut swap = self.get(swap_id).await?;
        swap.mm_deposit_confirmed()
            .map_err(|e| OtcServerError::InvalidState {
                message: format!("State transition failed: {e}"),
            })?;
        self.update(&swap).await?;
        Ok(())
    }

    /// Mark private key as sent to MM
    pub async fn mark_private_key_sent(&self, swap_id: Uuid) -> OtcServerResult<()> {
        let mut swap = self.get(swap_id).await?;
        swap.mark_private_key_sent()
            .map_err(|e| OtcServerError::InvalidState {
                message: format!("State transition failed: {e}"),
            })?;
        self.update(&swap).await?;
        Ok(())
    }

    /// Mark swap as failed
    pub async fn mark_failed(&self, swap_id: Uuid, reason: &str) -> OtcServerResult<()> {
        let mut swap = self.get(swap_id).await?;
        swap.mark_failed(reason.to_string())
            .map_err(|e| OtcServerError::InvalidState {
                message: format!("State transition failed: {e}"),
            })?;
        self.update(&swap).await?;
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
        self.update(&swap).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::db::conversions::chain_type_to_db;
    use crate::db::Database;
    use alloy::primitives::U256;
    use chrono::{Duration, Utc};
    use otc_models::{
        ChainType, Currency, Lot, MMDepositStatus, Quote, SettlementStatus, Swap, SwapStatus,
        TokenIdentifier, UserDepositStatus,
    };
    use serde_json;
    use uuid::Uuid;

    #[sqlx::test]
    async fn test_swap_round_trip(pool: sqlx::PgPool) -> sqlx::Result<()> {
        // Database will auto-initialize with schema
        let db = Database::from_pool(pool.clone()).await.unwrap();
        let swap_repo = db.swaps();

        // First create a quote that the swap will reference
        let quote = Quote {
            id: Uuid::new_v4(),
            from: Lot {
                currency: Currency {
                    chain: ChainType::Bitcoin,
                    token: TokenIdentifier::Native,
                    decimals: 8,
                },
                amount: U256::from(1000000u64), // 0.01 BTC
            },
            to: Lot {
                currency: Currency {
                    chain: ChainType::Ethereum,
                    token: TokenIdentifier::Native,
                    decimals: 18,
                },
                amount: U256::from(500000000000000000u64), // 0.5 ETH
            },
            market_maker_id: Uuid::new_v4(),
            expires_at: utc::now() + Duration::hours(1),
            created_at: utc::now(),
        };

        // Create test salt and nonce
        let mut user_salt = [0u8; 32];
        let mut mm_nonce = [0u8; 16];
        getrandom::getrandom(&mut user_salt).unwrap();
        getrandom::getrandom(&mut mm_nonce).unwrap();

        // Create a test swap
        let original_swap = Swap {
            id: Uuid::new_v4(),
            market_maker_id: quote.market_maker_id,
            quote: quote.clone(),
            user_deposit_salt: user_salt,
            user_deposit_address: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh".to_string(),
            mm_nonce,
            user_destination_address: "0x1234567890123456789012345678901234567890".to_string(),
            user_evm_account_address: "0x1234567890123456789012345678901234567890"
                .parse()
                .unwrap(),
            status: SwapStatus::WaitingUserDepositInitiated,
            user_deposit_status: None,
            mm_deposit_status: None,
            settlement_status: None,
            failure_reason: None,
            failure_at: None,
            mm_notified_at: None,
            mm_private_key_sent_at: None,
            created_at: utc::now(),
            updated_at: utc::now(),
        };

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
        assert_eq!(retrieved_swap.mm_nonce, original_swap.mm_nonce);
        assert_eq!(
            retrieved_swap.user_destination_address,
            original_swap.user_destination_address
        );
        assert_eq!(
            retrieved_swap.user_evm_account_address,
            original_swap.user_evm_account_address
        );
        assert_eq!(retrieved_swap.status, original_swap.status);

        Ok(())
    }

    #[sqlx::test]
    async fn test_swap_with_deposits(pool: sqlx::PgPool) -> sqlx::Result<()> {
        // Database will auto-initialize with schema
        let db = Database::from_pool(pool.clone()).await.unwrap();
        let swap_repo = db.swaps();

        // Create quote
        let quote = Quote {
            id: Uuid::new_v4(),
            from: Lot {
                currency: Currency {
                    chain: ChainType::Bitcoin,
                    token: TokenIdentifier::Native,
                    decimals: 8,
                },
                amount: U256::from(2000000u64),
            },
            to: Lot {
                currency: Currency {
                    chain: ChainType::Ethereum,
                    token: TokenIdentifier::Native,
                    decimals: 18,
                },
                amount: U256::from(1000000000000000000u64),
            },
            market_maker_id: Uuid::new_v4(),
            expires_at: utc::now() + Duration::hours(1),
            created_at: utc::now(),
        };

        // Create test salt and nonce
        let mut user_salt = [0u8; 32];
        let mut mm_nonce = [0u8; 16];
        getrandom::getrandom(&mut user_salt).unwrap();
        getrandom::getrandom(&mut mm_nonce).unwrap();

        // Create swap with deposit info
        let now = utc::now();
        let original_swap = Swap {
            id: Uuid::new_v4(),
            market_maker_id: quote.market_maker_id,
            quote: quote.clone(),
            user_deposit_salt: user_salt,
            user_deposit_address: "bc1qnahvmnz8vgsdmrr68l5mfr8v8q9fxqz3n5d9u0".to_string(),
            mm_nonce,
            user_destination_address: "0x9876543210987654321098765432109876543210".to_string(),
            user_evm_account_address: "0x1234567890123456789012345678901234567890"
                .parse()
                .unwrap(),
            status: SwapStatus::WaitingMMDepositConfirmed,
            user_deposit_status: Some(UserDepositStatus {
                tx_hash: "7d865e959b2466918c9863afca942d0fb89d7c9ac0c99bafc3749504ded97730"
                    .to_string(),
                amount: U256::from(2000000u64),
                deposit_detected_at: now,
                confirmed_at: None,
                confirmations: 6,
                last_checked: now,
            }),
            mm_deposit_status: Some(MMDepositStatus {
                tx_hash: "0x88df016429689c079f3b2f6ad39fa052532c56b6a39df8e3c84c03b8346cfc63"
                    .to_string(),
                amount: U256::from(1000000000000000000u64),
                deposit_detected_at: now + Duration::minutes(5),
                confirmations: 12,
                last_checked: now + Duration::minutes(5),
            }),
            settlement_status: None,
            failure_reason: None,
            failure_at: None,
            mm_notified_at: None,
            mm_private_key_sent_at: None,
            created_at: now,
            updated_at: now + Duration::minutes(5),
        };

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

        // Create quote
        let quote = Quote {
            id: Uuid::new_v4(),
            from: Lot {
                currency: Currency {
                    chain: ChainType::Bitcoin,
                    token: TokenIdentifier::Native,
                    decimals: 8,
                },
                amount: U256::from(1000000u64),
            },
            to: Lot {
                currency: Currency {
                    chain: ChainType::Ethereum,
                    token: TokenIdentifier::Native,
                    decimals: 18,
                },
                amount: U256::from(500000000000000000u64),
            },
            market_maker_id: Uuid::new_v4(),
            expires_at: utc::now() + Duration::hours(1),
            created_at: utc::now(),
        };

        // Create test salt and nonce
        let mut user_salt = [0u8; 32];
        let mut mm_nonce = [0u8; 16];
        getrandom::getrandom(&mut user_salt).unwrap();
        getrandom::getrandom(&mut mm_nonce).unwrap();

        // Create swap
        let swap = Swap {
            id: Uuid::new_v4(),
            market_maker_id: quote.market_maker_id,
            quote: quote.clone(),
            user_deposit_salt: user_salt,
            user_deposit_address: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh".to_string(),
            mm_nonce,
            user_destination_address: "0xabcdef1234567890abcdef1234567890abcdef12".to_string(),
            user_evm_account_address: "0x1234567890123456789012345678901234567890"
                .parse()
                .unwrap(),
            status: SwapStatus::WaitingUserDepositInitiated,
            user_deposit_status: None,
            mm_deposit_status: None,
            settlement_status: None,
            failure_reason: None,
            failure_at: None,
            mm_notified_at: None,
            mm_private_key_sent_at: None,
            created_at: utc::now(),
            updated_at: utc::now(),
        };

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
}
