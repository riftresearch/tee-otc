use otc_models::{Swap, SwapStatus, UserDepositStatus, MMDepositStatus, SettlementStatus};
use sqlx::postgres::PgPool;
use uuid::Uuid;

use super::conversions::{user_deposit_status_to_json, mm_deposit_status_to_json, settlement_status_to_json};
use super::row_mappers::FromRow;
use crate::error::{OtcServerError, OtcServerResult};

#[derive(Clone)]
pub struct SwapRepository {
    pool: PgPool,
}

impl SwapRepository {
    #[must_use] pub fn new(pool: PgPool) -> Self {
        Self { pool }
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
        
        sqlx::query(
            r"
            INSERT INTO swaps (
                id, quote_id, market_maker_id,
                user_deposit_salt, user_deposit_address, mm_nonce,
                user_destination_address, user_refund_address,
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
        .bind(&swap.market_maker_id)
        .bind(&swap.user_deposit_salt[..])
        .bind(&swap.user_deposit_address)
        .bind(&swap.mm_nonce[..])
        .bind(&swap.user_destination_address)
        .bind(&swap.user_refund_address)
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
                s.user_destination_address, s.user_refund_address,
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
    
    pub async fn update_settlement(&self, id: Uuid, status: &SettlementStatus) -> OtcServerResult<()> {
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
                s.user_destination_address, s.user_refund_address,
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
    
    /// Update entire swap record
    pub async fn update(&self, swap: &Swap) -> OtcServerResult<()> {
        let user_deposit_json = swap.user_deposit_status.as_ref()
            .map(user_deposit_status_to_json)
            .transpose()?;
        let mm_deposit_json = swap.mm_deposit_status.as_ref()
            .map(mm_deposit_status_to_json)
            .transpose()?;
        let settlement_json = swap.settlement_status.as_ref()
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
                s.user_destination_address, s.user_refund_address,
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
        ).map_err(|e| OtcServerError::InvalidState { message: format!("State transition failed: {e}") })?;
        
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
        ).map_err(|e| OtcServerError::InvalidState { message: format!("State transition failed: {e}") })?;
        
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
            .map_err(|e| OtcServerError::InvalidState { message: format!("State transition failed: {e}") })?;
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
            .map_err(|e| OtcServerError::InvalidState { message: format!("State transition failed: {e}") })?;
        self.update(&swap).await?;
        Ok(())
    }
    
    /// Update swap when user deposit is confirmed
    pub async fn user_deposit_confirmed(&self, swap_id: Uuid) -> OtcServerResult<()> {
        let mut swap = self.get(swap_id).await?;
        swap.user_deposit_confirmed()
            .map_err(|e| OtcServerError::InvalidState { message: format!("State transition failed: {e}") })?;
        self.update(&swap).await?;
        Ok(())
    }
    
    /// Update swap when MM deposit is confirmed
    pub async fn mm_deposit_confirmed(&self, swap_id: Uuid) -> OtcServerResult<()> {
        let mut swap = self.get(swap_id).await?;
        swap.mm_deposit_confirmed()
            .map_err(|e| OtcServerError::InvalidState { message: format!("State transition failed: {e}") })?;
        self.update(&swap).await?;
        Ok(())
    }
    
    /// Mark private key as sent to MM
    pub async fn mark_private_key_sent(&self, swap_id: Uuid) -> OtcServerResult<()> {
        let mut swap = self.get(swap_id).await?;
        swap.mark_private_key_sent()
            .map_err(|e| OtcServerError::InvalidState { message: format!("State transition failed: {e}") })?;
        self.update(&swap).await?;
        Ok(())
    }
    
    /// Mark swap as failed
    pub async fn mark_failed(&self, swap_id: Uuid, reason: &str) -> OtcServerResult<()> {
        let mut swap = self.get(swap_id).await?;
        swap.mark_failed(reason.to_string())
            .map_err(|e| OtcServerError::InvalidState { message: format!("State transition failed: {e}") })?;
        self.update(&swap).await?;
        Ok(())
    }
    
    /// Initiate user refund
    pub async fn initiate_user_refund(&self, swap_id: Uuid, reason: &str) -> OtcServerResult<()> {
        let mut swap = self.get(swap_id).await?;
        swap.initiate_user_refund(reason.to_string())
            .map_err(|e| OtcServerError::InvalidState { message: format!("State transition failed: {e}") })?;
        self.update(&swap).await?;
        Ok(())
    }
    
    /// Initiate refund to MM
    pub async fn initiate_mm_refund(&self, swap_id: Uuid, reason: &str) -> OtcServerResult<()> {
        let mut swap = self.get(swap_id).await?;
        swap.initiate_mm_refund(reason.to_string())
            .map_err(|e| OtcServerError::InvalidState { message: format!("State transition failed: {e}") })?;
        self.update(&swap).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::U256;
    use chrono::{Duration, Utc};
    use otc_models::{
        ChainType, Currency, Quote, Swap, SwapStatus, SettlementStatus,
        TokenIdentifier, UserDepositStatus, MMDepositStatus,
    };
    use crate::db::Database;
    use crate::db::conversions::chain_type_to_db;
    use uuid::Uuid;
    use serde_json;

    #[sqlx::test]
    async fn test_swap_round_trip(pool: sqlx::PgPool) -> sqlx::Result<()> {
        // Database will auto-initialize with schema
        let db = Database::from_pool(pool.clone()).await.unwrap();
        let swap_repo = db.swaps();

        // First create a quote that the swap will reference
        let quote = Quote {
            id: Uuid::new_v4(),
            from: Currency {
                chain: ChainType::Bitcoin,
                token: TokenIdentifier::Native,
                amount: U256::from(1000000u64), // 0.01 BTC
                decimals: 8,
            },
            to: Currency {
                chain: ChainType::Ethereum,
                token: TokenIdentifier::Native,
                amount: U256::from(500000000000000000u64), // 0.5 ETH
                decimals: 18,
            },
            market_maker_id: Uuid::new_v4(),
            expires_at: Utc::now() + Duration::hours(1),
            created_at: Utc::now(),
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
            user_refund_address: "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4".to_string(),
            status: SwapStatus::WaitingUserDepositInitiated,
            user_deposit_status: None,
            mm_deposit_status: None,
            settlement_status: None,
            failure_reason: None,
            failure_at: None,
            mm_notified_at: None,
            mm_private_key_sent_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        // First insert the quote
        sqlx::query(
            r"
            INSERT INTO quotes (
                id, from_chain, from_token, from_amount, from_decimals,
                to_chain, to_token, to_amount, to_decimals,
                market_maker_id, expires_at, created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ",
        )
        .bind(&quote.id)
        .bind(chain_type_to_db(&quote.from.chain))
        .bind(serde_json::to_value(&quote.from.token).unwrap())
        .bind(&quote.from.amount.to_string())
        .bind(quote.from.decimals as i16)
        .bind(chain_type_to_db(&quote.to.chain))
        .bind(serde_json::to_value(&quote.to.token).unwrap())
        .bind(&quote.to.amount.to_string())
        .bind(quote.to.decimals as i16)
        .bind(&quote.market_maker_id)
        .bind(&quote.expires_at)
        .bind(&quote.created_at)
        .execute(&pool)
        .await
        .unwrap();
        
        // Store the swap
        swap_repo.create(&original_swap).await.unwrap();

        // Retrieve the swap
        let retrieved_swap = swap_repo.get(original_swap.id).await.unwrap();

        // Validate data
        assert_eq!(retrieved_swap.id, original_swap.id);
        assert_eq!(retrieved_swap.quote.id, original_swap.quote.id);
        assert_eq!(retrieved_swap.market_maker_id, original_swap.market_maker_id);
        assert_eq!(retrieved_swap.user_deposit_salt, original_swap.user_deposit_salt);
        assert_eq!(retrieved_swap.user_deposit_address, original_swap.user_deposit_address);
        assert_eq!(retrieved_swap.mm_nonce, original_swap.mm_nonce);
        assert_eq!(retrieved_swap.user_destination_address, original_swap.user_destination_address);
        assert_eq!(retrieved_swap.user_refund_address, original_swap.user_refund_address);
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
            from: Currency {
                chain: ChainType::Bitcoin,
                token: TokenIdentifier::Native,
                amount: U256::from(2000000u64),
                decimals: 8,
            },
            to: Currency {
                chain: ChainType::Ethereum,
                token: TokenIdentifier::Native,
                amount: U256::from(1000000000000000000u64),
                decimals: 18,
            },
            market_maker_id: Uuid::new_v4(),
            expires_at: Utc::now() + Duration::hours(1),
            created_at: Utc::now(),
        };

        // Create test salt and nonce
        let mut user_salt = [0u8; 32];
        let mut mm_nonce = [0u8; 16];
        getrandom::getrandom(&mut user_salt).unwrap();
        getrandom::getrandom(&mut mm_nonce).unwrap();

        // Create swap with deposit info
        let now = Utc::now();
        let original_swap = Swap {
            id: Uuid::new_v4(),
            market_maker_id: quote.market_maker_id,
            quote: quote.clone(),
            user_deposit_salt: user_salt,
            user_deposit_address: "bc1qnahvmnz8vgsdmrr68l5mfr8v8q9fxqz3n5d9u0".to_string(),
            mm_nonce,
            user_destination_address: "0x9876543210987654321098765432109876543210".to_string(),
            user_refund_address: "bc1qrp33g0q5c5txsp9arysrx4k6zdkfs4nce4xj0gdcccefvpysxf3qccfmv3".to_string(),
            status: SwapStatus::WaitingMMDepositConfirmed,
            user_deposit_status: Some(UserDepositStatus {
                tx_hash: "7d865e959b2466918c9863afca942d0fb89d7c9ac0c99bafc3749504ded97730".to_string(),
                amount: U256::from(2000000u64),
                detected_at: now,
                confirmations: 6,
                last_checked: now,
            }),
            mm_deposit_status: Some(MMDepositStatus {
                tx_hash: "0x88df016429689c079f3b2f6ad39fa052532c56b6a39df8e3c84c03b8346cfc63".to_string(),
                amount: U256::from(1000000000000000000u64),
                detected_at: now + Duration::minutes(5),
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

        // First insert the quote
        sqlx::query(
            r"
            INSERT INTO quotes (
                id, from_chain, from_token, from_amount, from_decimals,
                to_chain, to_token, to_amount, to_decimals,
                market_maker_id, expires_at, created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ",
        )
        .bind(&quote.id)
        .bind(chain_type_to_db(&quote.from.chain))
        .bind(serde_json::to_value(&quote.from.token).unwrap())
        .bind(&quote.from.amount.to_string())
        .bind(quote.from.decimals as i16)
        .bind(chain_type_to_db(&quote.to.chain))
        .bind(serde_json::to_value(&quote.to.token).unwrap())
        .bind(&quote.to.amount.to_string())
        .bind(quote.to.decimals as i16)
        .bind(&quote.market_maker_id)
        .bind(&quote.expires_at)
        .bind(&quote.created_at)
        .execute(&pool)
        .await
        .unwrap();
        
        // Store and retrieve
        swap_repo.create(&original_swap).await.unwrap();
        let retrieved_swap = swap_repo.get(original_swap.id).await.unwrap();

        // Validate deposit info
        assert!(retrieved_swap.user_deposit_status.is_some());
        let user_deposit = retrieved_swap.user_deposit_status.unwrap();
        let original_user_deposit = original_swap.user_deposit_status.unwrap();
        assert_eq!(user_deposit.tx_hash, original_user_deposit.tx_hash);
        assert_eq!(user_deposit.amount, original_user_deposit.amount);
        assert!((user_deposit.detected_at - original_user_deposit.detected_at).num_seconds().abs() < 1);

        assert!(retrieved_swap.mm_deposit_status.is_some());
        let mm_deposit = retrieved_swap.mm_deposit_status.unwrap();
        let original_mm_deposit = original_swap.mm_deposit_status.unwrap();
        assert_eq!(mm_deposit.tx_hash, original_mm_deposit.tx_hash);
        assert_eq!(mm_deposit.amount, original_mm_deposit.amount);
        assert!((mm_deposit.detected_at - original_mm_deposit.detected_at).num_seconds().abs() < 1);

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
            from: Currency {
                chain: ChainType::Bitcoin,
                token: TokenIdentifier::Native,
                amount: U256::from(1000000u64),
                decimals: 8,
            },
            to: Currency {
                chain: ChainType::Ethereum,
                token: TokenIdentifier::Native,
                amount: U256::from(500000000000000000u64),
                decimals: 18,
            },
            market_maker_id: Uuid::new_v4(),
            expires_at: Utc::now() + Duration::hours(1),
            created_at: Utc::now(),
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
            user_refund_address: "bc1qnahvmnz8vgsdmrr68l5mfr8v8q9fxqz3n5d9u0".to_string(),
            status: SwapStatus::WaitingUserDepositInitiated,
            user_deposit_status: None,
            mm_deposit_status: None,
            settlement_status: None,
            failure_reason: None,
            failure_at: None,
            mm_notified_at: None,
            mm_private_key_sent_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        // First insert the quote
        sqlx::query(
            r"
            INSERT INTO quotes (
                id, from_chain, from_token, from_amount, from_decimals,
                to_chain, to_token, to_amount, to_decimals,
                market_maker_id, expires_at, created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ",
        )
        .bind(&quote.id)
        .bind(chain_type_to_db(&quote.from.chain))
        .bind(serde_json::to_value(&quote.from.token).unwrap())
        .bind(&quote.from.amount.to_string())
        .bind(quote.from.decimals as i16)
        .bind(chain_type_to_db(&quote.to.chain))
        .bind(serde_json::to_value(&quote.to.token).unwrap())
        .bind(&quote.to.amount.to_string())
        .bind(quote.to.decimals as i16)
        .bind(&quote.market_maker_id)
        .bind(&quote.expires_at)
        .bind(&quote.created_at)
        .execute(&pool)
        .await
        .unwrap();
        
        swap_repo.create(&swap).await.unwrap();

        // Update status
        swap_repo.update_status(swap.id, SwapStatus::WaitingUserDepositConfirmed).await.unwrap();
        
        let updated = swap_repo.get(swap.id).await.unwrap();
        assert_eq!(updated.status, SwapStatus::WaitingUserDepositConfirmed);

        // Update user deposit
        let deposit_amount = U256::from(1000000u64);
        let user_deposit = UserDepositStatus {
            tx_hash: "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890".to_string(),
            amount: deposit_amount,
            detected_at: Utc::now(),
            confirmations: 0,
            last_checked: Utc::now(),
        };
        swap_repo.update_user_deposit(swap.id, &user_deposit).await.unwrap();

        let updated = swap_repo.get(swap.id).await.unwrap();
        assert!(updated.user_deposit_status.is_some());
        let deposit = updated.user_deposit_status.unwrap();
        assert_eq!(deposit.amount, deposit_amount);

        // Update settlement status
        let settlement_status = SettlementStatus {
            tx_hash: "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string(),
            broadcast_at: Utc::now(),
            confirmations: 0,
            completed_at: None,
            fee: None,
        };
        swap_repo.update_settlement(swap.id, &settlement_status).await.unwrap();

        let updated = swap_repo.get(swap.id).await.unwrap();
        assert!(updated.settlement_status.is_some());
        assert_eq!(
            updated.settlement_status.unwrap().tx_hash,
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );

        Ok(())
    }

    #[sqlx::test]
    async fn test_get_active_swaps(pool: sqlx::PgPool) -> sqlx::Result<()> {
        // Database will auto-initialize with schema
        let db = Database::from_pool(pool.clone()).await.unwrap();
        let swap_repo = db.swaps();

        // Create quote
        let quote = Quote {
            id: Uuid::new_v4(),
            from: Currency {
                chain: ChainType::Bitcoin,
                token: TokenIdentifier::Native,
                amount: U256::from(1000000u64),
                decimals: 8,
            },
            to: Currency {
                chain: ChainType::Ethereum,
                token: TokenIdentifier::Native,
                amount: U256::from(500000000000000000u64),
                decimals: 18,
            },
            market_maker_id: Uuid::new_v4(),
            expires_at: Utc::now() + Duration::hours(1),
            created_at: Utc::now(),
        };

        // Create multiple swaps with different statuses
        let statuses = [
            SwapStatus::WaitingUserDepositInitiated,  // Active
            SwapStatus::WaitingUserDepositConfirmed,  // Active
            SwapStatus::WaitingMMDepositInitiated,    // Active
            SwapStatus::WaitingMMDepositConfirmed,    // Active
            SwapStatus::Settled,                      // Not active
            SwapStatus::Failed,                       // Not active
            SwapStatus::RefundingUser,                // Active (refunding is now active)
        ];

        // First insert the quote
        sqlx::query(
            r"
            INSERT INTO quotes (
                id, from_chain, from_token, from_amount, from_decimals,
                to_chain, to_token, to_amount, to_decimals,
                market_maker_id, expires_at, created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ",
        )
        .bind(&quote.id)
        .bind(chain_type_to_db(&quote.from.chain))
        .bind(serde_json::to_value(&quote.from.token).unwrap())
        .bind(&quote.from.amount.to_string())
        .bind(quote.from.decimals as i16)
        .bind(chain_type_to_db(&quote.to.chain))
        .bind(serde_json::to_value(&quote.to.token).unwrap())
        .bind(&quote.to.amount.to_string())
        .bind(quote.to.decimals as i16)
        .bind(&quote.market_maker_id)
        .bind(&quote.expires_at)
        .bind(&quote.created_at)
        .execute(&pool)
        .await
        .unwrap();

        let mut swap_ids = Vec::new();
        for (i, status) in statuses.iter().enumerate() {
            // Create unique salt and nonce for each swap
            let mut user_salt = [0u8; 32];
            let mut mm_nonce = [0u8; 16];
            user_salt[0] = i as u8;
            mm_nonce[0] = (i + 100) as u8;
            
            let swap = Swap {
                id: Uuid::new_v4(),
                market_maker_id: quote.market_maker_id,
                quote: quote.clone(),
                user_deposit_salt: user_salt,
                user_deposit_address: format!("bc1q{:038}00", i),
                mm_nonce,
                user_destination_address: format!("0x{:040}", i + 100),
                user_refund_address: format!("bc1q{:038}01", i + 100),
                status: *status,
                user_deposit_status: None,
                mm_deposit_status: None,
                settlement_status: None,
                failure_reason: None,
                failure_at: None,
                mm_notified_at: None,
                mm_private_key_sent_at: None,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            };

            swap_repo.create(&swap).await.unwrap();
            swap_ids.push((swap.id, *status));
        }

        // Get active swaps
        let active_swaps = swap_repo.get_active_swaps().await.unwrap();

        // Should return 5 swaps (all except settled and failed)
        assert_eq!(active_swaps.len(), 5);

        // Verify only active statuses are returned
        for swap in &active_swaps {
            match swap.status {
                SwapStatus::WaitingUserDepositInitiated |
                SwapStatus::WaitingUserDepositConfirmed |
                SwapStatus::WaitingMMDepositInitiated |
                SwapStatus::WaitingMMDepositConfirmed |
                SwapStatus::RefundingUser => {
                    // These are expected active statuses
                }
                _ => panic!("Unexpected status in active swaps: {:?}", swap.status),
            }
        }

        Ok(())
    }
}