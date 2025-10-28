use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use otc_models::{Currency, Lot};
use snafu::prelude::*;
use sqlx::{postgres::PgRow, PgPool, Row};
use tracing::info;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Deposit {
    pub private_key: String,
    pub holdings: Lot,
    pub funding_tx_hash: String,
}

#[derive(Debug, Clone)]
pub enum FillStatus {
    Full(Vec<Deposit>),
    Partial(Vec<Deposit>),
    Empty,
}

#[derive(Debug, Snafu)]
pub enum DepositRepositoryError {
    #[snafu(display("Database error: {source}"))]
    Database { source: sqlx::Error },

    #[snafu(display("Invalid U256 value: {value}"))]
    InvalidU256 { value: String },
}

pub type DepositRepositoryResult<T, E = DepositRepositoryError> = std::result::Result<T, E>;

#[allow(async_fn_in_trait)]
pub trait DepositStore {
    async fn balance(&self, currency: &Currency) -> DepositRepositoryResult<U256>;
    async fn take_deposits_that_fill_lot(&self, lot: &Lot, max_deposits: Option<usize>) -> DepositRepositoryResult<FillStatus>;
    async fn store_deposit(&self, deposit: &Deposit, swap_settlement_timestamp: DateTime<Utc>, associated_swap_id: Uuid) -> DepositRepositoryResult<()>;
    async fn get_latest_deposit_vault_timestamp(&self) -> DepositRepositoryResult<Option<DateTime<Utc>>>;
    async fn peek_all_available_deposits(&self, chain: Option<String>) -> DepositRepositoryResult<Vec<Deposit>>;
    async fn unreserve_deposits(&self, private_keys: &[String]) -> DepositRepositoryResult<usize>;
}

#[derive(Clone)]
pub struct DepositRepository {
    pool: PgPool,
}

impl DepositRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    fn serialize_currency(currency: &Currency) -> (String, serde_json::Value, i16) {
        let chain = match currency.chain {
            otc_models::ChainType::Bitcoin => "bitcoin".to_string(),
            otc_models::ChainType::Ethereum => "ethereum".to_string(),
        };
        let token = match &currency.token {
            otc_models::TokenIdentifier::Native => serde_json::json!({"type": "Native"}),
            otc_models::TokenIdentifier::Address(addr) => {
                serde_json::json!({"type": "Address", "data": addr})
            }
        };
        (chain, token, currency.decimals as i16)
    }

    fn deserialize_currency(chain: &str, token: &serde_json::Value, decimals: i16) -> Currency {
        let chain_type = match chain {
            "bitcoin" => otc_models::ChainType::Bitcoin,
            "ethereum" => otc_models::ChainType::Ethereum,
            _ => otc_models::ChainType::Bitcoin, // Default fallback
        };

        let token_id = if let Some(token_type) = token.get("type").and_then(|v| v.as_str()) {
            match token_type {
                "Native" => otc_models::TokenIdentifier::Native,
                "Address" => {
                    let addr = token
                        .get("data")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    otc_models::TokenIdentifier::Address(addr)
                }
                _ => otc_models::TokenIdentifier::Native,
            }
        } else {
            otc_models::TokenIdentifier::Native
        };

        Currency {
            chain: chain_type,
            token: token_id,
            decimals: decimals as u8,
        }
    }
}

impl Deposit {
    pub fn new(
        private_key: impl Into<String>,
        holdings: Lot,
        funding_tx_hash: impl Into<String>,
    ) -> Self {
        Self {
            private_key: private_key.into(),
            holdings,
            funding_tx_hash: funding_tx_hash.into(),
        }
    }

    pub fn holdings(&self) -> &Lot {
        &self.holdings
    }
    pub fn private_key(&self) -> &str {
        &self.private_key
    }
}

impl DepositStore for DepositRepository {
    async fn balance(&self, currency: &Currency) -> DepositRepositoryResult<U256> {
        let (chain, token, decimals) = Self::serialize_currency(currency);

        let row: Option<(String,)> = sqlx::query_as(
            r#"
            SELECT COALESCE(SUM(amount)::TEXT, '0')
            FROM mm_deposits
            WHERE status = 'available'
              AND chain = $1
              AND token = $2
              AND decimals = $3
            "#,
        )
        .bind(&chain)
        .bind(&token)
        .bind(decimals)
        .fetch_optional(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        let amount_str = row.map(|t| t.0).unwrap_or_else(|| "0".to_string());
        let amount = U256::from_str_radix(&amount_str, 10)
            .map_err(|_| DepositRepositoryError::InvalidU256 { value: amount_str })?;
        Ok(amount)
    }

    async fn get_latest_deposit_vault_timestamp(&self) -> DepositRepositoryResult<Option<DateTime<Utc>>> {
        let row: Option<(Option<DateTime<Utc>>,)> = sqlx::query_as(
            r#"
            SELECT MAX(created_at)
            FROM mm_deposits
            "#,
        )
        .fetch_optional(&self.pool)
        .await.context(DatabaseSnafu)?;

        Ok(row.and_then(|t| t.0))
    }

    async fn take_deposits_that_fill_lot(&self, lot: &Lot, max_deposits: Option<usize>) -> DepositRepositoryResult<FillStatus> {
        let (chain, token, decimals) = Self::serialize_currency(&lot.currency);
        let target = lot.amount.to_string();
        let reservation_id = Uuid::new_v4();
        let now = utc::now();

        let rows: Vec<PgRow> = if let Some(limit) = max_deposits {
            sqlx::query(
                r#"
                WITH ordered AS (
                    SELECT private_key, amount, created_at
                    FROM mm_deposits
                    WHERE status = 'available'
                      AND chain = $1
                      AND token = $2
                      AND decimals = $3
                    ORDER BY created_at, private_key
                    LIMIT $7
                    FOR UPDATE SKIP LOCKED
                ),
                pref AS (
                    SELECT private_key, amount, created_at,
                           SUM(amount) OVER (ORDER BY created_at, private_key) AS run
                    FROM ordered
                ),
                take AS (
                    SELECT private_key FROM pref WHERE (run - amount) < $4::numeric
                )
                UPDATE mm_deposits i
                SET status = 'reserved',
                    reserved_by = $5,
                    reserved_at = $6
                FROM take
                WHERE i.private_key = take.private_key
                RETURNING i.private_key, i.amount::TEXT, i.funding_tx_hash;
                "#,
            )
            .bind(&chain)
            .bind(&token)
            .bind(decimals)
            .bind(&target)
            .bind(reservation_id)
            .bind(now)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .context(DatabaseSnafu)?
        } else {
            sqlx::query(
                r#"
                WITH ordered AS (
                    SELECT private_key, amount, created_at
                    FROM mm_deposits
                    WHERE status = 'available'
                      AND chain = $1
                      AND token = $2
                      AND decimals = $3
                    ORDER BY created_at, private_key
                    FOR UPDATE SKIP LOCKED
                ),
                pref AS (
                    SELECT private_key, amount, created_at,
                           SUM(amount) OVER (ORDER BY created_at, private_key) AS run
                    FROM ordered
                ),
                take AS (
                    SELECT private_key FROM pref WHERE (run - amount) < $4::numeric
                )
                UPDATE mm_deposits i
                SET status = 'reserved',
                    reserved_by = $5,
                    reserved_at = $6
                FROM take
                WHERE i.private_key = take.private_key
                RETURNING i.private_key, i.amount::TEXT, i.funding_tx_hash;
                "#,
            )
            .bind(&chain)
            .bind(&token)
            .bind(decimals)
            .bind(&target)
            .bind(reservation_id)
            .bind(now)
            .fetch_all(&self.pool)
            .await
            .context(DatabaseSnafu)?
        };

        if rows.is_empty() {
            return Ok(FillStatus::Empty);
        }

        let mut deposits: Vec<Deposit> = Vec::with_capacity(rows.len());
        let mut sum = U256::from(0);

        for row in rows {
            let private_key: String = row.get("private_key");
            let amount_str: String = row.get("amount");
            let amount = U256::from_str_radix(&amount_str, 10)
                .map_err(|_| DepositRepositoryError::InvalidU256 { value: amount_str })?;

            let funding_tx_hash: String = row.get("funding_tx_hash");

            sum = sum.saturating_add(amount);

            deposits.push(Deposit {
                private_key,
                holdings: Lot {
                    currency: lot.currency.clone(),
                    amount,
                },
                funding_tx_hash,
            });
        }

        info!("Collected Deposits to fill {lot:?}: {deposits:?}");
        if sum >= lot.amount {
            Ok(FillStatus::Full(deposits))
        } else {
            Ok(FillStatus::Partial(deposits))
        }
    }

    async fn store_deposit(&self, deposit: &Deposit, swap_settlement_timestamp: DateTime<Utc>, associated_swap_id: Uuid) -> DepositRepositoryResult<()> {
        let (chain, token, decimals) = Self::serialize_currency(&deposit.holdings.currency);
        let amount = deposit.holdings.amount.to_string();
        let created_at = swap_settlement_timestamp;

        sqlx::query(
            r#"
            INSERT INTO mm_deposits (
                private_key, chain, token, decimals, amount, status, funding_tx_hash, created_at, associated_swap_id
            ) VALUES ($1, $2, $3, $4, $5::numeric, 'available', $6, $7, $8)
            ON CONFLICT (private_key) DO NOTHING
            "#,
        )
        .bind(&deposit.private_key)
        .bind(&chain)
        .bind(&token)
        .bind(decimals)
        .bind(&amount)
        .bind(&deposit.funding_tx_hash)
        .bind(created_at)
        .bind(associated_swap_id)
        .execute(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        Ok(())
    }

    async fn peek_all_available_deposits(&self, chain: Option<String>) -> DepositRepositoryResult<Vec<Deposit>> {
        let rows: Vec<PgRow> = if let Some(chain_filter) = chain {
            sqlx::query(
                r#"
                SELECT private_key, chain, token, decimals, amount::TEXT, funding_tx_hash
                FROM mm_deposits
                WHERE status = 'available'
                  AND chain = $1
                ORDER BY created_at, private_key;
                "#,
            )
            .bind(&chain_filter)
            .fetch_all(&self.pool)
            .await
            .context(DatabaseSnafu)?
        } else {
            sqlx::query(
                r#"
                SELECT private_key, chain, token, decimals, amount::TEXT, funding_tx_hash
                FROM mm_deposits
                WHERE status = 'available'
                ORDER BY created_at, private_key;
                "#,
            )
            .fetch_all(&self.pool)
            .await
            .context(DatabaseSnafu)?
        };

        let mut deposits = Vec::with_capacity(rows.len());
        
        for row in rows {
            let private_key: String = row.get("private_key");
            let chain: String = row.get("chain");
            let token: serde_json::Value = row.get("token");
            let decimals: i16 = row.get("decimals");
            let amount_str: String = row.get("amount");
            let funding_tx_hash: String = row.get("funding_tx_hash");

            let amount = U256::from_str_radix(&amount_str, 10)
                .map_err(|_| DepositRepositoryError::InvalidU256 { value: amount_str })?;

            let currency = Self::deserialize_currency(&chain, &token, decimals);

            deposits.push(Deposit {
                private_key,
                holdings: Lot {
                    currency,
                    amount,
                },
                funding_tx_hash,
            });
        }

        Ok(deposits)
    }

    async fn unreserve_deposits(&self, private_keys: &[String]) -> DepositRepositoryResult<usize> {
        if private_keys.is_empty() {
            return Ok(0);
        }

        let result = sqlx::query(
            r#"
            UPDATE mm_deposits
            SET status = 'available',
                reserved_by = NULL,
                reserved_at = NULL
            WHERE private_key = ANY($1)
              AND status = 'reserved'
            "#,
        )
        .bind(private_keys)
        .execute(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        Ok(result.rows_affected() as usize)
    }
}
