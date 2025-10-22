use alloy::primitives::U256;
use otc_models::{Currency, Lot};
use snafu::prelude::*;
use sqlx::{postgres::PgRow, PgPool, Row};
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
    async fn take_deposits_that_fill_lot(&self, lot: &Lot) -> DepositRepositoryResult<FillStatus>;
    async fn store_deposit(&self, deposit: &Deposit) -> DepositRepositoryResult<()>;
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

    async fn take_deposits_that_fill_lot(&self, lot: &Lot) -> DepositRepositoryResult<FillStatus> {
        let (chain, token, decimals) = Self::serialize_currency(&lot.currency);
        let target = lot.amount.to_string();
        let reservation_id = Uuid::new_v4();
        let now = utc::now();

        let rows: Vec<PgRow> = sqlx::query(
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
        .context(DatabaseSnafu)?;

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

        if sum >= lot.amount {
            Ok(FillStatus::Full(deposits))
        } else {
            Ok(FillStatus::Partial(deposits))
        }
    }

    async fn store_deposit(&self, deposit: &Deposit) -> DepositRepositoryResult<()> {
        let (chain, token, decimals) = Self::serialize_currency(&deposit.holdings.currency);
        let amount = deposit.holdings.amount.to_string();
        let now = utc::now();

        sqlx::query(
            r#"
            INSERT INTO mm_deposits (
                private_key, chain, token, decimals, amount, status, funding_tx_hash, created_at
            ) VALUES ($1, $2, $3, $4, $5::numeric, 'available', $6, $7)
            ON CONFLICT (private_key) DO NOTHING
            "#,
        )
        .bind(&deposit.private_key)
        .bind(&chain)
        .bind(&token)
        .bind(decimals)
        .bind(&amount)
        .bind(&deposit.funding_tx_hash)
        .bind(now)
        .execute(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        Ok(())
    }
}
