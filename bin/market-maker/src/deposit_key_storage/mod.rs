use alloy::primitives::U256;
use otc_models::{Currency, Lot};
use snafu::{prelude::*, Snafu};
use sqlx::{
    migrate::Migrator,
    postgres::{PgPool, PgPoolOptions, PgRow},
    Row,
};
use uuid::Uuid;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

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
pub enum Error {
    #[snafu(display("Database error: {}", source))]
    Database { source: sqlx::Error },

    #[snafu(display("Migration error: {}", source))]
    Migration { source: sqlx::migrate::MigrateError },

    #[snafu(display("Invalid U256 value: {}", value))]
    InvalidU256 { value: String },
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[allow(async_fn_in_trait)]
pub trait DepositKeyStorageTrait {
    /// Get the balance of all keys in the vault for a given currency
    /// This should be a sum of all the balances of the keys in the vault for a given currency
    /// Should be summed at a database level (ideally)
    async fn balance(&self, currency: &Currency) -> Result<U256>;

    /// Will collect as many deposits as possible that sum up to at LEAST the lot amount
    /// If there are not enough deposits to fill the lot, it will return as many as possible
    async fn take_deposits_that_fill_lot(&self, lot: &Lot) -> Result<FillStatus>;

    /// Store a deposit in the vault
    async fn store_deposit(&self, deposit: &Deposit) -> Result<()>;
}

#[derive(Clone)]
pub struct DepositKeyStorage {
    pool: PgPool,
}

impl DepositKeyStorage {
    pub async fn new(
        database_url: &str,
        db_max_connections: u32,
        db_min_connections: u32,
    ) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(db_max_connections)
            .min_connections(db_min_connections)
            .acquire_timeout(std::time::Duration::from_secs(5))
            .idle_timeout(std::time::Duration::from_secs(600))
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    // Scope this pool to the deposit_key_storage schema so unqualified SQL stays isolated
                    sqlx::query("SET search_path TO deposit_key_storage, public")
                        .execute(conn)
                        .await
                        .map(|_| ())
                })
            })
            .connect(database_url)
            .await
            .context(DatabaseSnafu)?;

        MIGRATOR.run(&pool).await.context(MigrationSnafu)?;

        Ok(Self { pool })
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

impl DepositKeyStorageTrait for DepositKeyStorage {
    async fn balance(&self, currency: &Currency) -> Result<U256> {
        let (chain, token, decimals) = Self::serialize_currency(currency);

        // Sum only available deposits at the database level
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
            .map_err(|_| Error::InvalidU256 { value: amount_str })?;
        Ok(amount)
    }

    async fn take_deposits_that_fill_lot(&self, lot: &Lot) -> Result<FillStatus> {
        let (chain, token, decimals) = Self::serialize_currency(&lot.currency);
        let target = lot.amount.to_string();
        let reservation_id = Uuid::new_v4();

        // Greedy prefix-sum reservation under concurrency
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
                reserved_at = NOW()
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
                .map_err(|_| Error::InvalidU256 { value: amount_str })?;

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

    async fn store_deposit(&self, deposit: &Deposit) -> Result<()> {
        let (chain, token, decimals) = Self::serialize_currency(&deposit.holdings.currency);
        let amount = deposit.holdings.amount.to_string();

        // Use ON CONFLICT DO NOTHING to make this idempotent - if the private_key already exists,
        sqlx::query(
            r#"
            INSERT INTO mm_deposits (
                private_key, chain, token, decimals, amount, status, funding_tx_hash
            ) VALUES ($1, $2, $3, $4, $5::numeric, 'available', $6)
            ON CONFLICT (private_key) DO NOTHING
            "#,
        )
        .bind(&deposit.private_key)
        .bind(&chain)
        .bind(&token)
        .bind(decimals)
        .bind(&amount)
        .bind(&deposit.funding_tx_hash)
        .execute(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        Ok(())
    }
}
