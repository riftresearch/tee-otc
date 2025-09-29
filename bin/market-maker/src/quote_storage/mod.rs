use std::time::Duration;

use chrono::{DateTime, Utc};
use otc_models::{ChainType, Currency, Lot, Quote, TokenIdentifier};
use snafu::prelude::*;
use sqlx::{
    migrate::Migrator,
    postgres::{PgPool, PgPoolOptions, PgRow},
    Row,
};
use tokio::{task::JoinSet, time};
use tracing::{error, info};
use uuid::Uuid;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[derive(Debug, Snafu)]
pub enum QuoteStorageError {
    #[snafu(display("Database error: {}", source))]
    Database { source: sqlx::Error },

    #[snafu(display("Migration error: {}", source))]
    Migration { source: sqlx::migrate::MigrateError },

    #[snafu(display("Invalid chain type: {}", chain))]
    InvalidChainType { chain: String },

    #[snafu(display("Invalid token identifier"))]
    InvalidTokenIdentifier,

    #[snafu(display("Invalid U256 value: {}", value))]
    InvalidU256 { value: String },
}

pub type Result<T> = std::result::Result<T, QuoteStorageError>;

#[derive(Clone)]
pub struct QuoteStorage {
    pool: PgPool,
}

impl QuoteStorage {
    pub async fn new(
        database_url: &str,
        db_max_connections: u32,
        db_min_connections: u32,
        join_set: &mut JoinSet<crate::Result<()>>,
    ) -> Result<Self> {
        info!("Connecting to market maker database...");

        let pool = PgPoolOptions::new()
            .max_connections(db_max_connections)
            .min_connections(db_min_connections)
            .max_lifetime(Duration::from_secs(300))
            .idle_timeout(Duration::from_secs(60))
            .acquire_timeout(Duration::from_millis(200))
            .test_before_acquire(true)
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    // Scope this pool to the quote_storage schema so unqualified SQL stays isolated
                    sqlx::query("SET search_path TO quote_storage, public")
                        .execute(conn)
                        .await
                        .map(|_| ())
                })
            })
            .connect(database_url)
            .await
            .context(DatabaseSnafu)?;

        info!("Running market maker database migrations...");
        MIGRATOR.run(&pool).await.context(MigrationSnafu)?;
        info!("Market maker database initialization complete");

        Self::from_pool(pool, join_set).await
    }

    pub async fn from_pool(
        pool: PgPool,
        join_set: &mut JoinSet<crate::Result<()>>,
    ) -> Result<Self> {
        let storage = Self { pool };

        let cleanup_storage = storage.clone();
        join_set.spawn(async move {
            cleanup_storage.run_cleanup_task().await;
            Ok(())
        });
        Ok(storage)
    }

    pub async fn store_quote(&self, quote: &Quote) -> Result<()> {
        let (from_chain, from_token, from_decimals) =
            self.serialize_currency(&quote.from.currency)?;
        let (to_chain, to_token, to_decimals) = self.serialize_currency(&quote.to.currency)?;

        let from_amount = quote.from.amount.to_string();
        let to_amount = quote.to.amount.to_string();

        sqlx::query(
            r#"
            INSERT INTO mm_quotes (
                id,
                market_maker_id,
                from_chain,
                from_token,
                from_amount,
                from_decimals,
                to_chain,
                to_token,
                to_amount,
                to_decimals,
                expires_at,
                created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(quote.id)
        .bind(quote.market_maker_id)
        .bind(from_chain)
        .bind(from_token)
        .bind(from_amount)
        .bind(from_decimals)
        .bind(to_chain)
        .bind(to_token)
        .bind(to_amount)
        .bind(to_decimals)
        .bind(quote.expires_at)
        .bind(quote.created_at)
        .execute(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        Ok(())
    }

    pub async fn get_quote(&self, id: Uuid) -> Result<Quote> {
        let row = sqlx::query(
            r#"
            SELECT 
                id,
                market_maker_id,
                from_chain,
                from_token,
                from_amount,
                from_decimals,
                to_chain,
                to_token,
                to_amount,
                to_decimals,
                expires_at,
                created_at
            FROM mm_quotes
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        self.deserialize_quote(&row)
    }

    pub async fn get_active_quotes(&self, market_maker_id: Uuid) -> Result<Vec<Quote>> {
        let rows = sqlx::query(
            r#"
            SELECT 
                id,
                market_maker_id,
                from_chain,
                from_token,
                from_amount,
                from_decimals,
                to_chain,
                to_token,
                to_amount,
                to_decimals,
                expires_at,
                created_at
            FROM mm_quotes
            WHERE market_maker_id = $1 
            AND expires_at > NOW()
            ORDER BY created_at DESC
            "#,
        )
        .bind(market_maker_id)
        .fetch_all(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        let mut quotes = Vec::new();
        for row in rows {
            quotes.push(self.deserialize_quote(&row)?);
        }

        Ok(quotes)
    }

    pub async fn mark_sent_to_rfq(&self, id: Uuid) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE mm_quotes
            SET sent_to_rfq = TRUE
            WHERE id = $1
            "#,
        )
        .bind(id)
        .execute(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        Ok(())
    }

    pub async fn mark_sent_to_otc(&self, id: Uuid) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE mm_quotes
            SET sent_to_otc = TRUE
            WHERE id = $1
            "#,
        )
        .bind(id)
        .execute(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        Ok(())
    }

    pub async fn delete_expired_quotes(&self) -> Result<u64> {
        let result = sqlx::query(
            r#"
            DELETE FROM mm_quotes
            WHERE expires_at < NOW()
            "#,
        )
        .execute(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        Ok(result.rows_affected())
    }

    async fn run_cleanup_task(&self) {
        let mut interval = time::interval(time::Duration::from_secs(600)); // 10 minutes

        loop {
            interval.tick().await;

            match self.delete_expired_quotes().await {
                Ok(count) => {
                    if count > 0 {
                        info!("Deleted {} expired quotes", count);
                    }
                }
                Err(e) => {
                    error!("Failed to delete expired quotes: {}", e);
                }
            }
        }
    }

    fn serialize_currency(&self, currency: &Currency) -> Result<(String, serde_json::Value, i16)> {
        let chain = match currency.chain {
            ChainType::Bitcoin => "bitcoin".to_string(),
            ChainType::Ethereum => "ethereum".to_string(),
        };

        let token = match &currency.token {
            TokenIdentifier::Native => serde_json::json!({"type": "Native"}),
            TokenIdentifier::Address(addr) => {
                serde_json::json!({"type": "Address", "data": addr})
            }
        };

        Ok((chain, token, currency.decimals as i16))
    }

    fn deserialize_quote(&self, row: &PgRow) -> Result<Quote> {
        let id: Uuid = row.get("id");
        let market_maker_id: Uuid = row.get("market_maker_id");

        let from_chain: String = row.get("from_chain");
        let from_token: serde_json::Value = row.get("from_token");
        let from_amount: String = row.get("from_amount");
        let from_decimals: i16 = row.get("from_decimals");

        let to_chain: String = row.get("to_chain");
        let to_token: serde_json::Value = row.get("to_token");
        let to_amount: String = row.get("to_amount");
        let to_decimals: i16 = row.get("to_decimals");

        let expires_at: DateTime<Utc> = row.get("expires_at");
        let created_at: DateTime<Utc> = row.get("created_at");

        let from_currency = self.deserialize_currency(&from_chain, from_token, from_decimals)?;
        let to_currency = self.deserialize_currency(&to_chain, to_token, to_decimals)?;

        let from_amount =
            alloy::primitives::U256::from_str_radix(&from_amount, 10).map_err(|_| {
                QuoteStorageError::InvalidU256 {
                    value: from_amount.clone(),
                }
            })?;

        let to_amount = alloy::primitives::U256::from_str_radix(&to_amount, 10).map_err(|_| {
            QuoteStorageError::InvalidU256 {
                value: to_amount.clone(),
            }
        })?;

        Ok(Quote {
            id,
            market_maker_id,
            from: Lot {
                currency: from_currency,
                amount: from_amount,
            },
            to: Lot {
                currency: to_currency,
                amount: to_amount,
            },
            expires_at,
            created_at,
        })
    }

    fn deserialize_currency(
        &self,
        chain: &str,
        token: serde_json::Value,
        decimals: i16,
    ) -> Result<Currency> {
        let chain_type = match chain {
            "bitcoin" => ChainType::Bitcoin,
            "ethereum" => ChainType::Ethereum,
            _ => {
                return InvalidChainTypeSnafu { chain }.fail();
            }
        };

        let token_identifier = if let Some(token_type) = token.get("type") {
            match token_type.as_str() {
                Some("Native") => TokenIdentifier::Native,
                Some("Address") => {
                    if let Some(addr) = token.get("data").and_then(|v| v.as_str()) {
                        TokenIdentifier::Address(addr.to_string())
                    } else {
                        return InvalidTokenIdentifierSnafu.fail();
                    }
                }
                _ => return InvalidTokenIdentifierSnafu.fail(),
            }
        } else {
            return InvalidTokenIdentifierSnafu.fail();
        };

        Ok(Currency {
            chain: chain_type,
            token: token_identifier,
            decimals: decimals as u8,
        })
    }
}
