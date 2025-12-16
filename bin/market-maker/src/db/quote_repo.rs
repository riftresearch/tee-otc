use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use otc_models::{ChainType, Currency, Fees, Lot, Quote, SwapRates, TokenIdentifier};
use serde_json;
use snafu::prelude::*;
use sqlx::{postgres::PgRow, PgPool, Row};
use tokio::{task::JoinSet, time};
use tracing::{error, info};
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum QuoteRepositoryError {
    #[snafu(display("Database error: {source}"))]
    Database { source: sqlx::Error },

    #[snafu(display("Invalid chain type: {chain}"))]
    InvalidChainType { chain: String },

    #[snafu(display("Invalid token identifier"))]
    InvalidTokenIdentifier,

    #[snafu(display("Invalid U256 value: {value}"))]
    InvalidU256 { value: String },

    #[snafu(display("Invalid rates: {source}"))]
    InvalidRates { source: serde_json::Error },
}

pub type QuoteRepositoryResult<T, E = QuoteRepositoryError> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct QuoteRepository {
    pool: PgPool,
}

impl QuoteRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn start_cleanup_task(&self, join_set: &mut JoinSet<crate::Result<()>>) {
        let cleanup_repo = self.clone();
        join_set.spawn(async move {
            cleanup_repo.run_cleanup_task().await;
            Ok(())
        });
    }

    pub async fn store_quote(&self, quote: &Quote) -> QuoteRepositoryResult<()> {
        let (from_chain, from_token, from_decimals) =
            self.serialize_currency(&quote.from.currency)?;
        let (to_chain, to_token, to_decimals) = self.serialize_currency(&quote.to.currency)?;

        let from_amount = quote.from.amount.to_string();
        let to_amount = quote.to.amount.to_string();
        let min_input = quote.min_input.to_string();
        let max_input = quote.max_input.to_string();
        let rates = serde_json::to_value(&quote.rates).context(InvalidRatesSnafu)?;
        let fees = serde_json::to_value(&quote.fees).context(InvalidRatesSnafu)?;

        sqlx::query(
            r#"
            INSERT INTO mm_quotes (
                id,
                market_maker_id,
                from_chain,
                from_token,
                from_decimals,
                from_amount,
                to_chain,
                to_token,
                to_decimals,
                to_amount,
                min_input,
                max_input,
                affiliate,
                rates,
                fees,
                expires_at,
                created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(quote.id)
        .bind(quote.market_maker_id)
        .bind(from_chain)
        .bind(from_token)
        .bind(from_decimals)
        .bind(from_amount)
        .bind(to_chain)
        .bind(to_token)
        .bind(to_decimals)
        .bind(to_amount)
        .bind(min_input)
        .bind(max_input)
        .bind(&quote.affiliate)
        .bind(rates)
        .bind(fees)
        .bind(quote.expires_at)
        .bind(quote.created_at)
        .execute(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        Ok(())
    }

    pub async fn get_quote(&self, id: Uuid) -> QuoteRepositoryResult<Quote> {
        let row = sqlx::query(
            r#"
            SELECT 
                id,
                market_maker_id,
                from_chain,
                from_token,
                from_decimals,
                from_amount,
                to_chain,
                to_token,
                to_decimals,
                to_amount,
                min_input,
                max_input,
                affiliate,
                rates,
                fees,
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

    pub async fn get_active_quotes(&self, market_maker_id: Uuid) -> QuoteRepositoryResult<Vec<Quote>> {
        let now = utc::now();

        let rows = sqlx::query(
            r#"
            SELECT 
                id,
                market_maker_id,
                from_chain,
                from_token,
                from_decimals,
                from_amount,
                to_chain,
                to_token,
                to_decimals,
                to_amount,
                min_input,
                max_input,
                affiliate,
                rates,
                fees,
                expires_at,
                created_at
            FROM mm_quotes
            WHERE market_maker_id = $1 
            AND expires_at > $2
            ORDER BY created_at DESC
            "#,
        )
        .bind(market_maker_id)
        .bind(now)
        .fetch_all(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        let mut quotes = Vec::new();
        for row in rows {
            quotes.push(self.deserialize_quote(&row)?);
        }

        Ok(quotes)
    }

    pub async fn mark_sent_to_rfq(&self, id: Uuid) -> QuoteRepositoryResult<()> {
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

    pub async fn mark_sent_to_otc(&self, id: Uuid) -> QuoteRepositoryResult<()> {
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

    pub async fn delete_expired_quotes(&self) -> QuoteRepositoryResult<u64> {
        let now = utc::now();

        let result = sqlx::query(
            r#"
            DELETE FROM mm_quotes
            WHERE expires_at < $1
            "#,
        )
        .bind(now)
        .execute(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        Ok(result.rows_affected())
    }

    async fn run_cleanup_task(&self) {
        let mut interval = time::interval(time::Duration::from_secs(600));

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

    fn serialize_currency(
        &self,
        currency: &Currency,
    ) -> QuoteRepositoryResult<(String, serde_json::Value, i16)> {
        let chain = currency.chain.to_db_string();

        let token = match &currency.token {
            TokenIdentifier::Native => serde_json::json!({"type": "Native"}),
            TokenIdentifier::Address(addr) => {
                serde_json::json!({"type": "Address", "data": addr})
            }
        };

        Ok((chain.to_string(), token, currency.decimals as i16))
    }

    fn deserialize_quote(&self, row: &PgRow) -> QuoteRepositoryResult<Quote> {
        let id: Uuid = row.get("id");
        let market_maker_id: Uuid = row.get("market_maker_id");

        let from_chain: String = row.get("from_chain");
        let from_token: serde_json::Value = row.get("from_token");
        let from_decimals: i16 = row.get("from_decimals");
        let from_amount_str: String = row.get("from_amount");

        let to_chain: String = row.get("to_chain");
        let to_token: serde_json::Value = row.get("to_token");
        let to_decimals: i16 = row.get("to_decimals");
        let to_amount_str: String = row.get("to_amount");

        let min_input_str: String = row.get("min_input");
        let max_input_str: String = row.get("max_input");

        let expires_at: DateTime<Utc> = row.get("expires_at");
        let created_at: DateTime<Utc> = row.get("created_at");

        let from_currency = self.deserialize_currency(&from_chain, from_token, from_decimals)?;
        let to_currency = self.deserialize_currency(&to_chain, to_token, to_decimals)?;

        let from_amount = U256::from_str_radix(&from_amount_str, 10).map_err(|_| {
            QuoteRepositoryError::InvalidU256 {
                value: from_amount_str.clone(),
            }
        })?;

        let to_amount = U256::from_str_radix(&to_amount_str, 10).map_err(|_| {
            QuoteRepositoryError::InvalidU256 {
                value: to_amount_str.clone(),
            }
        })?;

        let min_input = U256::from_str_radix(&min_input_str, 10).map_err(|_| {
            QuoteRepositoryError::InvalidU256 {
                value: min_input_str.clone(),
            }
        })?;

        let max_input = U256::from_str_radix(&max_input_str, 10).map_err(|_| {
            QuoteRepositoryError::InvalidU256 {
                value: max_input_str.clone(),
            }
        })?;

        let affiliate: Option<String> = row.try_get("affiliate").context(DatabaseSnafu)?;

        let rates_json: serde_json::Value = row.try_get("rates").context(DatabaseSnafu)?;
        let rates: SwapRates = serde_json::from_value(rates_json).context(InvalidRatesSnafu)?;

        let fees_json: serde_json::Value = row.try_get("fees").context(DatabaseSnafu)?;
        let fees: Fees = serde_json::from_value(fees_json).context(InvalidRatesSnafu)?;

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
            rates,
            fees,
            min_input,
            max_input,
            affiliate,
            expires_at,
            created_at,
        })
    }

    fn deserialize_currency(
        &self,
        chain: &str,
        token: serde_json::Value,
        decimals: i16,
    ) -> QuoteRepositoryResult<Currency> {
        let chain_type = match chain {
            "bitcoin" => ChainType::Bitcoin,
            "ethereum" => ChainType::Ethereum,
            "base" => ChainType::Base,
            _ => {
                return InvalidChainTypeSnafu {
                    chain: chain.to_string(),
                }
                .fail();
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
