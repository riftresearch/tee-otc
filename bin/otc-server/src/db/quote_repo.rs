use chrono::{DateTime, Utc};
use otc_models::Quote;
use sqlx::postgres::PgPool;
use uuid::Uuid;

use crate::error::OtcServerResult;

use super::conversions::{currency_to_db, u256_to_db};
use super::row_mappers::FromRow;

#[derive(Clone)]
pub struct QuoteRepository {
    pool: PgPool,
}

impl QuoteRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create(&self, quote: &Quote) -> OtcServerResult<()> {
        let (from_chain, from_token, from_decimals) = currency_to_db(&quote.from.currency)?;
        let (to_chain, to_token, to_decimals) = currency_to_db(&quote.to.currency)?;
        let from_amount = u256_to_db(&quote.from.amount);
        let to_amount = u256_to_db(&quote.to.amount);
        let fee_liquidity = u256_to_db(&quote.fees.liquidity_fee);
        let fee_protocol = u256_to_db(&quote.fees.protocol_fee);
        let fee_network = u256_to_db(&quote.fees.network_fee);
        let min_input = u256_to_db(&quote.min_input);
        let max_input = u256_to_db(&quote.max_input);

        sqlx::query(
            r#"
            INSERT INTO quotes (
                id, 
                from_chain, from_token, from_decimals, from_amount,
                to_chain, to_token, to_decimals, to_amount,
                liquidity_fee_bps, protocol_fee_bps, network_fee_sats,
                fee_liquidity, fee_protocol, fee_network,
                min_input, max_input,
                affiliate,
                market_maker_id, 
                expires_at, 
                created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
            "#,
        )
        .bind(quote.id)
        .bind(from_chain)
        .bind(from_token)
        .bind(from_decimals as i16)
        .bind(from_amount)
        .bind(to_chain)
        .bind(to_token)
        .bind(to_decimals as i16)
        .bind(to_amount)
        .bind(quote.rates.liquidity_fee_bps as i64)
        .bind(quote.rates.protocol_fee_bps as i64)
        .bind(quote.rates.network_fee_sats as i64)
        .bind(fee_liquidity)
        .bind(fee_protocol)
        .bind(fee_network)
        .bind(min_input)
        .bind(max_input)
        .bind(&quote.affiliate)
        .bind(quote.market_maker_id)
        .bind(quote.expires_at)
        .bind(quote.created_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get(&self, id: Uuid) -> OtcServerResult<Quote> {
        let row = sqlx::query(
            r#"
            SELECT 
                id,
                from_chain, from_token, from_decimals, from_amount,
                to_chain, to_token, to_decimals, to_amount,
                liquidity_fee_bps, protocol_fee_bps, network_fee_sats,
                fee_liquidity, fee_protocol, fee_network,
                min_input, max_input,
                affiliate,
                market_maker_id,
                expires_at,
                created_at
            FROM quotes
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await?;

        Quote::from_row(&row)
    }

    pub async fn get_active_by_market_maker(
        &self,
        market_maker_id: Uuid,
    ) -> OtcServerResult<Vec<Quote>> {
        // Use application time to avoid DB/app clock skew issues
        let now = utc::now();
        let rows = sqlx::query(
            r#"
            SELECT 
                id,
                from_chain, from_token, from_decimals, from_amount,
                to_chain, to_token, to_decimals, to_amount,
                liquidity_fee_bps, protocol_fee_bps, network_fee_sats,
                fee_liquidity, fee_protocol, fee_network,
                min_input, max_input,
                affiliate,
                market_maker_id,
                expires_at,
                created_at
            FROM quotes
            WHERE market_maker_id = $1 
            AND expires_at > $2
            ORDER BY created_at DESC
            "#,
        )
        .bind(market_maker_id)
        .bind(now)
        .fetch_all(&self.pool)
        .await?;

        let mut quotes = Vec::new();
        for row in rows {
            quotes.push(Quote::from_row(&row)?);
        }

        Ok(quotes)
    }

    pub async fn get_expired(&self, limit: i64) -> OtcServerResult<Vec<Quote>> {
        let now = utc::now();

        let rows = sqlx::query(
            r#"
            SELECT 
                id,
                from_chain, from_token, from_decimals, from_amount,
                to_chain, to_token, to_decimals, to_amount,
                liquidity_fee_bps, protocol_fee_bps, network_fee_sats,
                fee_liquidity, fee_protocol, fee_network,
                min_input, max_input,
                affiliate,
                market_maker_id,
                expires_at,
                created_at
            FROM quotes
            WHERE expires_at <= $2
            ORDER BY expires_at ASC
            LIMIT $1
            "#,
        )
        .bind(limit)
        .bind(now)
        .fetch_all(&self.pool)
        .await?;

        let mut quotes = Vec::new();
        for row in rows {
            quotes.push(Quote::from_row(&row)?);
        }

        Ok(quotes)
    }

    pub async fn delete_expired(&self, before: DateTime<Utc>) -> OtcServerResult<u64> {
        let result = sqlx::query(
            r#"
            DELETE FROM quotes
            WHERE expires_at < $1
            AND id NOT IN (SELECT quote_id FROM swaps)
            "#,
        )
        .bind(before)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use crate::db::Database;
    use alloy::primitives::U256;
    use chrono::Duration;
    use otc_models::{ChainType, Currency, Fees, Lot, Quote, SwapRates, TokenIdentifier};
    use uuid::Uuid;

    fn make_test_quote(mm_id: Uuid, expires_at: chrono::DateTime<chrono::Utc>) -> Quote {
        Quote {
            id: Uuid::now_v7(),
            market_maker_id: mm_id,
            from: Lot {
                currency: Currency {
                    chain: ChainType::Bitcoin,
                    token: TokenIdentifier::Native,
                    decimals: 8,
                },
                amount: U256::from(1_000_000u64),
            },
            to: Lot {
                currency: Currency {
                    chain: ChainType::Ethereum,
                    token: TokenIdentifier::address(
                        "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf".to_string(),
                    ),
                    decimals: 8,
                },
                amount: U256::from(996_700u64),
            },
            rates: SwapRates::new(13, 10, 1000),
            fees: Fees {
                liquidity_fee: U256::from(1300u64),
                protocol_fee: U256::from(1000u64),
                network_fee: U256::from(1000u64),
            },
            min_input: U256::from(10_000u64),
            max_input: U256::from(100_000_000u64),
            affiliate: None,
            expires_at,
            created_at: utc::now(),
        }
    }

    #[sqlx::test]
    async fn test_quote_round_trip(pool: sqlx::PgPool) -> sqlx::Result<()> {
        // Database will auto-initialize with schema
        let db = Database::from_pool(pool.clone()).await.unwrap();
        let quote_repo = db.quotes();

        let original_quote = make_test_quote(Uuid::now_v7(), utc::now() + Duration::minutes(10));

        // Store the quote
        quote_repo.create(&original_quote).await.unwrap();

        // Retrieve the quote
        let retrieved_quote = quote_repo.get(original_quote.id).await.unwrap();

        // Validate all fields match
        assert_eq!(retrieved_quote.id, original_quote.id);
        assert_eq!(retrieved_quote.market_maker_id, original_quote.market_maker_id);

        // Validate from lot
        assert_eq!(retrieved_quote.from.currency.chain, original_quote.from.currency.chain);
        assert_eq!(retrieved_quote.from.currency.token, original_quote.from.currency.token);
        assert_eq!(retrieved_quote.from.amount, original_quote.from.amount);

        // Validate to lot
        assert_eq!(retrieved_quote.to.currency.chain, original_quote.to.currency.chain);
        assert_eq!(retrieved_quote.to.currency.token, original_quote.to.currency.token);
        assert_eq!(retrieved_quote.to.amount, original_quote.to.amount);

        // Validate rates
        assert_eq!(retrieved_quote.rates.liquidity_fee_bps, original_quote.rates.liquidity_fee_bps);
        assert_eq!(retrieved_quote.rates.protocol_fee_bps, original_quote.rates.protocol_fee_bps);
        assert_eq!(retrieved_quote.rates.network_fee_sats, original_quote.rates.network_fee_sats);

        // Validate fees
        assert_eq!(retrieved_quote.fees.liquidity_fee, original_quote.fees.liquidity_fee);
        assert_eq!(retrieved_quote.fees.protocol_fee, original_quote.fees.protocol_fee);
        assert_eq!(retrieved_quote.fees.network_fee, original_quote.fees.network_fee);

        // Validate bounds
        assert_eq!(retrieved_quote.min_input, original_quote.min_input);
        assert_eq!(retrieved_quote.max_input, original_quote.max_input);

        Ok(())
    }

    #[sqlx::test]
    async fn test_get_active_quotes_by_market_maker(pool: sqlx::PgPool) -> sqlx::Result<()> {
        let db = Database::from_pool(pool.clone()).await.unwrap();
        let quote_repo = db.quotes();

        let mm_id = Uuid::now_v7();

        // Create quotes - some expired, some active
        let expired_quote = make_test_quote(mm_id, utc::now() - Duration::hours(1));
        let active_quote1 = make_test_quote(mm_id, utc::now() + Duration::minutes(30));
        let active_quote2 = make_test_quote(mm_id, utc::now() + Duration::hours(1));

        // Store all quotes
        quote_repo.create(&expired_quote).await.unwrap();
        quote_repo.create(&active_quote1).await.unwrap();
        quote_repo.create(&active_quote2).await.unwrap();

        // Get active quotes
        let active_quotes = quote_repo.get_active_by_market_maker(mm_id).await.unwrap();

        // Should only return the two active quotes
        assert_eq!(active_quotes.len(), 2);

        let active_ids: Vec<Uuid> = active_quotes.iter().map(|q| q.id).collect();
        assert!(active_ids.contains(&active_quote1.id));
        assert!(active_ids.contains(&active_quote2.id));
        assert!(!active_ids.contains(&expired_quote.id));

        Ok(())
    }
}
