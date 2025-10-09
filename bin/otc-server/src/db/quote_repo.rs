use chrono::{DateTime, Utc};
use otc_models::Quote;
use sqlx::postgres::PgPool;
use uuid::Uuid;

use crate::error::OtcServerResult;

use super::conversions::{fee_schedule_to_json, lot_to_db};
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
        let (from_chain, from_token, from_amount, from_decimals) = lot_to_db(&quote.from)?;
        let (to_chain, to_token, to_amount, to_decimals) = lot_to_db(&quote.to)?;
        let fee_schedule_json = fee_schedule_to_json(&quote.fee_schedule)?;

        sqlx::query(
            r#"
            INSERT INTO quotes (
                id, 
                from_chain, from_token, from_amount, from_decimals,
                to_chain, to_token, to_amount, to_decimals,
                fee_schedule,
                market_maker_id, 
                expires_at, 
                created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            "#,
        )
        .bind(quote.id)
        .bind(from_chain)
        .bind(from_token)
        .bind(from_amount)
        .bind(from_decimals as i16)
        .bind(to_chain)
        .bind(to_token)
        .bind(to_amount)
        .bind(to_decimals as i16)
        .bind(fee_schedule_json)
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
                from_chain, from_token, from_amount, from_decimals,
                to_chain, to_token, to_amount, to_decimals,
                fee_schedule,
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
                from_chain, from_token, from_amount, from_decimals,
                to_chain, to_token, to_amount, to_decimals,
                fee_schedule,
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
                from_chain, from_token, from_amount, from_decimals,
                to_chain, to_token, to_amount, to_decimals,
                fee_schedule,
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
    use otc_models::{ChainType, Currency, FeeSchedule, Lot, Quote, TokenIdentifier};
    use uuid::Uuid;

    #[sqlx::test]
    async fn test_quote_round_trip(pool: sqlx::PgPool) -> sqlx::Result<()> {
        // Database will auto-initialize with schema
        let db = Database::from_pool(pool.clone()).await.unwrap();
        let quote_repo = db.quotes();

        // Create a test quote
        let original_quote = Quote {
            id: Uuid::new_v4(),
            from: Lot {
                currency: Currency {
                    chain: ChainType::Bitcoin,
                    token: TokenIdentifier::Native,
                    decimals: 8,
                },
                amount: U256::from(1000000u64), // 0.01 BTC in sats
            },
            to: Lot {
                currency: Currency {
                    chain: ChainType::Ethereum,
                    token: TokenIdentifier::Native,
                    decimals: 18,
                },
                amount: U256::from(500000000000000000u64), // 0.5 ETH in wei
            },
            fee_schedule: FeeSchedule {
                network_fee_sats: 1000,
                liquidity_fee_sats: 2000,
                protocol_fee_sats: 500,
            },
            market_maker_id: Uuid::new_v4(),
            expires_at: utc::now() + Duration::minutes(10),
            created_at: utc::now(),
        };

        // Store the quote
        quote_repo.create(&original_quote).await.unwrap();

        // Retrieve the quote
        let retrieved_quote = quote_repo.get(original_quote.id).await.unwrap();

        // Validate all fields match
        assert_eq!(retrieved_quote.id, original_quote.id);
        assert_eq!(
            retrieved_quote.market_maker_id,
            original_quote.market_maker_id
        );

        // Validate from currency
        assert_eq!(
            retrieved_quote.from.currency.chain,
            original_quote.from.currency.chain
        );
        assert_eq!(
            retrieved_quote.from.currency.token,
            original_quote.from.currency.token
        );
        assert_eq!(retrieved_quote.from.amount, original_quote.from.amount);

        // Validate to currency
        assert_eq!(
            retrieved_quote.to.currency.chain,
            original_quote.to.currency.chain
        );
        assert_eq!(
            retrieved_quote.to.currency.token,
            original_quote.to.currency.token
        );
        assert_eq!(retrieved_quote.to.amount, original_quote.to.amount);
        assert_eq!(retrieved_quote.fee_schedule, original_quote.fee_schedule);

        // Validate timestamps (with some tolerance for DB precision)
        assert!(
            (retrieved_quote.expires_at - original_quote.expires_at)
                .num_seconds()
                .abs()
                < 1
        );
        assert!(
            (retrieved_quote.created_at - original_quote.created_at)
                .num_seconds()
                .abs()
                < 1
        );

        Ok(())
    }

    #[sqlx::test]
    async fn test_quote_with_token_address(pool: sqlx::PgPool) -> sqlx::Result<()> {
        // Database will auto-initialize with schema
        let db = Database::from_pool(pool.clone()).await.unwrap();
        let quote_repo = db.quotes();

        // Create a quote with ERC20 tokens
        let original_quote = Quote {
            id: Uuid::new_v4(),
            from: Lot {
                currency: Currency {
                    chain: ChainType::Ethereum,
                    token: TokenIdentifier::Address(
                        "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".to_string(),
                    ), // USDC
                    decimals: 6,
                },
                amount: U256::from(1000000000u64), // 1000 USDC (6 decimals)
            },
            to: Lot {
                currency: Currency {
                    chain: ChainType::Ethereum,
                    token: TokenIdentifier::Address(
                        "0x6B175474E89094C44Da98b954EedeAC495271d0F".to_string(),
                    ), // DAI
                    decimals: 18,
                },
                amount: U256::from(1000000000000000000000u128), // 1000 DAI (18 decimals)
            },
            fee_schedule: FeeSchedule {
                network_fee_sats: 1500,
                liquidity_fee_sats: 2500,
                protocol_fee_sats: 750,
            },
            market_maker_id: Uuid::new_v4(),
            expires_at: utc::now() + Duration::minutes(5),
            created_at: utc::now(),
        };

        // Store and retrieve
        quote_repo.create(&original_quote).await.unwrap();
        let retrieved_quote = quote_repo.get(original_quote.id).await.unwrap();

        // Validate token addresses are preserved
        match (
            &retrieved_quote.from.currency.token,
            &original_quote.from.currency.token,
        ) {
            (TokenIdentifier::Address(retrieved), TokenIdentifier::Address(original)) => {
                assert_eq!(retrieved, original);
            }
            _ => panic!("Token identifier type mismatch"),
        }

        match (
            &retrieved_quote.to.currency.token,
            &original_quote.to.currency.token,
        ) {
            (TokenIdentifier::Address(retrieved), TokenIdentifier::Address(original)) => {
                assert_eq!(retrieved, original);
            }
            _ => panic!("Token identifier type mismatch"),
        }

        Ok(())
    }

    #[sqlx::test]
    async fn test_quote_large_u256_values(pool: sqlx::PgPool) -> sqlx::Result<()> {
        // Database will auto-initialize with schema
        let db = Database::from_pool(pool.clone()).await.unwrap();
        let quote_repo = db.quotes();

        // Create a quote with very large U256 values
        let large_amount = U256::from_str_radix(
            "115792089237316195423570985008687907853269984665640564039457584007913129639935",
            10,
        )
        .unwrap(); // Max U256 value

        let original_quote = Quote {
            id: Uuid::new_v4(),
            from: Lot {
                currency: Currency {
                    chain: ChainType::Ethereum,
                    token: TokenIdentifier::Native,
                    decimals: 18,
                },
                amount: large_amount,
            },
            to: Lot {
                currency: Currency {
                    chain: ChainType::Bitcoin,
                    token: TokenIdentifier::Native,
                    decimals: 8,
                },
                amount: U256::from(21000000u64) * U256::from(100000000u64), // 21M BTC in sats
            },
            fee_schedule: FeeSchedule {
                network_fee_sats: 5000,
                liquidity_fee_sats: 6000,
                protocol_fee_sats: 2000,
            },
            market_maker_id: Uuid::new_v4(),
            expires_at: utc::now() + Duration::hours(1),
            created_at: utc::now(),
        };

        // Store and retrieve
        quote_repo.create(&original_quote).await.unwrap();
        let retrieved_quote = quote_repo.get(original_quote.id).await.unwrap();

        // Validate large values are preserved exactly
        assert_eq!(retrieved_quote.from.amount, original_quote.from.amount);
        assert_eq!(retrieved_quote.to.amount, original_quote.to.amount);

        Ok(())
    }

    #[sqlx::test]
    async fn test_get_active_quotes_by_market_maker(pool: sqlx::PgPool) -> sqlx::Result<()> {
        // Database will auto-initialize with schema
        let db = Database::from_pool(pool.clone()).await.unwrap();
        let quote_repo = db.quotes();

        let mm_identifier = Uuid::new_v4();

        // Create multiple quotes - some expired, some active
        let expired_quote = Quote {
            id: Uuid::new_v4(),
            from: Lot {
                currency: Currency {
                    chain: ChainType::Bitcoin,
                    token: TokenIdentifier::Native,
                    decimals: 8,
                },
                amount: U256::from(100000u64),
            },
            to: Lot {
                currency: Currency {
                    chain: ChainType::Ethereum,
                    token: TokenIdentifier::Native,
                    decimals: 18,
                },
                amount: U256::from(1000000000000000000u64),
            },
            fee_schedule: FeeSchedule {
                network_fee_sats: 100,
                liquidity_fee_sats: 200,
                protocol_fee_sats: 50,
            },
            market_maker_id: mm_identifier,
            expires_at: utc::now() - Duration::hours(1), // Already expired
            created_at: utc::now() - Duration::hours(2),
        };

        let active_quote1 = Quote {
            id: Uuid::new_v4(),
            from: Lot {
                currency: Currency {
                    chain: ChainType::Bitcoin,
                    token: TokenIdentifier::Native,
                    decimals: 8,
                },
                amount: U256::from(200000u64),
            },
            to: Lot {
                currency: Currency {
                    chain: ChainType::Ethereum,
                    token: TokenIdentifier::Native,
                    decimals: 18,
                },
                amount: U256::from(2000000000000000000u64),
            },
            fee_schedule: FeeSchedule {
                network_fee_sats: 150,
                liquidity_fee_sats: 250,
                protocol_fee_sats: 75,
            },
            market_maker_id: mm_identifier,
            expires_at: utc::now() + Duration::minutes(30),
            created_at: utc::now(),
        };

        let active_quote2 = Quote {
            id: Uuid::new_v4(),
            from: Lot {
                currency: Currency {
                    chain: ChainType::Ethereum,
                    token: TokenIdentifier::Native,
                    decimals: 18,
                },
                amount: U256::from(3000000000000000000u64),
            },
            to: Lot {
                currency: Currency {
                    chain: ChainType::Bitcoin,
                    token: TokenIdentifier::Native,
                    decimals: 8,
                },
                amount: U256::from(300000u64),
            },
            fee_schedule: FeeSchedule {
                network_fee_sats: 175,
                liquidity_fee_sats: 275,
                protocol_fee_sats: 95,
            },
            market_maker_id: mm_identifier,
            expires_at: utc::now() + Duration::hours(1),
            created_at: utc::now(),
        };

        // Store all quotes
        quote_repo.create(&expired_quote).await.unwrap();
        quote_repo.create(&active_quote1).await.unwrap();
        quote_repo.create(&active_quote2).await.unwrap();

        // Get active quotes
        let active_quotes = quote_repo
            .get_active_by_market_maker(mm_identifier)
            .await
            .unwrap();

        // Should only return the two active quotes
        assert_eq!(active_quotes.len(), 2);

        let active_ids: Vec<Uuid> = active_quotes.iter().map(|q| q.id).collect();
        assert!(active_ids.contains(&active_quote1.id));
        assert!(active_ids.contains(&active_quote2.id));
        assert!(!active_ids.contains(&expired_quote.id));

        Ok(())
    }
}
