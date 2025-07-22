use chrono::{DateTime, Utc};
use otc_models::Quote;
use sqlx::postgres::PgPool;
use uuid::Uuid;

use super::conversions::currency_to_db;
use super::row_mappers::FromRow;
use super::DbResult;

#[derive(Clone)]
pub struct QuoteRepository {
    pool: PgPool,
}

impl QuoteRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    
    pub async fn create(&self, quote: &Quote) -> DbResult<()> {
        let (from_chain, from_token, from_amount, from_decimals) = currency_to_db(&quote.from)?;
        let (to_chain, to_token, to_amount, to_decimals) = currency_to_db(&quote.to)?;
        
        sqlx::query(
            r#"
            INSERT INTO quotes (
                id, 
                from_chain, from_token, from_amount, from_decimals,
                to_chain, to_token, to_amount, to_decimals,
                market_maker_identifier, 
                expires_at, 
                created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
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
        .bind(quote.market_maker_identifier.clone())
        .bind(quote.expires_at)
        .bind(quote.created_at)
        .execute(&self.pool)
        .await?;
        
        Ok(())
    }
    
    pub async fn get(&self, id: Uuid) -> DbResult<Quote> {
        let row = sqlx::query(
            r#"
            SELECT 
                id,
                from_chain, from_token, from_amount, from_decimals,
                to_chain, to_token, to_amount, to_decimals,
                market_maker_identifier,
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
    
    pub async fn get_active_by_market_maker(&self, mm_identifier: &str) -> DbResult<Vec<Quote>> {
        let rows = sqlx::query(
            r#"
            SELECT 
                id,
                from_chain, from_token, from_amount, from_decimals,
                to_chain, to_token, to_amount, to_decimals,
                market_maker_identifier,
                expires_at,
                created_at
            FROM quotes
            WHERE market_maker_identifier = $1 
            AND expires_at > NOW()
            ORDER BY created_at DESC
            "#,
        )
        .bind(mm_identifier)
        .fetch_all(&self.pool)
        .await?;
        
        let mut quotes = Vec::new();
        for row in rows {
            quotes.push(Quote::from_row(&row)?);
        }
        
        Ok(quotes)
    }
    
    pub async fn get_expired(&self, limit: i64) -> DbResult<Vec<Quote>> {
        let rows = sqlx::query(
            r#"
            SELECT 
                id,
                from_chain, from_token, from_amount, from_decimals,
                to_chain, to_token, to_amount, to_decimals,
                market_maker_identifier,
                expires_at,
                created_at
            FROM quotes
            WHERE expires_at <= NOW()
            ORDER BY expires_at ASC
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        
        let mut quotes = Vec::new();
        for row in rows {
            quotes.push(Quote::from_row(&row)?);
        }
        
        Ok(quotes)
    }
    
    pub async fn delete_expired(&self, before: DateTime<Utc>) -> DbResult<u64> {
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
    use alloy::primitives::U256;
    use chrono::{Duration, Utc};
    use otc_models::{ChainType, Currency, Quote, TokenIdentifier};
    use crate::db::Database;
    use uuid::Uuid;

    #[sqlx::test]
    async fn test_quote_round_trip(pool: sqlx::PgPool) -> sqlx::Result<()> {
        // Database will auto-initialize with schema
        let db = Database::from_pool(pool.clone()).await.unwrap();
        let quote_repo = db.quotes();
        
        // Create a test quote
        let original_quote = Quote {
            id: Uuid::new_v4(),
            from: Currency {
                chain: ChainType::Bitcoin,
                token: TokenIdentifier::Native,
                amount: U256::from(1000000u64), // 0.01 BTC in sats
                decimals: 8,
            },
            to: Currency {
                chain: ChainType::Ethereum,
                token: TokenIdentifier::Native,
                amount: U256::from(500000000000000000u64), // 0.5 ETH in wei
                decimals: 18,
            },
            market_maker_identifier: "test-mm-1".to_string(),
            expires_at: Utc::now() + Duration::minutes(10),
            created_at: Utc::now(),
        };
        
        // Store the quote
        quote_repo.create(&original_quote).await.unwrap();
        
        // Retrieve the quote
        let retrieved_quote = quote_repo.get(original_quote.id).await.unwrap();
        
        // Validate all fields match
        assert_eq!(retrieved_quote.id, original_quote.id);
        assert_eq!(retrieved_quote.market_maker_identifier, original_quote.market_maker_identifier);
        
        // Validate from currency
        assert_eq!(retrieved_quote.from.chain, original_quote.from.chain);
        assert_eq!(retrieved_quote.from.token, original_quote.from.token);
        assert_eq!(retrieved_quote.from.amount, original_quote.from.amount);
        
        // Validate to currency
        assert_eq!(retrieved_quote.to.chain, original_quote.to.chain);
        assert_eq!(retrieved_quote.to.token, original_quote.to.token);
        assert_eq!(retrieved_quote.to.amount, original_quote.to.amount);
        
        // Validate timestamps (with some tolerance for DB precision)
        assert!((retrieved_quote.expires_at - original_quote.expires_at).num_seconds().abs() < 1);
        assert!((retrieved_quote.created_at - original_quote.created_at).num_seconds().abs() < 1);
        
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
            from: Currency {
                chain: ChainType::Ethereum,
                token: TokenIdentifier::Address("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".to_string()), // USDC
                amount: U256::from(1000000000u64), // 1000 USDC (6 decimals)
                decimals: 6,
            },
            to: Currency {
                chain: ChainType::Ethereum,
                token: TokenIdentifier::Address("0x6B175474E89094C44Da98b954EedeAC495271d0F".to_string()), // DAI
                amount: U256::from(1000000000000000000000u128), // 1000 DAI (18 decimals)
                decimals: 18,
            },
            market_maker_identifier: "test-mm-2".to_string(),
            expires_at: Utc::now() + Duration::minutes(5),
            created_at: Utc::now(),
        };
        
        // Store and retrieve
        quote_repo.create(&original_quote).await.unwrap();
        let retrieved_quote = quote_repo.get(original_quote.id).await.unwrap();
        
        // Validate token addresses are preserved
        match (&retrieved_quote.from.token, &original_quote.from.token) {
            (TokenIdentifier::Address(retrieved), TokenIdentifier::Address(original)) => {
                assert_eq!(retrieved, original);
            }
            _ => panic!("Token identifier type mismatch"),
        }
        
        match (&retrieved_quote.to.token, &original_quote.to.token) {
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
            10
        ).unwrap(); // Max U256 value
        
        let original_quote = Quote {
            id: Uuid::new_v4(),
            from: Currency {
                chain: ChainType::Ethereum,
                token: TokenIdentifier::Native,
                amount: large_amount,
                decimals: 18,
            },
            to: Currency {
                chain: ChainType::Bitcoin,
                token: TokenIdentifier::Native,
                amount: U256::from(21000000u64) * U256::from(100000000u64), // 21M BTC in sats
                decimals: 8,
            },
            market_maker_identifier: "test-mm-3".to_string(),
            expires_at: Utc::now() + Duration::hours(1),
            created_at: Utc::now(),
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
        
        let mm_identifier = "test-mm-active";
        
        // Create multiple quotes - some expired, some active
        let expired_quote = Quote {
            id: Uuid::new_v4(),
            from: Currency {
                chain: ChainType::Bitcoin,
                token: TokenIdentifier::Native,
                amount: U256::from(100000u64),
                decimals: 8,
            },
            to: Currency {
                chain: ChainType::Ethereum,
                token: TokenIdentifier::Native,
                amount: U256::from(1000000000000000000u64),
                decimals: 18,
            },
            market_maker_identifier: mm_identifier.to_string(),
            expires_at: Utc::now() - Duration::hours(1), // Already expired
            created_at: Utc::now() - Duration::hours(2),
        };
        
        let active_quote1 = Quote {
            id: Uuid::new_v4(),
            from: Currency {
                chain: ChainType::Bitcoin,
                token: TokenIdentifier::Native,
                amount: U256::from(200000u64),
                decimals: 8,
            },
            to: Currency {
                chain: ChainType::Ethereum,
                token: TokenIdentifier::Native,
                amount: U256::from(2000000000000000000u64),
                decimals: 18,
            },
            market_maker_identifier: mm_identifier.to_string(),
            expires_at: Utc::now() + Duration::minutes(30),
            created_at: Utc::now(),
        };
        
        let active_quote2 = Quote {
            id: Uuid::new_v4(),
            from: Currency {
                chain: ChainType::Ethereum,
                token: TokenIdentifier::Native,
                amount: U256::from(3000000000000000000u64),
                decimals: 18,
            },
            to: Currency {
                chain: ChainType::Bitcoin,
                token: TokenIdentifier::Native,
                amount: U256::from(300000u64),
                decimals: 8,
            },
            market_maker_identifier: mm_identifier.to_string(),
            expires_at: Utc::now() + Duration::hours(1),
            created_at: Utc::now(),
        };
        
        // Store all quotes
        quote_repo.create(&expired_quote).await.unwrap();
        quote_repo.create(&active_quote1).await.unwrap();
        quote_repo.create(&active_quote2).await.unwrap();
        
        // Get active quotes
        let active_quotes = quote_repo.get_active_by_market_maker(mm_identifier).await.unwrap();
        
        // Should only return the two active quotes
        assert_eq!(active_quotes.len(), 2);
        
        let active_ids: Vec<Uuid> = active_quotes.iter().map(|q| q.id).collect();
        assert!(active_ids.contains(&active_quote1.id));
        assert!(active_ids.contains(&active_quote2.id));
        assert!(!active_ids.contains(&expired_quote.id));
        
        Ok(())
    }
}