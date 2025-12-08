use alloy::primitives::U256;
use chrono::Duration;
use market_maker::db::Database;
use otc_models::{ChainType, Currency, Quote, SwapRates, TokenIdentifier};
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use uuid::Uuid;

use crate::utils::PgConnectOptionsExt;

#[sqlx::test]
async fn test_quote_storage_round_trip(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) -> sqlx::Result<()> {
    let database = Database::connect(&connect_options.to_database_url(), 10, 2)
        .await
        .expect("Failed to connect to database");
    let storage = database.quotes();

    let original_quote = Quote {
        id: Uuid::new_v4(),
        market_maker_id: Uuid::new_v4(),
        from_currency: Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        },
        to_currency: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::Native,
            decimals: 18,
        },
        rates: SwapRates::new(13, 10, 1000),
        min_input: U256::from(10_000u64),
        max_input: U256::from(100_000_000u64),
        expires_at: utc::now() + Duration::minutes(10),
        created_at: utc::now(),
    };

    storage
        .store_quote(&original_quote)
        .await
        .expect("Failed to store quote");

    let retrieved_quote = storage
        .get_quote(original_quote.id)
        .await
        .expect("Failed to retrieve quote");

    assert_eq!(retrieved_quote.id, original_quote.id);
    assert_eq!(
        retrieved_quote.market_maker_id,
        original_quote.market_maker_id
    );
    assert_eq!(retrieved_quote.min_input, original_quote.min_input);
    assert_eq!(retrieved_quote.max_input, original_quote.max_input);
    assert_eq!(retrieved_quote.rates, original_quote.rates);

    Ok(())
}
