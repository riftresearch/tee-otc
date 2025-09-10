use alloy::primitives::U256;
use chrono::{Duration, Utc};
use market_maker::quote_storage::QuoteStorage;
use otc_models::{ChainType, Currency, Lot, Quote, TokenIdentifier};
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::utils::PgConnectOptionsExt;

#[sqlx::test]
async fn test_quote_storage_round_trip(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) -> sqlx::Result<()> {
    let mut join_set = JoinSet::new();
    let storage = QuoteStorage::new(&connect_options.to_database_url(), &mut join_set)
        .await
        .expect("Failed to create storage");

    let original_quote = Quote {
        id: Uuid::new_v4(),
        market_maker_id: Uuid::new_v4(),
        from: Lot {
            currency: Currency {
                chain: ChainType::Bitcoin,
                token: TokenIdentifier::Native,
                decimals: 8,
            },
            amount: U256::from(1000000u64),
        },
        to: Lot {
            currency: Currency {
                chain: ChainType::Ethereum,
                token: TokenIdentifier::Native,
                decimals: 18,
            },
            amount: U256::from(500000000000000000u64),
        },
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
    assert_eq!(retrieved_quote.from.amount, original_quote.from.amount);
    assert_eq!(retrieved_quote.to.amount, original_quote.to.amount);

    Ok(())
}
