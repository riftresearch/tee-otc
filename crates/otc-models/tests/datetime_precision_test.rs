use alloy::primitives::U256;
use chrono::{DateTime};
use otc_models::{ChainType, Currency, FeeSchedule, Lot, Quote, TokenIdentifier};
use uuid::Uuid;

#[test]
fn test_datetime_precision_affects_hash() {
    // Create a datetime with nanosecond precision
    let now = utc::now();

    // Simulate PostgreSQL round-trip (microsecond precision)
    let timestamp_micros = now.timestamp_micros();
    let restored_datetime = DateTime::from_timestamp_micros(timestamp_micros).unwrap();

    // Serialize both to JSON
    let json1 = serde_json::to_string(&now).unwrap();
    let json2 = serde_json::to_string(&restored_datetime).unwrap();

    println!("Original datetime JSON: {}", json1);
    println!("Restored datetime JSON: {}", json2);
    println!("Are they equal? {}", json1 == json2);

    // Now create two identical quotes except for timestamp precision
    let quote1 = Quote {
        id: Uuid::new_v4(),
        market_maker_id: Uuid::new_v4(),
        from: Lot {
            currency: Currency {
                chain: ChainType::Bitcoin,
                token: TokenIdentifier::Native,
                decimals: 8,
            },
            amount: U256::from(100000000u64),
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
            network_fee_sats: 1000,
            liquidity_fee_sats: 2000,
            protocol_fee_sats: 500,
        },
        expires_at: now,
        created_at: now,
    };

    let mut quote2 = quote1.clone();
    quote2.expires_at = restored_datetime;
    quote2.created_at = restored_datetime;

    let hash1 = quote1.hash();
    let hash2 = quote2.hash();

    println!("\nQuote 1 hash: {:?}", hash1);
    println!("Quote 2 hash: {:?}", hash2);
    println!("Hashes equal? {}", hash1 == hash2);

    // After the fix, hashes should be equal even when timestamps have different precision
    // because the hash function normalizes timestamps to microsecond precision
    assert_eq!(
        hash1, hash2,
        "Hashes should be equal after normalization to microsecond precision"
    );
}
