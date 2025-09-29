use alloy::primitives::U256;
use market_maker::deposit_key_storage::{DepositKeyStorage, DepositKeyStorageTrait};
use otc_models::{ChainType, Currency, Lot, TokenIdentifier};
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};

use crate::utils::PgConnectOptionsExt;
use market_maker::deposit_key_storage::{Deposit, FillStatus};

#[sqlx::test]
async fn test_deposit_vault_consumes_on_fill(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) -> sqlx::Result<()> {
    // Initialize storage (runs migrations under the hood)
    let vault = DepositKeyStorage::new(&connect_options.to_database_url(), 10, 2)
        .await
        .expect("Failed to create deposit key storage");

    // Helper to build ETH-native lots
    let eth_native = |amt: u64| Lot {
        currency: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::Native,
            decimals: 18,
        },
        amount: U256::from(amt),
    };

    // Prefund with two deposits: 5 and 7 (total 12)
    // A 10-lot should reserve both and consume all available deposits.
    vault
        .store_deposit(&Deposit::new("k1", eth_native(5), "tx1"))
        .await
        .expect("store k1");
    vault
        .store_deposit(&Deposit::new("k2", eth_native(7), "tx2"))
        .await
        .expect("store k2");

    // Request a 10-lot: expect Full and two deposits reserved
    let lot_eth_10 = eth_native(10);
    let res1 = vault
        .take_deposits_that_fill_lot(&lot_eth_10)
        .await
        .expect("reservation should succeed");

    match res1 {
        FillStatus::Full(ds) => {
            assert_eq!(ds.len(), 2, "should reserve both deposits for 10");
            let sum: U256 = ds
                .iter()
                .fold(U256::from(0u8), |acc, d| acc + d.holdings().amount);
            assert!(sum >= U256::from(10u64));
        }
        other => panic!("expected Full, got {other:?}"),
    }

    // Same request again should find no available deposits left
    let res2 = vault
        .take_deposits_that_fill_lot(&lot_eth_10)
        .await
        .expect("second reservation call should succeed");
    match res2 {
        FillStatus::Empty => {}
        other => panic!("expected Empty on second call, got {other:?}"),
    }

    Ok(())
}

#[sqlx::test]
async fn test_deposit_key_vault_take_deposits(
    _: PoolOptions<sqlx::Postgres>,
    connect_options: PgConnectOptions,
) -> sqlx::Result<()> {
    // Initialize vault (runs migrations)
    let vault = DepositKeyStorage::new(&connect_options.to_database_url(), 10, 2)
        .await
        .expect("Failed to create deposit key vault");

    // Seed ETH deposits via public API: 5, 7, 20 (total 32)
    let eth = |amt: u64| Lot {
        currency: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::Native,
            decimals: 18,
        },
        amount: U256::from(amt),
    };
    vault
        .store_deposit(&market_maker::deposit_key_storage::Deposit::new(
            "k1",
            eth(5),
            "tx1",
        ))
        .await
        .expect("store k1");
    vault
        .store_deposit(&market_maker::deposit_key_storage::Deposit::new(
            "k2",
            eth(7),
            "tx2",
        ))
        .await
        .expect("store k2");
    vault
        .store_deposit(&market_maker::deposit_key_storage::Deposit::new(
            "k3",
            eth(20),
            "tx3",
        ))
        .await
        .expect("store k3");

    // Take deposits to reach at least 10 => expect 5 + 7 = 12 reserved
    let lot_eth_10 = Lot {
        currency: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::Native,
            decimals: 18,
        },
        amount: U256::from(10u64),
    };
    let res1 = vault
        .take_deposits_that_fill_lot(&lot_eth_10)
        .await
        .expect("reservation should succeed");

    // Should be Full and reserve two rows (5 and 7); verify count and sum
    match res1 {
        market_maker::deposit_key_storage::FillStatus::Full(ds) => {
            assert_eq!(ds.len(), 2, "should reserve two deposits for 10");
            let sum: U256 = ds
                .iter()
                .fold(U256::from(0u8), |acc, d| acc + d.holdings().amount);
            assert!(sum >= U256::from(10u64));
        }
        other => panic!("expected Full, got {other:?}"),
    }

    // Take again for 10 => should take the remaining 20 as a single deposit
    let res2 = vault
        .take_deposits_that_fill_lot(&lot_eth_10)
        .await
        .expect("second reservation should succeed");
    match res2 {
        market_maker::deposit_key_storage::FillStatus::Full(ds) => {
            assert_eq!(ds.len(), 1, "should reserve one deposit for second 10");
            let sum: U256 = ds
                .iter()
                .fold(U256::from(0u8), |acc, d| acc + d.holdings().amount);
            assert!(sum >= U256::from(10u64));
        }
        other => panic!("expected Full on second reserve, got {other:?}"),
    }

    // Third take should return Empty
    let res3 = vault
        .take_deposits_that_fill_lot(&lot_eth_10)
        .await
        .expect("third reservation call should succeed");
    match res3 {
        market_maker::deposit_key_storage::FillStatus::Empty => {}
        other => panic!("expected Empty, got {other:?}"),
    }

    // Seed BTC deposits via API to test Partial
    let btc = |amt: u64| Lot {
        currency: Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        },
        amount: U256::from(amt),
    };
    vault
        .store_deposit(&market_maker::deposit_key_storage::Deposit::new(
            "b1",
            btc(3),
            "tx4",
        ))
        .await
        .expect("store b1");
    vault
        .store_deposit(&market_maker::deposit_key_storage::Deposit::new(
            "b2",
            btc(4),
            "tx5",
        ))
        .await
        .expect("store b2");

    let lot_btc_10 = Lot {
        currency: Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        },
        amount: U256::from(10u64),
    };
    let res4 = vault
        .take_deposits_that_fill_lot(&lot_btc_10)
        .await
        .expect("reservation should succeed");
    match res4 {
        market_maker::deposit_key_storage::FillStatus::Partial(ds) => {
            assert_eq!(
                ds.len(),
                2,
                "should reserve both deposits but still partial"
            );
            let sum: U256 = ds
                .iter()
                .fold(U256::from(0u8), |acc, d| acc + d.holdings().amount);
            assert!(sum < U256::from(10u64));
        }
        other => panic!("expected Partial, got {other:?}"),
    }

    Ok(())
}
