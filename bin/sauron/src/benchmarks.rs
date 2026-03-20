use std::sync::Arc;

use alloy::primitives::U256;
use chrono::{DateTime, TimeDelta, Utc};
use otc_models::{ChainType, TokenIdentifier};
use tokio::runtime::Runtime;
use uuid::Uuid;

use crate::watch::{SharedWatchEntry, WatchEntry, WatchStore};

pub const LARGE_WATCH_SET_SIZES: [usize; 2] = [10_000, 50_000];
pub const DEFAULT_BITCOIN_INDEXED_LOOKUP_CONCURRENCY: usize = 32;
pub const DEFAULT_EVM_INDEXED_LOOKUP_CONCURRENCY: usize = 8;

pub fn benchmark_runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime should build")
}

pub fn make_mixed_watch_entries(count: usize) -> Vec<WatchEntry> {
    let created_at = benchmark_created_at();
    let deposit_deadline = created_at + TimeDelta::hours(2);

    (0..count)
        .map(|index| {
            let source_chain = match index % 3 {
                0 => ChainType::Bitcoin,
                1 => ChainType::Ethereum,
                _ => ChainType::Base,
            };
            let source_token = benchmark_token(source_chain);
            let address = benchmark_address(source_chain, index, 1);

            WatchEntry {
                swap_id: Uuid::from_u128((index + 1) as u128),
                source_chain,
                source_token,
                address,
                min_amount: U256::from(1_u64),
                max_amount: U256::from(100_000_u64),
                deposit_deadline,
                created_at,
                updated_at: created_at + TimeDelta::milliseconds(index as i64),
            }
        })
        .collect()
}

pub fn make_single_chain_shared_watch_entries(
    count: usize,
    chain: ChainType,
) -> Vec<SharedWatchEntry> {
    let created_at = benchmark_created_at();
    let deposit_deadline = created_at + TimeDelta::hours(2);
    let source_token = benchmark_token(chain);

    (0..count)
        .map(|index| {
            let address = benchmark_address(chain, index, 100_000);

            Arc::new(WatchEntry {
                swap_id: Uuid::from_u128((index + 1) as u128),
                source_chain: chain,
                source_token: source_token.clone(),
                address,
                min_amount: U256::from(1_u64),
                max_amount: U256::from(100_000_u64),
                deposit_deadline,
                created_at,
                updated_at: created_at + TimeDelta::milliseconds(index as i64),
            })
        })
        .collect()
}

pub async fn populate_watch_store(entries: Vec<WatchEntry>) -> WatchStore {
    let store = WatchStore::default();
    store.replace_all(entries).await;
    store
}

fn benchmark_created_at() -> DateTime<Utc> {
    utc::now()
}

fn benchmark_token(chain: ChainType) -> TokenIdentifier {
    match chain {
        ChainType::Bitcoin => TokenIdentifier::Native,
        ChainType::Ethereum | ChainType::Base => {
            TokenIdentifier::address("0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf")
        }
    }
}

fn benchmark_address(chain: ChainType, index: usize, offset: usize) -> String {
    match chain {
        ChainType::Bitcoin => format!("btc-bench-address-{index:06}"),
        ChainType::Ethereum | ChainType::Base => format!("0x{:040x}", index + offset),
    }
}
