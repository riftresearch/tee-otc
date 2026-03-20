use std::{collections::HashMap, str::FromStr, sync::Arc, time::Instant};

use alloy::primitives::U256;
use chrono::{DateTime, Months, Utc};
use metrics::{counter, gauge, histogram};
use otc_models::{ChainType, TokenIdentifier};
use serde::Deserialize;
use snafu::ResultExt;
use sqlx::{postgres::PgNotification, PgPool, Row};
use tokio::sync::RwLock;
use tracing::info;
use uuid::Uuid;

use crate::error::{NotificationPayloadSnafu, ReplicaDatabaseQuerySnafu, Result};

const FULL_WATCH_QUERY: &str = r#"
SELECT
  s.id::text AS swap_id,
  q.from_chain AS from_chain,
  q.from_token AS from_token,
  s.deposit_vault_address AS address,
  q.min_input::text AS min_amount,
  q.max_input::text AS max_amount,
  s.created_at + INTERVAL '15 months' AS deposit_deadline,
  s.created_at AS created_at,
  s.updated_at AS updated_at
FROM public.swaps s
JOIN public.quotes q ON q.id = s.quote_id
WHERE s.status = 'waiting_user_deposit_initiated'
  AND s.created_at >= $1
ORDER BY s.updated_at ASC, s.id ASC
"#;

const TARGETED_WATCH_QUERY: &str = r#"
SELECT
  s.id::text AS swap_id,
  q.from_chain AS from_chain,
  q.from_token AS from_token,
  s.deposit_vault_address AS address,
  q.min_input::text AS min_amount,
  q.max_input::text AS max_amount,
  s.created_at + INTERVAL '15 months' AS deposit_deadline,
  s.created_at AS created_at,
  s.updated_at AS updated_at
FROM public.swaps s
JOIN public.quotes q ON q.id = s.quote_id
WHERE s.id = $1::uuid
  AND s.status = 'waiting_user_deposit_initiated'
  AND s.created_at >= $2
"#;

const SAURON_WATCH_COUNT_METRIC: &str = "sauron_watch_count";
const SAURON_WATCH_FULL_RECONCILE_DURATION_SECONDS: &str =
    "sauron_watch_full_reconcile_duration_seconds";
const SAURON_WATCH_REPOSITORY_LOAD_ALL_DURATION_SECONDS: &str =
    "sauron_watch_repository_load_all_duration_seconds";
const SAURON_WATCH_REPOSITORY_LOAD_SWAP_DURATION_SECONDS: &str =
    "sauron_watch_repository_load_swap_duration_seconds";
const SAURON_WATCH_SNAPSHOT_DURATION_SECONDS: &str = "sauron_watch_snapshot_duration_seconds";
const SAURON_WATCH_EXPIRED_PRUNED_TOTAL: &str = "sauron_watch_expired_pruned_total";

#[derive(Debug, Clone)]
pub struct WatchEntry {
    pub swap_id: Uuid,
    pub source_chain: ChainType,
    pub source_token: TokenIdentifier,
    pub address: String,
    pub min_amount: U256,
    pub max_amount: U256,
    pub deposit_deadline: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub type SharedWatchEntry = Arc<WatchEntry>;

#[derive(Debug, Default, Clone)]
pub struct WatchStore {
    inner: Arc<RwLock<WatchState>>,
}

#[derive(Debug, Default)]
struct WatchState {
    by_swap_id: HashMap<Uuid, SharedWatchEntry>,
    by_chain: HashMap<ChainType, HashMap<Uuid, SharedWatchEntry>>,
}

impl WatchStore {
    pub async fn replace_all(&self, entries: Vec<WatchEntry>) {
        let mut state = self.inner.write().await;
        let mut by_swap_id = HashMap::with_capacity(entries.len());
        let mut by_chain: HashMap<ChainType, HashMap<Uuid, SharedWatchEntry>> = HashMap::new();

        for entry in entries {
            let entry = Arc::new(entry);
            by_chain
                .entry(entry.source_chain)
                .or_default()
                .insert(entry.swap_id, entry.clone());
            by_swap_id.insert(entry.swap_id, entry);
        }

        state.by_swap_id = by_swap_id;
        state.by_chain = by_chain;
        gauge!(SAURON_WATCH_COUNT_METRIC).set(state.by_swap_id.len() as f64);
    }

    pub async fn upsert(&self, entry: WatchEntry) {
        let mut state = self.inner.write().await;
        remove_entry(&mut state, entry.swap_id);

        let entry = Arc::new(entry);
        state
            .by_chain
            .entry(entry.source_chain)
            .or_default()
            .insert(entry.swap_id, entry.clone());
        state.by_swap_id.insert(entry.swap_id, entry);
        gauge!(SAURON_WATCH_COUNT_METRIC).set(state.by_swap_id.len() as f64);
    }

    pub async fn remove(&self, swap_id: Uuid) {
        let mut state = self.inner.write().await;
        remove_entry(&mut state, swap_id);
        gauge!(SAURON_WATCH_COUNT_METRIC).set(state.by_swap_id.len() as f64);
    }

    pub async fn len(&self) -> usize {
        self.inner.read().await.by_swap_id.len()
    }

    pub async fn snapshot_for_chain(&self, chain: ChainType) -> Vec<SharedWatchEntry> {
        let started = Instant::now();
        let now = utc::now();
        let snapshot = self
            .inner
            .read()
            .await
            .by_chain
            .get(&chain)
            .into_iter()
            .flat_map(|entries| entries.values())
            .filter(|watch| is_watch_active(watch.as_ref(), now))
            .cloned()
            .collect::<Vec<_>>();
        histogram!(
            SAURON_WATCH_SNAPSHOT_DURATION_SECONDS,
            "chain" => chain.to_db_string().to_string(),
        )
        .record(started.elapsed().as_secs_f64());
        snapshot
    }

    pub async fn prune_expired(&self) -> usize {
        let now = utc::now();
        let mut state = self.inner.write().await;
        let expired_swap_ids = state
            .by_swap_id
            .iter()
            .filter_map(|(swap_id, watch)| {
                (!is_watch_active(watch.as_ref(), now)).then_some(*swap_id)
            })
            .collect::<Vec<_>>();

        for swap_id in &expired_swap_ids {
            remove_entry(&mut state, *swap_id);
        }

        if !expired_swap_ids.is_empty() {
            counter!(SAURON_WATCH_EXPIRED_PRUNED_TOTAL).increment(expired_swap_ids.len() as u64);
        }
        gauge!(SAURON_WATCH_COUNT_METRIC).set(state.by_swap_id.len() as f64);
        expired_swap_ids.len()
    }
}

#[derive(Debug, Clone)]
pub struct WatchRepository {
    pool: PgPool,
}

impl WatchRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn load_all(&self) -> Result<Vec<WatchEntry>> {
        let cutoff_time = deposit_vault_watch_cutoff();
        let started = Instant::now();
        let rows = sqlx::query(FULL_WATCH_QUERY)
            .bind(cutoff_time)
            .fetch_all(&self.pool)
            .await
            .context(ReplicaDatabaseQuerySnafu)?;
        histogram!(SAURON_WATCH_REPOSITORY_LOAD_ALL_DURATION_SECONDS)
            .record(started.elapsed().as_secs_f64());

        rows.into_iter().map(parse_watch_row).collect()
    }

    pub async fn load_swap(&self, swap_id: Uuid) -> Result<Option<WatchEntry>> {
        let cutoff_time = deposit_vault_watch_cutoff();
        let started = Instant::now();
        let row = sqlx::query(TARGETED_WATCH_QUERY)
            .bind(swap_id)
            .bind(cutoff_time)
            .fetch_optional(&self.pool)
            .await
            .context(ReplicaDatabaseQuerySnafu)?;
        histogram!(SAURON_WATCH_REPOSITORY_LOAD_SWAP_DURATION_SECONDS)
            .record(started.elapsed().as_secs_f64());

        row.map(parse_watch_row).transpose()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WatchChangeNotification {
    pub swap_id: Uuid,
}

impl WatchChangeNotification {
    pub fn parse(notification: &PgNotification) -> Result<Self> {
        serde_json::from_str(notification.payload()).context(NotificationPayloadSnafu)
    }
}

pub async fn full_reconcile(store: &WatchStore, repository: &WatchRepository) -> Result<()> {
    let started = Instant::now();
    let watches = repository.load_all().await?;
    let watch_count = watches.len();
    store.replace_all(watches).await;
    histogram!(SAURON_WATCH_FULL_RECONCILE_DURATION_SECONDS)
        .record(started.elapsed().as_secs_f64());
    info!(watch_count, "Reconciled full Sauron watch set from replica");
    Ok(())
}

fn remove_entry(state: &mut WatchState, swap_id: Uuid) {
    let Some(previous) = state.by_swap_id.remove(&swap_id) else {
        return;
    };

    if let Some(chain_entries) = state.by_chain.get_mut(&previous.source_chain) {
        chain_entries.remove(&swap_id);
        if chain_entries.is_empty() {
            state.by_chain.remove(&previous.source_chain);
        }
    }
}

fn is_watch_active(watch: &WatchEntry, now: DateTime<Utc>) -> bool {
    watch.deposit_deadline > now
}

fn parse_watch_row(row: sqlx::postgres::PgRow) -> Result<WatchEntry> {
    let swap_id_raw =
        row.try_get::<String, _>("swap_id")
            .map_err(|_| crate::error::Error::InvalidWatchRow {
                message: "missing swap_id".to_string(),
            })?;
    let swap_id =
        Uuid::parse_str(&swap_id_raw).map_err(|_| crate::error::Error::InvalidWatchRow {
            message: format!("swap_id {swap_id_raw} was not a valid UUID"),
        })?;

    let from_chain_raw: String =
        row.try_get("from_chain")
            .map_err(|_| crate::error::Error::InvalidWatchRow {
                message: format!("swap {swap_id} missing from_chain"),
            })?;
    let source_chain = ChainType::from_db_string(&from_chain_raw).ok_or_else(|| {
        crate::error::Error::InvalidWatchRow {
            message: format!("swap {swap_id} had invalid from_chain {from_chain_raw}"),
        }
    })?;

    let source_token_value: serde_json::Value =
        row.try_get("from_token")
            .map_err(|_| crate::error::Error::InvalidWatchRow {
                message: format!("swap {swap_id} missing from_token"),
            })?;
    let source_token: TokenIdentifier =
        serde_json::from_value(source_token_value).map_err(|_| {
            crate::error::Error::InvalidWatchRow {
                message: format!("swap {swap_id} had invalid from_token"),
            }
        })?;

    let address = normalize_address(
        source_chain,
        &row.try_get::<String, _>("address")
            .map_err(|_| crate::error::Error::InvalidWatchRow {
                message: format!("swap {swap_id} missing address"),
            })?,
    );

    let min_amount_raw = row.try_get::<String, _>("min_amount").map_err(|_| {
        crate::error::Error::InvalidWatchRow {
            message: format!("swap {swap_id} missing min_amount"),
        }
    })?;
    let min_amount =
        U256::from_str(&min_amount_raw).map_err(|_| crate::error::Error::InvalidWatchRow {
            message: format!("swap {swap_id} had invalid min_amount"),
        })?;

    let max_amount_raw = row.try_get::<String, _>("max_amount").map_err(|_| {
        crate::error::Error::InvalidWatchRow {
            message: format!("swap {swap_id} missing max_amount"),
        }
    })?;
    let max_amount =
        U256::from_str(&max_amount_raw).map_err(|_| crate::error::Error::InvalidWatchRow {
            message: format!("swap {swap_id} had invalid max_amount"),
        })?;

    Ok(WatchEntry {
        swap_id,
        source_chain,
        source_token: source_token.normalize(),
        address,
        min_amount,
        max_amount,
        deposit_deadline: row.try_get("deposit_deadline").map_err(|_| {
            crate::error::Error::InvalidWatchRow {
                message: format!("swap {swap_id} missing deposit_deadline"),
            }
        })?,
        created_at: row.try_get("created_at").map_err(|_| {
            crate::error::Error::InvalidWatchRow {
                message: format!("swap {swap_id} missing created_at"),
            }
        })?,
        updated_at: row.try_get("updated_at").map_err(|_| {
            crate::error::Error::InvalidWatchRow {
                message: format!("swap {swap_id} missing updated_at"),
            }
        })?,
    })
}

fn normalize_address(chain: ChainType, address: &str) -> String {
    match chain {
        ChainType::Bitcoin => address.to_string(),
        ChainType::Ethereum | ChainType::Base => address.to_lowercase(),
    }
}

fn deposit_vault_watch_cutoff() -> DateTime<Utc> {
    utc::now()
        .checked_sub_months(Months::new(15))
        .expect("15-month deposit vault watch cutoff should stay within supported datetime range")
}

#[cfg(test)]
mod tests {
    use super::{WatchEntry, WatchStore};
    use alloy::primitives::U256;
    use chrono::Duration;
    use otc_models::{ChainType, TokenIdentifier};
    use uuid::Uuid;

    fn watch_entry(
        swap_id: Uuid,
        source_chain: ChainType,
        address: &str,
        deposit_deadline: chrono::DateTime<chrono::Utc>,
    ) -> WatchEntry {
        WatchEntry {
            swap_id,
            source_chain,
            source_token: match source_chain {
                ChainType::Bitcoin => TokenIdentifier::Native,
                ChainType::Ethereum | ChainType::Base => {
                    TokenIdentifier::address("0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf")
                }
            },
            address: address.to_string(),
            min_amount: U256::from(1_u64),
            max_amount: U256::from(10_u64),
            deposit_deadline,
            created_at: utc::now(),
            updated_at: utc::now(),
        }
    }

    #[tokio::test]
    async fn snapshot_for_chain_returns_only_active_watches_for_that_chain() {
        let store = WatchStore::default();
        let now = utc::now();
        store
            .replace_all(vec![
                watch_entry(
                    Uuid::now_v7(),
                    ChainType::Bitcoin,
                    "btc-address",
                    now + Duration::minutes(5),
                ),
                watch_entry(
                    Uuid::now_v7(),
                    ChainType::Ethereum,
                    "0x0000000000000000000000000000000000000001",
                    now + Duration::minutes(5),
                ),
                watch_entry(
                    Uuid::now_v7(),
                    ChainType::Bitcoin,
                    "expired-btc-address",
                    now - Duration::minutes(1),
                ),
            ])
            .await;

        let bitcoin_watches = store.snapshot_for_chain(ChainType::Bitcoin).await;
        let ethereum_watches = store.snapshot_for_chain(ChainType::Ethereum).await;

        assert_eq!(bitcoin_watches.len(), 1);
        assert_eq!(bitcoin_watches[0].address, "btc-address");
        assert_eq!(ethereum_watches.len(), 1);
        assert_eq!(
            ethereum_watches[0].address,
            "0x0000000000000000000000000000000000000001"
        );
    }

    #[tokio::test]
    async fn upsert_replaces_existing_entry_and_remove_deletes_it() {
        let store = WatchStore::default();
        let swap_id = Uuid::now_v7();
        let now = utc::now();

        store
            .replace_all(vec![watch_entry(
                swap_id,
                ChainType::Bitcoin,
                "btc-address",
                now + Duration::minutes(5),
            )])
            .await;

        store
            .upsert(watch_entry(
                swap_id,
                ChainType::Ethereum,
                "0x0000000000000000000000000000000000000002",
                now + Duration::minutes(10),
            ))
            .await;

        assert!(store
            .snapshot_for_chain(ChainType::Bitcoin)
            .await
            .is_empty());
        let ethereum_watches = store.snapshot_for_chain(ChainType::Ethereum).await;
        assert_eq!(ethereum_watches.len(), 1);
        assert_eq!(
            ethereum_watches[0].address,
            "0x0000000000000000000000000000000000000002"
        );

        store.remove(swap_id).await;
        assert_eq!(store.len().await, 0);
    }

    #[tokio::test]
    async fn prune_expired_removes_expired_watches_from_store() {
        let store = WatchStore::default();
        let now = utc::now();

        store
            .replace_all(vec![
                watch_entry(
                    Uuid::now_v7(),
                    ChainType::Bitcoin,
                    "btc-address",
                    now + Duration::minutes(5),
                ),
                watch_entry(
                    Uuid::now_v7(),
                    ChainType::Base,
                    "0x0000000000000000000000000000000000000003",
                    now - Duration::minutes(1),
                ),
            ])
            .await;

        let pruned = store.prune_expired().await;

        assert_eq!(pruned, 1);
        assert_eq!(store.len().await, 1);
        assert!(store.snapshot_for_chain(ChainType::Base).await.is_empty());
    }
}
