use std::{collections::HashMap, str::FromStr, sync::Arc};

use alloy::primitives::U256;
use chrono::{DateTime, Months, Utc};
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
  q.expires_at AS deposit_deadline,
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
  q.expires_at AS deposit_deadline,
  s.created_at AS created_at,
  s.updated_at AS updated_at
FROM public.swaps s
JOIN public.quotes q ON q.id = s.quote_id
WHERE s.id = $1::uuid
  AND s.status = 'waiting_user_deposit_initiated'
  AND s.created_at >= $2
"#;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WatchKey {
    pub source_chain: ChainType,
    pub source_token: TokenIdentifier,
    pub address: String,
}

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

impl WatchEntry {
    #[must_use]
    pub fn key(&self) -> WatchKey {
        WatchKey {
            source_chain: self.source_chain,
            source_token: self.source_token.clone(),
            address: normalize_address(self.source_chain, &self.address),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct WatchStore {
    inner: Arc<RwLock<WatchState>>,
}

#[derive(Debug, Default)]
struct WatchState {
    by_swap_id: HashMap<Uuid, WatchEntry>,
    by_key: HashMap<WatchKey, Vec<Uuid>>,
}

impl WatchStore {
    pub async fn replace_all(&self, entries: Vec<WatchEntry>) {
        let mut state = self.inner.write().await;
        let mut by_swap_id = HashMap::with_capacity(entries.len());
        let mut by_key: HashMap<WatchKey, Vec<Uuid>> = HashMap::new();

        for entry in entries {
            let key = entry.key();
            by_key.entry(key).or_default().push(entry.swap_id);
            by_swap_id.insert(entry.swap_id, entry);
        }

        state.by_swap_id = by_swap_id;
        state.by_key = by_key;
    }

    pub async fn upsert(&self, entry: WatchEntry) {
        let mut state = self.inner.write().await;
        if let Some(previous) = state.by_swap_id.remove(&entry.swap_id) {
            let previous_key = previous.key();
            if let Some(swaps) = state.by_key.get_mut(&previous_key) {
                swaps.retain(|swap_id| *swap_id != previous.swap_id);
                if swaps.is_empty() {
                    state.by_key.remove(&previous_key);
                }
            }
        }

        let key = entry.key();
        state.by_key.entry(key).or_default().push(entry.swap_id);
        state.by_swap_id.insert(entry.swap_id, entry);
    }

    pub async fn remove(&self, swap_id: Uuid) {
        let mut state = self.inner.write().await;
        if let Some(previous) = state.by_swap_id.remove(&swap_id) {
            let previous_key = previous.key();
            if let Some(swaps) = state.by_key.get_mut(&previous_key) {
                swaps.retain(|candidate| *candidate != swap_id);
                if swaps.is_empty() {
                    state.by_key.remove(&previous_key);
                }
            }
        }
    }

    pub async fn len(&self) -> usize {
        self.inner.read().await.by_swap_id.len()
    }

    pub async fn snapshot(&self) -> Vec<WatchEntry> {
        self.inner
            .read()
            .await
            .by_swap_id
            .values()
            .cloned()
            .collect()
    }

    pub async fn matching_swaps(&self, key: &WatchKey) -> Vec<WatchEntry> {
        let state = self.inner.read().await;
        state
            .by_key
            .get(key)
            .into_iter()
            .flat_map(|swap_ids| swap_ids.iter())
            .filter_map(|swap_id| state.by_swap_id.get(swap_id).cloned())
            .collect()
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
        let rows = sqlx::query(FULL_WATCH_QUERY)
            .bind(cutoff_time)
            .fetch_all(&self.pool)
            .await
            .context(ReplicaDatabaseQuerySnafu)?;

        rows.into_iter().map(parse_watch_row).collect()
    }

    pub async fn load_swap(&self, swap_id: Uuid) -> Result<Option<WatchEntry>> {
        let cutoff_time = deposit_vault_watch_cutoff();
        let row = sqlx::query(TARGETED_WATCH_QUERY)
            .bind(swap_id)
            .bind(cutoff_time)
            .fetch_optional(&self.pool)
            .await
            .context(ReplicaDatabaseQuerySnafu)?;

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
    let watches = repository.load_all().await?;
    let watch_count = watches.len();
    store.replace_all(watches).await;
    info!(watch_count, "Reconciled full Sauron watch set from replica");
    Ok(())
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
