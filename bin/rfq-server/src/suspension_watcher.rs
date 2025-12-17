//! Watches the OTC server for suspended market makers and maintains a local cache.
//!
//! When configured, this module polls the OTC server's `/api/v2/market-makers/suspended`
//! endpoint at a regular interval and maintains a set of suspended market maker IDs
//! that should be excluded from the RFQ process.

use reqwest::Client;
use serde::Deserialize;
use std::{
    collections::HashSet,
    sync::Arc,
    time::Duration,
};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use url::Url;
use uuid::Uuid;

/// Polling interval for the suspension watcher.
///
/// Must be short enough to avoid a race window where an MM falls out of good standing
/// but the RFQ server still issues quotes before the cache is refreshed.
const POLL_INTERVAL: Duration = Duration::from_secs(30);

/// Response from the OTC server's suspended market makers endpoint.
#[derive(Debug, Deserialize)]
struct SuspendedMarketMakersResponse {
    market_maker_ids: Vec<Uuid>,
}

/// Watches for suspended market makers from the OTC server.
#[derive(Clone)]
pub struct SuspensionWatcher {
    suspended: Arc<RwLock<HashSet<Uuid>>>,
}

impl SuspensionWatcher {
    /// Creates a new suspension watcher and spawns the background polling task.
    ///
    /// The watcher will immediately fetch the initial list of suspended market makers,
    /// then poll at `POLL_INTERVAL` intervals.
    pub fn spawn(otc_server_url: Url) -> Self {
        let suspended = Arc::new(RwLock::new(HashSet::new()));
        let watcher = Self {
            suspended: suspended.clone(),
        };

        tokio::spawn(poll_loop(otc_server_url, suspended));

        watcher
    }

    /// Returns `true` if the given market maker is currently suspended.
    pub async fn is_suspended(&self, market_maker_id: Uuid) -> bool {
        self.suspended.read().await.contains(&market_maker_id)
    }

    /// Returns a snapshot of all currently suspended market maker IDs.
    #[allow(dead_code)]
    pub async fn get_suspended(&self) -> HashSet<Uuid> {
        self.suspended.read().await.clone()
    }
}

async fn poll_loop(otc_server_url: Url, suspended: Arc<RwLock<HashSet<Uuid>>>) {
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("failed to build reqwest client");

    let endpoint = otc_server_url
        .join("/api/v2/market-makers/suspended")
        .expect("failed to construct suspended endpoint URL");

    info!(%endpoint, "Starting suspension watcher");

    // Initial fetch
    fetch_and_update(&client, &endpoint, &suspended).await;

    // Polling loop
    let mut interval = tokio::time::interval(POLL_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;
        fetch_and_update(&client, &endpoint, &suspended).await;
    }
}

async fn fetch_and_update(client: &Client, endpoint: &Url, suspended: &Arc<RwLock<HashSet<Uuid>>>) {
    match fetch_suspended(client, endpoint).await {
        Ok(ids) => {
            let count = ids.len();
            let mut guard = suspended.write().await;
            *guard = ids;
            debug!(count, "Updated suspended market makers list");
        }
        Err(e) => {
            warn!(error = %e, "Failed to fetch suspended market makers; retaining previous list");
        }
    }
}

async fn fetch_suspended(
    client: &Client,
    endpoint: &Url,
) -> Result<HashSet<Uuid>, reqwest::Error> {
    let response = client
        .get(endpoint.as_str())
        .send()
        .await?
        .error_for_status()?;

    let body: SuspendedMarketMakersResponse = response.json().await?;
    
    if !body.market_maker_ids.is_empty() {
        info!(
            count = body.market_maker_ids.len(),
            "Fetched suspended market makers"
        );
    }

    Ok(body.market_maker_ids.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_suspension_watcher_empty_set() {
        let suspended = Arc::new(RwLock::new(HashSet::new()));
        let watcher = SuspensionWatcher {
            suspended: suspended.clone(),
        };

        let mm_id = Uuid::new_v4();
        assert!(!watcher.is_suspended(mm_id).await);
    }

    #[tokio::test]
    async fn test_suspension_watcher_contains() {
        let mm_id = Uuid::new_v4();
        let mut set = HashSet::new();
        set.insert(mm_id);

        let suspended = Arc::new(RwLock::new(set));
        let watcher = SuspensionWatcher {
            suspended: suspended.clone(),
        };

        assert!(watcher.is_suspended(mm_id).await);
        assert!(!watcher.is_suspended(Uuid::new_v4()).await);
    }
}

