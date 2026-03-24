pub mod bitcoin;
pub mod evm_erc20;

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::primitives::U256;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use metrics::{gauge, histogram};
use otc_models::{ChainType, TokenIdentifier};
use reqwest::StatusCode;
use snafu::ResultExt;
use tokio::{task::JoinSet, time::MissedTickBehavior};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    error::{DiscoveryTaskJoinSnafu, Error, Result},
    otc_client::OtcClient,
    watch::{SharedWatchEntry, WatchEntry, WatchStore},
};

const SAURON_INDEXED_LOOKUP_DURATION_SECONDS: &str = "sauron_indexed_lookup_duration_seconds";
const SAURON_BLOCK_SCAN_DURATION_SECONDS: &str = "sauron_block_scan_duration_seconds";
const SAURON_INDEXED_LOOKUP_QUEUE_DEPTH: &str = "sauron_indexed_lookup_queue_depth";
const SAURON_INDEXED_LOOKUP_INFLIGHT: &str = "sauron_indexed_lookup_inflight";
const SAURON_BLOCK_SCAN_DETECTIONS: &str = "sauron_block_scan_detections";
const SUBMISSION_RETRY_BASE_DELAY: Duration = Duration::from_secs(5);
const SUBMISSION_RETRY_MAX_DELAY: Duration = Duration::from_secs(5 * 60);
const SUBMISSION_RETRY_JITTER_MAX_MILLIS: u64 = 1_000;

#[derive(Clone)]
pub struct DiscoveryContext {
    pub watches: WatchStore,
    pub otc_client: OtcClient,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectedDeposit {
    pub swap_id: Uuid,
    pub source_chain: ChainType,
    pub source_token: TokenIdentifier,
    pub address: String,
    pub tx_hash: String,
    pub transfer_index: u64,
    pub amount: U256,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockCursor {
    pub height: u64,
    pub hash: String,
}

#[derive(Debug, Clone)]
pub struct BlockScan {
    pub new_cursor: BlockCursor,
    pub detections: Vec<DetectedDeposit>,
}

#[derive(Debug, Clone)]
struct PendingSubmission {
    detected: DetectedDeposit,
    attempts: u32,
    next_attempt_at: Instant,
}

#[async_trait]
pub trait DiscoveryBackend: Send + Sync {
    fn name(&self) -> &'static str;
    fn chain(&self) -> ChainType;
    fn poll_interval(&self) -> Duration;
    fn indexed_lookup_concurrency(&self) -> usize;

    async fn sync_watches(&self, _watches: &[SharedWatchEntry]) -> Result<()> {
        Ok(())
    }

    async fn indexed_lookup(&self, watch: &WatchEntry) -> Result<Option<DetectedDeposit>>;

    async fn current_cursor(&self) -> Result<BlockCursor>;

    async fn scan_new_blocks(
        &self,
        from_exclusive: &BlockCursor,
        watches: &[SharedWatchEntry],
    ) -> Result<BlockScan>;
}

pub async fn run_backends(
    backends: Vec<Arc<dyn DiscoveryBackend>>,
    context: DiscoveryContext,
) -> Result<()> {
    let mut join_set = JoinSet::new();

    for backend in backends {
        let backend_name = backend.name();
        let context = context.clone();
        join_set.spawn(async move {
            info!(backend = backend_name, "Starting Sauron discovery backend");
            run_backend_loop(backend, context).await
        });
    }

    while let Some(join_result) = join_set.join_next().await {
        let backend_result = join_result.context(DiscoveryTaskJoinSnafu)?;
        backend_result?;
    }

    Ok(())
}

async fn run_backend_loop(
    backend: Arc<dyn DiscoveryBackend>,
    context: DiscoveryContext,
) -> Result<()> {
    let mut indexed_lookup_tasks = JoinSet::new();
    let mut indexed_lookup_backfill = IndexedLookupBackfillState::default();
    let mut reported_candidates = HashMap::new();
    let mut pending_submissions = HashMap::new();
    let mut cursor = backend.current_cursor().await?;

    let mut ticker = tokio::time::interval(backend.poll_interval());
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.tick().await;

    loop {
        ticker.tick().await;

        let backend_watches = context.watches.snapshot_for_chain(backend.chain()).await;
        if let Err(error) = backend.sync_watches(&backend_watches).await {
            warn!(
                backend = backend.name(),
                %error,
                "Failed to sync backend-local watch state; keeping previous snapshot"
            );
        }
        let backend_watch_map = backend_watches
            .iter()
            .map(|watch| (watch.swap_id, watch.clone()))
            .collect::<HashMap<_, _>>();
        let current_watch_versions: HashMap<Uuid, DateTime<Utc>> = backend_watch_map
            .iter()
            .map(|(swap_id, watch)| (*swap_id, watch.updated_at))
            .collect();
        let active_watch_ids = current_watch_versions
            .keys()
            .copied()
            .collect::<HashSet<_>>();

        reported_candidates.retain(|swap_id, _| active_watch_ids.contains(swap_id));
        pending_submissions.retain(|swap_id, _| active_watch_ids.contains(swap_id));
        indexed_lookup_backfill.retain_active(&active_watch_ids);

        drain_indexed_lookup_tasks(
            &mut indexed_lookup_tasks,
            &current_watch_versions,
            &mut indexed_lookup_backfill,
            &context,
            backend.name(),
            &mut pending_submissions,
            &mut reported_candidates,
        )
        .await?;

        retry_pending_submissions(
            &context,
            backend.name(),
            &mut pending_submissions,
            &mut reported_candidates,
        )
        .await;

        indexed_lookup_backfill.sync_snapshot(&backend_watch_map);
        indexed_lookup_backfill
            .requeue_pending_submissions(&backend_watch_map, pending_submissions.keys().copied());
        spawn_indexed_lookup_tasks(
            backend.clone(),
            &backend_watch_map,
            &mut indexed_lookup_backfill,
            &mut indexed_lookup_tasks,
        );
        gauge!(
            SAURON_INDEXED_LOOKUP_QUEUE_DEPTH,
            "backend" => backend.name().to_string(),
        )
        .set(indexed_lookup_backfill.queue_len() as f64);
        gauge!(
            SAURON_INDEXED_LOOKUP_INFLIGHT,
            "backend" => backend.name().to_string(),
        )
        .set(indexed_lookup_backfill.inflight_len() as f64);

        if backend_watches.is_empty() {
            cursor = backend.current_cursor().await?;
            continue;
        }

        let scan_started = Instant::now();
        match backend.scan_new_blocks(&cursor, &backend_watches).await {
            Ok(scan) => {
                histogram!(
                    SAURON_BLOCK_SCAN_DURATION_SECONDS,
                    "backend" => backend.name().to_string(),
                )
                .record(scan_started.elapsed().as_secs_f64());
                cursor = scan.new_cursor;
                gauge!(
                    SAURON_BLOCK_SCAN_DETECTIONS,
                    "backend" => backend.name().to_string(),
                )
                .set(scan.detections.len() as f64);
                for detected in scan.detections {
                    report_detected_deposit(
                        &context,
                        backend.name(),
                        detected,
                        &mut pending_submissions,
                        &mut reported_candidates,
                    )
                    .await;
                }
            }
            Err(error) => {
                warn!(
                    backend = backend.name(),
                    error = %error,
                    "Block scan failed; keeping existing cursor"
                );
                histogram!(
                    SAURON_BLOCK_SCAN_DURATION_SECONDS,
                    "backend" => backend.name().to_string(),
                )
                .record(scan_started.elapsed().as_secs_f64());
            }
        }

        drain_indexed_lookup_tasks(
            &mut indexed_lookup_tasks,
            &current_watch_versions,
            &mut indexed_lookup_backfill,
            &context,
            backend.name(),
            &mut pending_submissions,
            &mut reported_candidates,
        )
        .await?;
    }
}

#[derive(Debug, Default)]
struct IndexedLookupBackfillState {
    completed_versions: HashMap<Uuid, DateTime<Utc>>,
    queued_versions: HashMap<Uuid, DateTime<Utc>>,
    inflight_versions: HashMap<Uuid, DateTime<Utc>>,
    queue: VecDeque<(Uuid, DateTime<Utc>)>,
}

impl IndexedLookupBackfillState {
    fn retain_active(&mut self, active_watch_ids: &HashSet<Uuid>) {
        self.completed_versions
            .retain(|swap_id, _| active_watch_ids.contains(swap_id));
        self.queued_versions
            .retain(|swap_id, _| active_watch_ids.contains(swap_id));
        self.inflight_versions
            .retain(|swap_id, _| active_watch_ids.contains(swap_id));
        self.queue
            .retain(|(swap_id, _)| active_watch_ids.contains(swap_id));
    }

    fn sync_snapshot(&mut self, watches: &HashMap<Uuid, SharedWatchEntry>) {
        for (swap_id, watch) in watches {
            let version = watch.updated_at;

            if self.completed_versions.get(swap_id) == Some(&version)
                || self.queued_versions.get(swap_id) == Some(&version)
                || self.inflight_versions.get(swap_id) == Some(&version)
            {
                continue;
            }

            self.queue.push_back((*swap_id, version));
            self.queued_versions.insert(*swap_id, version);
        }
    }

    fn requeue_pending_submissions<I>(
        &mut self,
        watches: &HashMap<Uuid, SharedWatchEntry>,
        pending_swap_ids: I,
    ) where
        I: IntoIterator<Item = Uuid>,
    {
        for swap_id in pending_swap_ids {
            let Some(watch) = watches.get(&swap_id) else {
                continue;
            };
            let version = watch.updated_at;

            if self.queued_versions.get(&swap_id) == Some(&version)
                || self.inflight_versions.get(&swap_id) == Some(&version)
            {
                continue;
            }

            self.queue.push_back((swap_id, version));
            self.queued_versions.insert(swap_id, version);
        }
    }

    fn take_ready(
        &mut self,
        watches: &HashMap<Uuid, SharedWatchEntry>,
        count: usize,
    ) -> Vec<(SharedWatchEntry, DateTime<Utc>)> {
        let mut ready = Vec::with_capacity(count);

        while ready.len() < count {
            let Some((swap_id, version)) = self.queue.pop_front() else {
                break;
            };
            self.queued_versions.remove(&swap_id);

            let Some(watch) = watches.get(&swap_id) else {
                continue;
            };
            if watch.updated_at != version {
                continue;
            }

            self.inflight_versions.insert(swap_id, version);
            ready.push((watch.clone(), version));
        }

        ready
    }

    fn finish(
        &mut self,
        swap_id: Uuid,
        version: DateTime<Utc>,
        current_watch_versions: &HashMap<Uuid, DateTime<Utc>>,
    ) -> bool {
        self.inflight_versions.remove(&swap_id);

        if current_watch_versions.get(&swap_id) == Some(&version) {
            self.completed_versions.insert(swap_id, version);
            return true;
        }

        false
    }

    fn queue_len(&self) -> usize {
        self.queue.len()
    }

    fn inflight_len(&self) -> usize {
        self.inflight_versions.len()
    }
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexedLookupBackfillBenchmarkStats {
    pub queued: usize,
    pub inflight: usize,
    pub ready: usize,
}

#[doc(hidden)]
pub struct IndexedLookupBackfillBenchmarkScenario {
    backend_watch_map: HashMap<Uuid, SharedWatchEntry>,
    current_watch_versions: HashMap<Uuid, DateTime<Utc>>,
    active_watch_ids: HashSet<Uuid>,
    backfill_state: IndexedLookupBackfillState,
    ready: usize,
}

#[doc(hidden)]
impl IndexedLookupBackfillBenchmarkScenario {
    pub fn stats(&self) -> IndexedLookupBackfillBenchmarkStats {
        IndexedLookupBackfillBenchmarkStats {
            queued: self.backfill_state.queue_len(),
            inflight: self.backfill_state.inflight_len(),
            ready: self.ready,
        }
    }

    pub fn watch_count(&self) -> usize {
        self.backend_watch_map.len()
    }

    pub fn current_watch_versions_count(&self) -> usize {
        self.current_watch_versions.len()
    }

    pub fn active_watch_ids_count(&self) -> usize {
        self.active_watch_ids.len()
    }
}

#[doc(hidden)]
pub fn benchmark_seed_indexed_lookup_backfill(watches: &[SharedWatchEntry]) -> usize {
    benchmark_prepare_indexed_lookup_backfill(watches)
        .stats()
        .queued
}

#[doc(hidden)]
pub fn benchmark_schedule_initial_indexed_lookups(
    watches: &[SharedWatchEntry],
    concurrency: usize,
) -> IndexedLookupBackfillBenchmarkStats {
    benchmark_prepare_initial_indexed_lookups(watches, concurrency).stats()
}

#[doc(hidden)]
pub fn benchmark_prepare_indexed_lookup_backfill(
    watches: &[SharedWatchEntry],
) -> IndexedLookupBackfillBenchmarkScenario {
    let backend_watch_map = watches
        .iter()
        .map(|watch| (watch.swap_id, watch.clone()))
        .collect::<HashMap<_, _>>();
    let current_watch_versions = backend_watch_map
        .iter()
        .map(|(swap_id, watch)| (*swap_id, watch.updated_at))
        .collect::<HashMap<_, _>>();
    let active_watch_ids = current_watch_versions
        .keys()
        .copied()
        .collect::<HashSet<_>>();
    let mut backfill_state = IndexedLookupBackfillState::default();
    backfill_state.sync_snapshot(&backend_watch_map);

    IndexedLookupBackfillBenchmarkScenario {
        backend_watch_map,
        current_watch_versions,
        active_watch_ids,
        backfill_state,
        ready: 0,
    }
}

#[doc(hidden)]
pub fn benchmark_prepare_initial_indexed_lookups(
    watches: &[SharedWatchEntry],
    concurrency: usize,
) -> IndexedLookupBackfillBenchmarkScenario {
    let mut scenario = benchmark_prepare_indexed_lookup_backfill(watches);
    scenario.ready = scenario
        .backfill_state
        .take_ready(&scenario.backend_watch_map, concurrency)
        .len();
    scenario
}

fn spawn_indexed_lookup_tasks(
    backend: Arc<dyn DiscoveryBackend>,
    watches: &HashMap<Uuid, SharedWatchEntry>,
    backfill_state: &mut IndexedLookupBackfillState,
    tasks: &mut JoinSet<(Uuid, DateTime<Utc>, Result<Option<DetectedDeposit>>)>,
) {
    let available_slots = backend
        .indexed_lookup_concurrency()
        .saturating_sub(backfill_state.inflight_len());
    let ready = backfill_state.take_ready(watches, available_slots);

    for (watch, version) in ready {
        let backend = backend.clone();
        tasks.spawn(async move {
            let swap_id = watch.swap_id;
            let started = Instant::now();
            let result = backend.indexed_lookup(watch.as_ref()).await;
            histogram!(
                SAURON_INDEXED_LOOKUP_DURATION_SECONDS,
                "backend" => backend.name().to_string(),
            )
            .record(started.elapsed().as_secs_f64());
            (swap_id, version, result)
        });
    }
}

async fn drain_indexed_lookup_tasks(
    tasks: &mut JoinSet<(Uuid, DateTime<Utc>, Result<Option<DetectedDeposit>>)>,
    current_watch_versions: &HashMap<Uuid, DateTime<Utc>>,
    backfill_state: &mut IndexedLookupBackfillState,
    context: &DiscoveryContext,
    backend_name: &str,
    pending_submissions: &mut HashMap<Uuid, PendingSubmission>,
    reported_candidates: &mut HashMap<Uuid, DetectedDeposit>,
) -> Result<()> {
    while let Some(join_result) = tasks.try_join_next() {
        let (swap_id, version, lookup_result) = join_result.context(DiscoveryTaskJoinSnafu)?;

        if !backfill_state.finish(swap_id, version, current_watch_versions) {
            continue;
        }

        match lookup_result {
            Ok(Some(detected)) => {
                report_detected_deposit(
                    context,
                    backend_name,
                    detected,
                    pending_submissions,
                    reported_candidates,
                )
                .await;
            }
            Ok(None) => {}
            Err(error) => {
                warn!(
                    backend = backend_name,
                    swap_id = %swap_id,
                    %error,
                    "Indexed lookup failed for active watch"
                );
            }
        }
    }

    Ok(())
}

async fn report_detected_deposit(
    context: &DiscoveryContext,
    backend_name: &str,
    detected: DetectedDeposit,
    pending_submissions: &mut HashMap<Uuid, PendingSubmission>,
    reported_candidates: &mut HashMap<Uuid, DetectedDeposit>,
) {
    if reported_candidates
        .get(&detected.swap_id)
        .is_some_and(|existing| existing == &detected)
    {
        return;
    }

    if pending_submissions
        .get(&detected.swap_id)
        .is_some_and(|pending| pending.detected == detected)
    {
        return;
    }

    submit_detected_deposit(
        context,
        backend_name,
        detected,
        pending_submissions,
        reported_candidates,
    )
    .await;
}

async fn retry_pending_submissions(
    context: &DiscoveryContext,
    backend_name: &str,
    pending_submissions: &mut HashMap<Uuid, PendingSubmission>,
    reported_candidates: &mut HashMap<Uuid, DetectedDeposit>,
) {
    let now = Instant::now();
    let pending = pending_submissions
        .values()
        .filter(|submission| submission.next_attempt_at <= now)
        .map(|submission| submission.detected.clone())
        .collect::<Vec<_>>();

    for detected in pending {
        submit_detected_deposit(
            context,
            backend_name,
            detected,
            pending_submissions,
            reported_candidates,
        )
        .await;
    }
}

async fn submit_detected_deposit(
    context: &DiscoveryContext,
    backend_name: &str,
    detected: DetectedDeposit,
    pending_submissions: &mut HashMap<Uuid, PendingSubmission>,
    reported_candidates: &mut HashMap<Uuid, DetectedDeposit>,
) {
    let request = otc_server::api::DepositObservationRequest {
        source_chain: detected.source_chain,
        source_token: detected.source_token.clone(),
        tx_hash: detected.tx_hash.clone(),
        amount: detected.amount,
        transfer_index: detected.transfer_index,
        address: detected.address.clone(),
        observed_at: detected.observed_at,
        participant_auth: None,
    };

    match context
        .otc_client
        .submit_deposit_observation(detected.swap_id, &request)
        .await
    {
        Ok(response) => {
            info!(
                backend = backend_name,
                swap_id = %detected.swap_id,
                tx_hash = %detected.tx_hash,
                transfer_index = detected.transfer_index,
                result = %response.result,
                status = %response.status,
                "Discovery backend submitted deposit observation"
            );
            pending_submissions.remove(&detected.swap_id);
            reported_candidates.insert(detected.swap_id, detected);
        }
        Err(error) => {
            if should_retry_submission(&error) {
                let now = Instant::now();
                let attempts = pending_submissions
                    .get(&detected.swap_id)
                    .map_or(1, |pending| pending.attempts.saturating_add(1));
                let retry_delay = submission_retry_delay(&detected, attempts);
                pending_submissions.insert(
                    detected.swap_id,
                    PendingSubmission {
                        detected: detected.clone(),
                        attempts,
                        next_attempt_at: now + retry_delay,
                    },
                );
                warn!(
                    backend = backend_name,
                    swap_id = %detected.swap_id,
                    tx_hash = %detected.tx_hash,
                    attempts,
                    retry_in_ms = retry_delay.as_millis(),
                    %error,
                    "Discovery backend submit was retryable; keeping candidate queued for retry"
                );
            } else {
                pending_submissions.remove(&detected.swap_id);
                warn!(
                    backend = backend_name,
                    swap_id = %detected.swap_id,
                    tx_hash = %detected.tx_hash,
                    %error,
                    "Discovery backend failed to submit deposit observation"
                );
            }
        }
    }
}

fn should_retry_submission(error: &Error) -> bool {
    match error {
        Error::OtcRequest { .. } => true,
        Error::OtcRejected { status, body } => {
            *status == StatusCode::SERVICE_UNAVAILABLE
                || (*status == StatusCode::CONFLICT
                    && otc_rejection_code(body).as_deref() == Some("tx_not_found_on_chain"))
        }
        _ => false,
    }
}

fn otc_rejection_code(body: &str) -> Option<String> {
    serde_json::from_str::<otc_server::api::DepositObservationErrorResponse>(body)
        .ok()
        .map(|response| response.error.code)
}

fn submission_retry_delay(detected: &DetectedDeposit, attempts: u32) -> Duration {
    let exponent = attempts.saturating_sub(1).min(6);
    let multiplier = 1_u64 << exponent;
    let base_delay_secs = SUBMISSION_RETRY_BASE_DELAY
        .as_secs()
        .saturating_mul(multiplier)
        .min(SUBMISSION_RETRY_MAX_DELAY.as_secs());
    let jitter_millis =
        submission_retry_jitter_millis(detected, attempts).min(SUBMISSION_RETRY_JITTER_MAX_MILLIS);

    Duration::from_secs(base_delay_secs) + Duration::from_millis(jitter_millis)
}

fn submission_retry_jitter_millis(detected: &DetectedDeposit, attempts: u32) -> u64 {
    let mut hash = detected.swap_id.as_u128() as u64 ^ detected.transfer_index;
    hash ^= (detected.swap_id.as_u128() >> 64) as u64;
    hash ^= u64::from(attempts);

    for byte in detected.tx_hash.as_bytes() {
        hash = hash
            .wrapping_mul(1099511628211)
            .wrapping_add(u64::from(*byte));
    }

    hash % (SUBMISSION_RETRY_JITTER_MAX_MILLIS.saturating_add(1))
}

#[cfg(test)]
mod tests {
    use super::{
        should_retry_submission, submission_retry_delay, DetectedDeposit,
        IndexedLookupBackfillState, SUBMISSION_RETRY_BASE_DELAY, SUBMISSION_RETRY_MAX_DELAY,
    };
    use crate::error::Error;
    use crate::watch::WatchEntry;
    use alloy::primitives::U256;
    use chrono::{Duration, Utc};
    use otc_models::{ChainType, TokenIdentifier};
    use reqwest::StatusCode;
    use std::{collections::HashMap, sync::Arc};
    use uuid::Uuid;

    #[test]
    fn retries_tx_not_found_rejection() {
        let error = Error::OtcRejected {
            status: StatusCode::CONFLICT,
            body: r#"{"error":{"code":"tx_not_found_on_chain","message":"candidate transaction was not found on chain"}}"#.to_string(),
        };

        assert!(should_retry_submission(&error));
    }

    #[test]
    fn does_not_retry_permanent_conflict_rejection() {
        let error = Error::OtcRejected {
            status: StatusCode::CONFLICT,
            body: r#"{"error":{"code":"transfer_not_found_in_tx","message":"candidate transfer was not found in the transaction"}}"#.to_string(),
        };

        assert!(!should_retry_submission(&error));
    }

    #[test]
    fn retries_service_unavailable_rejection() {
        let error = Error::OtcRejected {
            status: StatusCode::SERVICE_UNAVAILABLE,
            body: r#"{"error":{"code":"transient_failure","message":"service unavailable"}}"#
                .to_string(),
        };

        assert!(should_retry_submission(&error));
    }

    fn detected_deposit() -> DetectedDeposit {
        DetectedDeposit {
            swap_id: Uuid::now_v7(),
            source_chain: ChainType::Bitcoin,
            source_token: TokenIdentifier::Native,
            address: "btc-address".to_string(),
            tx_hash: "deadbeef".to_string(),
            transfer_index: 0,
            amount: U256::from(1_u64),
            observed_at: Utc::now(),
        }
    }

    #[test]
    fn submission_retry_delay_grows_with_attempts_and_is_capped() {
        let detected = detected_deposit();
        let first = submission_retry_delay(&detected, 1);
        let second = submission_retry_delay(&detected, 2);
        let capped = submission_retry_delay(&detected, 32);

        assert!(first >= SUBMISSION_RETRY_BASE_DELAY);
        assert!(second > first);
        assert!(capped <= SUBMISSION_RETRY_MAX_DELAY + Duration::seconds(1).to_std().unwrap());
    }

    fn watch_entry(swap_id: Uuid, updated_at: chrono::DateTime<Utc>) -> Arc<WatchEntry> {
        Arc::new(WatchEntry {
            swap_id,
            source_chain: ChainType::Bitcoin,
            source_token: TokenIdentifier::Native,
            address: "btc-address".to_string(),
            min_amount: U256::from(1_u64),
            max_amount: U256::from(10_u64),
            deposit_deadline: Utc::now() + Duration::minutes(5),
            created_at: Utc::now(),
            updated_at,
        })
    }

    #[test]
    fn backfill_state_enqueues_new_and_updated_watches_once() {
        let swap_id = Uuid::now_v7();
        let updated_at = Utc::now();
        let mut state = IndexedLookupBackfillState::default();
        let mut watches = HashMap::new();
        watches.insert(swap_id, watch_entry(swap_id, updated_at));

        state.sync_snapshot(&watches);
        state.sync_snapshot(&watches);
        assert_eq!(state.queue_len(), 1);

        let ready = state.take_ready(&watches, 1);
        assert_eq!(ready.len(), 1);
        assert!(state.finish(swap_id, updated_at, &HashMap::from([(swap_id, updated_at)]),));

        let newer_updated_at = updated_at + Duration::seconds(1);
        watches.insert(swap_id, watch_entry(swap_id, newer_updated_at));
        state.sync_snapshot(&watches);
        assert_eq!(state.queue_len(), 1);
    }

    #[test]
    fn backfill_state_discards_stale_lookup_results() {
        let swap_id = Uuid::now_v7();
        let updated_at = Utc::now();
        let newer_updated_at = updated_at + Duration::seconds(1);
        let mut state = IndexedLookupBackfillState::default();
        let watches = HashMap::from([(swap_id, watch_entry(swap_id, updated_at))]);

        state.sync_snapshot(&watches);
        let ready = state.take_ready(&watches, 1);
        assert_eq!(ready.len(), 1);

        assert!(!state.finish(
            swap_id,
            updated_at,
            &HashMap::from([(swap_id, newer_updated_at)]),
        ));
    }

    #[test]
    fn backfill_state_requeues_pending_submission_for_same_version() {
        let swap_id = Uuid::now_v7();
        let updated_at = Utc::now();
        let mut state = IndexedLookupBackfillState::default();
        let watches = HashMap::from([(swap_id, watch_entry(swap_id, updated_at))]);

        state.sync_snapshot(&watches);
        let ready = state.take_ready(&watches, 1);
        assert_eq!(ready.len(), 1);
        assert!(state.finish(swap_id, updated_at, &HashMap::from([(swap_id, updated_at)]),));

        state.requeue_pending_submissions(&watches, [swap_id]);
        assert_eq!(state.queue_len(), 1);
    }
}
