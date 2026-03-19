pub mod bitcoin;
pub mod evm_erc20;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use alloy::primitives::U256;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use otc_models::{ChainType, TokenIdentifier};
use reqwest::StatusCode;
use snafu::ResultExt;
use tokio::{task::JoinSet, time::MissedTickBehavior};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    error::{DiscoveryTaskJoinSnafu, Error, Result},
    otc_client::OtcClient,
    watch::{WatchEntry, WatchStore},
};

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
}

#[async_trait]
pub trait DiscoveryBackend: Send + Sync {
    fn name(&self) -> &'static str;
    fn chain(&self) -> ChainType;
    fn poll_interval(&self) -> Duration;

    async fn indexed_lookup(&self, watch: &WatchEntry) -> Result<Option<DetectedDeposit>>;

    async fn current_cursor(&self) -> Result<BlockCursor>;

    async fn scan_new_blocks(
        &self,
        from_exclusive: &BlockCursor,
        watches: &[WatchEntry],
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
    let mut known_watch_ids = HashSet::new();
    let mut reported_candidates = HashMap::new();
    let mut pending_submissions = HashMap::new();
    let mut cursor = backend.current_cursor().await?;

    let mut ticker = tokio::time::interval(backend.poll_interval());
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.tick().await;

    loop {
        ticker.tick().await;

        let watches = context.watches.snapshot().await;
        let backend_watches: Vec<WatchEntry> = watches
            .into_iter()
            .filter(|watch| watch.source_chain == backend.chain())
            .collect();

        let active_watch_ids: HashSet<Uuid> =
            backend_watches.iter().map(|watch| watch.swap_id).collect();
        known_watch_ids.retain(|swap_id| active_watch_ids.contains(swap_id));
        reported_candidates.retain(|swap_id, _| active_watch_ids.contains(swap_id));
        pending_submissions.retain(|swap_id, _| active_watch_ids.contains(swap_id));

        retry_pending_submissions(
            &context,
            backend.name(),
            &mut pending_submissions,
            &mut reported_candidates,
        )
        .await;

        for watch in &backend_watches {
            let is_new_watch = known_watch_ids.insert(watch.swap_id);
            let has_pending_submission = pending_submissions.contains_key(&watch.swap_id);

            if !is_new_watch && !has_pending_submission {
                continue;
            }

            match backend.indexed_lookup(watch).await {
                Ok(Some(detected)) => {
                    report_detected_deposit(
                        &context,
                        backend.name(),
                        detected,
                        &mut pending_submissions,
                        &mut reported_candidates,
                    )
                    .await;
                }
                Ok(None) => {}
                Err(error) => {
                    warn!(
                        backend = backend.name(),
                        swap_id = %watch.swap_id,
                        %error,
                        "Indexed lookup failed for active watch"
                    );
                }
            }
        }

        if backend_watches.is_empty() {
            cursor = backend.current_cursor().await?;
            continue;
        }

        match backend.scan_new_blocks(&cursor, &backend_watches).await {
            Ok(scan) => {
                cursor = scan.new_cursor;
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
                    %error,
                    "Block scan failed; resetting cursor to current tip"
                );
                cursor = backend.current_cursor().await?;
            }
        }
    }
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
    let pending = pending_submissions
        .values()
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
                let attempts = pending_submissions
                    .get(&detected.swap_id)
                    .map_or(1, |pending| pending.attempts.saturating_add(1));
                pending_submissions.insert(
                    detected.swap_id,
                    PendingSubmission {
                        detected: detected.clone(),
                        attempts,
                    },
                );
                warn!(
                    backend = backend_name,
                    swap_id = %detected.swap_id,
                    tx_hash = %detected.tx_hash,
                    attempts,
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

#[cfg(test)]
mod tests {
    use super::should_retry_submission;
    use crate::error::Error;
    use reqwest::StatusCode;

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
}
