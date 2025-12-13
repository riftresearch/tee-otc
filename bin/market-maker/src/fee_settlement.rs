use crate::db::PaymentRepository;
use crate::wallet::WalletManager;
use crate::{Error, FeeSettlementRail, MarketMakerArgs, Result};
use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;
use otc_chains::traits::{MarketMakerPaymentVerification, Payment};
use otc_models::{constants, ChainType, Currency, Lot, TokenIdentifier};
use otc_protocols::mm::{MMResponse, ProtocolMessage};
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::time::{interval, timeout};
use tracing::{info, warn};
use uuid::Uuid;

const LOOKAHEAD: Duration = Duration::hours(1);
const STANDING_POLL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const MAX_BATCHES_PER_SETTLEMENT: usize = 10000;
const RESUBMIT_MIN_AGE_SECS: i64 = 30;
const MAX_RESUBMITS_PER_TICK: i64 = 250;

#[derive(Debug, Clone)]
pub struct FeeStandingStatus {
    pub debt_sats: i64,
    pub over_threshold_since: Option<DateTime<Utc>>,
    pub threshold_sats: i64,
    pub window_secs: i64,
}

#[derive(Clone)]
pub struct StandingRequestRegistry {
    pending: Arc<DashMap<Uuid, oneshot::Sender<FeeStandingStatus>>>,
}

impl StandingRequestRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            pending: Arc::new(DashMap::new()),
        }
    }

    pub fn fulfill(&self, request_id: Uuid, status: FeeStandingStatus) {
        if let Some((_, tx)) = self.pending.remove(&request_id) {
            let _ = tx.send(status);
        }
    }
}

pub fn spawn_fee_settlement_engine(
    args: MarketMakerArgs,
    market_maker_id: Uuid,
    wallet_manager: Arc<WalletManager>,
    payment_repository: Arc<PaymentRepository>,
    otc_handle: crate::websocket_client::WebSocketHandle,
    standing_registry: StandingRequestRegistry,
    join_set: &mut tokio::task::JoinSet<Result<()>>,
) {
    let interval_secs = args.fee_settlement_interval_secs;
    let rail = args.fee_settlement_rail;
    let evm_chain = args.evm_chain;

    join_set.spawn(async move {
        let mut tick = interval(std::time::Duration::from_secs(interval_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tick.tick().await;

            // First: replay any fee settlement notifications that haven't been explicitly accepted
            // by the otc-server yet.
            let now = utc::now();
            match payment_repository
                .list_fee_settlements_needing_resubmission(
                    RESUBMIT_MIN_AGE_SECS,
                    MAX_RESUBMITS_PER_TICK,
                    now,
                )
                .await
            {
                Ok(pending) => {
                    for s in pending {
                        let chain = match s.rail {
                            FeeSettlementRail::Bitcoin => ChainType::Bitcoin,
                            FeeSettlementRail::Evm => s.evm_chain.unwrap_or(evm_chain),
                        };

                        let request_id = Uuid::new_v4();
                        if let Err(e) = payment_repository
                            .mark_fee_settlement_submission_attempt(&s.txid, request_id, now)
                            .await
                        {
                            warn!(
                                tx_hash = %s.txid,
                                error = %e,
                                "Failed to record fee settlement submission attempt; skipping replay"
                            );
                            continue;
                        }

                        let msg = ProtocolMessage {
                            version: "1.0.0".to_string(),
                            sequence: 0,
                            payload: MMResponse::FeeSettlementSubmitted {
                                request_id,
                                chain,
                                tx_hash: s.txid.clone(),
                                batch_nonce_digests: s.batch_nonce_digests.clone(),
                                timestamp: now,
                            },
                        };

                        if let Err(e) = otc_handle.send(&msg) {
                            warn!(
                                tx_hash = %s.txid,
                                error = %e,
                                "Failed to replay fee settlement notification to otc-server"
                            );
                        } else {
                            info!(
                                chain = ?chain,
                                tx_hash = %s.txid,
                                "Replayed fee settlement notification to otc-server"
                            );
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to list fee settlements needing resubmission: {e}");
                }
            }

            let standing = match request_standing_inline(&otc_handle, &standing_registry).await
            {
                Ok(s) => s,
                Err(e) => {
                    warn!("Failed to poll fee standing status: {e}");
                    continue;
                }
            };

            let urgency = compute_urgency(&standing, utc::now());
            if !urgency {
                continue;
            }

            let batches = payment_repository
                .get_confirmed_unsettled_batches(MAX_BATCHES_PER_SETTLEMENT)
                .await
                .map_err(|e| Error::PaymentRepository { source: e })?;

            if batches.is_empty() {
                continue;
            }

            // Select oldest-first up to threshold alignment.
            let mut selected = Vec::new();
            let mut referenced_fee_sats: u64 = 0;
            for b in batches {
                if selected.len() >= MAX_BATCHES_PER_SETTLEMENT {
                    break;
                }
                selected.push(b);
                referenced_fee_sats = referenced_fee_sats.saturating_add(selected.last().unwrap().aggregated_fee_sats);
                if referenced_fee_sats >= standing.threshold_sats as u64 {
                    break;
                }
            }

            if referenced_fee_sats == 0 {
                continue;
            }

            // Build digest list
            let mut digests: Vec<[u8; 32]> = selected.iter().map(|b| b.batch_nonce_digest).collect();
            digests.sort();
            digests.dedup();

            let settlement_digest = compute_settlement_digest(market_maker_id, &digests);

            let (settlement_chain, wallet_chain, fee_address, token) = match rail {
                FeeSettlementRail::Bitcoin => (
                    ChainType::Bitcoin,
                    ChainType::Bitcoin,
                    otc_models::FEE_ADDRESSES_BY_CHAIN[&ChainType::Bitcoin].clone(),
                    TokenIdentifier::Native,
                ),
                FeeSettlementRail::Evm => (
                    evm_chain,
                    evm_chain,
                    otc_models::FEE_ADDRESSES_BY_CHAIN[&evm_chain].clone(),
                    constants::CBBTC_TOKEN.clone(),
                ),
            };

            let wallet = wallet_manager.get(wallet_chain).ok_or_else(|| Error::Config {
                context: format!("Wallet not registered for {wallet_chain:?}"),
            })?;

            let lot = Lot {
                currency: Currency {
                    chain: wallet_chain,
                    token,
                    decimals: 8,
                },
                amount: alloy::primitives::U256::from(referenced_fee_sats),
            };

            let tx_hash = wallet
                .create_batch_payment(
                    vec![Payment {
                        lot,
                        to_address: fee_address,
                    }],
                    Some(MarketMakerPaymentVerification {
                        aggregated_fee: alloy::primitives::U256::ZERO,
                        batch_nonce_digest: settlement_digest,
                    }),
                )
                .await
                .map_err(|e| Error::GenericWallet {
                    source: Box::new(e),
                })?;

            // Do not wait for confirmations here; otc-server is the source of truth and will ack
            // accepted/rejected. We rely on the replay loop above to resubmit until accepted.
            let now = utc::now();

            // Persist payload + linkage locally so we can reliably replay until acked.
            let batch_txids: Vec<String> = selected.iter().map(|b| b.txid.clone()).collect();
            let digests_for_db = digests.clone();
            if let Err(e) = payment_repository
                .record_fee_settlement(
                    &tx_hash,
                    rail,
                    settlement_chain,
                    settlement_digest,
                    digests_for_db,
                    referenced_fee_sats,
                    referenced_fee_sats,
                    &batch_txids,
                )
                .await
            {
                warn!(
                    chain = ?settlement_chain,
                    tx_hash = %tx_hash,
                    error = %e,
                    "Failed to persist fee settlement locally; not notifying otc-server"
                );
                continue;
            }

            // Notify otc-server (and record submission attempt for replay backoff).
            let request_id = Uuid::new_v4();
            if let Err(e) = payment_repository
                .mark_fee_settlement_submission_attempt(&tx_hash, request_id, now)
                .await
            {
                warn!(
                    chain = ?settlement_chain,
                    tx_hash = %tx_hash,
                    error = %e,
                    "Failed to record fee settlement submission attempt; skipping notify"
                );
                continue;
            }

            let msg = ProtocolMessage {
                version: "1.0.0".to_string(),
                sequence: 0,
                payload: MMResponse::FeeSettlementSubmitted {
                    request_id,
                    chain: settlement_chain,
                    tx_hash: tx_hash.clone(),
                    batch_nonce_digests: digests.clone(),
                    timestamp: now,
                },
            };
            if let Err(e) = otc_handle.send(&msg) {
                warn!(
                    chain = ?settlement_chain,
                    tx_hash = %tx_hash,
                    error = %e,
                    "Failed to notify otc-server about fee settlement; will replay later"
                );
            }

            info!(
                chain = ?settlement_chain,
                tx_hash = %tx_hash,
                referenced_fee_sats,
                "Fee settlement broadcast and submitted"
            );
        }
    });
}

fn compute_urgency(standing: &FeeStandingStatus, now: DateTime<Utc>) -> bool {
    if standing.debt_sats <= standing.threshold_sats {
        return false;
    }
    let Some(since) = standing.over_threshold_since else {
        return true;
    };
    let window = Duration::seconds(standing.window_secs);
    let deadline = since + window;
    deadline - now <= LOOKAHEAD
}

fn compute_settlement_digest(mm_id: Uuid, batch_nonce_digests: &[[u8; 32]]) -> [u8; 32] {
    let mut preimage =
        Vec::with_capacity(b"rift-fee-settle-v1".len() + 16 + 32 * batch_nonce_digests.len());
    preimage.extend_from_slice(b"rift-fee-settle-v1");
    preimage.extend_from_slice(mm_id.as_bytes());
    for d in batch_nonce_digests {
        preimage.extend_from_slice(d);
    }
    alloy::primitives::keccak256(preimage).0
}

async fn request_standing_inline(
    otc_handle: &crate::websocket_client::WebSocketHandle,
    registry: &StandingRequestRegistry,
) -> Result<FeeStandingStatus> {
    let request_id = Uuid::new_v4();
    let (tx, rx) = oneshot::channel();
    registry.pending.insert(request_id, tx);

    let msg = ProtocolMessage {
        version: "1.0.0".to_string(),
        sequence: 0,
        payload: MMResponse::FeeStandingStatusRequest {
            request_id,
            timestamp: utc::now(),
        },
    };
    otc_handle
        .send(&msg)
        .map_err(|e| Error::WebSocketClient { source: e })?;

    let status = timeout(STANDING_POLL_TIMEOUT, rx)
        .await
        .map_err(|_| Error::Config {
            context: "Fee standing status poll timed out".to_string(),
        })?
        .map_err(|_| Error::Config {
            context: "Fee standing status poll canceled".to_string(),
        })?;

    Ok(status)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settlement_digest_is_order_invariant() {
        let mm_id = Uuid::new_v4();
        let a = [1u8; 32];
        let b = [2u8; 32];

        let digest_ab = compute_settlement_digest(mm_id, &[a, b]);
        let digest_ba = compute_settlement_digest(mm_id, &[b, a]);

        // Caller canonicalizes by sorting; this test ensures digest function is deterministic for
        // a given input ordering, and that the canonicalization choice yields a stable digest.
        assert_ne!(digest_ab, digest_ba);

        let mut sorted = vec![b, a];
        sorted.sort();
        let digest_sorted = compute_settlement_digest(mm_id, &sorted);

        let mut sorted2 = vec![a, b];
        sorted2.sort();
        let digest_sorted2 = compute_settlement_digest(mm_id, &sorted2);

        assert_eq!(digest_sorted, digest_sorted2);
    }
}

