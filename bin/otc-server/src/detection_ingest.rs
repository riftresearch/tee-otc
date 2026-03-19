use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::{
    hex,
    primitives::Address,
    sol,
    sol_types::{eip712_domain, SolStruct},
};
use axum::{
    extract::{rejection::JsonRejection, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use dashmap::DashMap;
use metrics::{counter, gauge, histogram};
use otc_auth::ApiKeyStore;
use otc_models::{
    constants::EXPECTED_CHAIN_IDS, RealizedSwap, Swap, SwapStatus, UserDepositStatus,
};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    api::{
        DepositObservationAcceptedResponse, DepositObservationError,
        DepositObservationErrorResponse, DepositObservationRequest, ParticipantAuth,
        ParticipantAuthKind,
    },
    error::OtcServerError,
    server::AppState,
};

pub type ParticipantCooldownMap = DashMap<String, Instant>;

pub const DETECTOR_ACCEPTED_TOTAL_METRIC: &str = "otc_detector_submissions_accepted_total";
pub const DETECTOR_REJECTED_TOTAL_METRIC: &str = "otc_detector_submissions_rejected_total";
pub const DETECTOR_AUTH_FAILURES_TOTAL_METRIC: &str = "otc_detector_auth_failures_total";
pub const PARTICIPANT_ACCEPTED_TOTAL_METRIC: &str =
    "otc_participant_signed_submissions_accepted_total";
pub const PARTICIPANT_REJECTED_TOTAL_METRIC: &str =
    "otc_participant_signed_submissions_rejected_total";
pub const PARTICIPANT_COOLDOWN_REJECTED_TOTAL_METRIC: &str =
    "otc_participant_signed_cooldown_rejections_total";
pub const DETECTOR_VERIFICATION_DURATION_METRIC: &str =
    "otc_detector_verification_duration_seconds";
pub const DETECTOR_INGEST_INFLIGHT_METRIC: &str = "otc_detector_ingest_inflight";
const PARTICIPANT_SIGNATURE_MAX_AGE_MINUTES: i64 = 10;

sol! {
    #[derive(Debug)]
    struct ParticipantDepositDetectionAuthPayload {
        string swapId;
        string txHash;
        uint64 signedAt;
    }
}

#[derive(Clone, Copy, Debug)]
enum SubmissionLane {
    TrustedDetector,
    ParticipantSigned,
}

impl SubmissionLane {
    fn as_str(self) -> &'static str {
        match self {
            Self::TrustedDetector => "trusted_detector",
            Self::ParticipantSigned => "participant_signed",
        }
    }
}

pub async fn deposit_observation(
    State(state): State<AppState>,
    Path(swap_id): Path<Uuid>,
    headers: HeaderMap,
    payload: Result<Json<DepositObservationRequest>, JsonRejection>,
) -> Response {
    let request = match payload {
        Ok(Json(request)) => request,
        Err(err) => {
            return detection_error(
                StatusCode::BAD_REQUEST,
                "bad_request",
                format!("invalid deposit observation payload: {err}"),
            )
        }
    };

    let (trusted_detector, participant_auth) =
        match classify_submission_lane(&state, &headers, request.participant_auth.clone()) {
            Ok(auth) => auth,
            Err(response) => return response,
        };

    handle_detection_request(state, swap_id, request, trusted_detector, participant_auth).await
}

async fn handle_detection_request(
    state: AppState,
    swap_id: Uuid,
    request: DepositObservationRequest,
    trusted_detector: Option<AuthenticatedDetector>,
    participant_auth: Option<ParticipantAuth>,
) -> Response {
    let lane = submission_lane(trusted_detector.as_ref(), participant_auth.as_ref());
    gauge!(DETECTOR_INGEST_INFLIGHT_METRIC).increment(1.0);
    let started = Instant::now();

    let response = match handle_detection_request_inner(
        state,
        swap_id,
        request,
        trusted_detector,
        participant_auth,
    )
    .await
    {
        Ok(response) => response,
        Err(err) => err,
    };

    gauge!(DETECTOR_INGEST_INFLIGHT_METRIC).decrement(1.0);
    histogram!(
        DETECTOR_VERIFICATION_DURATION_METRIC,
        "lane" => lane.as_str(),
    )
    .record(started.elapsed().as_secs_f64());

    response
}

async fn handle_detection_request_inner(
    state: AppState,
    swap_id: Uuid,
    request: DepositObservationRequest,
    trusted_detector: Option<AuthenticatedDetector>,
    participant_auth: Option<ParticipantAuth>,
) -> Result<Response, Response> {
    let lane = submission_lane(trusted_detector.as_ref(), participant_auth.as_ref());

    let mut swap = match state.db.swaps().get(swap_id).await {
        Ok(swap) => swap,
        Err(OtcServerError::NotFound) => {
            return Err(rejection(
                lane,
                trusted_detector.as_ref(),
                "swap_not_found",
                StatusCode::CONFLICT,
                "swap not found",
            ))
        }
        Err(err) => return Err(transient_failure(err)),
    };

    if is_duplicate_submission(&swap, &request.tx_hash) {
        record_acceptance(
            lane,
            trusted_detector.as_ref(),
            swap.quote.from.currency.chain,
        );
        return Ok(detection_success(
            StatusCode::OK,
            "duplicate",
            swap.id,
            swap.status,
        ));
    }

    if let Some(participant_auth) = participant_auth.as_ref() {
        verify_participant_submission(&state, swap_id, &swap, &request, participant_auth).await?;
    }

    if swap.status != SwapStatus::WaitingUserDepositInitiated {
        return Err(rejection(
            lane,
            trusted_detector.as_ref(),
            "swap_not_waiting_user_deposit_initiated",
            StatusCode::CONFLICT,
            format!(
                "swap {} is not waiting for user deposit initiation",
                swap.id
            ),
        ));
    }

    if request.source_chain != swap.quote.from.currency.chain {
        return Err(rejection(
            lane,
            trusted_detector.as_ref(),
            "chain_mismatch",
            StatusCode::CONFLICT,
            "candidate sourceChain does not match the swap source chain",
        ));
    }

    if request.source_token.normalize() != swap.quote.from.currency.token.normalize() {
        return Err(rejection(
            lane,
            trusted_detector.as_ref(),
            "asset_mismatch",
            StatusCode::CONFLICT,
            "candidate sourceToken does not match the swap source token",
        ));
    }

    if !addresses_match(
        swap.quote.from.currency.chain,
        &request.address,
        &swap.deposit_vault_address,
    ) {
        return Err(rejection(
            lane,
            trusted_detector.as_ref(),
            "address_mismatch",
            StatusCode::CONFLICT,
            "candidate address does not match the swap deposit vault",
        ));
    }

    let chain_ops = match state.chain_registry.get(&swap.quote.from.currency.chain) {
        Some(chain_ops) => chain_ops,
        None => {
            return Err(transient_failure(OtcServerError::Internal {
                message: format!(
                    "chain operations unavailable for {:?}",
                    swap.quote.from.currency.chain
                ),
            }))
        }
    };

    let verified = match chain_ops
        .verify_user_deposit_candidate(
            &swap.deposit_vault_address,
            &swap.quote.from.currency,
            &request.tx_hash,
            request.transfer_index,
        )
        .await
    {
        Ok(otc_chains::UserDepositCandidateStatus::TxNotFound) => {
            return Err(rejection(
                lane,
                trusted_detector.as_ref(),
                "tx_not_found_on_chain",
                StatusCode::CONFLICT,
                "candidate transaction was not found on chain",
            ))
        }
        Ok(otc_chains::UserDepositCandidateStatus::TransferNotFound) => {
            return Err(rejection(
                lane,
                trusted_detector.as_ref(),
                "transfer_not_found_in_tx",
                StatusCode::CONFLICT,
                "candidate transfer was not found in the transaction",
            ))
        }
        Ok(otc_chains::UserDepositCandidateStatus::Verified(verified)) => verified,
        Err(err) => return Err(transient_failure(err)),
    };

    if !swap.quote.is_valid_input(verified.amount) {
        return Err(rejection(
            lane,
            trusted_detector.as_ref(),
            "amount_out_of_bounds",
            StatusCode::CONFLICT,
            "candidate amount is outside the swap bounds",
        ));
    }

    let realized = match RealizedSwap::from_quote(&swap.quote, verified.amount.to::<u64>()) {
        Some(realized) => realized,
        None => {
            return Err(rejection(
                lane,
                trusted_detector.as_ref(),
                "dust_output",
                StatusCode::CONFLICT,
                "candidate amount would result in dust output",
            ))
        }
    };

    let deposit_status = UserDepositStatus {
        tx_hash: request.tx_hash.clone(),
        amount: verified.amount,
        deposit_detected_at: utc::now(),
        confirmed_at: None,
        confirmations: verified.confirmations,
        last_checked: utc::now(),
    };

    match state
        .db
        .swaps()
        .user_deposit_detected(swap.id, deposit_status, Some(realized))
        .await
    {
        Ok(()) => {
            info!(
                swap_id = %swap.id,
                lane = lane.as_str(),
                tx_hash = %request.tx_hash,
                "Accepted externally detected user deposit",
            );
        }
        Err(OtcServerError::InvalidState { .. }) => {
            swap = match state.db.swaps().get(swap.id).await {
                Ok(swap) => swap,
                Err(err) => return Err(transient_failure(err)),
            };

            if is_duplicate_submission(&swap, &request.tx_hash) {
                record_acceptance(
                    lane,
                    trusted_detector.as_ref(),
                    swap.quote.from.currency.chain,
                );
                return Ok(detection_success(
                    StatusCode::OK,
                    "duplicate",
                    swap.id,
                    swap.status,
                ));
            }

            return Err(rejection(
                lane,
                trusted_detector.as_ref(),
                "swap_not_waiting_user_deposit_initiated",
                StatusCode::CONFLICT,
                "swap is no longer eligible for first-deposit detection",
            ));
        }
        Err(err) => return Err(transient_failure(err)),
    }

    record_acceptance(
        lane,
        trusted_detector.as_ref(),
        swap.quote.from.currency.chain,
    );

    state
        .mm_registry
        .notify_user_deposit(
            &swap.market_maker_id,
            &swap.id,
            &swap.quote.id,
            &swap.deposit_vault_address,
            &request.tx_hash,
            verified.amount,
        )
        .await;

    if let Err(err) = state
        .user_deposit_confirmation_scheduler
        .refresh_swap(swap.id)
        .await
    {
        warn!(
            swap_id = %swap.id,
            tx_hash = %request.tx_hash,
            "Failed to refresh user-deposit confirmation schedule after ingest: {}",
            err
        );
    }

    let response_status = match state.db.swaps().get(swap.id).await {
        Ok(updated_swap) => updated_swap.status,
        Err(_) => SwapStatus::WaitingUserDepositConfirmed,
    };

    Ok(detection_success(
        StatusCode::ACCEPTED,
        "accepted",
        swap.id,
        response_status,
    ))
}

async fn verify_participant_submission(
    state: &AppState,
    swap_id: Uuid,
    swap: &Swap,
    request: &DepositObservationRequest,
    participant_auth: &ParticipantAuth,
) -> Result<(), Response> {
    if participant_auth.kind != ParticipantAuthKind::ParticipantEip712 {
        return Err(rejection(
            SubmissionLane::ParticipantSigned,
            None,
            "participant_signature_invalid",
            StatusCode::UNAUTHORIZED,
            "unsupported participant signature kind",
        ));
    }

    let now = utc::now();
    let oldest_allowed = now - chrono::Duration::minutes(PARTICIPANT_SIGNATURE_MAX_AGE_MINUTES);
    if participant_auth.signed_at > now {
        return Err(rejection(
            SubmissionLane::ParticipantSigned,
            None,
            "participant_signature_invalid",
            StatusCode::UNAUTHORIZED,
            "participant signedAt is in the future",
        ));
    }

    if participant_auth.signed_at < oldest_allowed {
        return Err(rejection(
            SubmissionLane::ParticipantSigned,
            None,
            "participant_signature_expired",
            StatusCode::UNAUTHORIZED,
            "participant signedAt must be within the last 10 minutes",
        ));
    }

    let signer_address = match parse_evm_address(&participant_auth.signer) {
        Ok(address) => address,
        Err(response) => return Err(response),
    };

    let allowed_signers = allowed_participant_signers(swap);
    let signer_chain_id = match EXPECTED_CHAIN_IDS.get(&participant_auth.signer_chain) {
        Some(chain_id) => *chain_id,
        None => {
            return Err(rejection(
                SubmissionLane::ParticipantSigned,
                None,
                "participant_signer_not_allowed",
                StatusCode::CONFLICT,
                "participant signer chain is not EVM-capable",
            ))
        }
    };

    let signer_allowed = allowed_signers.iter().any(|(chain, allowed)| {
        *chain == participant_auth.signer_chain && *allowed == signer_address
    });
    if !signer_allowed {
        return Err(rejection(
            SubmissionLane::ParticipantSigned,
            None,
            "participant_signer_not_allowed",
            StatusCode::CONFLICT,
            "participant signer does not match an allowed swap participant address",
        ));
    }

    let signature_bytes = hex::decode(participant_auth.signature.trim()).map_err(|_| {
        rejection(
            SubmissionLane::ParticipantSigned,
            None,
            "participant_signature_invalid",
            StatusCode::UNAUTHORIZED,
            "participant signature is not valid hex",
        )
    })?;

    let payload = participant_auth_payload(swap_id, &request.tx_hash, participant_auth);
    let domain = eip712_domain! {
        name: "Rift OTC Deposit Detection",
        version: "1",
        chain_id: signer_chain_id,
    };
    let digest = payload.eip712_signing_hash(&domain);

    let signer_chain = match state.chain_registry.get_evm(&participant_auth.signer_chain) {
        Some(chain) => chain,
        None => {
            return Err(transient_failure(OtcServerError::Internal {
                message: format!(
                    "EVM chain operations unavailable for participant signer chain {:?}",
                    participant_auth.signer_chain
                ),
            }))
        }
    };

    match signer_chain
        .verify_participant_signature(&participant_auth.signer, digest, &signature_bytes)
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            return Err(rejection(
                SubmissionLane::ParticipantSigned,
                None,
                "participant_signature_invalid",
                StatusCode::UNAUTHORIZED,
                "participant signature verification failed",
            ))
        }
        Err(err) => return Err(transient_failure(err)),
    }

    apply_participant_cooldown(
        &state.participant_submission_cooldowns,
        signer_address,
        state.participant_detection_min_interval,
    )?;

    Ok(())
}

fn participant_auth_payload(
    swap_id: Uuid,
    tx_hash: &str,
    participant_auth: &ParticipantAuth,
) -> ParticipantDepositDetectionAuthPayload {
    ParticipantDepositDetectionAuthPayload {
        swapId: swap_id.to_string(),
        txHash: tx_hash.to_string(),
        signedAt: participant_auth.signed_at.timestamp().max(0) as u64,
    }
}

fn submission_lane(
    trusted_detector: Option<&AuthenticatedDetector>,
    participant_auth: Option<&ParticipantAuth>,
) -> SubmissionLane {
    if trusted_detector.is_some() {
        SubmissionLane::TrustedDetector
    } else {
        debug_assert!(participant_auth.is_some());
        SubmissionLane::ParticipantSigned
    }
}

fn classify_submission_lane(
    state: &AppState,
    headers: &HeaderMap,
    participant_auth: Option<ParticipantAuth>,
) -> Result<(Option<AuthenticatedDetector>, Option<ParticipantAuth>), Response> {
    let detector_headers_present =
        headers.contains_key("X-API-ID") || headers.contains_key("X-API-SECRET");

    if detector_headers_present && participant_auth.is_some() {
        return Err(detection_error(
            StatusCode::BAD_REQUEST,
            "ambiguous_auth",
            "provide either trusted detector headers or participantAuth, not both",
        ));
    }

    if detector_headers_present {
        let detector = authenticate_trusted_detector(&state.detector_api_key_store, headers)?;
        return Ok((Some(detector), None));
    }

    if let Some(participant_auth) = participant_auth {
        if !state.participant_signed_detection_enabled {
            return Err(detection_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "participant_submission_disabled",
                "participant-signed deposit observations are disabled",
            ));
        }

        return Ok((None, Some(participant_auth)));
    }

    Err(detection_error(
        StatusCode::UNAUTHORIZED,
        "missing_auth",
        "trusted detector headers or participantAuth are required",
    ))
}

fn allowed_participant_signers(swap: &Swap) -> Vec<(otc_models::ChainType, Address)> {
    let mut signers = Vec::new();

    if let Some(chain) = evm_participant_chain(swap.quote.from.currency.chain) {
        if let Ok(address) = participant_address_from_string(&swap.refund_address) {
            signers.push((chain, address));
        }
    }

    if let Some(chain) = evm_participant_chain(swap.quote.to.currency.chain) {
        if let Ok(address) = participant_address_from_string(&swap.user_destination_address) {
            signers.push((chain, address));
        }
    }

    signers
}

fn evm_participant_chain(chain: otc_models::ChainType) -> Option<otc_models::ChainType> {
    match chain {
        otc_models::ChainType::Ethereum | otc_models::ChainType::Base => Some(chain),
        otc_models::ChainType::Bitcoin => None,
    }
}

fn participant_address_from_string(address: &str) -> Result<Address, ()> {
    address.parse::<Address>().map_err(|_| ())
}

fn parse_evm_address(address: &str) -> Result<Address, Response> {
    address.parse::<Address>().map_err(|_| {
        rejection(
            SubmissionLane::ParticipantSigned,
            None,
            "participant_signature_invalid",
            StatusCode::UNAUTHORIZED,
            "participant signer is not a valid EVM address",
        )
    })
}

fn is_duplicate_submission(swap: &Swap, tx_hash: &str) -> bool {
    swap.user_deposit_status
        .as_ref()
        .map(|status| status.tx_hash.eq_ignore_ascii_case(tx_hash))
        .unwrap_or(false)
}

fn addresses_match(chain: otc_models::ChainType, left: &str, right: &str) -> bool {
    match chain {
        otc_models::ChainType::Ethereum | otc_models::ChainType::Base => {
            let left = left.parse::<Address>();
            let right = right.parse::<Address>();
            matches!((left, right), (Ok(left), Ok(right)) if left == right)
        }
        otc_models::ChainType::Bitcoin => left == right,
    }
}

fn canonical_chain(chain: otc_models::ChainType) -> &'static str {
    match chain {
        otc_models::ChainType::Bitcoin => "bitcoin",
        otc_models::ChainType::Ethereum => "ethereum",
        otc_models::ChainType::Base => "base",
    }
}

fn apply_participant_cooldown(
    cooldowns: &ParticipantCooldownMap,
    signer: Address,
    min_interval: Duration,
) -> Result<(), Response> {
    let signer_key = signer.to_string().to_lowercase();
    let now = Instant::now();

    if let Some(entry) = cooldowns.get(&signer_key) {
        if *entry > now {
            counter!(PARTICIPANT_COOLDOWN_REJECTED_TOTAL_METRIC).increment(1);
            return Err(detection_error(
                StatusCode::TOO_MANY_REQUESTS,
                "participant_submission_too_soon",
                "participant signer is still in cooldown",
            ));
        }
    }

    cooldowns.remove_if(&signer_key, |_, locked_until| *locked_until <= now);
    cooldowns.insert(signer_key, now + min_interval);
    Ok(())
}

fn authenticate_trusted_detector(
    store: &Arc<ApiKeyStore>,
    headers: &HeaderMap,
) -> Result<AuthenticatedDetector, Response> {
    let detector_id = match headers.get("X-API-ID") {
        Some(value) => value.to_str().ok().and_then(|v| Uuid::parse_str(v).ok()),
        None => None,
    };
    let detector_secret = headers
        .get("X-API-SECRET")
        .and_then(|value| value.to_str().ok());

    let Some(detector_id) = detector_id else {
        counter!(
            DETECTOR_AUTH_FAILURES_TOTAL_METRIC,
            "presented_id" => "missing",
        )
        .increment(1);
        return Err(detection_error(
            StatusCode::UNAUTHORIZED,
            "detector_auth_failed",
            "missing or invalid X-API-ID header",
        ));
    };

    let Some(detector_secret) = detector_secret else {
        counter!(
            DETECTOR_AUTH_FAILURES_TOTAL_METRIC,
            "presented_id" => detector_id.to_string(),
        )
        .increment(1);
        return Err(detection_error(
            StatusCode::UNAUTHORIZED,
            "detector_auth_failed",
            "missing X-API-SECRET header",
        ));
    };

    match store.validate(&detector_id, detector_secret) {
        Ok(tag) => Ok(AuthenticatedDetector { tag }),
        Err(_) => {
            counter!(
                DETECTOR_AUTH_FAILURES_TOTAL_METRIC,
                "presented_id" => detector_id.to_string(),
            )
            .increment(1);
            Err(detection_error(
                StatusCode::UNAUTHORIZED,
                "detector_auth_failed",
                "trusted detector authentication failed",
            ))
        }
    }
}

fn record_acceptance(
    lane: SubmissionLane,
    trusted_detector: Option<&AuthenticatedDetector>,
    chain: otc_models::ChainType,
) {
    match lane {
        SubmissionLane::TrustedDetector => {
            counter!(
                DETECTOR_ACCEPTED_TOTAL_METRIC,
                "detector_tag" => trusted_detector
                    .map(|detector| detector.tag.clone())
                    .unwrap_or_else(|| "unknown".to_string()),
                "chain" => canonical_chain(chain),
            )
            .increment(1);
        }
        SubmissionLane::ParticipantSigned => {
            counter!(
                PARTICIPANT_ACCEPTED_TOTAL_METRIC,
                "chain" => canonical_chain(chain),
            )
            .increment(1);
        }
    }
}

fn rejection(
    lane: SubmissionLane,
    trusted_detector: Option<&AuthenticatedDetector>,
    code: &'static str,
    status: StatusCode,
    message: impl Into<String>,
) -> Response {
    match lane {
        SubmissionLane::TrustedDetector => {
            counter!(
                DETECTOR_REJECTED_TOTAL_METRIC,
                "detector_tag" => trusted_detector
                    .map(|detector| detector.tag.clone())
                    .unwrap_or_else(|| "unknown".to_string()),
                "code" => code,
            )
            .increment(1);
        }
        SubmissionLane::ParticipantSigned => {
            counter!(
                PARTICIPANT_REJECTED_TOTAL_METRIC,
                "code" => code,
            )
            .increment(1);
        }
    }

    detection_error(status, code, message)
}

fn transient_failure(err: impl ToString) -> Response {
    warn!(
        "Deposit detection request failed transiently: {}",
        err.to_string()
    );
    detection_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "service_unavailable",
        err.to_string(),
    )
}

fn detection_success(
    status: StatusCode,
    result: &'static str,
    swap_id: Uuid,
    swap_status: SwapStatus,
) -> Response {
    let body = Json(DepositObservationAcceptedResponse {
        result: result.to_string(),
        swap_id,
        status: swap_status_code(swap_status),
    });
    (status, body).into_response()
}

fn detection_error(status: StatusCode, code: &'static str, message: impl Into<String>) -> Response {
    (
        status,
        Json(DepositObservationErrorResponse {
            error: DepositObservationError {
                code: code.to_string(),
                message: message.into(),
            },
        }),
    )
        .into_response()
}

fn swap_status_code(status: SwapStatus) -> String {
    serde_json::to_string(&status)
        .unwrap_or_else(|_| "\"unknown\"".to_string())
        .trim_matches('"')
        .to_string()
}

#[derive(Clone, Debug)]
struct AuthenticatedDetector {
    tag: String,
}
