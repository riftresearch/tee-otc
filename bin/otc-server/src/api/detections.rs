use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use otc_models::{ChainType, TokenIdentifier};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DepositObservationRequest {
    pub source_chain: ChainType,
    pub source_token: TokenIdentifier,
    pub tx_hash: String,
    #[serde(with = "otc_models::serde_utils::u256_decimal")]
    pub amount: U256,
    pub transfer_index: u64,
    pub address: String,
    pub observed_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub participant_auth: Option<ParticipantAuth>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ParticipantAuthKind {
    ParticipantEip712,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParticipantAuth {
    pub kind: ParticipantAuthKind,
    pub signer: String,
    pub signer_chain: ChainType,
    pub signature: String,
    pub signed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DepositObservationAcceptedResponse {
    pub result: String,
    pub swap_id: Uuid,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DepositObservationErrorResponse {
    pub error: DepositObservationError,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DepositObservationError {
    pub code: String,
    pub message: String,
}
