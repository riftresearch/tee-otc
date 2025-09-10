use crate::{Quote, SwapStatus};
use alloy::primitives::{Address, U256};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const MM_NEVER_DEPOSITS_TIMEOUT: Duration = Duration::minutes(60);
pub const MM_DEPOSIT_NEVER_CONFIRMED_TIMEOUT: Duration = Duration::minutes(60 * 24); // 24 hours

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Swap {
    pub id: Uuid,
    pub market_maker_id: Uuid,

    pub quote: Quote,

    // Salt for deterministic wallet generation when combined with the TEE master key
    pub user_deposit_salt: [u8; 32],
    pub user_deposit_address: String, // cached for convenience, can be derived from the salt and master key

    // Nonce for the market maker to embed in their payment address
    pub mm_nonce: [u8; 16],

    // User's addresses
    pub user_destination_address: String,
    pub user_evm_account_address: Address,

    // Core status
    pub status: SwapStatus,

    // Deposit tracking (JSONB in database)
    pub user_deposit_status: Option<UserDepositStatus>,
    pub mm_deposit_status: Option<MMDepositStatus>,

    // Settlement tracking
    pub settlement_status: Option<SettlementStatus>,

    // Failure/timeout tracking
    pub failure_reason: Option<String>,
    pub failure_at: Option<DateTime<Utc>>,

    // MM coordination
    pub mm_notified_at: Option<DateTime<Utc>>,
    pub mm_private_key_sent_at: Option<DateTime<Utc>>,

    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RefundSwapReason {
    MarketMakerNeverInitiatedDeposit,
    MarketMakerDepositNeverConfirmed,
}

impl Swap {
    pub fn can_be_refunded(&self) -> Option<RefundSwapReason> {
        if !matches!(
            self.status,
            SwapStatus::WaitingMMDepositInitiated | SwapStatus::WaitingMMDepositConfirmed
        ) {
            return None;
        }
        // if the status is not waiting mm deposit initiated or confirmed, then it can't be refunded
        match &self.mm_deposit_status {
            None => {
                // This means we must be in WaitingMMDepositInitiated
                // so we need to see if the user deposit has been confirmed or not
                let user_deposit = self.user_deposit_status.as_ref().unwrap();
                let user_deposit_confirmed_at = user_deposit.confirmed_at.expect(
                    "User deposit must be confirmed if we are in WaitingMMDepositInitiated",
                );
                let now = utc::now();
                let diff = now - user_deposit_confirmed_at;
                if diff > MM_NEVER_DEPOSITS_TIMEOUT {
                    Some(RefundSwapReason::MarketMakerNeverInitiatedDeposit)
                } else {
                    None
                }
            }
            Some(mm_deposit) => {
                let mm_deposit_detected_at = mm_deposit.deposit_detected_at;
                let now = utc::now();
                let diff = now - mm_deposit_detected_at;
                if diff > MM_DEPOSIT_NEVER_CONFIRMED_TIMEOUT {
                    Some(RefundSwapReason::MarketMakerDepositNeverConfirmed)
                } else {
                    None
                }
            }
        }
    }
}

// JSONB types for rich deposit/settlement data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserDepositStatus {
    pub tx_hash: String,
    pub amount: U256,
    pub deposit_detected_at: DateTime<Utc>,
    pub confirmations: u64,
    pub last_checked: DateTime<Utc>,
    pub confirmed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MMDepositStatus {
    pub tx_hash: String,
    pub amount: U256,
    pub deposit_detected_at: DateTime<Utc>,
    pub confirmations: u64,
    pub last_checked: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementStatus {
    pub tx_hash: String,
    pub broadcast_at: DateTime<Utc>,
    pub confirmations: u64,
    pub completed_at: Option<DateTime<Utc>>,
    pub fee: Option<U256>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferInfo {
    pub tx_hash: String,
    pub amount: U256,
    pub detected_at: DateTime<Utc>,
    pub confirmations: u64,
}
