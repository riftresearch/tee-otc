use crate::{Quote, SwapStatus};
use alloy::primitives::{Address, U256};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const MM_NEVER_DEPOSITS_TIMEOUT: Duration = Duration::minutes(60);
pub const MM_DEPOSIT_NEVER_CONFIRMED_TIMEOUT: Duration = Duration::minutes(60 * 24); // 24 hours
pub const MM_DEPOSIT_RISK_WINDOW: Duration = Duration::minutes(10); // if one of the refund cases is within this window the market maker should consider this risky

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Swap {
    pub id: Uuid,
    pub market_maker_id: Uuid,

    pub quote: Quote,
    pub metadata: Metadata,

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

    // Refund tracking
    pub latest_refund: Option<LatestRefund>,

    // Failure/timeout tracking
    pub failure_reason: Option<String>,
    pub failure_at: Option<DateTime<Utc>>,

    // MM coordination
    pub mm_notified_at: Option<DateTime<Utc>>,
    pub mm_private_key_sent_at: Option<DateTime<Utc>>,

    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Metadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub affiliate: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_asset: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RefundSwapReason {
    MarketMakerNeverInitiatedDeposit,
    MarketMakerDepositNeverConfirmed,
}

/// Returns true if a refund is approaching because the market maker never initiated their deposit.
/// This checks against the user deposit confirmation time.
pub fn can_be_refunded_soon_bc_mm_not_initiated(
    user_deposit_confirmed_at: Option<DateTime<Utc>>,
) -> bool {
    let confirmed_at = match user_deposit_confirmed_at {
        Some(ts) => ts,
        None => return false,
    };
    let elapsed = utc::now() - confirmed_at;
    elapsed >= (MM_NEVER_DEPOSITS_TIMEOUT - MM_DEPOSIT_RISK_WINDOW)
}

/// Returns true if a refund is approaching because the market maker's deposit was never confirmed.
/// This checks against the MM deposit detection time (or batch creation time as a proxy).
pub fn can_be_refunded_soon_bc_mm_not_confirmed(mm_deposit_detected_at: DateTime<Utc>) -> bool {
    let elapsed = utc::now() - mm_deposit_detected_at;
    elapsed >= (MM_DEPOSIT_NEVER_CONFIRMED_TIMEOUT - MM_DEPOSIT_RISK_WINDOW)
}

/// Returns true if a refund eligibility timeout is approaching within the configured risk window.
/// This is a wrapper that checks both refund scenarios based on swap status.
pub fn can_be_refunded_soon(
    status: SwapStatus,
    user_deposit_confirmed_at: Option<DateTime<Utc>>,
    mm_deposit_detected_at: Option<DateTime<Utc>>,
) -> bool {
    match status {
        SwapStatus::WaitingMMDepositInitiated => {
            can_be_refunded_soon_bc_mm_not_initiated(user_deposit_confirmed_at)
        }
        SwapStatus::WaitingMMDepositConfirmed => match mm_deposit_detected_at {
            Some(ts) => can_be_refunded_soon_bc_mm_not_confirmed(ts),
            None => false,
        },
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn detects_near_refund_windows_for_user_deposits() {
        let window = MM_NEVER_DEPOSITS_TIMEOUT - MM_DEPOSIT_RISK_WINDOW;
        let now = utc::now();

        let safely_before_window = now - (window - Duration::seconds(5));
        assert!(!can_be_refunded_soon(
            SwapStatus::WaitingMMDepositInitiated,
            Some(safely_before_window),
            None,
        ));

        let past_window = now - (window + Duration::seconds(5));
        assert!(can_be_refunded_soon(
            SwapStatus::WaitingMMDepositInitiated,
            Some(past_window),
            None,
        ));

        assert!(!can_be_refunded_soon(
            SwapStatus::WaitingMMDepositInitiated,
            None,
            None,
        ));
    }

    #[test]
    fn detects_near_refund_windows_for_mm_deposits() {
        let window = MM_DEPOSIT_NEVER_CONFIRMED_TIMEOUT - MM_DEPOSIT_RISK_WINDOW;
        let now = utc::now();

        let safely_before_window = now - (window - Duration::seconds(5));
        assert!(!can_be_refunded_soon(
            SwapStatus::WaitingMMDepositConfirmed,
            None,
            Some(safely_before_window),
        ));

        let past_window = now - (window + Duration::seconds(5));
        assert!(can_be_refunded_soon(
            SwapStatus::WaitingMMDepositConfirmed,
            None,
            Some(past_window),
        ));

        assert!(!can_be_refunded_soon(
            SwapStatus::WaitingMMDepositConfirmed,
            None,
            None,
        ));
    }
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
pub struct LatestRefund {
    pub timestamp: DateTime<Utc>,
    pub recipient_address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferInfo {
    pub tx_hash: String,
    pub amount: U256,
    pub detected_at: DateTime<Utc>,
    pub confirmations: u64,
}
