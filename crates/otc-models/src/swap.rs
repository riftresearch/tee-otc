use crate::{Quote, SwapStatus};
use alloy::primitives::{Address, U256};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const MM_NEVER_DEPOSITS_TIMEOUT: Duration = Duration::minutes(60 * 24); // 24 hours
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
    UserInitiatedEarlyRefund,
    MarketMakerNeverInitiatedDeposit,
    MarketMakerDepositNeverConfirmed,
    UserInitiatedRefundAgain,
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

    #[test]
    fn allows_immediate_refund_for_insufficient_deposit() {
        use crate::{ChainType, Currency, FeeSchedule, Lot, Quote, TokenIdentifier};
        use alloy::primitives::{Address, U256};

        // Create a minimal swap in WaitingUserDepositInitiated state
        let swap = Swap {
            id: uuid::Uuid::new_v4(),
            market_maker_id: uuid::Uuid::new_v4(),
            quote: Quote {
                id: uuid::Uuid::new_v4(),
                from: Lot {
                    currency: Currency {
                        chain: ChainType::Bitcoin,
                        token: TokenIdentifier::Native,
                        decimals: 8,
                    },
                    amount: U256::from(10_000_000u64), // 0.1 BTC
                },
                to: Lot {
                    currency: Currency {
                        chain: ChainType::Ethereum,
                        token: TokenIdentifier::Native,
                        decimals: 18,
                    },
                    amount: U256::from(500000000000000000u64),
                },
                fee_schedule: FeeSchedule {
                    network_fee_sats: 1000,
                    liquidity_fee_sats: 2000,
                    protocol_fee_sats: 500,
                },
                market_maker_id: uuid::Uuid::new_v4(),
                expires_at: utc::now() + Duration::hours(1),
                created_at: utc::now(),
            },
            metadata: Metadata::default(),
            user_deposit_salt: [0u8; 32],
            user_deposit_address: "test_address".to_string(),
            mm_nonce: [0u8; 16],
            user_destination_address: "0x1234".to_string(),
            user_evm_account_address: Address::ZERO,
            status: SwapStatus::WaitingUserDepositInitiated,
            user_deposit_status: None, // No deposit detected yet, or insufficient
            mm_deposit_status: None,
            settlement_status: None,
            latest_refund: None,
            failure_reason: None,
            failure_at: None,
            mm_notified_at: None,
            mm_private_key_sent_at: None,
            created_at: utc::now(),
            updated_at: utc::now(),
        };

        // Should allow immediate refund without any time constraints
        let refund_reason = swap.can_be_refunded();
        assert!(
            refund_reason.is_some(),
            "Swap in WaitingUserDepositInitiated should be refundable immediately"
        );

        match refund_reason.unwrap() {
            RefundSwapReason::UserInitiatedEarlyRefund => {
                // This is the expected reason
            }
            other => panic!(
                "Expected UserInitiatedEarlyRefund, got {:?}",
                other
            ),
        }
    }

    #[test]
    fn early_refund_does_not_require_timeout() {
        use crate::{ChainType, Currency, FeeSchedule, Lot, Quote, TokenIdentifier};
        use alloy::primitives::{Address, U256};

        // Create a swap that was just created (no time elapsed)
        let swap = Swap {
            id: uuid::Uuid::new_v4(),
            market_maker_id: uuid::Uuid::new_v4(),
            quote: Quote {
                id: uuid::Uuid::new_v4(),
                from: Lot {
                    currency: Currency {
                        chain: ChainType::Ethereum,
                        token: TokenIdentifier::Address(
                            "0x0000000000000000000000000000000000000001".to_string()
                        ),
                        decimals: 8,
                    },
                    amount: U256::from(1_000_000u64), // 1 token
                },
                to: Lot {
                    currency: Currency {
                        chain: ChainType::Bitcoin,
                        token: TokenIdentifier::Native,
                        decimals: 8,
                    },
                    amount: U256::from(950_000u64),
                },
                fee_schedule: FeeSchedule {
                    network_fee_sats: 1000,
                    liquidity_fee_sats: 2000,
                    protocol_fee_sats: 500,
                },
                market_maker_id: uuid::Uuid::new_v4(),
                expires_at: utc::now() + Duration::hours(1),
                created_at: utc::now(),
            },
            metadata: Metadata::default(),
            user_deposit_salt: [0u8; 32],
            user_deposit_address: "0xdeposit".to_string(),
            mm_nonce: [0u8; 16],
            user_destination_address: "bc1qtest".to_string(),
            user_evm_account_address: Address::ZERO,
            status: SwapStatus::WaitingUserDepositInitiated,
            user_deposit_status: Some(UserDepositStatus {
                tx_hash: "0xtxhash".to_string(),
                amount: U256::from(999_999u64), // Insufficient by 1 unit
                deposit_detected_at: utc::now(), // Just now
                confirmations: 0,
                last_checked: utc::now(),
                confirmed_at: None,
            }),
            mm_deposit_status: None,
            settlement_status: None,
            latest_refund: None,
            failure_reason: None,
            failure_at: None,
            mm_notified_at: None,
            mm_private_key_sent_at: None,
            created_at: utc::now(),
            updated_at: utc::now(),
        };

        // Even though this swap was just created (no timeout), it should still be
        // immediately refundable because it's in WaitingUserDepositInitiated state
        let refund_reason = swap.can_be_refunded();
        assert!(
            refund_reason.is_some(),
            "Should allow immediate refund even without timeout when in WaitingUserDepositInitiated"
        );

        assert!(
            matches!(
                refund_reason.unwrap(),
                RefundSwapReason::UserInitiatedEarlyRefund
            ),
            "Should return UserInitiatedEarlyRefund reason"
        );
    }
}

impl Swap {
    pub fn can_be_refunded(&self) -> Option<RefundSwapReason> {
        // Early-stage refund: Allow immediate refund if user deposited but swap hasn't progressed
        if self.status == SwapStatus::WaitingUserDepositInitiated {
            // Only allow refund if there are actually funds deposited
            return Some(RefundSwapReason::UserInitiatedEarlyRefund);
        }

        if self.status == SwapStatus::RefundingUser { 
            return Some(RefundSwapReason::UserInitiatedRefundAgain);
        }

        // Mid/late-stage refunds: Only after MM should have deposited
        if !matches!(
            self.status,
            SwapStatus::WaitingMMDepositInitiated | SwapStatus::WaitingMMDepositConfirmed
        ) {
            // dont allow any other states to be refunded
            return None;
        }

        match &self.mm_deposit_status {
            None => {
                // Status is WaitingMMDepositInitiated - MM never initiated deposit
                let user_deposit = self.user_deposit_status.as_ref().expect(
                    "User deposit must exist if we reached WaitingMMDepositInitiated"
                );
                let user_deposit_confirmed_at = user_deposit.confirmed_at.expect(
                    "User deposit must be confirmed if we are in WaitingMMDepositInitiated"
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
                // Status is WaitingMMDepositConfirmed or RefundingUser - MM deposit not confirming
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
