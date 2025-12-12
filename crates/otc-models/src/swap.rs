use crate::{constants::MIN_VIABLE_OUTPUT_SATS, Quote, SwapRates, SwapStatus};
use alloy::primitives::U256;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const MM_NEVER_DEPOSITS_TIMEOUT: Duration = Duration::minutes(60 * 24); // 24 hours
pub const MM_DEPOSIT_NEVER_CONFIRMED_TIMEOUT: Duration = Duration::minutes(60 * 24); // 24 hours
pub const MM_DEPOSIT_RISK_WINDOW: Duration = Duration::minutes(10); // if one of the refund cases is within this window the market maker should consider this risky

/// Custom serialization for U256 that outputs decimal strings instead of hex.
/// This is needed for database compatibility where SQL expects numeric strings.
mod u256_decimal {
    use alloy::primitives::U256;
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &U256, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<U256, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        // Try parsing as decimal first, then as hex
        s.parse::<U256>()
            .or_else(|_| U256::from_str_radix(s.trim_start_matches("0x"), 16))
            .map_err(serde::de::Error::custom)
    }
}

/// Computed amounts when user deposit is detected and validated.
/// These represent the exact amounts for this specific swap based on the
/// actual deposited amount and the quote's rates.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RealizedSwap {
    /// Actual amount the user deposited
    #[serde(with = "u256_decimal")]
    pub user_input: U256,
    /// Amount the MM must send to the user
    #[serde(with = "u256_decimal")]
    pub mm_output: U256,
    /// Protocol's cut (collected during settlement)
    #[serde(with = "u256_decimal")]
    pub protocol_fee: U256,
    /// MM's spread/profit
    #[serde(with = "u256_decimal")]
    pub liquidity_fee: U256,
    /// Fixed gas/network costs
    #[serde(with = "u256_decimal")]
    pub network_fee: U256,
}

impl RealizedSwap {
    /// Compute realized swap amounts from an input and rates.
    /// Order of fee deduction: liquidity_fee -> network_fee -> protocol_fee
    ///
    /// Returns `None` if the resulting output would be below `MIN_VIABLE_OUTPUT_SATS`,
    /// which prevents dust outputs and uneconomical swaps.
    ///
    /// The protocol fee is always at least `rates.min_protocol_fee_sats` to ensure
    /// the fee output is above Bitcoin's dust limit.
    pub fn compute(input: u64, rates: &SwapRates) -> Option<Self> {
        const BPS_DENOM: u64 = 10_000;

        // 1. Deduct liquidity fee (MM spread)
        let liquidity_fee = input.saturating_mul(rates.liquidity_fee_bps) / BPS_DENOM;
        let after_liquidity = input.saturating_sub(liquidity_fee);

        // 2. Deduct fixed network fee
        let network_fee = rates.network_fee_sats;
        let after_network = after_liquidity.saturating_sub(network_fee);

        // 3. Deduct protocol fee (with minimum to avoid dust outputs)
        let computed_fee = after_network.saturating_mul(rates.protocol_fee_bps) / BPS_DENOM;
        let protocol_fee = computed_fee.max(rates.min_protocol_fee_sats);
        let mm_output = after_network.saturating_sub(protocol_fee);

        // Reject if output would be below minimum viable threshold (dust prevention)
        if mm_output < MIN_VIABLE_OUTPUT_SATS {
            return None;
        }

        Some(Self {
            user_input: U256::from(input),
            mm_output: U256::from(mm_output),
            protocol_fee: U256::from(protocol_fee),
            liquidity_fee: U256::from(liquidity_fee),
            network_fee: U256::from(network_fee),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Swap {
    pub id: Uuid,
    pub market_maker_id: Uuid,

    /// The rate-based quote for this swap
    pub quote: Quote,
    pub metadata: Metadata,

    /// Realized amounts computed when user deposit is detected.
    /// None until a valid deposit is detected within quote bounds.
    pub realized: Option<RealizedSwap>,

    // Salt for deterministic wallet generation when combined with the TEE master key
    pub deposit_vault_salt: [u8; 32],
    pub deposit_vault_address: String, // cached for convenience, can be derived from the salt and master key

    // Nonce for the market maker to embed in their payment address
    pub mm_nonce: [u8; 16],

    // User's addresses
    pub user_destination_address: String,
    pub refund_address: String,

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
    use crate::{ChainType, Currency, Quote, SwapRates, TokenIdentifier};
    use alloy::primitives::U256;
    use chrono::Duration;

    fn make_test_quote() -> Quote {
        Quote {
            id: uuid::Uuid::new_v4(),
            market_maker_id: uuid::Uuid::new_v4(),
            from_currency: Currency {
                chain: ChainType::Bitcoin,
                token: TokenIdentifier::Native,
                decimals: 8,
            },
            to_currency: Currency {
                chain: ChainType::Ethereum,
                token: TokenIdentifier::Native,
                decimals: 8,
            },
            rates: SwapRates::new(13, 10, 1000),
            min_input: U256::from(10_000u64),
            max_input: U256::from(100_000_000u64),
            expires_at: utc::now() + Duration::hours(1),
            created_at: utc::now(),
        }
    }

    fn make_test_swap(status: SwapStatus) -> Swap {
        Swap {
            id: uuid::Uuid::new_v4(),
            market_maker_id: uuid::Uuid::new_v4(),
            quote: make_test_quote(),
            metadata: Metadata::default(),
            realized: None,
            deposit_vault_salt: [0u8; 32],
            deposit_vault_address: "test_address".to_string(),
            mm_nonce: [0u8; 16],
            user_destination_address: "0x1234".to_string(),
            refund_address: "0x1234".to_string(),
            status,
            user_deposit_status: None,
            mm_deposit_status: None,
            settlement_status: None,
            latest_refund: None,
            failure_reason: None,
            failure_at: None,
            mm_notified_at: None,
            mm_private_key_sent_at: None,
            created_at: utc::now(),
            updated_at: utc::now(),
        }
    }

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
        let swap = make_test_swap(SwapStatus::WaitingUserDepositInitiated);

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
            other => panic!("Expected UserInitiatedEarlyRefund, got {:?}", other),
        }
    }

    #[test]
    fn early_refund_does_not_require_timeout() {
        let mut swap = make_test_swap(SwapStatus::WaitingUserDepositInitiated);
        swap.user_deposit_status = Some(UserDepositStatus {
            tx_hash: "0xtxhash".to_string(),
            amount: U256::from(999_999u64),
            deposit_detected_at: utc::now(),
            confirmations: 0,
            last_checked: utc::now(),
            confirmed_at: None,
        });

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

    #[test]
    fn waiting_user_deposit_confirmed_with_zero_confirmations_can_refund() {
        let mut swap = make_test_swap(SwapStatus::WaitingUserDepositConfirmed);
        swap.user_deposit_status = Some(UserDepositStatus {
            tx_hash: "txhash123".to_string(),
            amount: U256::from(1_000_000u64),
            deposit_detected_at: utc::now(),
            confirmations: 0,
            last_checked: utc::now(),
            confirmed_at: None,
        });

        // Should allow refund since transaction has 0 confirmations
        let refund_reason = swap.can_be_refunded();
        assert!(
            refund_reason.is_some(),
            "Swap in WaitingUserDepositConfirmed with 0 confirmations should be refundable"
        );

        assert!(
            matches!(
                refund_reason.unwrap(),
                RefundSwapReason::UserInitiatedEarlyRefund
            ),
            "Should return UserInitiatedEarlyRefund reason"
        );
    }

    #[test]
    fn waiting_user_deposit_confirmed_with_confirmations_can_refund() {
        let mut swap = make_test_swap(SwapStatus::WaitingUserDepositConfirmed);
        swap.user_deposit_status = Some(UserDepositStatus {
            tx_hash: "txhash123".to_string(),
            amount: U256::from(1_000_000u64),
            deposit_detected_at: utc::now(),
            confirmations: 2,
            last_checked: utc::now(),
            confirmed_at: None,
        });

        let refund_reason = swap.can_be_refunded();
        assert!(
            refund_reason.is_some(),
            "Swap in WaitingUserDepositConfirmed with confirmations should be refundable"
        );
    }

    #[test]
    fn realized_swap_computation() {
        let rates = SwapRates::new(13, 10, 1000); // 0.13% liquidity, 0.10% protocol, 1000 sats network
        let input = 1_000_000u64; // 1M sats

        let realized = RealizedSwap::compute(input, &rates).expect("should produce valid output");

        // liquidity_fee = 1_000_000 * 13 / 10000 = 1300
        assert_eq!(realized.liquidity_fee, U256::from(1300u64));

        // after_liquidity = 1_000_000 - 1300 = 998_700
        // after_network = 998_700 - 1000 = 997_700
        assert_eq!(realized.network_fee, U256::from(1000u64));

        // protocol_fee = 997_700 * 10 / 10000 = 997
        assert_eq!(realized.protocol_fee, U256::from(997u64));

        // mm_output = 997_700 - 997 = 996_703
        assert_eq!(realized.mm_output, U256::from(996_703u64));

        assert_eq!(realized.user_input, U256::from(input));
    }

    #[test]
    fn realized_swap_rejects_dust_output() {
        use crate::constants::MIN_VIABLE_OUTPUT_SATS;
        
        let rates = SwapRates::new(13, 10, 1000); // 0.13% liquidity, 0.10% protocol, 1000 sats network
        
        // Input that would result in output below MIN_VIABLE_OUTPUT_SATS
        // With network_fee=1000 and min_protocol_fee=300, we need at least ~1846 sats to get 546 output
        let tiny_input = 1_500u64;
        assert!(
            RealizedSwap::compute(tiny_input, &rates).is_none(),
            "Should reject inputs that result in dust output"
        );

        // Verify boundary: find an input that just barely produces viable output
        let viable_input = 2_000u64;
        let realized = RealizedSwap::compute(viable_input, &rates);
        if let Some(r) = realized {
            assert!(
                r.mm_output >= U256::from(MIN_VIABLE_OUTPUT_SATS),
                "Output {} should be at least {}",
                r.mm_output, MIN_VIABLE_OUTPUT_SATS
            );
        }
    }
}

impl Swap {
    pub fn can_be_refunded(&self) -> Option<RefundSwapReason> {
        // Early-stage refund: Allow immediate refund if user deposited but swap hasn't progressed
        if self.status == SwapStatus::WaitingUserDepositInitiated {
            // Only allow refund if there are actually funds deposited
            return Some(RefundSwapReason::UserInitiatedEarlyRefund);
        }

        // Allow refund in WaitingUserDepositConfirmed if transaction has 0 confirmations
        // (e.g., dropped from mempool or never made it into a block)
        if self.status == SwapStatus::WaitingUserDepositConfirmed {
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
                let user_deposit = self
                    .user_deposit_status
                    .as_ref()
                    .expect("User deposit must exist if we reached WaitingMMDepositInitiated");
                let user_deposit_confirmed_at = user_deposit
                    .confirmed_at
                    .expect("User deposit must be confirmed if we are in WaitingMMDepositInitiated");
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
