use crate::{MMDepositStatus, RealizedSwap, SettlementStatus, Swap, SwapStatus, UserDepositStatus};
use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use snafu::{ensure, Snafu};

#[derive(Debug, Snafu)]
pub enum TransitionError {
    #[snafu(display("Invalid state transition from {:?} to {:?}", from, to))]
    InvalidTransition { from: SwapStatus, to: SwapStatus },

    #[snafu(display("Missing required data for transition: {}", reason))]
    MissingData { reason: String },

    #[snafu(display("Swap has already failed: {}", reason))]
    AlreadyFailed { reason: String },
}

pub type TransitionResult = Result<(), TransitionError>;

impl Swap {
    /// Transition when user deposit is detected
    /// If `realized` is Some, it means the deposit was within quote bounds and amounts were computed.
    pub fn user_deposit_detected(
        &mut self,
        tx_hash: String,
        amount: U256,
        confirmations: u64,
        realized: Option<RealizedSwap>,
    ) -> TransitionResult {
        ensure!(
            matches!(self.status, SwapStatus::WaitingUserDepositInitiated | SwapStatus::WaitingUserDepositConfirmed),
            InvalidTransitionSnafu {
                from: self.status,
                to: SwapStatus::WaitingUserDepositConfirmed,
            }
        );

        let now = utc::now();
        self.user_deposit_status = Some(UserDepositStatus {
            tx_hash,
            amount,
            deposit_detected_at: now,
            confirmations,
            last_checked: now,
            confirmed_at: None,
        });

        self.realized = realized;
        self.status = SwapStatus::WaitingUserDepositConfirmed;
        self.updated_at = now;

        Ok(())
    }

    /// Transition when user deposit is confirmed
    pub fn user_deposit_confirmed(&mut self) -> TransitionResult {
        ensure!(
            self.status == SwapStatus::WaitingUserDepositConfirmed,
            InvalidTransitionSnafu {
                from: self.status,
                to: SwapStatus::WaitingMMDepositInitiated,
            }
        );

        ensure!(
            self.user_deposit_status.is_some(),
            MissingDataSnafu {
                reason: "User deposit status not found",
            }
        );

        self.user_deposit_status.as_mut().unwrap().confirmed_at = Some(utc::now());
        self.status = SwapStatus::WaitingMMDepositInitiated;
        self.updated_at = utc::now();

        Ok(())
    }

    /// Transition when MM deposit is detected
    pub fn mm_deposit_detected(
        &mut self,
        tx_hash: String,
        amount: U256,
        confirmations: u64,
    ) -> TransitionResult {
        ensure!(
            self.status == SwapStatus::WaitingMMDepositInitiated,
            InvalidTransitionSnafu {
                from: self.status,
                to: SwapStatus::WaitingMMDepositConfirmed,
            }
        );

        let now = utc::now();
        self.mm_deposit_status = Some(MMDepositStatus {
            tx_hash,
            amount,
            deposit_detected_at: now,
            confirmations,
            last_checked: now,
        });

        self.status = SwapStatus::WaitingMMDepositConfirmed;
        self.updated_at = now;

        Ok(())
    }

    /// Update confirmation count for deposits
    pub fn update_confirmations(
        &mut self,
        user_confirmations: Option<u64>,
        mm_confirmations: Option<u64>,
    ) -> TransitionResult {
        let now = utc::now();

        if let (Some(confirmations), Some(status)) =
            (user_confirmations, &mut self.user_deposit_status)
        {
            status.confirmations = confirmations;
            status.last_checked = now;
        }

        if let (Some(confirmations), Some(status)) = (mm_confirmations, &mut self.mm_deposit_status)
        {
            status.confirmations = confirmations;
            status.last_checked = now;
        }

        self.updated_at = now;
        Ok(())
    }

    /// Transition when MM deposit is confirmed
    pub fn mm_deposit_confirmed(&mut self, swap_settlement_timestamp: &DateTime<Utc>) -> TransitionResult {
        ensure!(
            self.status == SwapStatus::WaitingMMDepositConfirmed,
            InvalidTransitionSnafu {
                from: self.status,
                to: SwapStatus::Settled,
            }
        );

        ensure!(
            self.mm_deposit_status.is_some(),
            MissingDataSnafu {
                reason: "MM deposit status not found",
            }
        );

        self.status = SwapStatus::Settled;
        self.updated_at = *swap_settlement_timestamp;

        Ok(())
    }

    /// Record that MM was notified
    pub fn mark_mm_notified(&mut self) -> TransitionResult {
        self.mm_notified_at = Some(utc::now());
        self.updated_at = utc::now();
        Ok(())
    }

    /// Record that private key was sent to MM
    pub fn mark_private_key_sent(&mut self) -> TransitionResult {
        ensure!(
            self.status == SwapStatus::Settled,
            MissingDataSnafu {
                reason: "Can only send private key after settlement",
            }
        );

        self.mm_private_key_sent_at = Some(utc::now());
        self.updated_at = utc::now();
        Ok(())
    }

    /// Record settlement transaction details
    pub fn record_settlement(
        &mut self,
        tx_hash: String,
        confirmations: u64,
        fee: Option<U256>,
    ) -> TransitionResult {
        ensure!(
            self.status == SwapStatus::Settled,
            InvalidTransitionSnafu {
                from: self.status,
                to: SwapStatus::Settled,
            }
        );

        let now = utc::now();
        self.settlement_status = Some(SettlementStatus {
            tx_hash,
            broadcast_at: now,
            confirmations,
            completed_at: Some(now),
            fee,
        });

        self.updated_at = now;
        Ok(())
    }

    /// Update settlement confirmations
    pub fn update_settlement_confirmations(&mut self, confirmations: u64) -> TransitionResult {
        if let Some(settlement) = &mut self.settlement_status {
            settlement.confirmations = confirmations;
            self.updated_at = utc::now();
            Ok(())
        } else {
            Err(TransitionError::MissingData {
                reason: "Settlement status not found".to_string(),
            })
        }
    }

    /// Initiate refund to user
    pub fn initiate_user_refund(&mut self, reason: String) -> TransitionResult {
        ensure!(
            matches!(
                self.status,
                SwapStatus::WaitingUserDepositInitiated
                    | SwapStatus::WaitingUserDepositConfirmed
                    | SwapStatus::WaitingMMDepositInitiated
            ),
            InvalidTransitionSnafu {
                from: self.status,
                to: SwapStatus::RefundingUser,
            }
        );

        self.status = SwapStatus::RefundingUser;
        self.failure_reason = Some(reason);
        self.updated_at = utc::now();
        Ok(())
    }

    /// Mark swap as failed
    pub fn mark_failed(&mut self, reason: String) -> TransitionResult {
        self.status = SwapStatus::Failed;
        self.failure_reason = Some(reason);
        self.updated_at = utc::now();
        Ok(())
    }

    /// Check if swap has timed out
    #[must_use]
    pub fn has_failed(&self) -> bool {
        self.failure_at.is_some()
    }

    /// Check if swap is in an active state (not settled or failed)
    #[must_use]
    pub fn is_active(&self) -> bool {
        !matches!(self.status, SwapStatus::Settled | SwapStatus::Failed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChainType, Currency, Fees, Lot, Metadata, Quote, SwapRates, TokenIdentifier};
    use chrono::Duration;
    use uuid::Uuid;

    fn create_test_swap() -> Swap {
        Swap {
            id: Uuid::new_v4(),
            quote: Quote {
                id: Uuid::new_v4(),
                market_maker_id: Uuid::new_v4(),
                from: Lot {
                    currency: Currency {
                        chain: ChainType::Ethereum,
                        token: TokenIdentifier::Native,
                        decimals: 18,
                    },
                    amount: U256::from(1_000_000u64),
                },
                to: Lot {
                    currency: Currency {
                        chain: ChainType::Bitcoin,
                        token: TokenIdentifier::Native,
                        decimals: 8,
                    },
                    amount: U256::from(996_700u64),
                },
                rates: SwapRates::new(13, 10, 1000),
                fees: Fees {
                    liquidity_fee: U256::from(1300u64),
                    protocol_fee: U256::from(1000u64),
                    network_fee: U256::from(1000u64),
                },
                min_input: U256::from(10_000u64),
                max_input: U256::from(100_000_000u64),
                expires_at: utc::now() + Duration::hours(1),
                created_at: utc::now(),
            },
            market_maker_id: Uuid::new_v4(),
            metadata: Metadata::default(),
            realized: None,
            deposit_vault_salt: [0u8; 32],
            deposit_vault_address: "0x123".to_string(),
            mm_nonce: [0u8; 16],
            user_destination_address: "0x123".to_string(),
            refund_address: "0x1234".to_string(),
            status: SwapStatus::WaitingUserDepositInitiated,
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
    fn test_user_deposit_detected() {
        let mut swap = create_test_swap();

        // Compute realized amounts
        let realized = RealizedSwap::compute(1_000_000, &swap.quote.rates)
            .expect("1M sats should produce valid output");

        // Valid transition
        swap.user_deposit_detected("0xabc123".to_string(), U256::from(1000000u64), 1, Some(realized))
            .unwrap();

        assert_eq!(swap.status, SwapStatus::WaitingUserDepositConfirmed);
        assert!(swap.user_deposit_status.is_some());
        assert!(swap.realized.is_some());
        assert_eq!(
            swap.user_deposit_status.as_ref().unwrap().tx_hash,
            "0xabc123"
        );

        // Valid transition - can replace deposit (e.g., RBF transaction)
        let new_realized = RealizedSwap::compute(1_000_000, &swap.quote.rates)
            .expect("1M sats should produce valid output");
        let result = swap.user_deposit_detected("0xdef456".to_string(), U256::from(1000000u64), 1, Some(new_realized));
        assert!(result.is_ok());
        assert_eq!(
            swap.user_deposit_status.as_ref().unwrap().tx_hash,
            "0xdef456"
        );
    }

    #[test]
    fn test_full_happy_path() {
        let mut swap = create_test_swap();
        let realized = RealizedSwap::compute(1_000_000, &swap.quote.rates)
            .expect("1M sats should produce valid output");

        // User deposits
        swap.user_deposit_detected("0xuser123".to_string(), U256::from(1000000u64), 1, Some(realized))
            .unwrap();
        assert_eq!(swap.status, SwapStatus::WaitingUserDepositConfirmed);

        // User deposit confirmed
        swap.user_deposit_confirmed().unwrap();
        assert_eq!(swap.status, SwapStatus::WaitingMMDepositInitiated);

        // MM deposits (using the computed mm_output)
        let mm_output = swap.realized.as_ref().unwrap().mm_output;
        swap.mm_deposit_detected("0xmm456".to_string(), mm_output, 1)
            .unwrap();
        assert_eq!(swap.status, SwapStatus::WaitingMMDepositConfirmed);

        // MM deposit confirmed
        swap.mm_deposit_confirmed(&utc::now()).unwrap();
        assert_eq!(swap.status, SwapStatus::Settled);

        // Record settlement
        swap.record_settlement("0xsettle789".to_string(), 6, Some(U256::from(1000u64)))
            .unwrap();
        assert!(swap.settlement_status.is_some());
    }

    #[test]
    fn test_timeout_refund() {
        let mut swap = create_test_swap();
        swap.failure_at = Some(utc::now() - Duration::hours(1)); // Already timed out

        assert!(swap.has_failed());

        // Can refund user from waiting state
        swap.initiate_user_refund("Timeout waiting for user deposit".to_string())
            .unwrap();
        assert_eq!(swap.status, SwapStatus::RefundingUser);
    }
}
