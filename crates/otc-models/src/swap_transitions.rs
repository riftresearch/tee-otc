use crate::{Swap, SwapStatus, UserDepositStatus, MMDepositStatus, SettlementStatus};
use alloy::primitives::U256;
use chrono::Utc;
use snafu::{Snafu, ensure};

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
    pub fn user_deposit_detected(
        &mut self,
        tx_hash: String,
        amount: U256,
        confirmations: u64,
    ) -> TransitionResult {
        ensure!(
            self.status == SwapStatus::WaitingUserDepositInitiated,
            InvalidTransitionSnafu {
                from: self.status,
                to: SwapStatus::WaitingUserDepositConfirmed,
            }
        );
        
        let now = Utc::now();
        self.user_deposit_status = Some(UserDepositStatus {
            tx_hash,
            amount,
            detected_at: now,
            confirmations,
            last_checked: now,
        });
        
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
        
        self.status = SwapStatus::WaitingMMDepositInitiated;
        self.updated_at = Utc::now();
        
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
        
        let now = Utc::now();
        self.mm_deposit_status = Some(MMDepositStatus {
            tx_hash,
            amount,
            detected_at: now,
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
        let now = Utc::now();
        
        if let (Some(confirmations), Some(status)) = (user_confirmations, &mut self.user_deposit_status) {
            status.confirmations = confirmations;
            status.last_checked = now;
        }
        
        if let (Some(confirmations), Some(status)) = (mm_confirmations, &mut self.mm_deposit_status) {
            status.confirmations = confirmations;
            status.last_checked = now;
        }
        
        self.updated_at = now;
        Ok(())
    }
    
    /// Transition when MM deposit is confirmed
    pub fn mm_deposit_confirmed(&mut self) -> TransitionResult {
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
        self.updated_at = Utc::now();
        
        Ok(())
    }
    
    /// Record that MM was notified
    pub fn mark_mm_notified(&mut self) -> TransitionResult {
        self.mm_notified_at = Some(Utc::now());
        self.updated_at = Utc::now();
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
        
        self.mm_private_key_sent_at = Some(Utc::now());
        self.updated_at = Utc::now();
        Ok(())
    }
    
    /// Record settlement transaction details
    pub fn record_settlement(&mut self, tx_hash: String, confirmations: u64, fee: Option<U256>) -> TransitionResult {
        ensure!(
            self.status == SwapStatus::Settled,
            InvalidTransitionSnafu {
                from: self.status,
                to: SwapStatus::Settled,
            }
        );
        
        let now = Utc::now();
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
            self.updated_at = Utc::now();
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
                SwapStatus::WaitingUserDepositInitiated |
                SwapStatus::WaitingUserDepositConfirmed |
                SwapStatus::WaitingMMDepositInitiated
            ),
            InvalidTransitionSnafu {
                from: self.status,
                to: SwapStatus::RefundingUser,
            }
        );
        
        self.status = SwapStatus::RefundingUser;
        self.failure_reason = Some(reason);
        self.updated_at = Utc::now();
        Ok(())
    }
    
    /// Initiate refund to MM
    pub fn initiate_mm_refund(&mut self, reason: String) -> TransitionResult {
        ensure!(
            matches!(
                self.status,
                SwapStatus::WaitingMMDepositConfirmed | SwapStatus::Settled
            ),
            InvalidTransitionSnafu {
                from: self.status,
                to: SwapStatus::RefundingMM,
            }
        );
        
        self.status = SwapStatus::RefundingMM;
        self.failure_reason = Some(reason);
        self.updated_at = Utc::now();
        Ok(())
    }
    
    /// Mark swap as failed
    pub fn mark_failed(&mut self, reason: String) -> TransitionResult {
        self.status = SwapStatus::Failed;
        self.failure_reason = Some(reason);
        self.updated_at = Utc::now();
        Ok(())
    }
    
    /// Check if swap has timed out
    #[must_use] pub fn has_failed(&self) -> bool {
        self.failure_at.is_some()
    }
    
    /// Check if swap is in an active state (not settled or failed)
    #[must_use] pub fn is_active(&self) -> bool {
        !matches!(self.status, SwapStatus::Settled | SwapStatus::Failed)
    }
    
    /// Get required confirmations based on chain and amount
    #[must_use] pub fn get_required_confirmations(&self) -> (u64, u64) {
        // TODO: Implement logic based on chain type and amount
        // For now, return default values
        (3, 3) // (user_confirmations, mm_confirmations)
    }
}

#[cfg(test)]
mod tests {
    use crate::{ChainType, Currency, Quote, TokenIdentifier};

    use super::*;
    use chrono::Duration;
    use uuid::Uuid;
    
    fn create_test_swap() -> Swap {
        Swap {
            id: Uuid::new_v4(),
            quote: Quote {
                id: Uuid::new_v4(),
                market_maker_id: Uuid::new_v4(),
                from: Currency {
                    chain: ChainType::Ethereum,
                    token: TokenIdentifier::Native,
                    amount: U256::from(1000000u64),
                    decimals: 18,
                },
                to: Currency {
                    chain: ChainType::Bitcoin,
                    token: TokenIdentifier::Native,
                    amount: U256::from(1000000u64),
                    decimals: 8,
                },
                expires_at: Utc::now() + Duration::hours(1),
                created_at: Utc::now(),
            },
            market_maker_id: Uuid::new_v4(),
            user_deposit_salt: [0u8; 32],
            user_deposit_address: "0x123".to_string(),
            mm_nonce: [0u8; 16],
            user_destination_address: "0x123".to_string(),
            user_refund_address: "bc1q123".to_string(),
            status: SwapStatus::WaitingUserDepositInitiated,
            user_deposit_status: None,
            mm_deposit_status: None,
            settlement_status: None,
            failure_reason: None,
            failure_at: None,
            mm_notified_at: None,
            mm_private_key_sent_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }
    
    #[test]
    fn test_user_deposit_detected() {
        let mut swap = create_test_swap();
        
        // Valid transition
        swap.user_deposit_detected(
            "0xabc123".to_string(),
            U256::from(1000000u64),
            1,
        ).unwrap();
        
        assert_eq!(swap.status, SwapStatus::WaitingUserDepositConfirmed);
        assert!(swap.user_deposit_status.is_some());
        assert_eq!(swap.user_deposit_status.as_ref().unwrap().tx_hash, "0xabc123");
        
        // Invalid transition - can't deposit again
        let result = swap.user_deposit_detected(
            "0xdef456".to_string(),
            U256::from(1000000u64),
            1,
        );
        assert!(result.is_err());
    }
    
    #[test]
    fn test_full_happy_path() {
        let mut swap = create_test_swap();
        
        // User deposits
        swap.user_deposit_detected(
            "0xuser123".to_string(),
            U256::from(1000000u64),
            1,
        ).unwrap();
        assert_eq!(swap.status, SwapStatus::WaitingUserDepositConfirmed);
        
        // User deposit confirmed
        swap.user_deposit_confirmed().unwrap();
        assert_eq!(swap.status, SwapStatus::WaitingMMDepositInitiated);
        
        // MM deposits
        swap.mm_deposit_detected(
            "0xmm456".to_string(),
            U256::from(500000u64),
            1,
        ).unwrap();
        assert_eq!(swap.status, SwapStatus::WaitingMMDepositConfirmed);
        
        // MM deposit confirmed
        swap.mm_deposit_confirmed().unwrap();
        assert_eq!(swap.status, SwapStatus::Settled);
        
        // Record settlement
        swap.record_settlement("0xsettle789".to_string(), 6, Some(U256::from(1000u64))).unwrap();
        assert!(swap.settlement_status.is_some());
    }
    
    #[test]
    fn test_timeout_refund() {
        let mut swap = create_test_swap();
        swap.failure_at = Some(Utc::now() - Duration::hours(1)); // Already timed out
        
        assert!(swap.has_failed());
        
        // Can refund user from waiting state
        swap.initiate_user_refund("Timeout waiting for user deposit".to_string()).unwrap();
        assert_eq!(swap.status, SwapStatus::RefundingUser);
    }
}