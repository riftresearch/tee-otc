use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(feature = "sqlx", sqlx(type_name = "swap_status", rename_all = "snake_case"))]
pub enum SwapStatus {
    WaitingUserDepositInitiated,
    WaitingUserDepositConfirmed,
    WaitingMMDepositInitiated,
    WaitingMMDepositConfirmed,
    Settled,
    RefundingUser,
    RefundingMM,
    Failed,
}