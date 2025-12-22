use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(
    feature = "sqlx",
    sqlx(type_name = "swap_status", rename_all = "snake_case")
)]
pub enum SwapStatus {
    WaitingUserDepositInitiated,
    WaitingUserDepositConfirmed,
    #[serde(rename = "waiting_mm_deposit_initiated")]
    #[cfg_attr(feature = "sqlx", sqlx(rename = "waiting_mm_deposit_initiated"))]
    WaitingMMDepositInitiated,
    #[serde(rename = "waiting_mm_deposit_confirmed")]
    #[cfg_attr(feature = "sqlx", sqlx(rename = "waiting_mm_deposit_confirmed"))]
    WaitingMMDepositConfirmed,
    Settled,
    RefundingUser,
    Failed,
}

#[test]
fn test_serialization() {
    let status = SwapStatus::WaitingMMDepositConfirmed;
    let json = serde_json::to_string(&status).unwrap();
    assert_eq!(json, r#""waiting_mm_deposit_confirmed""#);
}
