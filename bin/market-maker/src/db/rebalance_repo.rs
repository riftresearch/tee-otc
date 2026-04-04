use chrono::{DateTime, Utc};
use otc_models::ChainType;
use snafu::prelude::*;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::planner::{PlannerAsset, Rebalance};

#[derive(Debug, Snafu)]
pub enum RebalanceRepositoryError {
    #[snafu(display("Database error: {source}"))]
    Database { source: sqlx::Error },

    #[snafu(display("Unknown planner asset value: {value}"))]
    UnknownPlannerAsset { value: String },

    #[snafu(display("Unknown rebalance state value: {value}"))]
    UnknownRebalanceState { value: String },

    #[snafu(display("Unknown EVM chain value: {value}"))]
    UnknownChain { value: String },
}

pub type RebalanceRepositoryResult<T, E = RebalanceRepositoryError> = std::result::Result<T, E>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebalanceJobState {
    PendingSourceTransfer,
    WaitingSourceConfirmations,
    PendingWithdrawalSubmission,
    WaitingWithdrawalCompletion,
    Completed,
    Failed,
}

impl RebalanceJobState {
    #[must_use]
    pub fn is_active(self) -> bool {
        !matches!(self, Self::Completed | Self::Failed)
    }
}

#[derive(Debug, Clone)]
pub struct StoredRebalance {
    pub id: Uuid,
    pub rebalance: Rebalance,
    pub evm_chain: ChainType,
    pub state: RebalanceJobState,
    pub recipient_address: String,
    pub source_confirmations_required: u32,
    pub source_tx_hash: Option<String>,
    pub withdrawal_id: Option<String>,
    pub completion_tx_hash: Option<String>,
    pub failure_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct RebalanceRepository {
    pool: PgPool,
}

impl RebalanceRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create_rebalance(
        &self,
        rebalance: Rebalance,
        evm_chain: ChainType,
        recipient_address: &str,
        source_confirmations_required: u32,
    ) -> RebalanceRepositoryResult<StoredRebalance> {
        let id = Uuid::now_v7();
        let now = utc::now();

        sqlx::query(
            r#"
            INSERT INTO mm_rebalances (
                id,
                src_asset,
                dst_asset,
                amount_sats,
                evm_chain,
                state,
                recipient_address,
                source_confirmations_required,
                created_at,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)
            "#,
        )
        .bind(id)
        .bind(planner_asset_to_db(rebalance.src_asset))
        .bind(planner_asset_to_db(rebalance.dst_asset))
        .bind(rebalance.amount as i64)
        .bind(evm_chain.to_db_string())
        .bind(rebalance_job_state_to_db(
            RebalanceJobState::PendingSourceTransfer,
        ))
        .bind(recipient_address)
        .bind(source_confirmations_required as i32)
        .bind(now)
        .execute(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        self.get_rebalance(id)
            .await?
            .expect("just-created rebalance must exist")
            .pipe(Ok)
    }

    pub async fn get_rebalance(
        &self,
        id: Uuid,
    ) -> RebalanceRepositoryResult<Option<StoredRebalance>> {
        let row = sqlx::query(
            r#"
            SELECT
                id,
                src_asset,
                dst_asset,
                amount_sats,
                evm_chain,
                state,
                recipient_address,
                source_confirmations_required,
                source_tx_hash,
                withdrawal_id,
                completion_tx_hash,
                failure_reason,
                created_at,
                updated_at
            FROM mm_rebalances
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        row.map(parse_row).transpose()
    }

    pub async fn list_active_rebalances(&self) -> RebalanceRepositoryResult<Vec<StoredRebalance>> {
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                src_asset,
                dst_asset,
                amount_sats,
                evm_chain,
                state,
                recipient_address,
                source_confirmations_required,
                source_tx_hash,
                withdrawal_id,
                completion_tx_hash,
                failure_reason,
                created_at,
                updated_at
            FROM mm_rebalances
            WHERE state NOT IN ('completed', 'failed')
            ORDER BY created_at ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        rows.into_iter().map(parse_row).collect()
    }

    pub async fn mark_waiting_source_confirmations(
        &self,
        id: Uuid,
        source_tx_hash: &str,
    ) -> RebalanceRepositoryResult<()> {
        self.update_state(
            id,
            RebalanceJobState::WaitingSourceConfirmations,
            Some(source_tx_hash),
            None,
            None,
            None,
        )
        .await
    }

    pub async fn mark_pending_withdrawal_submission(
        &self,
        id: Uuid,
    ) -> RebalanceRepositoryResult<()> {
        self.update_state(
            id,
            RebalanceJobState::PendingWithdrawalSubmission,
            None,
            None,
            None,
            None,
        )
        .await
    }

    pub async fn mark_waiting_withdrawal_completion(
        &self,
        id: Uuid,
        withdrawal_id: &str,
    ) -> RebalanceRepositoryResult<()> {
        self.update_state(
            id,
            RebalanceJobState::WaitingWithdrawalCompletion,
            None,
            Some(withdrawal_id),
            None,
            None,
        )
        .await
    }

    pub async fn mark_completed(
        &self,
        id: Uuid,
        completion_tx_hash: &str,
    ) -> RebalanceRepositoryResult<()> {
        self.update_state(
            id,
            RebalanceJobState::Completed,
            None,
            None,
            Some(completion_tx_hash),
            None,
        )
        .await
    }

    pub async fn mark_failed(
        &self,
        id: Uuid,
        failure_reason: &str,
    ) -> RebalanceRepositoryResult<()> {
        self.update_state(
            id,
            RebalanceJobState::Failed,
            None,
            None,
            None,
            Some(failure_reason),
        )
        .await
    }

    async fn update_state(
        &self,
        id: Uuid,
        state: RebalanceJobState,
        source_tx_hash: Option<&str>,
        withdrawal_id: Option<&str>,
        completion_tx_hash: Option<&str>,
        failure_reason: Option<&str>,
    ) -> RebalanceRepositoryResult<()> {
        sqlx::query(
            r#"
            UPDATE mm_rebalances
            SET
                state = $2,
                source_tx_hash = COALESCE($3, source_tx_hash),
                withdrawal_id = COALESCE($4, withdrawal_id),
                completion_tx_hash = COALESCE($5, completion_tx_hash),
                failure_reason = $6,
                updated_at = $7
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(rebalance_job_state_to_db(state))
        .bind(source_tx_hash)
        .bind(withdrawal_id)
        .bind(completion_tx_hash)
        .bind(failure_reason)
        .bind(utc::now())
        .execute(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        Ok(())
    }
}

fn parse_row(row: sqlx::postgres::PgRow) -> RebalanceRepositoryResult<StoredRebalance> {
    let src_asset = planner_asset_from_db(row.try_get("src_asset").context(DatabaseSnafu)?)?;
    let dst_asset = planner_asset_from_db(row.try_get("dst_asset").context(DatabaseSnafu)?)?;
    let evm_chain = ChainType::from_db_string(
        &row.try_get::<String, _>("evm_chain")
            .context(DatabaseSnafu)?,
    )
    .ok_or_else(|| RebalanceRepositoryError::UnknownChain {
        value: row
            .try_get::<String, _>("evm_chain")
            .unwrap_or_else(|_| "<missing>".to_string()),
    })?;

    Ok(StoredRebalance {
        id: row.try_get("id").context(DatabaseSnafu)?,
        rebalance: Rebalance {
            src_asset,
            dst_asset,
            amount: row
                .try_get::<i64, _>("amount_sats")
                .context(DatabaseSnafu)? as u64,
        },
        evm_chain,
        state: rebalance_job_state_from_db(row.try_get("state").context(DatabaseSnafu)?)?,
        recipient_address: row.try_get("recipient_address").context(DatabaseSnafu)?,
        source_confirmations_required: row
            .try_get::<i32, _>("source_confirmations_required")
            .context(DatabaseSnafu)? as u32,
        source_tx_hash: row.try_get("source_tx_hash").context(DatabaseSnafu)?,
        withdrawal_id: row.try_get("withdrawal_id").context(DatabaseSnafu)?,
        completion_tx_hash: row.try_get("completion_tx_hash").context(DatabaseSnafu)?,
        failure_reason: row.try_get("failure_reason").context(DatabaseSnafu)?,
        created_at: row.try_get("created_at").context(DatabaseSnafu)?,
        updated_at: row.try_get("updated_at").context(DatabaseSnafu)?,
    })
}

fn planner_asset_to_db(asset: PlannerAsset) -> &'static str {
    match asset {
        PlannerAsset::AssetA => "asset_a",
        PlannerAsset::AssetB => "asset_b",
    }
}

fn planner_asset_from_db(value: String) -> RebalanceRepositoryResult<PlannerAsset> {
    match value.as_str() {
        "asset_a" => Ok(PlannerAsset::AssetA),
        "asset_b" => Ok(PlannerAsset::AssetB),
        _ => UnknownPlannerAssetSnafu { value }.fail(),
    }
}

fn rebalance_job_state_to_db(state: RebalanceJobState) -> &'static str {
    match state {
        RebalanceJobState::PendingSourceTransfer => "pending_source_transfer",
        RebalanceJobState::WaitingSourceConfirmations => "waiting_source_confirmations",
        RebalanceJobState::PendingWithdrawalSubmission => "pending_withdrawal_submission",
        RebalanceJobState::WaitingWithdrawalCompletion => "waiting_withdrawal_completion",
        RebalanceJobState::Completed => "completed",
        RebalanceJobState::Failed => "failed",
    }
}

fn rebalance_job_state_from_db(value: String) -> RebalanceRepositoryResult<RebalanceJobState> {
    match value.as_str() {
        "pending_source_transfer" => Ok(RebalanceJobState::PendingSourceTransfer),
        "waiting_source_confirmations" => Ok(RebalanceJobState::WaitingSourceConfirmations),
        "pending_withdrawal_submission" => Ok(RebalanceJobState::PendingWithdrawalSubmission),
        "waiting_withdrawal_completion" => Ok(RebalanceJobState::WaitingWithdrawalCompletion),
        "completed" => Ok(RebalanceJobState::Completed),
        "failed" => Ok(RebalanceJobState::Failed),
        _ => UnknownRebalanceStateSnafu { value }.fail(),
    }
}

trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}

impl<T> Pipe for T {}

#[cfg(test)]
mod tests {
    use super::*;

    #[sqlx::test]
    async fn create_and_resume_active_rebalance(pool: sqlx::PgPool) -> sqlx::Result<()> {
        let db = crate::db::Database::from_pool(pool).await.unwrap();
        let repo = db.rebalances();

        let stored = repo
            .create_rebalance(
                Rebalance {
                    src_asset: PlannerAsset::AssetA,
                    dst_asset: PlannerAsset::AssetB,
                    amount: 42,
                },
                ChainType::Base,
                "dest-addr",
                3,
            )
            .await
            .unwrap();

        repo.mark_waiting_source_confirmations(stored.id, "source-tx")
            .await
            .unwrap();

        let active = repo.list_active_rebalances().await.unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(
            active[0].state,
            RebalanceJobState::WaitingSourceConfirmations
        );
        assert_eq!(active[0].source_tx_hash.as_deref(), Some("source-tx"));

        Ok(())
    }
}
