use chrono::{DateTime, Utc};
use otc_models::ChainType;
use snafu::prelude::*;
use sqlx::PgPool;
use sqlx::Row;
use uuid::Uuid;

// Provides `utc::now()` for consistent UTC time across the crate.
use utc;

#[derive(Debug, Snafu)]
pub enum PaymentRepositoryError {
    #[snafu(display("Database error: {source}"))]
    Database { source: sqlx::Error },

    #[snafu(display("Invalid batch nonce digest length: {len}"))]
    InvalidDigest { len: usize },

    #[snafu(display("Unknown chain value: {value}"))]
    UnknownChain { value: String },

    #[snafu(display("Unknown batch status value: {value}"))]
    UnknownBatchStatus { value: String },

    #[snafu(display("Unknown fee settlement rail value: {value}"))]
    UnknownFeeSettlementRail { value: String },

    #[snafu(display("Unknown fee settlement ack status value: {value}"))]
    UnknownFeeSettlementAckStatus { value: String },
}

pub type PaymentRepositoryResult<T, E = PaymentRepositoryError> = std::result::Result<T, E>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchStatus {
    Created,
    Confirmed,
    Cancelled,
}

#[derive(Clone)]
pub struct PaymentRepository {
    pool: PgPool,
}

#[derive(Debug, Clone)]
pub struct StoredBatch {
    pub txid: String,
    pub chain: ChainType,
    pub swap_ids: Vec<Uuid>,
    pub batch_nonce_digest: [u8; 32],
    pub aggregated_fee_sats: u64,
    pub fee_settlement_txid: Option<String>,
    pub created_at: DateTime<Utc>,
    pub status: BatchStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeeSettlementAckStatus {
    Pending,
    Accepted,
    Rejected,
}

#[derive(Debug, Clone)]
pub struct StoredFeeSettlement {
    pub txid: String,
    pub rail: crate::FeeSettlementRail,
    pub evm_chain: Option<ChainType>,
    pub batch_nonce_digests: Vec<[u8; 32]>,
    pub otc_ack_status: FeeSettlementAckStatus,
    pub otc_last_submitted_at: Option<DateTime<Utc>>,
}

impl PaymentRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn has_payment_been_made(
        &self,
        swap_id: Uuid,
    ) -> PaymentRepositoryResult<Option<String>> {
        let row: Option<(String,)> = sqlx::query_as(
            r#"
            SELECT txid
            FROM mm_payments
            WHERE swap_id = $1
            "#,
        )
        .bind(swap_id)
        .fetch_optional(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        Ok(row.map(|(txid,)| txid))
    }

    pub async fn set_payment(
        &self,
        swap_id: Uuid,
        txid: impl Into<String>,
    ) -> PaymentRepositoryResult<()> {
        let txid = txid.into();

        sqlx::query(
            r#"
            INSERT INTO mm_payments (swap_id, txid)
            VALUES ($1, $2)
            ON CONFLICT (swap_id)
            DO UPDATE SET txid = EXCLUDED.txid
            "#,
        )
        .bind(swap_id)
        .bind(txid)
        .execute(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        Ok(())
    }

    pub async fn set_batch_payment(
        &self,
        swap_ids: Vec<Uuid>,
        txid: impl Into<String>,
        chain: ChainType,
        batch_nonce_digest: [u8; 32],
        aggregated_fee_sats: u64,
    ) -> PaymentRepositoryResult<()> {
        let txid = txid.into();

        let mut transaction = self.pool.begin().await.context(DatabaseSnafu)?;
        let created_at = utc::now();

        sqlx::query(
            r#"
            INSERT INTO mm_batches (txid, chain, swap_ids, batch_nonce_digest, aggregated_fee_sats, status, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (txid)
            DO UPDATE SET
                chain = EXCLUDED.chain,
                swap_ids = EXCLUDED.swap_ids,
                batch_nonce_digest = EXCLUDED.batch_nonce_digest,
                aggregated_fee_sats = EXCLUDED.aggregated_fee_sats,
                status = EXCLUDED.status,
                created_at = EXCLUDED.created_at
            "#,
        )
        .bind(&txid)
        .bind(chain.to_db_string())
        .bind(&swap_ids)
        .bind(batch_nonce_digest.as_slice())
        .bind(aggregated_fee_sats as i64)
        .bind(batch_status_to_db(&BatchStatus::Created))
        .bind(created_at)
        .execute(&mut *transaction)
        .await
        .context(DatabaseSnafu)?;

        for swap_id in swap_ids {
            sqlx::query(
                r#"
                INSERT INTO mm_payments (swap_id, txid)
                VALUES ($1, $2)
                ON CONFLICT (swap_id)
                DO UPDATE SET txid = EXCLUDED.txid
                "#,
            )
            .bind(swap_id)
            .bind(&txid)
            .execute(&mut *transaction)
            .await
            .context(DatabaseSnafu)?;
        }

        transaction.commit().await.context(DatabaseSnafu)?;

        Ok(())
    }

    pub async fn update_batch_status(
        &self,
        txid: &str,
        status: BatchStatus,
    ) -> PaymentRepositoryResult<()> {
        sqlx::query(
            r#"
            UPDATE mm_batches
            SET status = $1
            WHERE txid = $2
            "#,
        )
        .bind(batch_status_to_db(&status))
        .bind(txid)
        .execute(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        Ok(())
    }

    pub async fn get_batches_by_status(
        &self,
        status: BatchStatus,
    ) -> PaymentRepositoryResult<Vec<StoredBatch>> {
        let rows = sqlx::query(
            r#"
            SELECT txid, chain, swap_ids, batch_nonce_digest, aggregated_fee_sats, fee_settlement_txid, created_at, status
            FROM mm_batches
            WHERE status = $1
            ORDER BY created_at ASC
            "#,
        )
        .bind(batch_status_to_db(&status))
        .fetch_all(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        self.parse_batch_rows(rows)
    }

    pub async fn get_confirmed_unsettled_batches(
        &self,
        limit: usize,
    ) -> PaymentRepositoryResult<Vec<StoredBatch>> {
        let rows = sqlx::query(
            r#"
            SELECT txid, chain, swap_ids, batch_nonce_digest, aggregated_fee_sats, fee_settlement_txid, created_at, status
            FROM mm_batches
            WHERE status = 'confirmed'
              AND fee_settlement_txid IS NULL
            ORDER BY created_at ASC
            LIMIT $1
            "#,
        )
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        self.parse_batch_rows(rows)
    }

    pub async fn record_fee_settlement(
        &self,
        txid: &str,
        rail: crate::FeeSettlementRail,
        chain: ChainType,
        settlement_digest: [u8; 32],
        batch_nonce_digests: Vec<[u8; 32]>,
        referenced_fee_sats: u64,
        amount_sats: u64,
        batch_txids: &[String],
    ) -> PaymentRepositoryResult<()> {
        let mut transaction = self.pool.begin().await.context(DatabaseSnafu)?;
        let created_at = utc::now();

        let settlement_digest: Vec<u8> = settlement_digest.to_vec();
        let batch_nonce_digests: Vec<Vec<u8>> =
            batch_nonce_digests.into_iter().map(|d| d.to_vec()).collect();

        sqlx::query(
            r#"
            INSERT INTO mm_fee_settlements (
                txid, rail, evm_chain, settlement_digest, batch_nonce_digests,
                referenced_fee_sats, amount_sats, status, created_at,
                otc_ack_status, otc_submit_attempts
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'submitted', $8, 'pending', 0)
            ON CONFLICT (txid)
            DO NOTHING
            "#,
        )
        .bind(txid)
        .bind(match rail {
            crate::FeeSettlementRail::Evm => "evm",
            crate::FeeSettlementRail::Bitcoin => "bitcoin",
        })
        .bind(match rail {
            crate::FeeSettlementRail::Evm => Some(chain.to_db_string()),
            crate::FeeSettlementRail::Bitcoin => None,
        })
        .bind(settlement_digest)
        .bind(batch_nonce_digests)
        .bind(referenced_fee_sats as i64)
        .bind(amount_sats as i64)
        .bind(created_at)
        .execute(&mut *transaction)
        .await
        .context(DatabaseSnafu)?;

        for batch_txid in batch_txids {
            sqlx::query(
                r#"
                UPDATE mm_batches
                SET fee_settlement_txid = $1
                WHERE txid = $2
                "#,
            )
            .bind(txid)
            .bind(batch_txid)
            .execute(&mut *transaction)
            .await
            .context(DatabaseSnafu)?;
        }

        transaction.commit().await.context(DatabaseSnafu)?;
        Ok(())
    }

    /// Check if there's already a pending fee settlement for the given rail/chain.
    /// Returns true if a pending settlement exists (and we should NOT create a new one).
    pub async fn has_pending_fee_settlement(
        &self,
        rail: crate::FeeSettlementRail,
        evm_chain: Option<ChainType>,
    ) -> PaymentRepositoryResult<bool> {
        let (rail_str, chain_filter) = match rail {
            crate::FeeSettlementRail::Bitcoin => ("bitcoin", None),
            crate::FeeSettlementRail::Evm => ("evm", evm_chain.map(|c| c.to_db_string())),
        };

        let count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM mm_fee_settlements
            WHERE rail = $1
              AND ($2::text IS NULL OR evm_chain = $2)
              AND otc_ack_status = 'pending'
            "#,
        )
        .bind(rail_str)
        .bind(chain_filter)
        .fetch_one(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        Ok(count > 0)
    }

    /// Maximum number of submission attempts before giving up on a fee settlement.
    const MAX_SUBMIT_ATTEMPTS: i64 = 100;

    pub async fn list_fee_settlements_needing_resubmission(
        &self,
        min_resubmit_age_secs: i64,
        limit: i64,
        now: DateTime<Utc>,
    ) -> PaymentRepositoryResult<Vec<StoredFeeSettlement>> {
        // Only retry 'pending' settlements (not 'accepted' or 'rejected').
        // Rejected settlements should not be retried indefinitely - if the server
        // rejected it, the MM needs to investigate the rejection reason.
        // We also cap retries to avoid infinite loops for settlements that keep timing out.
        let rows = sqlx::query(
            r#"
            SELECT txid, rail, evm_chain, batch_nonce_digests, otc_ack_status, otc_last_submitted_at
            FROM mm_fee_settlements
            WHERE otc_ack_status = 'pending'
              AND otc_submit_attempts < $4
              AND (
                otc_last_submitted_at IS NULL
                OR otc_last_submitted_at <= ($1::timestamptz - ($2::bigint * interval '1 second'))
              )
            ORDER BY created_at ASC
            LIMIT $3
            "#,
        )
        .bind(now)
        .bind(min_resubmit_age_secs)
        .bind(limit)
        .bind(Self::MAX_SUBMIT_ATTEMPTS)
        .fetch_all(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let txid: String = row.try_get("txid").context(DatabaseSnafu)?;
            let rail_s: String = row.try_get("rail").context(DatabaseSnafu)?;
            let evm_chain_s: Option<String> = row.try_get("evm_chain").context(DatabaseSnafu)?;
            let batch_nonce_digests_raw: Vec<Vec<u8>> =
                row.try_get("batch_nonce_digests").context(DatabaseSnafu)?;
            let ack_s: String = row.try_get("otc_ack_status").context(DatabaseSnafu)?;
            let otc_last_submitted_at: Option<DateTime<Utc>> =
                row.try_get("otc_last_submitted_at").context(DatabaseSnafu)?;

            let rail = match rail_s.as_str() {
                "evm" => crate::FeeSettlementRail::Evm,
                "bitcoin" => crate::FeeSettlementRail::Bitcoin,
                other => {
                    return Err(PaymentRepositoryError::UnknownFeeSettlementRail {
                        value: other.to_string(),
                    });
                }
            };

            let evm_chain = match (rail, evm_chain_s) {
                (crate::FeeSettlementRail::Evm, Some(s)) => Some(
                    ChainType::from_db_string(&s)
                        .ok_or(PaymentRepositoryError::UnknownChain { value: s })?,
                ),
                _ => None,
            };

            let otc_ack_status = match ack_s.as_str() {
                "pending" => FeeSettlementAckStatus::Pending,
                "accepted" => FeeSettlementAckStatus::Accepted,
                "rejected" => FeeSettlementAckStatus::Rejected,
                other => {
                    return Err(PaymentRepositoryError::UnknownFeeSettlementAckStatus {
                        value: other.to_string(),
                    });
                }
            };

            let mut batch_nonce_digests = Vec::with_capacity(batch_nonce_digests_raw.len());
            for bytes in batch_nonce_digests_raw {
                if bytes.len() != 32 {
                    return Err(PaymentRepositoryError::InvalidDigest { len: bytes.len() });
                }
                let mut digest = [0u8; 32];
                digest.copy_from_slice(&bytes);
                batch_nonce_digests.push(digest);
            }

            out.push(StoredFeeSettlement {
                txid,
                rail,
                evm_chain,
                batch_nonce_digests,
                otc_ack_status,
                otc_last_submitted_at,
            });
        }

        Ok(out)
    }

    pub async fn mark_fee_settlement_submission_attempt(
        &self,
        txid: &str,
        request_id: Uuid,
        now: DateTime<Utc>,
    ) -> PaymentRepositoryResult<()> {
        sqlx::query(
            r#"
            UPDATE mm_fee_settlements
            SET
                otc_last_request_id = $2,
                otc_last_submitted_at = $3,
                otc_submit_attempts = otc_submit_attempts + 1,
                otc_ack_status = 'pending',
                otc_ack_reason = NULL,
                otc_acked_at = NULL
            WHERE txid = $1
            "#,
        )
        .bind(txid)
        .bind(request_id)
        .bind(now)
        .execute(&self.pool)
        .await
        .context(DatabaseSnafu)?;
        Ok(())
    }

    pub async fn mark_fee_settlement_ack(
        &self,
        txid: &str,
        accepted: bool,
        rejection_reason: Option<&str>,
        now: DateTime<Utc>,
    ) -> PaymentRepositoryResult<()> {
        let status = if accepted { "accepted" } else { "rejected" };
        sqlx::query(
            r#"
            UPDATE mm_fee_settlements
            SET
                otc_ack_status = $2,
                otc_ack_reason = $3,
                otc_acked_at = $4
            WHERE txid = $1
            "#,
        )
        .bind(txid)
        .bind(status)
        .bind(rejection_reason)
        .bind(now)
        .execute(&self.pool)
        .await
        .context(DatabaseSnafu)?;
        Ok(())
    }

    pub async fn list_batches(&self, newest_seen_batch_timestamp: Option<DateTime<Utc>>) -> PaymentRepositoryResult<Vec<StoredBatch>> {
        let rows = sqlx::query(
            r#"
            SELECT txid, chain, swap_ids, batch_nonce_digest, aggregated_fee_sats, fee_settlement_txid, created_at, status
            FROM mm_batches
            WHERE created_at > $1
            ORDER BY created_at ASC
            "#,
        )
        .bind(newest_seen_batch_timestamp.unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap()))
        .fetch_all(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        self.parse_batch_rows(rows)
    }

    fn parse_batch_rows(
        &self,
        rows: Vec<sqlx::postgres::PgRow>,
    ) -> PaymentRepositoryResult<Vec<StoredBatch>> {
        let mut batches = Vec::with_capacity(rows.len());
        for row in rows {
            let txid: String = row.try_get("txid").context(DatabaseSnafu)?;
            let chain: String = row.try_get("chain").context(DatabaseSnafu)?;
            let swap_ids: Vec<Uuid> = row.try_get("swap_ids").context(DatabaseSnafu)?;
            let digest: Vec<u8> = row.try_get("batch_nonce_digest").context(DatabaseSnafu)?;
            let aggregated_fee_sats: i64 = row.try_get("aggregated_fee_sats").context(DatabaseSnafu)?;
            let aggregated_fee_sats = aggregated_fee_sats as u64;
            let fee_settlement_txid: Option<String> = row.try_get("fee_settlement_txid").context(DatabaseSnafu)?;
            let created_at: DateTime<Utc> = row.try_get("created_at").context(DatabaseSnafu)?;
            let status: String = row.try_get("status").context(DatabaseSnafu)?;

            if digest.len() != 32 {
                return Err(PaymentRepositoryError::InvalidDigest { len: digest.len() });
            }
            let mut batch_nonce_digest = [0u8; 32];
            batch_nonce_digest.copy_from_slice(&digest);

            batches.push(StoredBatch {
                txid,
                chain: ChainType::from_db_string(&chain).ok_or(PaymentRepositoryError::UnknownChain { value: chain })?,
                swap_ids,
                batch_nonce_digest,
                aggregated_fee_sats,
                fee_settlement_txid,
                created_at,
                status: batch_status_from_db(&status)?,
            });
        }

        Ok(batches)
    }
}



fn batch_status_to_db(status: &BatchStatus) -> &'static str {
    match status {
        BatchStatus::Created => "created",
        BatchStatus::Confirmed => "confirmed",
        BatchStatus::Cancelled => "cancelled",
    }
}

fn batch_status_from_db(value: &str) -> PaymentRepositoryResult<BatchStatus> {
    match value {
        "created" => Ok(BatchStatus::Created),
        "confirmed" => Ok(BatchStatus::Confirmed),
        "cancelled" => Ok(BatchStatus::Cancelled),
        other => Err(PaymentRepositoryError::UnknownBatchStatus {
            value: other.to_string(),
        }),
    }
}
