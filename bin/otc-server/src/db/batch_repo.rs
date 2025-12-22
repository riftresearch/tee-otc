use chrono::{DateTime, Utc};
use otc_chains::traits::MarketMakerBatch;
use otc_models::ChainType;
use sqlx::postgres::PgPool;
use sqlx::Row;
use uuid::Uuid;

use crate::error::{OtcServerError, OtcServerResult};

#[derive(Clone)]
pub struct BatchRepository {
    pool: PgPool,
}

impl BatchRepository {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Add a new batch payment record to the database
    pub async fn add_batch(
        &self,
        chain: ChainType,
        tx_hash: String,
        batch: &MarketMakerBatch,
        swap_ids: Vec<Uuid>,
        market_maker_id: Uuid,
    ) -> OtcServerResult<()> {
        let chain_str = chain.to_db_string();
        let batch_json = serde_json::to_value(batch).map_err(|e| OtcServerError::InvalidData {
            message: format!("Failed to serialize batch: {e}"),
        })?;
        let swap_ids_json =
            serde_json::to_value(&swap_ids).map_err(|e| OtcServerError::InvalidData {
                message: format!("Failed to serialize swap ids: {e}"),
            })?;
        let batch_nonce_digest: Vec<u8> = batch.payment_verification.batch_nonce_digest.to_vec();

        sqlx::query(
            r#"
            INSERT INTO batches (
                chain,
                tx_hash,
                full_batch,
                swap_ids,
                market_maker_id,
                batch_nonce_digest,
                created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, NOW())
            "#,
        )
        .bind(chain_str)
        .bind(&tx_hash)
        .bind(batch_json)
        .bind(swap_ids_json)
        .bind(market_maker_id)
        .bind(batch_nonce_digest)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Mark a batch as confirmed by otc-server (idempotent).
    pub async fn mark_confirmed(
        &self,
        chain: &ChainType,
        tx_hash: &str,
        confirmed_at: DateTime<Utc>,
    ) -> OtcServerResult<()> {
        let chain_str = chain.to_db_string();
        sqlx::query(
            r#"
            UPDATE batches
            SET confirmed_at = $3
            WHERE chain = $1
              AND tx_hash = $2
              AND confirmed_at IS NULL
            "#,
        )
        .bind(chain_str)
        .bind(tx_hash)
        .bind(confirmed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Get a batch payment by chain type and transaction hash
    pub async fn get_batch(
        &self,
        chain: &ChainType,
        tx_hash: &str,
    ) -> OtcServerResult<Option<(MarketMakerBatch, Vec<Uuid>)>> {
        let chain_str = chain.to_db_string();

        let row = sqlx::query(
            r#"
            SELECT 
                full_batch,
                swap_ids
            FROM batches
            WHERE chain = $1 AND tx_hash = $2
            "#,
        )
        .bind(chain_str)
        .bind(tx_hash)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => {
                let batch_json: serde_json::Value = row.get("full_batch");
                let swap_ids_json: serde_json::Value = row.get("swap_ids");

                let batch: MarketMakerBatch = serde_json::from_value(batch_json).map_err(|e| {
                    OtcServerError::InvalidData {
                        message: format!("Failed to deserialize batch: {e}"),
                    }
                })?;
                let swap_ids: Vec<Uuid> = serde_json::from_value(swap_ids_json).map_err(|e| {
                    OtcServerError::InvalidData {
                        message: format!("Failed to deserialize swap ids: {e}"),
                    }
                })?;

                Ok(Some((batch, swap_ids)))
            }
            None => Ok(None),
        }
    }

    /// Get the timestamp of the latest batch payment sent by a specific market maker
    pub async fn get_latest_known_batch_timestamp_by_market_maker(
        &self,
        market_maker_id: &Uuid,
    ) -> OtcServerResult<Option<DateTime<Utc>>> {
        let row = sqlx::query(
            r#"
            SELECT 
                created_at
            FROM batches
            WHERE market_maker_id = $1
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(market_maker_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| r.get("created_at")))
    }

    /// Fetch confirmed batches by their `batch_nonce_digest` for a specific market maker.
    ///
    /// Only returns batches that have been marked as confirmed (i.e., `confirmed_at IS NOT NULL`).
    pub async fn get_confirmed_batches_by_nonce_digests(
        &self,
        market_maker_id: Uuid,
        batch_nonce_digests: &[[u8; 32]],
    ) -> OtcServerResult<Vec<MarketMakerBatch>> {
        if batch_nonce_digests.is_empty() {
            return Ok(Vec::new());
        }

        let digests: Vec<Vec<u8>> = batch_nonce_digests.iter().map(|d| d.to_vec()).collect();

        let rows = sqlx::query(
            r#"
            SELECT full_batch
            FROM batches
            WHERE market_maker_id = $1
              AND batch_nonce_digest = ANY($2)
              AND confirmed_at IS NOT NULL
            "#,
        )
        .bind(market_maker_id)
        .bind(digests)
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let batch_json: serde_json::Value = row.get("full_batch");
            let batch: MarketMakerBatch = serde_json::from_value(batch_json).map_err(|e| {
                OtcServerError::InvalidData {
                    message: format!("Failed to deserialize batch: {e}"),
                }
            })?;
            out.push(batch);
        }
        Ok(out)
    }

}
