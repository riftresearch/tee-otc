use chrono::{DateTime, Utc};
use otc_chains::traits::MarketMakerBatch;
use otc_models::ChainType;
use sqlx::postgres::PgPool;
use sqlx::Row;
use uuid::Uuid;

use crate::error::{OtcServerError, OtcServerResult};

use super::conversions::chain_type_to_db;

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
        let chain_str = chain_type_to_db(&chain);
        let batch_json = serde_json::to_value(batch).map_err(|e| OtcServerError::InvalidData {
            message: format!("Failed to serialize batch: {e}"),
        })?;
        let swap_ids_json =
            serde_json::to_value(&swap_ids).map_err(|e| OtcServerError::InvalidData {
                message: format!("Failed to serialize swap ids: {e}"),
            })?;

        sqlx::query(
            r#"
            INSERT INTO batches (
                chain,
                tx_hash,
                full_batch,
                swap_ids,
                market_maker_id,
                created_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            "#,
        )
        .bind(chain_str)
        .bind(&tx_hash)
        .bind(batch_json)
        .bind(swap_ids_json)
        .bind(market_maker_id)
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
        let chain_str = chain_type_to_db(chain);

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

}
