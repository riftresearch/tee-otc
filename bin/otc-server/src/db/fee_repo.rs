use chrono::{DateTime, Duration, Utc};
use sqlx::postgres::PgPool;
use sqlx::Row;
use uuid::Uuid;

use crate::error::OtcServerResult;

/// Protocol fee debt threshold in satoshis. MMs with debt above this enter grace window.
#[cfg(not(feature = "test-fast-fee-settle"))]
pub const GOOD_STANDING_THRESHOLD_SATS: i64 = 100_000;
#[cfg(feature = "test-fast-fee-settle")]
pub const GOOD_STANDING_THRESHOLD_SATS: i64 = 1_000; // Lower for fast test triggering

/// Grace window duration. MMs over threshold for longer than this are not in good standing.
#[cfg(not(feature = "test-fast-fee-settle"))]
pub const GOOD_STANDING_WINDOW: Duration = Duration::hours(24);
#[cfg(feature = "test-fast-fee-settle")]
pub const GOOD_STANDING_WINDOW: Duration = Duration::seconds(30);

/// Prometheus metric: current protocol fee debt per market maker (sats).
pub const MM_FEE_DEBT_SATS_METRIC: &str = "otc_mm_fee_debt_sats";

#[derive(Clone)]
pub struct FeeRepository {
    pool: PgPool,
}

impl FeeRepository {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Accrue protocol fee debt for a fully-confirmed MM batch.
    ///
    /// Idempotent via unique index on `(kind, market_maker_id, batch_nonce_digest)` for `kind='batch_accrual'`.
    pub async fn accrue_batch_fee(
        &self,
        market_maker_id: Uuid,
        batch_nonce_digest: [u8; 32],
        fee_sats: i64,
        now: DateTime<Utc>,
    ) -> OtcServerResult<()> {
        if fee_sats <= 0 {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

        // Ensure state row exists
        sqlx::query(
            r#"
            INSERT INTO mm_fee_state (market_maker_id, debt_sats, updated_at)
            VALUES ($1, 0, $2)
            ON CONFLICT (market_maker_id) DO NOTHING
            "#,
        )
        .bind(market_maker_id)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        let batch_nonce_digest: Vec<u8> = batch_nonce_digest.to_vec();

        // Insert accrual ledger entry (idempotent)
        let inserted = sqlx::query(
            r#"
            INSERT INTO mm_protocol_fee_ledger (
                id,
                market_maker_id,
                delta_sats,
                kind,
                batch_nonce_digest,
                created_at
            )
            VALUES ($1, $2, $3, 'batch_accrual', $4, $5)
            ON CONFLICT (kind, market_maker_id, batch_nonce_digest)
            WHERE kind = 'batch_accrual'
            DO NOTHING
            "#,
        )
        .bind(Uuid::now_v7())
        .bind(market_maker_id)
        .bind(fee_sats)
        .bind(batch_nonce_digest)
        .bind(now)
        .execute(&mut *tx)
        .await?
        .rows_affected();

        if inserted == 0 {
            tx.commit().await?;
            return Ok(());
        }

        // Update cached state (threshold transition detection uses old debt_sats)
        sqlx::query(
            r#"
            UPDATE mm_fee_state
            SET
                debt_sats = debt_sats + $2,
                over_threshold_since =
                    CASE
                        WHEN debt_sats <= $3 AND (debt_sats + $2) > $3 THEN $4
                        ELSE over_threshold_since
                    END,
                updated_at = $4
            WHERE market_maker_id = $1
            "#,
        )
        .bind(market_maker_id)
        .bind(fee_sats)
        .bind(GOOD_STANDING_THRESHOLD_SATS)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    /// Returns `true` iff the MM is in good standing.
    pub async fn is_good_standing(&self, market_maker_id: Uuid, now: DateTime<Utc>) -> OtcServerResult<bool> {
        tracing::info!("Checking good standing for market maker {}", market_maker_id);
        let row = sqlx::query(
            r#"
            SELECT debt_sats, over_threshold_since
            FROM mm_fee_state
            WHERE market_maker_id = $1
            "#,
        )
        .bind(market_maker_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            tracing::info!("No debt state yet for market maker {}, treating as good standing", market_maker_id);
            // No debt state yet => treat as good standing.
            return Ok(true);
        };

        let debt_sats: i64 = row.try_get("debt_sats")?;
        let over_threshold_since: Option<DateTime<Utc>> = row.try_get("over_threshold_since")?;

        if debt_sats <= GOOD_STANDING_THRESHOLD_SATS {
            tracing::info!("Debt below threshold for market maker {}, treating as good standing", market_maker_id);
            return Ok(true);
        }

        let Some(since) = over_threshold_since else {
            tracing::info!("No over threshold since for market maker {}, treating as bad standing", market_maker_id);
            // Defensive: if state is inconsistent, fail closed.
            return Ok(false);
        };

        let is_good = now - since <= GOOD_STANDING_WINDOW;
        tracing::info!("Now: {}, Since: {}, Good standing window: {}", now, since, GOOD_STANDING_WINDOW);
        tracing::info!("Market maker {} final standing: {}", market_maker_id, is_good);

        Ok(is_good)
    }

    pub async fn get_fee_state(
        &self,
        market_maker_id: Uuid,
    ) -> OtcServerResult<(i64, Option<DateTime<Utc>>)> {
        let row = sqlx::query(
            r#"
            SELECT debt_sats, over_threshold_since
            FROM mm_fee_state
            WHERE market_maker_id = $1
            "#,
        )
        .bind(market_maker_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok((0, None));
        };

        let debt_sats: i64 = row.try_get("debt_sats")?;
        let over_threshold_since: Option<DateTime<Utc>> = row.try_get("over_threshold_since")?;
        Ok((debt_sats, over_threshold_since))
    }

    /// Record a confirmed settlement payment and apply it to the MM's fee state.
    ///
    /// Idempotent on `(chain, tx_hash)` via `mm_fee_settlements_unique_tx`.
    pub async fn record_settlement(
        &self,
        market_maker_id: Uuid,
        chain: &str,
        tx_hash: &str,
        settlement_digest: [u8; 32],
        amount_sats: i64,
        batch_nonce_digests: &[[u8; 32]],
        referenced_fee_sats: i64,
        now: DateTime<Utc>,
    ) -> OtcServerResult<()> {
        if amount_sats < 0 || referenced_fee_sats < 0 {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

        // Ensure state row exists
        sqlx::query(
            r#"
            INSERT INTO mm_fee_state (market_maker_id, debt_sats, updated_at)
            VALUES ($1, 0, $2)
            ON CONFLICT (market_maker_id) DO NOTHING
            "#,
        )
        .bind(market_maker_id)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        let settlement_digest: Vec<u8> = settlement_digest.to_vec();
        let batch_nonce_digests: Vec<Vec<u8>> =
            batch_nonce_digests.iter().map(|d| d.to_vec()).collect();

        let inserted = sqlx::query(
            r#"
            INSERT INTO mm_fee_settlements (
                id,
                market_maker_id,
                chain,
                tx_hash,
                settlement_digest,
                amount_sats,
                batch_nonce_digests,
                referenced_fee_sats,
                confirmed_at,
                created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)
            ON CONFLICT (chain, tx_hash) DO NOTHING
            "#,
        )
        .bind(Uuid::now_v7())
        .bind(market_maker_id)
        .bind(chain)
        .bind(tx_hash)
        .bind(settlement_digest.clone())
        .bind(amount_sats)
        .bind(batch_nonce_digests)
        .bind(referenced_fee_sats)
        .bind(now)
        .execute(&mut *tx)
        .await?
        .rows_affected();

        if inserted == 0 {
            tx.commit().await?;
            return Ok(());
        }

        // Insert settlement ledger entry (idempotent)
        let delta_sats = -amount_sats;
        sqlx::query(
            r#"
            INSERT INTO mm_protocol_fee_ledger (
                id,
                market_maker_id,
                delta_sats,
                kind,
                ref_chain,
                ref_tx_hash,
                settlement_digest,
                created_at
            )
            VALUES ($1, $2, $3, 'settlement_payment', $4, $5, $6, $7)
            ON CONFLICT (kind, market_maker_id, ref_chain, ref_tx_hash)
            WHERE kind = 'settlement_payment'
            DO NOTHING
            "#,
        )
        .bind(Uuid::now_v7())
        .bind(market_maker_id)
        .bind(delta_sats)
        .bind(chain)
        .bind(tx_hash)
        .bind(settlement_digest)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        // Update cached state: apply payment, clear over-threshold if now compliant.
        sqlx::query(
            r#"
            UPDATE mm_fee_state
            SET
                debt_sats = debt_sats + $2,
                over_threshold_since =
                    CASE
                        WHEN (debt_sats + $2) <= $3 THEN NULL
                        ELSE over_threshold_since
                    END,
                last_payment_at = $4,
                updated_at = $4
            WHERE market_maker_id = $1
            "#,
        )
        .bind(market_maker_id)
        .bind(delta_sats)
        .bind(GOOD_STANDING_THRESHOLD_SATS)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    /// List all market makers with their current fee debt state.
    ///
    /// Used for periodic metrics export. Returns `(market_maker_id, debt_sats)` tuples.
    pub async fn list_all_fee_states(&self) -> OtcServerResult<Vec<(Uuid, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT market_maker_id, debt_sats
            FROM mm_fee_state
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let mm_id: Uuid = row.try_get("market_maker_id")?;
            let debt_sats: i64 = row.try_get("debt_sats")?;
            out.push((mm_id, debt_sats));
        }
        Ok(out)
    }

    /// Returns UUIDs of all market makers currently in bad standing.
    ///
    /// A market maker is in bad standing if their debt exceeds the threshold
    /// and they have been over the threshold for longer than the grace window.
    ///
    /// This query must match the semantics of `is_good_standing`:
    /// - debt > threshold AND over_threshold_since is NULL → bad (inconsistent state, fail closed)
    /// - debt > threshold AND over_threshold_since < cutoff → bad (grace window expired)
    pub async fn list_bad_standing(&self, now: DateTime<Utc>) -> OtcServerResult<Vec<Uuid>> {
        let cutoff = now - GOOD_STANDING_WINDOW;

        let rows = sqlx::query(
            r#"
            SELECT market_maker_id
            FROM mm_fee_state
            WHERE debt_sats > $1
              AND (over_threshold_since IS NULL OR over_threshold_since < $2)
            "#,
        )
        .bind(GOOD_STANDING_THRESHOLD_SATS)
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(row.try_get("market_maker_id")?);
        }
        Ok(out)
    }
}

