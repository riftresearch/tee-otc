use chrono::{DateTime, Utc};
use otc_models::ChainType;
use snafu::{prelude::*, Snafu};
use sqlx::{
    migrate::Migrator,
    postgres::{PgPool, PgPoolOptions},
    Row,
};
use uuid::Uuid;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Database error: {}", source))]
    Database { source: sqlx::Error },

    #[snafu(display("Migration error: {}", source))]
    Migration { source: sqlx::migrate::MigrateError },

    #[snafu(display("Invalid batch nonce digest length: {}", len))]
    InvalidDigest { len: usize },

    #[snafu(display("Unknown chain value: {}", value))]
    UnknownChain { value: String },
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone)]
pub struct PaymentStorage {
    pool: PgPool,
}

#[derive(Debug, Clone)]
pub struct StoredBatch {
    pub txid: String,
    pub chain: ChainType,
    pub swap_ids: Vec<Uuid>,
    pub batch_nonce_digest: [u8; 32],
    pub created_at: DateTime<Utc>,
}

impl PaymentStorage {
    pub async fn new(
        database_url: &str,
        db_max_connections: u32,
        db_min_connections: u32,
    ) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(db_max_connections)
            .min_connections(db_min_connections)
            .acquire_timeout(std::time::Duration::from_secs(5))
            .idle_timeout(std::time::Duration::from_secs(600))
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    // Scope this pool to the payment_storage schema so unqualified SQL stays isolated
                    sqlx::query("SET search_path TO payment_storage, public")
                        .execute(conn)
                        .await
                        .map(|_| ())
                })
            })
            .connect(database_url)
            .await
            .context(DatabaseSnafu)?;

        MIGRATOR.run(&pool).await.context(MigrationSnafu)?;

        Ok(Self { pool })
    }

    pub async fn has_payment_been_made(&self, swap_id: Uuid) -> Result<Option<String>> {
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

    pub async fn set_payment(&self, swap_id: Uuid, txid: impl Into<String>) -> Result<()> {
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
    ) -> Result<()> {
        let txid = txid.into();

        // Use a single transaction to insert all payments with the same txid
        let mut tx = self.pool.begin().await.context(DatabaseSnafu)?;

        sqlx::query(
            r#"
            INSERT INTO mm_batches (txid, chain, swap_ids, batch_nonce_digest)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (txid)
            DO UPDATE SET
                chain = EXCLUDED.chain,
                swap_ids = EXCLUDED.swap_ids,
                batch_nonce_digest = EXCLUDED.batch_nonce_digest
            "#,
        )
        .bind(&txid)
        .bind(chain_to_db(&chain))
        .bind(&swap_ids)
        .bind(batch_nonce_digest.as_slice())
        .execute(&mut *tx)
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
            .execute(&mut *tx)
            .await
            .context(DatabaseSnafu)?;
        }

        tx.commit().await.context(DatabaseSnafu)?;

        Ok(())
    }

    pub async fn list_batches(&self) -> Result<Vec<StoredBatch>> {
        let rows = sqlx::query(
            r#"
            SELECT txid, chain, swap_ids, batch_nonce_digest, created_at
            FROM mm_batches
            ORDER BY created_at ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .context(DatabaseSnafu)?;

        let mut batches = Vec::with_capacity(rows.len());
        for row in rows {
            let txid: String = row.try_get("txid").context(DatabaseSnafu)?;
            let chain: String = row.try_get("chain").context(DatabaseSnafu)?;
            let swap_ids: Vec<Uuid> = row.try_get("swap_ids").context(DatabaseSnafu)?;
            let digest: Vec<u8> = row.try_get("batch_nonce_digest").context(DatabaseSnafu)?;
            let created_at: DateTime<Utc> = row.try_get("created_at").context(DatabaseSnafu)?;

            if digest.len() != 32 {
                return Err(Error::InvalidDigest { len: digest.len() });
            }
            let mut batch_nonce_digest = [0u8; 32];
            batch_nonce_digest.copy_from_slice(&digest);

            batches.push(StoredBatch {
                txid,
                chain: chain_from_db(&chain)?,
                swap_ids,
                batch_nonce_digest,
                created_at,
            });
        }

        Ok(batches)
    }
}

fn chain_to_db(chain: &ChainType) -> &'static str {
    match chain {
        ChainType::Bitcoin => "bitcoin",
        ChainType::Ethereum => "ethereum",
    }
}

fn chain_from_db(raw: &str) -> Result<ChainType> {
    match raw {
        "bitcoin" => Ok(ChainType::Bitcoin),
        "ethereum" => Ok(ChainType::Ethereum),
        other => Err(Error::UnknownChain {
            value: other.to_string(),
        }),
    }
}
