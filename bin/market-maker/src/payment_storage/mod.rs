use snafu::{prelude::*, Snafu};
use sqlx::{
    migrate::Migrator,
    postgres::{PgPool, PgPoolOptions},
};
use uuid::Uuid;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Database error: {}", source))]
    Database { source: sqlx::Error },

    #[snafu(display("Migration error: {}", source))]
    Migration { source: sqlx::migrate::MigrateError },
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone)]
pub struct PaymentStorage {
    pool: PgPool,
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
    ) -> Result<()> {
        let txid = txid.into();

        // Use a single transaction to insert all payments with the same txid
        let mut tx = self.pool.begin().await.context(DatabaseSnafu)?;

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
}
