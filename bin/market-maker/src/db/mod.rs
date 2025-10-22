pub mod deposit_repo;
pub mod payment_repo;
pub mod quote_repo;

pub use deposit_repo::{
    Deposit, DepositRepository, DepositRepositoryError, DepositRepositoryResult, DepositStore,
    FillStatus,
};
pub use payment_repo::{
    PaymentRepository, PaymentRepositoryError, PaymentRepositoryResult, StoredBatch,
};
pub use quote_repo::{QuoteRepository, QuoteRepositoryError, QuoteRepositoryResult};

use snafu::prelude::*;
use sqlx::{
    migrate::Migrator,
    postgres::{PgPool, PgPoolOptions},
};
use std::time::Duration;
use tracing::info;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[derive(Debug, Snafu)]
pub enum DatabaseError {
    #[snafu(display("Database connection error: {source}"))]
    Connection { source: sqlx::Error },

    #[snafu(display("Database migration error: {source}"))]
    Migration { source: sqlx::migrate::MigrateError },
}

pub type DatabaseResult<T, E = DatabaseError> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct Database {
    pool: PgPool,
}

impl Database {
    pub async fn connect(
        database_url: &str,
        max_db_connections: u32,
        min_db_connections: u32,
    ) -> DatabaseResult<Self> {
        info!("Connecting to market maker database...");

        let pool = PgPoolOptions::new()
            .max_connections(max_db_connections)
            .min_connections(min_db_connections)
            .acquire_timeout(Duration::from_secs(5))
            .idle_timeout(Duration::from_secs(600))
            .connect(database_url)
            .await
            .context(ConnectionSnafu)?;

        Self::from_pool(pool).await
    }

    pub async fn from_pool(pool: PgPool) -> DatabaseResult<Self> {
        info!("Running market maker database migrations...");
        MIGRATOR.run(&pool).await.context(MigrationSnafu)?;
        info!("Market maker database initialization complete");
        Ok(Self { pool })
    }

    #[must_use]
    pub fn pool(&self) -> PgPool {
        self.pool.clone()
    }

    #[must_use]
    pub fn quotes(&self) -> QuoteRepository {
        QuoteRepository::new(self.pool())
    }

    #[must_use]
    pub fn deposits(&self) -> DepositRepository {
        DepositRepository::new(self.pool())
    }

    #[must_use]
    pub fn payments(&self) -> PaymentRepository {
        PaymentRepository::new(self.pool())
    }
}
