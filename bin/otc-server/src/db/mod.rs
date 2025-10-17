pub mod batch_repo;
pub mod conversions;
pub mod quote_repo;
pub mod row_mappers;
pub mod swap_repo;

pub use batch_repo::BatchRepository;
pub use swap_repo::SwapRepository;

use crate::{db::quote_repo::QuoteRepository, error::OtcServerResult};
use sqlx::{
    migrate::Migrator,
    postgres::{PgPool, PgPoolOptions},
};
use std::time::Duration;
use tracing::info;

// Embeds all migration files from ./migrations at compile time
static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[derive(Clone)]
pub struct Database {
    pool: PgPool,
}

impl Database {
    /// Create a new Database instance with connection pooling and automatic migrations
    pub async fn connect(
        database_url: &str,
        max_db_connections: u32,
        min_db_connections: u32,
    ) -> OtcServerResult<Self> {
        info!("Connecting to database...");

        let pool = PgPoolOptions::new()
            .max_connections(max_db_connections)
            .min_connections(min_db_connections)
            .acquire_timeout(Duration::from_secs(5))
            .idle_timeout(Duration::from_secs(600))
            .connect(database_url)
            .await?;

        Self::from_pool(pool).await
    }

    /// Create a Database instance from an existing pool (useful for tests)
    pub async fn from_pool(pool: PgPool) -> OtcServerResult<Self> {
        info!("Running database migrations...");
        MIGRATOR.run(&pool).await?;
        info!("Database initialization complete");
        Ok(Self { pool })
    }

    #[must_use]
    pub fn swaps(&self) -> SwapRepository {
        SwapRepository::new(self.pool.clone(), self.quotes())
    }

    #[must_use]
    pub fn quotes(&self) -> QuoteRepository {
        QuoteRepository::new(self.pool.clone())
    }

    #[must_use]
    pub fn batches(&self) -> BatchRepository {
        BatchRepository::new(self.pool.clone())
    }
}
