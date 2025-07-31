pub mod conversions;
pub mod row_mappers;
pub mod swap_repo;

pub use swap_repo::SwapRepository;

use crate::error::OtcServerResult;
use sqlx::{postgres::{PgPool, PgPoolOptions}, migrate::Migrator};
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
    pub async fn connect(database_url: &str) -> OtcServerResult<Self> {
        info!("Connecting to database...");
        
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .min_connections(2)
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
    
    
    #[must_use] pub fn swaps(&self) -> SwapRepository {
        SwapRepository::new(self.pool.clone())
    }
}