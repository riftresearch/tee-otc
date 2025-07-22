pub mod conversions;
pub mod quote_repo;
pub mod row_mappers;
pub mod swap_repo;

pub use quote_repo::QuoteRepository;
pub use swap_repo::SwapRepository;

use snafu::Snafu;
use sqlx::{postgres::{PgPool, PgPoolOptions}, migrate::Migrator};
use std::time::Duration;
use tracing::info;

#[derive(Debug, Snafu)]
pub enum DbError {
    #[snafu(display("Database query failed: {}", source))]
    Query { source: sqlx::Error },
    
    #[snafu(display("Record not found"))]
    NotFound,
    
    #[snafu(display("Invalid data format: {}", message))]
    InvalidData { message: String },
    
    #[snafu(display("Transaction failed: {}", source))]
    Transaction { source: sqlx::Error },
    
    #[snafu(display("Migration failed: {}", source))]
    Migration { source: sqlx::migrate::MigrateError },
}

impl From<sqlx::Error> for DbError {
    fn from(err: sqlx::Error) -> Self {
        match err {
            sqlx::Error::RowNotFound => DbError::NotFound,
            _ => DbError::Query { source: err },
        }
    }
}

impl From<sqlx::migrate::MigrateError> for DbError {
    fn from(err: sqlx::migrate::MigrateError) -> Self {
        DbError::Migration { source: err }
    }
}

pub type DbResult<T> = Result<T, DbError>;

// Embeds all migration files from ./migrations at compile time
static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[derive(Clone)]
pub struct Database {
    pool: PgPool,
}

impl Database {
    /// Create a new Database instance with connection pooling and automatic migrations
    pub async fn connect(database_url: &str) -> DbResult<Self> {
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
    pub async fn from_pool(pool: PgPool) -> DbResult<Self> {
        info!("Running database migrations...");
        MIGRATOR.run(&pool).await?;
        info!("Database initialization complete");
        Ok(Self { pool })
    }
    
    pub fn quotes(&self) -> QuoteRepository {
        QuoteRepository::new(self.pool.clone())
    }
    
    pub fn swaps(&self) -> SwapRepository {
        SwapRepository::new(self.pool.clone())
    }
}