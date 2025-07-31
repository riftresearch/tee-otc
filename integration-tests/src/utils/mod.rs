use std::env::current_dir;

use sqlx::postgres::PgConnectOptions;
use ctor::ctor;
use tracing_subscriber::EnvFilter;
use tokio::net::TcpListener;

pub trait PgConnectOptionsExt {
    fn to_database_url(&self) -> String;
}

impl PgConnectOptionsExt for PgConnectOptions {
    fn to_database_url(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.get_username(),
            "password",
            self.get_host(),
            self.get_port(),
            self.get_database().expect("database should be set")
        )
    }
}

pub async fn get_free_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("Should be able to bind to port");
    
    listener.local_addr().expect("Should have a local address").port()
}

pub const TEST_MARKET_MAKER_ID: &str = "550e8400-e29b-41d4-a716-446655440000";
pub const TEST_API_KEY_ID: &str = "d2e0a695-e3b1-494e-b645-1b41a72d7e75";
pub const TEST_API_KEY: &str = "7KNJu1t1j9DtVqS0d8FB6pfX0nkqr4TX";
pub const TEST_MM_WHITELIST_FILE: &str = "integration-tests/src/utils/test_whitelisted_market_makers.json";
pub const INTEGRATION_TEST_TIMEOUT_SECS: u64 = 10;

pub fn get_whitelist_file_path() -> String {
    // Convert relative path to absolute path from workspace root
    let mut current_dir = current_dir().expect("Should be able to get current directory");
    
    // If we're already in integration-tests, go up to workspace root
    if current_dir.file_name().and_then(|n| n.to_str()) == Some("integration-tests") {
        current_dir = current_dir.parent().unwrap().to_path_buf();
    }
    
    let whitelist_file_path = current_dir.join(TEST_MM_WHITELIST_FILE);
    whitelist_file_path.to_string_lossy().to_string()
}

#[ctor]
fn init_test_tracing() {
    let has_nocapture = std::env::args().any(|arg| arg == "--nocapture" || arg == "--show-output");
    if has_nocapture {
        tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
            )
            .try_init()
            .ok();
    }
}