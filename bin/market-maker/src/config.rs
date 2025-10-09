use snafu::prelude::*;
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum ConfigError {
    #[snafu(display("Invalid URL: {}", url))]
    InvalidUrl { url: String },
    #[snafu(display("Invalid UUID: {}", uuid))]
    InvalidUuid { uuid: String, error: uuid::Error },
}

#[derive(Debug, Clone)]
pub struct Config {
    pub market_maker_id: Uuid,
    pub api_secret: String,
    pub otc_ws_url: String,
    pub reconnect_interval_secs: u64,
    pub max_reconnect_attempts: u32,
}
