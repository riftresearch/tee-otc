use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum ConfigError {
    #[snafu(display("Invalid URL: {}", url))]
    InvalidUrl { url: String },
}

#[derive(Debug, Clone)]
pub struct Config {
    pub market_maker_id: String,
    pub api_key_id: String,
    pub api_key: String,
    pub otc_ws_url: String,
    pub auto_accept: bool,
    pub reconnect_interval_secs: u64,
    pub max_reconnect_attempts: u32,
}