pub mod bitcoin_wallet;
mod client;
mod config;
pub mod evm_wallet;
mod handlers;
mod strategy;
pub mod wallet;

use clap::Parser;
use common::handle_background_thread_result;
use config::Config;
use snafu::{prelude::*, ResultExt};
use tokio::task::JoinSet;
use tracing::info;

use crate::wallet::WalletManager;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Configuration error: {}", source))]
    Config { source: config::ConfigError },

    #[snafu(display("Client error: {}", source))]
    Client { source: client::ClientError },

    #[snafu(display("Background thread error: {}", source))]
    BackgroundThread {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Parser, Debug)]
#[command(name = "market-maker")]
#[command(about = "Market Maker client for TEE-OTC")]
pub struct MarketMakerArgs {
    /// Market maker identifier
    #[arg(long, env = "MM_ID")]
    pub market_maker_id: String,

    /// API key ID (UUID) for authentication
    #[arg(long, env = "MM_API_KEY_ID")]
    pub api_key_id: String,

    /// API key for authentication
    #[arg(long, env = "MM_API_KEY")]
    pub api_key: String,

    /// OTC server WebSocket URL
    #[arg(long, env = "OTC_WS_URL", default_value = "ws://localhost:3000/ws/mm")]
    pub otc_ws_url: String,

    /// Auto-accept all quotes (for testing)
    #[arg(long, env = "AUTO_ACCEPT", default_value = "false")]
    pub auto_accept: bool,

    /// Log level
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    pub log_level: String,
}

pub async fn run_market_maker(args: MarketMakerArgs) -> Result<()> {
    let mut join_set = JoinSet::new();

    info!("Starting market maker with ID: {}", args.market_maker_id);

    let config = Config {
        market_maker_id: args.market_maker_id,
        api_key_id: args.api_key_id,
        api_key: args.api_key,
        otc_ws_url: args.otc_ws_url,
        auto_accept: args.auto_accept,
        reconnect_interval_secs: 5,
        max_reconnect_attempts: 5,
    };

    let wallet_manager = WalletManager::new();
    // TODO: Add wallet implementations to wallet_manager

    let otc_fill_client = client::OtcFillClient::new(config, wallet_manager);
    join_set.spawn(async move { otc_fill_client.run().await });
    // TODO(shanmu): Add RFQ client to handle Market Maker quote creation here

    handle_background_thread_result(join_set.join_next().await).context(BackgroundThreadSnafu)?;

    Ok(())
}
