use std::net::IpAddr;

use snafu::{prelude::*, Whatever};
use clap::Parser;

pub mod api;
pub mod auth;
pub mod config;
pub mod db;
pub mod error;
pub mod server;
pub mod services;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to set global subscriber"))]
    SetGlobalSubscriber { source: tracing::subscriber::SetGlobalDefaultError },
    
    #[snafu(display("Failed to bind server"))]
    ServerBind { source: std::io::Error },
    
    #[snafu(display("Server failed to start"))]
    ServerStart { source: std::io::Error },
    
    #[snafu(display("Failed to connect to database"))]
    DatabaseConnection { source: sqlx::Error },
    
    #[snafu(display("Database query failed"))]
    DatabaseQuery { source: sqlx::Error },
    
    #[snafu(display("Database initialization failed: {}", source))]
    DatabaseInit { source: error::OtcServerError },

    #[snafu(display("Generic error: {}", source))]
    Generic { source: Whatever },
}

impl From<Whatever> for Error {
    fn from(err: Whatever) -> Self {
        Error::Generic { source: err }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Parser, Debug)]
#[command(name = "otc-server")]
#[command(about = "TEE-OTC server for cross-chain swaps")]
pub struct OtcServerArgs {
    /// Host to bind to
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    pub host: IpAddr,
    
    /// Port to bind to
    #[arg(short, long, default_value = "3000")]
    pub port: u16,
    
    /// Database URL
    #[arg(long, env = "DATABASE_URL", default_value = "postgres://otc_user:otc_password@localhost:5432/otc_db")]
    pub database_url: String,

    /// Log level
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    pub log_level: String,

    /// Ethereum Mainnet RPC URL
    #[arg(long, env = "EVM_RPC_URL")]
    pub ethereum_mainnet_rpc_url: String,

    /// Ethereum Mainnet Token Indexer URL
    #[arg(long, env = "EVM_TOKEN_INDEXER_URL")]
    pub ethereum_mainnet_token_indexer_url: String,

    /// Ethereum Mainnet Chain ID
    #[arg(long, env = "EVM_CHAIN_ID", default_value = "1")]
    pub ethereum_mainnet_chain_id: u64,

    /// Bitcoin RPC URL
    #[arg(long, env = "BITCOIN_RPC_URL")]
    pub bitcoin_rpc_url: String,

    /// Electrum HTTP Server URL
    #[arg(long, env = "ELECTRUM_HTTP_SERVER_URL")]
    pub esplora_http_server_url: String,

    /// Bitcoin Network
    #[arg(long, env = "BITCOIN_NETWORK", default_value = "bitcoin")]
    pub bitcoin_network: bitcoin::Network,

    /// API keys file
    #[arg(long, env = "WHITELISTED_MM_FILE", default_value = "prod_whitelisted_market_makers.json")]
    pub whitelist_file: String,
}

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 3000;
const DEFAULT_DATABASE_URL: &str = "postgres://otc_user:otc_password@localhost:5432/otc_db";
const DEFAULT_LOG_LEVEL: &str = "info";
const DEFAULT_WHITELISTED_MM_FILE: &str = "prod_whitelisted_market_makers.json";