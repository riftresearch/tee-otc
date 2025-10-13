use std::{
    fs,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};

use bitcoincore_rpc_async::Auth;
use clap::Parser;
use metrics_exporter_prometheus::BuildError;
use snafu::{prelude::*, Whatever};

pub mod api;
pub mod config;
pub mod db;
pub mod error;
pub mod server;
pub mod services;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to set global subscriber"))]
    SetGlobalSubscriber {
        source: tracing::subscriber::SetGlobalDefaultError,
    },

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

    #[snafu(display("Failed to install metrics recorder: {}", source))]
    MetricsRecorder { source: BuildError },

    #[snafu(display("Failed to bind metrics listener on {}: {}", addr, source))]
    MetricsServerBind {
        source: std::io::Error,
        addr: SocketAddr,
    },

    #[snafu(display("Metrics server error: {}", source))]
    MetricsServer { source: std::io::Error },
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
    #[arg(short, long, default_value = "4422")]
    pub port: u16,

    /// Address for the Prometheus metrics exporter
    #[arg(long, env = "METRICS_LISTEN_ADDR")]
    pub metrics_listen_addr: Option<SocketAddr>,

    /// Database URL
    #[arg(
        long,
        env = "DATABASE_URL",
        default_value = "postgres://otc_user:otc_password@localhost:5432/otc_db"
    )]
    pub database_url: String,

    /// Database max connections
    #[arg(long, env = "DB_MAX_CONNECTIONS", default_value = "128")]
    pub db_max_connections: u32,

    /// Database min connections
    #[arg(long, env = "DB_MIN_CONNECTIONS", default_value = "16")]
    pub db_min_connections: u32,

    /// Log level
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    pub log_level: String,

    /// Config directory where the master key is stored
    #[arg(long, env = "CONFIG_DIR")]
    pub config_dir: String,

    /// Ethereum Mainnet RPC URL
    #[arg(long, env = "EVM_RPC_URL")]
    pub ethereum_mainnet_rpc_url: String,

    /// Ethereum Mainnet Token Indexer URL
    #[arg(long, env = "UNTRUSTED_EVM_TOKEN_INDEXER_URL")]
    pub untrusted_ethereum_mainnet_token_indexer_url: String,

    /// Bitcoin RPC URL
    #[arg(long, env = "BITCOIN_RPC_URL")]
    pub bitcoin_rpc_url: String,

    /// Bitcoin RPC Auth
    #[arg(long, env = "BITCOIN_RPC_AUTH", default_value = "none", value_parser = parse_auth)]
    pub bitcoin_rpc_auth: Auth,

    /// Electrum HTTP Server URL
    #[arg(long, env = "ELECTRUM_HTTP_SERVER_URL")]
    pub untrusted_esplora_http_server_url: String,

    /// Bitcoin Network
    #[arg(long, env = "BITCOIN_NETWORK", default_value = "bitcoin")]
    pub bitcoin_network: bitcoin::Network,

    /// Chain monitor interval in seconds
    #[arg(long, env = "CHAIN_MONITOR_INTERVAL", default_value = "10")]
    pub chain_monitor_interval_seconds: u64,

    /// Maximum number of swaps to monitor concurrently
    #[arg(long, env = "MAX_CONCURRENT_SWAPS", default_value = "250")]
    pub max_concurrent_swaps: usize,

    /// CORS domain to allow (supports wildcards like "*.example.com")
    #[arg(long = "corsdomain", env = "CORS_DOMAIN")]
    pub cors_domain: Option<String>,

    /// Chainalysis Address Screener host, e.g. https://api.chainalysis.com
    #[arg(long, env = "CHAINALYSIS_HOST")]
    pub chainalysis_host: Option<String>,

    /// Chainalysis API token
    #[arg(long, env = "CHAINALYSIS_TOKEN")]
    pub chainalysis_token: Option<String>,

    /// Dstack socket path
    #[arg(long, env = "DSTACK_SOCK_PATH", default_value = "/var/run/dstack.sock")]
    pub dstack_sock_path: String,
}

fn parse_auth(s: &str) -> Result<Auth, String> {
    if s.to_lowercase() == "none" {
        Ok(Auth::None)
    } else if fs::exists(s).map_err(|e| e.to_string())? {
        Ok(Auth::CookieFile(PathBuf::from(s)))
    } else {
        let mut split = s.splitn(2, ":");
        let u = split.next().ok_or("Invalid auth string")?;
        let p = split.next().ok_or("Invalid auth string")?;
        Ok(Auth::UserPass(u.to_string(), p.to_string()))
    }
}
