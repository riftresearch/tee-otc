use clap::Parser;
use snafu::prelude::*;
use std::net::IpAddr;

pub mod error;
pub mod mm_registry;
pub mod quote_aggregator;
pub mod server;

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

    #[snafu(display("Failed to load API keys: {}", source))]
    ApiKeyLoad { source: snafu::Whatever },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Parser, Debug)]
#[command(name = "rfq-server")]
#[command(about = "RFQ server for collecting and aggregating market maker quotes")]
pub struct RfqServerArgs {
    /// Host to bind to
    #[arg(short = 'H', long, env = "HOST", default_value = "127.0.0.1")]
    pub host: IpAddr,

    /// Port to bind to
    #[arg(short, long, env = "PORT", default_value = "3001")]
    pub port: u16,

    /// Log level
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    pub log_level: String,

    /// API keys file for market maker authentication
    #[arg(
        long,
        env = "WHITELISTED_MM_FILE",
        default_value = "prod_whitelisted_market_makers.json"
    )]
    pub whitelist_file: String,

    /// Quote request timeout in milliseconds
    #[arg(long, env = "QUOTE_TIMEOUT_MILLISECONDS", default_value = "500")]
    pub quote_timeout_milliseconds: u64,

    /// CORS domain to allow (supports wildcards like "*.example.com")
    #[arg(long = "corsdomain", env = "CORS_DOMAIN")]
    pub cors_domain: Option<String>,

    /// Chainalysis Address Screener host, e.g. https://api.chainalysis.com
    #[arg(long, env = "CHAINALYSIS_HOST")]
    pub chainalysis_host: Option<String>,

    /// Chainalysis API token
    #[arg(long, env = "CHAINALYSIS_TOKEN")]
    pub chainalysis_token: Option<String>,
}
