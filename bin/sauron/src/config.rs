use std::{fs, path::PathBuf};

use bitcoincore_rpc_async::Auth;
use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(name = "sauron")]
#[command(about = "Replica-backed user deposit detector for tee-otc")]
pub struct SauronArgs {
    /// Log level
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    pub log_level: String,

    /// Replica database URL used for watch loading and notifications
    #[arg(long, env = "OTC_REPLICA_DATABASE_URL")]
    pub otc_replica_database_url: String,

    /// Replica database name, used for logging and operational clarity
    #[arg(long, env = "OTC_REPLICA_DATABASE_NAME", default_value = "otc_db")]
    pub otc_replica_database_name: String,

    /// Notification channel used for watch-set invalidation
    #[arg(
        long,
        env = "OTC_REPLICA_NOTIFICATION_CHANNEL",
        default_value = "sauron_watch_set_changed"
    )]
    pub otc_replica_notification_channel: String,

    /// Base URL for the OTC server deposit observation route
    #[arg(long, env = "OTC_INTERNAL_BASE_URL")]
    pub otc_internal_base_url: String,

    /// Trusted detector API identifier
    #[arg(long, env = "OTC_DETECTOR_API_ID")]
    pub otc_detector_api_id: String,

    /// Trusted detector API secret
    #[arg(long, env = "OTC_DETECTOR_API_SECRET")]
    pub otc_detector_api_secret: String,

    /// Electrum HTTP Server URL
    #[arg(long, env = "ELECTRUM_HTTP_SERVER_URL")]
    pub electrum_http_server_url: String,

    /// Direct Bitcoin Core RPC URL used for tip, block, and mempool reconciliation
    #[arg(long, env = "BITCOIN_RPC_URL")]
    pub bitcoin_rpc_url: String,

    /// Bitcoin Core RPC authentication
    #[arg(long, env = "BITCOIN_RPC_AUTH", value_parser = parse_auth)]
    pub bitcoin_rpc_auth: Auth,

    /// Bitcoin Core ZMQ raw transaction endpoint
    #[arg(long, env = "BITCOIN_ZMQ_RAWTX_ENDPOINT")]
    pub bitcoin_zmq_rawtx_endpoint: String,

    /// Bitcoin Core ZMQ mempool sequence endpoint
    #[arg(long, env = "BITCOIN_ZMQ_SEQUENCE_ENDPOINT")]
    pub bitcoin_zmq_sequence_endpoint: String,

    /// Ethereum Mainnet RPC URL
    #[arg(long, env = "EVM_RPC_URL")]
    pub ethereum_mainnet_rpc_url: String,

    /// Ethereum Mainnet Token Indexer URL
    #[arg(long, env = "ETHEREUM_TOKEN_INDEXER_URL")]
    pub ethereum_token_indexer_url: String,

    /// Ethereum allowed token address
    #[arg(
        long,
        env = "ETHEREUM_ALLOWED_TOKEN",
        default_value = "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf"
    )]
    pub ethereum_allowed_token: String,

    /// Base RPC URL
    #[arg(long, env = "BASE_RPC_URL")]
    pub base_rpc_url: String,

    /// Base Token Indexer URL
    #[arg(long, env = "BASE_TOKEN_INDEXER_URL")]
    pub base_token_indexer_url: String,

    /// Base allowed token address
    #[arg(
        long,
        env = "BASE_ALLOWED_TOKEN",
        default_value = "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf"
    )]
    pub base_allowed_token: String,

    /// Low-frequency full watch-set reconcile interval
    #[arg(
        long,
        env = "SAURON_RECONCILE_INTERVAL_SECONDS",
        default_value = "3600"
    )]
    pub sauron_reconcile_interval_seconds: u64,

    /// Temporary poll interval for the first Bitcoin detector implementation
    #[arg(
        long,
        env = "SAURON_BITCOIN_SCAN_INTERVAL_SECONDS",
        default_value = "15"
    )]
    pub sauron_bitcoin_scan_interval_seconds: u64,

    /// Maximum number of concurrent Bitcoin indexed lookups
    #[arg(
        long,
        env = "SAURON_BITCOIN_INDEXED_LOOKUP_CONCURRENCY",
        default_value = "32"
    )]
    pub sauron_bitcoin_indexed_lookup_concurrency: usize,

    /// Maximum number of concurrent EVM indexed lookups per backend
    #[arg(
        long,
        env = "SAURON_EVM_INDEXED_LOOKUP_CONCURRENCY",
        default_value = "8"
    )]
    pub sauron_evm_indexed_lookup_concurrency: usize,
}

fn parse_auth(s: &str) -> Result<Auth, String> {
    if s.eq_ignore_ascii_case("none") {
        Ok(Auth::None)
    } else if fs::exists(s).map_err(|error| error.to_string())? {
        Ok(Auth::CookieFile(PathBuf::from(s)))
    } else {
        let mut split = s.splitn(2, ':');
        let user = split.next().ok_or("Invalid auth string")?;
        let password = split.next().ok_or("Invalid auth string")?;
        Ok(Auth::UserPass(user.to_string(), password.to_string()))
    }
}
