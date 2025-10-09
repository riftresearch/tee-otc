use blockchain_utils::{handle_background_thread_result, init_logger, shutdown_signal};
use clap::{Parser, Subcommand};
use devnet::bitcoin_devnet::MiningMode;
use devnet::evm_devnet::ForkConfig;
use devnet::{RiftDevnet, RiftDevnetCache};
use snafu::{ResultExt, Whatever};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run devnet server (interactive mode)
    Server {
        /// Address to fund with cbBTC and Ether
        #[arg(short = 'a', long)]
        fund_address: Vec<String>,

        /// RPC URL to fork from, if unset will not fork
        #[arg(short = 'f', long)]
        fork_url: Option<String>,

        /// Block number to fork from, if unset and `fork_url` is set, will use the latest block
        #[arg(short = 'b', long)]
        fork_block_number: Option<u64>,

        #[arg(env = "TOKEN_INDEXER_DATABASE_URL", short = 't', long)]
        token_indexer_database_url: Option<String>,
    },
    /// Create and save a cached devnet for faster subsequent runs
    Cache,
}

#[tokio::main]
async fn main() -> Result<(), Whatever> {
    init_logger("info").whatever_context("Failed to initialize logger")?;

    let cli = Cli::parse();

    match cli.command {
        Commands::Server {
            fund_address,
            fork_url,
            fork_block_number,
            token_indexer_database_url,
        } => {
            let fork_config = fork_url.map(|fork_url| ForkConfig {
                url: fork_url,
                block_number: fork_block_number,
            });
            run_server(fund_address, fork_config, token_indexer_database_url).await
        }
        Commands::Cache => run_cache().await,
    }
}

async fn run_server(
    fund_address: Vec<String>,
    fork_config: Option<ForkConfig>,
    token_indexer_database_url: Option<String>,
) -> Result<(), Whatever> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init()
        .ok();
    let server_start = tokio::time::Instant::now();
    info!("[Devnet Server] Starting devnet server...");

    let mut devnet_builder = RiftDevnet::builder()
        .interactive(true)
        .using_esplora(true)
        .bitcoin_mining_mode(MiningMode::Interval(5));

    for address in fund_address {
        devnet_builder = devnet_builder.funded_evm_address(address);
    }
    if let Some(token_indexer_database_url) = token_indexer_database_url {
        devnet_builder = devnet_builder.using_token_indexer(token_indexer_database_url);
    }

    if let Some(fork_config) = fork_config {
        devnet_builder = devnet_builder.fork_config(fork_config);
    }
    info!("[Devnet Server] Building devnet...");
    let build_start = tokio::time::Instant::now();
    let (mut devnet, _funding_sats) = devnet_builder
        .build()
        .await
        .whatever_context("Failed to build devnet")?;
    info!(
        "[Devnet Server] Devnet built in {:?}",
        build_start.elapsed()
    );
    info!(
        "[Devnet Server] Total startup time: {:?}",
        server_start.elapsed()
    );

    tokio::select! {
        _ = shutdown_signal() => {
            info!("[Devnet Server] Shutdown signal received; shutting down...");
        }
        res = devnet.join_set.join_next() => {
            handle_background_thread_result(res).unwrap();
        }
    }

    drop(devnet);
    Ok(())
}

async fn run_cache() -> Result<(), Whatever> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init()
        .ok();
    let _cache_start = tokio::time::Instant::now();
    info!("[Devnet Cache] Creating cached devnet...");

    // Create cache instance and save the devnet
    let cache = RiftDevnetCache::new();

    // clear the cache directory then save
    tokio::fs::remove_dir_all(&cache.cache_dir).await.ok();

    // Build devnet using for_cached configuration
    let build_start = tokio::time::Instant::now();
    let (devnet, _funding_sats) = RiftDevnet::builder_for_cached()
        .build()
        .await
        .whatever_context("Failed to build devnet")?;
    info!("[Devnet Cache] Devnet built in {:?}", build_start.elapsed());

    info!("[Devnet Cache] Devnet created successfully, saving to cache...");
    cache
        .save_devnet(devnet)
        .await
        .whatever_context("Failed to save devnet to cache")?;
    Ok(())
}
