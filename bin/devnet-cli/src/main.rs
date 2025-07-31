use clap::{Parser, Subcommand};
use common::{handle_background_thread_result, init_logger};
use devnet::evm_devnet::ForkConfig;
use devnet::{RiftDevnet, RiftDevnetCache};
use snafu::{ResultExt, Whatever};
use tracing::info;
use tokio::signal;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Address to fund with cbBTC and Ether (used when no subcommand provided)
    #[arg(short = 'a', long, global = true)]
    fund_address: Vec<String>,

    /// RPC URL to fork from, if unset will not fork (used when no subcommand provided)
    #[arg(short = 'f', long, global = true)]
    fork_url: Option<String>,

    /// Block number to fork from, if unset and `fork_url` is set, will use the latest block (used when no subcommand provided)
    #[arg(short = 'b', long, global = true)]
    fork_block_number: Option<u64>,
}

#[derive(Subcommand)]
enum Commands {
    /// Run devnet server (interactive mode)
    Server,
    /// Create and save a cached devnet for faster subsequent runs
    Cache,
}


#[tokio::main]
async fn main() -> Result<(), Whatever> {
    init_logger("info").whatever_context("Failed to initialize logger")?;

    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Server) | None => {
            // Default to server mode when no subcommand is provided
            // For server mode, fork_url is required
            let fork_config = if let Some(fork_url) = cli.fork_url {
                Some(ForkConfig {
                    url: fork_url,
                    block_number: cli.fork_block_number,
                })
            } else {
                None
            };
            run_server(cli.fund_address, fork_config).await
        }
        Some(Commands::Cache) => run_cache().await,
    }
}

async fn run_server(
    fund_address: Vec<String>,
    fork_config: Option<ForkConfig>,
) -> Result<(), Whatever> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init()
        .ok();
    let server_start = tokio::time::Instant::now();
    info!("[Devnet Server] Starting devnet server...");


    let mut devnet_builder = RiftDevnet::builder().interactive(true).using_esplora(true);

    for address in fund_address {
        devnet_builder = devnet_builder.funded_evm_address(address);
    }

    if let Some(fork_config) = fork_config {
        devnet_builder = devnet_builder.fork_config(fork_config);
    }
    info!("[Devnet Server] Building devnet...");
    let build_start = tokio::time::Instant::now();
    let (mut devnet, _funding_sats) = devnet_builder.build().await.whatever_context("Failed to build devnet")?;
    info!(
        "[Devnet Server] Devnet built in {:?}",
        build_start.elapsed()
    );
    info!(
        "[Devnet Server] Total startup time: {:?}",
        server_start.elapsed()
    );

    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("[Devnet Server] Ctrl+C received, shutting down...");
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
    let (devnet, _funding_sats) = RiftDevnet::builder_for_cached().build().await.whatever_context("Failed to build devnet")?;
    info!("[Devnet Cache] Devnet built in {:?}", build_start.elapsed());

    info!("[Devnet Cache] Devnet created successfully, saving to cache...");
    cache.save_devnet(devnet).await.whatever_context("Failed to save devnet to cache")?;
    Ok(())
}
