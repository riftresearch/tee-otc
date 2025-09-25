use blockchain_utils::shutdown_signal;
use clap::Parser;
use rfq_server::{server::run_server, Result, RfqServerArgs};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let args = RfqServerArgs::parse();

    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log_level)),
        )
        .init();

    tokio::select! {
        result = run_server(args) => result,
        _ = shutdown_signal() => {
            tracing::info!("Shutdown signal received; stopping rfq-server");
            Ok(())
        }
    }
}
