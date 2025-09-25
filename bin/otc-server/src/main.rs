use blockchain_utils::{init_logger, shutdown_signal};
use clap::Parser;
use otc_server::{server::run_server, OtcServerArgs, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let args = OtcServerArgs::parse();

    init_logger(&args.log_level).expect("Logger should initialize");

    tokio::select! {
        result = run_server(args) => result,
        _ = shutdown_signal() => {
            tracing::info!("Shutdown signal received; stopping otc-server");
            Ok(())
        }
    }
}
