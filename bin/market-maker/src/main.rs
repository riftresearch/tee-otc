use blockchain_utils::{init_logger, shutdown_signal};
use clap::Parser;
use market_maker::{run_market_maker, MarketMakerArgs};

#[tokio::main]
async fn main() -> market_maker::Result<()> {
    let args = MarketMakerArgs::parse();

    init_logger(&args.log_level).expect("Logger should initialize");

    tokio::select! {
        result = run_market_maker(args) => result,
        _ = shutdown_signal() => {
            tracing::info!("Shutdown signal received; stopping market-maker");
            Ok(())
        }
    }
}
