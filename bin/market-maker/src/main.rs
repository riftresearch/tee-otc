use blockchain_utils::init_logger;
use clap::Parser;
use market_maker::{run_market_maker, MarketMakerArgs};

#[tokio::main]
async fn main() -> market_maker::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let args = MarketMakerArgs::parse();

    init_logger(&args.log_level).expect("Logger should initialize");

    run_market_maker(args).await
}
