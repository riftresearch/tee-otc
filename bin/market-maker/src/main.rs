use clap::Parser;
use common::init_logger;
use market_maker::{run_market_maker, MarketMakerArgs};

#[tokio::main]
async fn main() -> market_maker::Result<()> {
    let args = MarketMakerArgs::parse();

    init_logger(&args.log_level).expect("Logger should initialize");

    run_market_maker(args).await
}