use clap::Parser;
use market_maker::{run_market_maker, MarketMakerArgs};
use blockchain_utils::init_logger;

#[tokio::main]
async fn main() -> market_maker::Result<()> {
    let args = MarketMakerArgs::parse();

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Initialize tracing with optional console subscriber
    let console_layer = args.enable_tokio_console_subscriber.then(|| {
        console_subscriber::ConsoleLayer::builder()
            .server_addr(([0, 0, 0, 0], 6669))
            .spawn()
    });
    init_logger(&args.log_level, console_layer).expect("Logger should initialize");
    
    if args.enable_tokio_console_subscriber {
        tracing::info!("Tokio console subscriber initialized and listening on 0.0.0.0:6669");
    }
    run_market_maker(args).await
}
