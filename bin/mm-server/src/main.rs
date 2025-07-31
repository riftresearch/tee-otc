use mm_server::Args;
use mm_server::server::run_mm;
use snafu::prelude::*;
use tokio::time::sleep;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;
use clap::Parser;
use std::time::Duration;

#[derive(Debug, Snafu)]
enum MainError {
    #[snafu(display("Failed to set global subscriber"))]
    SetGlobalSubscriber { source: tracing::subscriber::SetGlobalDefaultError },
}

#[tokio::main]
async fn main() -> Result<(), MainError> {
    let args = Args::parse();
    
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();
    
    tracing::subscriber::set_global_default(subscriber)
        .context(SetGlobalSubscriberSnafu)?;

    info!("Starting Market Maker: {}", args.mm_name);
    info!("Configuration:");
    info!("  - RFQ URL: {}", args.rfq_url);
    info!("  - Spread: {} bps", args.spread_bps);
    info!("  - Settlement enabled: {}", args.enable_settlement);
    
    loop {
        match run_mm(args.rfq_url.clone(), args.spread_bps as u16, args.mm_name.clone()).await {
            Ok(_) => break,
            Err(e) => {
                error!("MM error: {}, retrying...", e);
                sleep(Duration::from_secs(5)).await;
            }
        }
    }
    
    Ok(())
}
