use std::net::SocketAddr;
use std::time::Duration as StdDuration;
use mm_server::{run_client, Args};
use snafu::prelude::*;
use tokio::time::sleep;
use tracing::{error, Level};
use tracing_subscriber::FmtSubscriber;
use clap::Parser;
use url::Url;

#[derive(Debug, Snafu)]
enum MainError {
    #[snafu(display("Failed to set global subscriber"))]
    SetGlobalSubscriber { source: tracing::subscriber::SetGlobalDefaultError },
    
    #[snafu(display("Server error: {source}"))]
    Server { source: mm_server::Error },
}

#[tokio::main]
async fn main() -> Result<(), MainError> {
    let args = Args::parse();
    
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    
    tracing::subscriber::set_global_default(subscriber)
        .context(SetGlobalSubscriberSnafu)?;


    let url = Url::parse(&args.rfq_url).expect("Invalid URL");

    loop {
        match run_client(&url).await {
            Ok(_) => break,
            Err(e) => {
                error!("Connection error: {}, retrying...", e);
                sleep(StdDuration::from_secs(5)).await;  // Simple backoff
            }
        }
    }
    
    Ok(())
}