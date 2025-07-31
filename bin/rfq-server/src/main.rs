use std::net::SocketAddr;
use rfq_server::{run_server, Args};
use snafu::prelude::*;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;
use clap::Parser;

#[derive(Debug, Snafu)]
enum MainError {
    #[snafu(display("Failed to set global subscriber"))]
    SetGlobalSubscriber { source: tracing::subscriber::SetGlobalDefaultError },
    #[snafu(display("Server error: {source}"))]
    Server { source: rfq_server::Error },
}

#[tokio::main]
async fn main() -> Result<(), MainError> {
    let args = Args::parse();
    
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();
    
    tracing::subscriber::set_global_default(subscriber)
        .context(SetGlobalSubscriberSnafu)?;
    
    info!("Starting RFQ server on {}:{}", args.host, args.port);
    
    let addr = SocketAddr::from((args.host, args.port));
    
    run_server(addr).await.context(ServerSnafu)?;
    
    Ok(())
}
