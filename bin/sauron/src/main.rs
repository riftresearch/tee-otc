use blockchain_utils::shutdown_signal;
use clap::Parser;
use sauron::{run, Result, SauronArgs};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

#[tokio::main]
async fn main() -> Result<()> {
    let args = SauronArgs::parse();

    let fmt_env_filter = EnvFilter::new(&args.log_level);
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_line_number(true)
        .with_filter(fmt_env_filter);

    tracing_subscriber::registry().with(fmt_layer).init();

    tokio::select! {
        result = run(args) => result,
        _ = shutdown_signal() => {
            tracing::info!("Shutdown signal received; stopping sauron");
            Ok(())
        }
    }
}
