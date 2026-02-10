use blockchain_utils::shutdown_signal;
use clap::Parser;
use otc_server::{server::run_server, OtcServerArgs, Result};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

#[tokio::main]
async fn main() -> Result<()> {
    let args = OtcServerArgs::parse();

    // Build the tracing subscriber with multiple layers
    // Create env filter for stdout
    let fmt_env_filter = EnvFilter::new(&args.log_level);

    // 1. Standard fmt layer for stdout logging
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_line_number(true)
        .with_filter(fmt_env_filter);

    // 2. Optional Loki layer for shipping logs to Grafana Loki
    let loki_task = if let Some(loki_url) = &args.loki_url {
        match url::Url::parse(loki_url) {
            Ok(url) => {
                // Create a separate env filter for Loki (same config as fmt)
                let loki_env_filter = EnvFilter::new(&args.log_level);

                match tracing_loki::builder()
                    .label("service", "otc-server")
                    .and_then(|builder| builder.build_url(url))
                {
                    Ok((loki_layer, task)) => {
                        // Apply the same filter to Loki layer
                        let filtered_loki_layer = loki_layer.with_filter(loki_env_filter);

                        // Initialize subscriber with all layers
                        tracing_subscriber::registry()
                            .with(fmt_layer)
                            .with(filtered_loki_layer)
                            .init();

                        // Spawn the Loki background task
                        tokio::spawn(task);
                        tracing::info!("Loki logging enabled, shipping logs to {}", loki_url);
                        true
                    }
                    Err(e) => {
                        eprintln!(
                            "Failed to initialize Loki layer: {}, continuing without Loki",
                            e
                        );
                        // Initialize without Loki layer
                        tracing_subscriber::registry().with(fmt_layer).init();
                        false
                    }
                }
            }
            Err(e) => {
                eprintln!("Invalid LOKI_URL: {}, continuing without Loki", e);
                // Initialize without Loki layer
                tracing_subscriber::registry().with(fmt_layer).init();
                false
            }
        }
    } else {
        // No Loki URL provided, initialize without Loki layer
        tracing_subscriber::registry().with(fmt_layer).init();
        false
    };

    if !loki_task {
        tracing::info!("Loki logging not configured (set LOKI_URL to enable)");
    }

    tokio::select! {
        result = run_server(args) => result,
        _ = shutdown_signal() => {
            tracing::info!("Shutdown signal received; stopping otc-server");
            Ok(())
        }
    }
}
