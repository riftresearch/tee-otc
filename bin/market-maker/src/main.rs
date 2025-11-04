use clap::Parser;
use market_maker::{run_market_maker, MarketMakerArgs};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

#[tokio::main]
async fn main() -> market_maker::Result<()> {
    let args = MarketMakerArgs::parse();

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Build the tracing subscriber with multiple layers
    // Create env filter for stdout
    let fmt_env_filter = EnvFilter::new(&args.log_level);
    
    // 1. Standard fmt layer for stdout logging
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_line_number(true)
        .with_filter(fmt_env_filter);

    // 2. Optional tokio console subscriber layer
    let console_layer = args.enable_tokio_console_subscriber.then(|| {
        console_subscriber::ConsoleLayer::builder()
            .server_addr(([0, 0, 0, 0], 6669))
            .spawn()
    });

    // 3. Optional Loki layer for shipping logs to Grafana Loki
    let loki_task = if let Some(loki_url) = &args.loki_url {
        match url::Url::parse(loki_url) {
            Ok(url) => {
                // Create a separate env filter for Loki (same config as fmt)
                let loki_env_filter = EnvFilter::new(&args.log_level);
                
                match tracing_loki::builder()
                    .label("service", "market-maker")
                    .and_then(|builder| builder.label("mm_id", &args.market_maker_id))
                    .and_then(|builder| builder.label("mm_tag", &args.market_maker_tag))
                    .and_then(|builder| builder.build_url(url))
                {
                    Ok((loki_layer, task)) => {
                        // Apply the same filter to Loki layer
                        let filtered_loki_layer = loki_layer.with_filter(loki_env_filter);
                        
                        // Initialize subscriber with all layers
                        tracing_subscriber::registry()
                            .with(console_layer)
                            .with(fmt_layer)
                            .with(filtered_loki_layer)
                            .init();
                        
                        // Spawn the Loki background task
                        tokio::spawn(task);
                        tracing::info!("Loki logging enabled, shipping logs to {}", loki_url);
                        true
                    }
                    Err(e) => {
                        eprintln!("Failed to initialize Loki layer: {}, continuing without Loki", e);
                        // Initialize without Loki layer
                        tracing_subscriber::registry()
                            .with(console_layer)
                            .with(fmt_layer)
                            .init();
                        false
                    }
                }
            }
            Err(e) => {
                eprintln!("Invalid LOKI_URL: {}, continuing without Loki", e);
                // Initialize without Loki layer
                tracing_subscriber::registry()
                    .with(console_layer)
                    .with(fmt_layer)
                    .init();
                false
            }
        }
    } else {
        // No Loki URL provided, initialize without Loki layer
        tracing_subscriber::registry()
            .with(console_layer)
            .with(fmt_layer)
            .init();
        false
    };
    
    if args.enable_tokio_console_subscriber {
        tracing::info!("Tokio console subscriber initialized and listening on 0.0.0.0:6669");
    }
    
    if !loki_task {
        tracing::info!("Loki logging not configured (set LOKI_URL to enable)");
    }
    
    run_market_maker(args).await
}
