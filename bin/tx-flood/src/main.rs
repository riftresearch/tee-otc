mod args;
mod engine;
mod status;
mod tui;
mod wallets;

use std::{
    io::{self, Write},
    sync::Arc,
};

use anyhow::{anyhow, Context, Result};
use args::Args;
use clap::Parser;
use status::{SwapStage, SwapUpdate, UiEvent};
use tokio::{
    signal,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
};
use tracing::info;
use tracing_subscriber::{
    fmt, fmt::writer::MakeWriter, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider().install_default().expect("Failed to install rustls crypto provider");

    // Parse args once to get the env_file path if specified
    let args = Args::parse();
    
    // Load environment variables from the specified file or default .env
    if let Some(env_file) = &args.env_file {
        dotenvy::from_path(env_file)
            .with_context(|| format!("Failed to load env file: {}", env_file.display()))?;
    } else {
        // Attempt to load default .env file, ignore if not present
        let _ = dotenvy::dotenv();
    }
    
    // Re-parse args to pick up environment variables from the loaded file
    let args = Args::parse();
    let config = Arc::new(args.into_config()?);

    let log_rx = init_tracing(&config.log_level)?;

    let mut wallet_resources = wallets::setup_wallets(&config).await?;
    let payment_wallets = wallet_resources.payment_wallets.clone();

    let (update_tx, update_rx) = unbounded_channel();
    let log_forward_tx = update_tx.clone();
    tokio::spawn(async move {
        let mut log_rx = log_rx;
        while let Some(line) = log_rx.recv().await {
            if log_forward_tx.send(UiEvent::Log(line)).is_err() {
                break;
            }
        }
    });

    let (tui_handle, exit_rx_handle) =
        tui::spawn_tui(update_rx, config.total_swaps, payment_wallets.chain_type());
    let mut exit_rx = Some(exit_rx_handle);

    enum LoadOutcome {
        Completed(engine::RunSummary),
        Cancelled,
        Failed(anyhow::Error),
    }

    let mut load_test = std::pin::Pin::from(Box::new(engine::run_load_test(
        config.clone(),
        payment_wallets,
        update_tx.clone(),
    )));

    let outcome = tokio::select! {
        result = &mut load_test => match result {
            Ok(summary) => LoadOutcome::Completed(summary),
            Err(err) => LoadOutcome::Failed(err),
        },
        _ = signal::ctrl_c() => LoadOutcome::Cancelled,
    };

    wallet_resources.join_set.shutdown().await;
    while let Some(result) = wallet_resources.join_set.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                tracing::warn!(error = %err, "wallet background task exited with error")
            }
            Err(join_err) => tracing::warn!(error = %join_err, "wallet background task join error"),
        }
    }

    let mut exit_error: Option<anyhow::Error> = None;

    let mut wait_for_user = false;

    match outcome {
        LoadOutcome::Completed(summary) => {
            info!(
                success = summary.succeeded,
                failed = summary.failed,
                total = summary.succeeded + summary.failed,
                "load test complete"
            );
            info!("Press Ctrl+C or Q to exit");
            wait_for_user = true;
        }
        LoadOutcome::Failed(err) => {
            tracing::error!(error = %err, "load test failed");
            exit_error = Some(err);
            info!("Press Ctrl+C or Q to exit");
            wait_for_user = true;
        }
        LoadOutcome::Cancelled => {
            tracing::info!("Interrupt received, shutting down");
            exit_error = Some(anyhow!("operation cancelled by user"));
        }
    }

    if wait_for_user {
        if let Some(rx) = exit_rx.take() {
            let _ = rx.await;
        }
    }

    let _ = update_tx.send(UiEvent::Swap(SwapUpdate::new(0, SwapStage::Shutdown)));
    drop(update_tx);

    if let Some(rx) = exit_rx.take() {
        let _ = rx.await;
    }

    match tui_handle.join() {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(err),
        Err(panic) => match panic.downcast::<String>() {
            Ok(msg) => Err(anyhow!("tui thread panicked: {}", *msg)),
            Err(panic) => match panic.downcast::<&'static str>() {
                Ok(msg) => Err(anyhow!("tui thread panicked: {}", *msg)),
                Err(_) => Err(anyhow!("tui thread panicked")),
            },
        },
    }?;

    if let Some(err) = exit_error {
        return Err(err);
    }

    Ok(())
}

fn init_tracing(level: &str) -> Result<UnboundedReceiver<String>> {
    let filter = EnvFilter::try_new(level)
        .or_else(|_| EnvFilter::try_new("info"))
        .context("failed to parse log level")?;

    let (log_tx, log_rx) = unbounded_channel();
    let writer = LogWriterFactory { tx: log_tx };

    tracing_subscriber::registry()
        .with(filter)
        .with(
            fmt::layer()
                .with_writer(writer)
                .with_target(false)
                .with_ansi(false),
        )
        .init();

    Ok(log_rx)
}

#[derive(Clone)]
struct LogWriterFactory {
    tx: UnboundedSender<String>,
}

impl<'a> MakeWriter<'a> for LogWriterFactory {
    type Writer = LogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        LogWriter {
            tx: self.tx.clone(),
            buffer: Vec::new(),
        }
    }
}

struct LogWriter {
    tx: UnboundedSender<String>,
    buffer: Vec<u8>,
}

impl Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(buf);

        while let Some(pos) = self.buffer.iter().position(|b| *b == b'\n') {
            let line_bytes: Vec<u8> = self.buffer.drain(..=pos).collect();
            let line = if line_bytes.len() > 1 {
                &line_bytes[..line_bytes.len() - 1]
            } else {
                &[]
            };
            let line_str = String::from_utf8_lossy(line).to_string();
            let _ = self.tx.send(line_str);
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.buffer.is_empty() {
            let line = String::from_utf8_lossy(&self.buffer).to_string();
            let _ = self.tx.send(line);
            self.buffer.clear();
        }
        Ok(())
    }
}

impl Drop for LogWriter {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}
