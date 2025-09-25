mod alloy_ext;
mod bitcoin_wallet;
mod fee_calc;
mod receive_auth_helper;
pub use alloy_ext::*;
pub use bitcoin_wallet::*;
pub use fee_calc::*;
pub use receive_auth_helper::*;
use snafu::ResultExt;

pub fn handle_background_thread_result<T, E>(
    result: Option<Result<Result<T, E>, tokio::task::JoinError>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    E: std::error::Error + Send + Sync + 'static,
{
    match result {
        Some(Ok(thread_result)) => match thread_result {
            Ok(_) => Err("Background thread completed unexpectedly".into()),
            Err(e) => Err(format!("Background thread panicked: {e}").into()),
        },
        Some(Err(e)) => Err(format!("Join set failed: {e}").into()),
        None => Err("Join set panicked with no result".into()),
    }
}

#[derive(Debug, snafu::Snafu)]
pub enum InitLoggerError {
    #[snafu(display("Failed to initialize logger: {}", source))]
    LoggerFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub fn init_logger(log_level: &str) -> Result<(), InitLoggerError> {
    tracing_subscriber::fmt()
        .with_env_filter(log_level)
        .try_init()
        .context(LoggerFailedSnafu)?;

    Ok(())
}

/// Awaits the first shutdown signal (SIGTERM or SIGINT) and then returns.
pub async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install handler for SIGTERM");
        let mut sigint =
            signal(SignalKind::interrupt()).expect("failed to install handler for SIGINT");

        tokio::select! {
            _ = sigterm.recv() => {},
            _ = sigint.recv() => {},
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    }
}
