mod alloy_ext;
mod bitcoin_wallet;
mod generic_erc20;
pub use alloy_ext::*;
pub use bitcoin_wallet::*;
pub use generic_erc20::*;
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
