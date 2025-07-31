use snafu::prelude::*;

pub mod server;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to bind server"))]
    ServerBind { source: std::io::Error },
    
    #[snafu(display("Server failed to start"))]
    ServerStart { source: std::io::Error },
    
    #[snafu(display("WebSocket error: {}", message))]
    WebSocket { message: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub use server::{run_server, Args};
