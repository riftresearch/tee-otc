use snafu::prelude::*;

pub mod fixed_point;
pub mod price_feed;
pub mod quote;
pub mod server;
pub mod settlement;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to bind server"))]
    ServerBind { source: std::io::Error },
    
    #[snafu(display("Server failed to start"))]
    ServerStart { source: std::io::Error },
    
    #[snafu(display("WebSocket error: {}", message))]
    WebSocket { message: String },
    
    #[snafu(display("Serialization error: {}", source))]
    Serialization { source: serde_json::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
pub use server::Args;
