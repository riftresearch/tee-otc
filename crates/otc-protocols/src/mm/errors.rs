use snafu::prelude::*;
use uuid::Uuid;

/// Protocol-level errors that can occur during message processing
#[derive(Debug, Snafu)]
pub enum ProtocolError {
    #[snafu(display("Protocol version mismatch: expected {expected}, received {received}"))]
    VersionMismatch { expected: String, received: String },

    #[snafu(display("Invalid message: {message}"))]
    InvalidMessage { message: String },

    #[snafu(display("Message sequence error: expected {expected}, received {received}"))]
    SequenceError { expected: u64, received: u64 },

    #[snafu(display("Serialization error: {message}"))]
    Serialization { message: String },

    #[snafu(display("Request {request_id} timed out after {timeout_ms}ms"))]
    Timeout { request_id: Uuid, timeout_ms: u64 },
}

/// Result type for protocol operations
pub type ProtocolResult<T> = Result<T, ProtocolError>;
