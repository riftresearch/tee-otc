//! Authentication utilities for WebSocket connections

use async_trait::async_trait;
use axum::http::HeaderMap;
use snafu::prelude::*;
use uuid::Uuid;

/// Error types for authentication
#[derive(Debug, Snafu)]
pub enum AuthError {
    #[snafu(display("Missing required header: {}", header))]
    MissingHeader { header: String },

    #[snafu(display("Invalid header value: {}", header))]
    InvalidHeaderValue { header: String },

    #[snafu(display("Invalid UUID format: {}", source))]
    InvalidUuid { source: uuid::Error },

    #[snafu(display("Authentication failed: {}", message))]
    AuthenticationFailed { message: String },
}

pub type Result<T, E = AuthError> = std::result::Result<T, E>;

/// Validator for API credentials
///
/// Implement this trait to validate market maker credentials from WebSocket
/// connection headers.
#[async_trait]
pub trait AuthValidator: Send + Sync + Clone + 'static {
    /// Validate API credentials and return the market maker UUID
    ///
    /// # Arguments
    /// * `api_id` - The API ID from the X-API-ID header
    /// * `api_secret` - The API secret from the X-API-SECRET header
    ///
    /// # Returns
    /// * `Ok(Uuid)` - The validated market maker UUID
    /// * `Err(AuthError)` - Authentication failed
    async fn validate(&self, api_id: &str, api_secret: &str) -> Result<Uuid>;
}

/// Extract authentication credentials from HTTP headers
///
/// Looks for `X-API-ID` and `X-API-SECRET` headers.
///
/// # Returns
/// * `Ok((api_id, api_secret))` - The extracted credentials
/// * `Err(AuthError)` - Missing or invalid headers
pub fn extract_auth_headers(headers: &HeaderMap) -> Result<(String, String)> {
    let api_id = headers
        .get("x-api-id")
        .ok_or_else(|| AuthError::MissingHeader {
            header: "x-api-id".to_string(),
        })?
        .to_str()
        .map_err(|_| AuthError::InvalidHeaderValue {
            header: "x-api-id".to_string(),
        })?
        .to_string();

    let api_secret = headers
        .get("x-api-secret")
        .ok_or_else(|| AuthError::MissingHeader {
            header: "x-api-secret".to_string(),
        })?
        .to_str()
        .map_err(|_| AuthError::InvalidHeaderValue {
            header: "x-api-secret".to_string(),
        })?
        .to_string();

    Ok((api_id, api_secret))
}

