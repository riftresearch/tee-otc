use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum OtcServerError {
    #[snafu(display("Database query failed: {}", source))]
    DatabaseQuery { source: sqlx::Error },
    
    #[snafu(display("Record not found"))]
    NotFound,
    
    #[snafu(display("Invalid data format: {}", message))]
    InvalidData { message: String },
    
    #[snafu(display("Transaction failed: {}", source))]
    DatabaseTransaction { source: sqlx::Error },
    
    #[snafu(display("Database migration failed: {}", source))]
    Migration { source: sqlx::migrate::MigrateError },
    
    #[snafu(display("Invalid state: {}", message))]
    InvalidState { message: String },
    
    #[snafu(display("Configuration error: {}", message))]
    Configuration { message: String },
    
    #[snafu(display("API validation error: {}", message))]
    Validation { message: String },
    
    #[snafu(display("Service unavailable: {}", service))]
    ServiceUnavailable { service: String },
    
    #[snafu(display("WebSocket error: {}", message))]
    WebSocket { message: String },
    
    #[snafu(display("Market maker error: {}", message))]
    MarketMaker { message: String },
    
    #[snafu(display("Swap operation failed: {}", message))]
    SwapOperation { message: String },
    
    #[snafu(display("Monitoring error: {}", message))]
    Monitoring { message: String },
    
    #[snafu(display("Authentication failed: {}", message))]
    Authentication { message: String },
    
    #[snafu(display("Authorization failed: {}", message))]
    Authorization { message: String },
    
    #[snafu(display("Internal server error: {}", message))]
    Internal { message: String },
    
    #[snafu(display("Bad request: {}", message))]
    BadRequest { message: String },
    
    #[snafu(display("Conflict: {}", message))]
    Conflict { message: String },
    
    #[snafu(display("Timeout: {}", message))]
    Timeout { message: String },
}

impl From<sqlx::Error> for OtcServerError {
    fn from(err: sqlx::Error) -> Self {
        match err {
            sqlx::Error::RowNotFound => OtcServerError::NotFound,
            _ => OtcServerError::DatabaseQuery { source: err },
        }
    }
}

impl From<sqlx::migrate::MigrateError> for OtcServerError {
    fn from(err: sqlx::migrate::MigrateError) -> Self {
        OtcServerError::Migration { source: err }
    }
}

impl IntoResponse for OtcServerError {
    fn into_response(self) -> Response {
        let (status, error_message) = match &self {
            OtcServerError::NotFound => (StatusCode::NOT_FOUND, "Resource not found"),
            OtcServerError::Validation { .. } => (StatusCode::BAD_REQUEST, "Validation error"),
            OtcServerError::BadRequest { .. } => (StatusCode::BAD_REQUEST, "Bad request"),
            OtcServerError::Authentication { .. } => (StatusCode::UNAUTHORIZED, "Authentication failed"),
            OtcServerError::Authorization { .. } => (StatusCode::FORBIDDEN, "Authorization failed"),
            OtcServerError::Conflict { .. } => (StatusCode::CONFLICT, "Resource conflict"),
            OtcServerError::Timeout { .. } => (StatusCode::REQUEST_TIMEOUT, "Request timeout"),
            OtcServerError::ServiceUnavailable { .. } => (StatusCode::SERVICE_UNAVAILABLE, "Service unavailable"),
            OtcServerError::WebSocket { .. } => (StatusCode::BAD_GATEWAY, "WebSocket error"),
            OtcServerError::MarketMaker { .. } => (StatusCode::BAD_GATEWAY, "Market maker error"),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error"),
        };

        let body = Json(json!({
            "error": {
                "code": status.as_u16(),
                "message": error_message,
                "details": self.to_string(),
            }
        }));

        (status, body).into_response()
    }
}

pub type OtcServerResult<T> = Result<T, OtcServerError>;