use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum RfqServerError {
    #[snafu(display("Bad request: {}", message))]
    BadRequest { message: String },

    #[snafu(display("Internal server error: {}", message))]
    Internal { message: String },

    #[snafu(display("Forbidden: {}", message))]
    Forbidden { message: String },

    #[snafu(display("Service unavailable: {}", service))]
    ServiceUnavailable { service: String },

    #[snafu(display("Request timeout: {}", message))]
    Timeout { message: String },

    #[snafu(display("No quotes available"))]
    NoQuotesAvailable,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

impl IntoResponse for RfqServerError {
    fn into_response(self) -> Response {
        let (status, error_message) = match &self {
            RfqServerError::BadRequest { .. } => (StatusCode::BAD_REQUEST, self.to_string()),
            RfqServerError::Internal { .. } => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
            RfqServerError::Forbidden { .. } => (StatusCode::FORBIDDEN, self.to_string()),
            RfqServerError::ServiceUnavailable { .. } => {
                (StatusCode::SERVICE_UNAVAILABLE, self.to_string())
            }
            RfqServerError::Timeout { .. } => (StatusCode::REQUEST_TIMEOUT, self.to_string()),
            RfqServerError::NoQuotesAvailable => (StatusCode::NOT_FOUND, self.to_string()),
        };

        let body = Json(ErrorResponse {
            error: error_message,
        });

        (status, body).into_response()
    }
}
