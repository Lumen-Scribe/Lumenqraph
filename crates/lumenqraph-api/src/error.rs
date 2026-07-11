//! HTTP error type so handlers and middleware can use `?` and return typed
//! status codes.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

pub enum ApiError {
    /// A client-facing status + message (4xx).
    Status(StatusCode, String),
    /// An unexpected internal failure (500); details are logged, not exposed.
    Internal(anyhow::Error),
}

impl ApiError {
    pub fn unauthorized(msg: impl Into<String>) -> Self {
        ApiError::Status(StatusCode::UNAUTHORIZED, msg.into())
    }
    pub fn too_many_requests() -> Self {
        ApiError::Status(StatusCode::TOO_MANY_REQUESTS, "rate limit exceeded".into())
    }
    pub fn bad_request(msg: impl Into<String>) -> Self {
        ApiError::Status(StatusCode::BAD_REQUEST, msg.into())
    }
    pub fn not_found(msg: impl Into<String>) -> Self {
        ApiError::Status(StatusCode::NOT_FOUND, msg.into())
    }
}

impl<E: Into<anyhow::Error>> From<E> for ApiError {
    fn from(e: E) -> Self {
        ApiError::Internal(e.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::Status(s, m) => (s, m),
            ApiError::Internal(e) => {
                tracing::error!(error = %e, "request failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal error".to_string(),
                )
            }
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}

pub type ApiResult<T> = Result<T, ApiError>;
