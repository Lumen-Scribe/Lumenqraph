//! HTTP error type so handlers and middleware can use `?` and return typed
//! status codes.
//!
//! Every error response carries a stable `code` field alongside the
//! human-readable `error` message:
//!
//! ```json
//! { "code": "not_found", "error": "no events found for that event_id" }
//! ```
//!
//! The `code` values are stable identifiers that SDKs and integrations can
//! branch on without string-matching the prose message. The prose message is
//! intended for humans and may change; the `code` will not.
//!
//! ## Stable error codes
//!
//! | Code                  | HTTP status | When                                                    |
//! |-----------------------|-------------|---------------------------------------------------------|
//! | `bad_request`         | 400         | Malformed input, invalid parameter value, wrong type.  |
//! | `unauthorized`        | 401         | Missing or revoked API key.                            |
//! | `not_found`           | 404         | Requested resource does not exist.                     |
//! | `rate_limited`        | 429         | Caller exceeded the request-per-minute limit. Carries a `Retry-After` header. |
//! | `simulation_failed`   | 400         | RPC simulation returned an error (contract trap, etc.).|
//! | `spec_unavailable`    | 404         | Contract interface not indexed (or Stellar Asset Contract). |
//! | `internal_error`      | 500         | Unexpected server-side failure (details are logged).   |

use std::fmt;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

/// Stable, machine-readable error code included in every error response.
///
/// Use these codes — not the `error` message string — to branch in SDKs and
/// integrations. Codes are snake_case strings; the set is append-only (new
/// variants may be added, but existing ones will not be renamed or removed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    BadRequest,
    Unauthorized,
    NotFound,
    RateLimited,
    SimulationFailed,
    SpecUnavailable,
    FeatureDisabled,
    InternalError,
}

impl ErrorCode {
    /// The stable wire string sent to clients.
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::BadRequest => "bad_request",
            ErrorCode::Unauthorized => "unauthorized",
            ErrorCode::NotFound => "not_found",
            ErrorCode::RateLimited => "rate_limited",
            ErrorCode::SimulationFailed => "simulation_failed",
            ErrorCode::SpecUnavailable => "spec_unavailable",
            ErrorCode::FeatureDisabled => "feature_disabled",
            ErrorCode::InternalError => "internal_error",
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug)]
pub enum ApiError {
    /// A client-facing status + code + message (4xx).
    Status(StatusCode, ErrorCode, String),
    /// A 429 carrying the number of seconds the caller should wait before
    /// retrying. Rendered with a `Retry-After` header so SDKs can back off
    /// precisely instead of guessing with exponential backoff.
    RateLimited {
        retry_after_secs: Option<u64>,
        message: String,
    },
    /// An unexpected internal failure (500); details are logged, not exposed.
    Internal(anyhow::Error),
}

impl ApiError {
    pub fn unauthorized(msg: impl Into<String>) -> Self {
        ApiError::Status(StatusCode::UNAUTHORIZED, ErrorCode::Unauthorized, msg.into())
    }
    /// A 429 response. Pass the computed wait time from `RateLimitStatus` so the
    /// response carries a `Retry-After` header; `None` omits the header.
    pub fn too_many_requests(retry_after_secs: Option<u64>) -> Self {
        ApiError::RateLimited {
            retry_after_secs,
            message: "rate limit exceeded".into(),
        }
    }
    pub fn bad_request(msg: impl Into<String>) -> Self {
        ApiError::Status(StatusCode::BAD_REQUEST, ErrorCode::BadRequest, msg.into())
    }
    pub fn not_found(msg: impl Into<String>) -> Self {
        ApiError::Status(StatusCode::NOT_FOUND, ErrorCode::NotFound, msg.into())
    }
    pub fn simulation_failed(msg: impl Into<String>) -> Self {
        ApiError::Status(
            StatusCode::BAD_REQUEST,
            ErrorCode::SimulationFailed,
            msg.into(),
        )
    }
    pub fn spec_unavailable(msg: impl Into<String>) -> Self {
        ApiError::Status(
            StatusCode::NOT_FOUND,
            ErrorCode::SpecUnavailable,
            msg.into(),
        )
    }
    pub fn feature_disabled(msg: impl Into<String>) -> Self {
        ApiError::Status(
            StatusCode::NOT_IMPLEMENTED,
            ErrorCode::FeatureDisabled,
            msg.into(),
        )
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        ApiError::Internal(e)
    }
}

impl From<sqlx::Error> for ApiError {
    fn from(e: sqlx::Error) -> Self {
        ApiError::Internal(e.into())
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(e: serde_json::Error) -> Self {
        ApiError::Internal(e.into())
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApiError::Status(_, _, msg) => write!(f, "{}", msg),
            ApiError::RateLimited { message, .. } => write!(f, "{}", message),
            ApiError::Internal(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for ApiError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ApiError::Internal(e) => Some(e.as_ref()),
            ApiError::Status(_, _, _) => None,
            ApiError::RateLimited { .. } => None,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message, retry_after_secs) = match self {
            ApiError::Status(s, c, m) => (s, c, m, None),
            ApiError::RateLimited {
                retry_after_secs,
                message,
            } => (
                StatusCode::TOO_MANY_REQUESTS,
                ErrorCode::RateLimited,
                message,
                retry_after_secs,
            ),
            ApiError::Internal(e) => {
                tracing::error!(error = %e, "request failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ErrorCode::InternalError,
                    "internal error".to_string(),
                    None,
                )
            }
        };
        let mut response = (
            status,
            Json(json!({ "code": code.as_str(), "error": message })),
        )
            .into_response();
        if let Some(secs) = retry_after_secs {
            if let Ok(value) = secs.to_string().parse() {
                response.headers_mut().insert("Retry-After", value);
            }
        }
        response
    }
}

pub fn rate_limit_error() -> Json<serde_json::Value> {
    Json(json!({ "code": ErrorCode::RateLimited.as_str(), "error": "rate limit exceeded" }))
}

pub type ApiResult<T> = Result<T, ApiError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limited_sets_retry_after_header() {
        let resp = ApiError::too_many_requests(Some(42)).into_response();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            resp.headers()
                .get("Retry-After")
                .and_then(|v| v.to_str().ok()),
            Some("42"),
            "429 responses must carry a Retry-After header with the computed wait"
        );
    }

    #[test]
    fn rate_limited_without_hint_omits_retry_after_header() {
        let resp = ApiError::too_many_requests(None).into_response();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(resp.headers().get("Retry-After").is_none());
    }

    #[test]
    fn non_rate_limited_errors_have_no_retry_after_header() {
        let resp = ApiError::not_found("nope").into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert!(resp.headers().get("Retry-After").is_none());
    }
}
