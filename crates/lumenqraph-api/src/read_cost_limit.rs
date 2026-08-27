//! Request cost limiting for expensive read routes (/call and /simulate).
//! Prevents RPC amplification by bounding request size and per-call timeout.

use serde_json::Value;

/// Maximum size (in bytes) for a single /call or /simulate request.
/// Prevents maliciously large args from overloading the RPC.
const MAX_READ_REQUEST_SIZE: usize = 256 * 1024; // 256KB

/// Maximum size (in bytes) for the JSON args field.
/// Deeply nested or large arg structures amplify RPC work disproportionately.
const MAX_ARGS_SIZE: usize = 128 * 1024; // 128KB

#[derive(Debug, Clone)]
pub struct ReadCostLimitConfig {
    /// Max request body size in bytes. 0 means unlimited.
    pub max_request_size: usize,
    /// Max args field size in bytes. 0 means unlimited.
    pub max_args_size: usize,
}

impl Default for ReadCostLimitConfig {
    fn default() -> Self {
        Self {
            max_request_size: MAX_READ_REQUEST_SIZE,
            max_args_size: MAX_ARGS_SIZE,
        }
    }
}

#[derive(Debug, Clone)]
pub enum CostError {
    /// Request body is too large (413 Payload Too Large).
    RequestTooLarge { size: usize, limit: usize },
    /// Args field exceeds size limit (400 Bad Request).
    ArgsTooLarge { size: usize, limit: usize },
}

impl CostError {
    pub fn http_status(&self) -> axum::http::StatusCode {
        match self {
            CostError::RequestTooLarge { .. } => axum::http::StatusCode::PAYLOAD_TOO_LARGE,
            CostError::ArgsTooLarge { .. } => axum::http::StatusCode::BAD_REQUEST,
        }
    }

    pub fn message(&self) -> String {
        match self {
            CostError::RequestTooLarge { size, limit } => {
                format!("request body too large: {} bytes (limit: {} bytes)", size, limit)
            }
            CostError::ArgsTooLarge { size, limit } => {
                format!("args field too large: {} bytes (limit: {} bytes)", size, limit)
            }
        }
    }
}

/// Validate a CallRequest against cost limits.
pub fn validate_call_request(
    body_bytes: usize,
    args: &Value,
    config: &ReadCostLimitConfig,
) -> Result<(), CostError> {
    if config.max_request_size > 0 && body_bytes > config.max_request_size {
        return Err(CostError::RequestTooLarge {
            size: body_bytes,
            limit: config.max_request_size,
        });
    }

    if config.max_args_size > 0 {
        let args_json = serde_json::to_string(args).unwrap_or_default();
        if args_json.len() > config.max_args_size {
            return Err(CostError::ArgsTooLarge {
                size: args_json.len(),
                limit: config.max_args_size,
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rejects_oversized_request() {
        let config = ReadCostLimitConfig {
            max_request_size: 1000,
            max_args_size: 100,
        };

        let err = validate_call_request(1001, &json!({}), &config).unwrap_err();
        match err {
            CostError::RequestTooLarge { size, limit } => {
                assert_eq!(size, 1001);
                assert_eq!(limit, 1000);
            }
            _ => panic!("expected RequestTooLarge"),
        }
    }

    #[test]
    fn rejects_oversized_args() {
        let config = ReadCostLimitConfig {
            max_request_size: 10000,
            max_args_size: 100,
        };

        let large_string = "x".repeat(150);
        let args = json!({ "value": large_string });

        let err = validate_call_request(500, &args, &config).unwrap_err();
        match err {
            CostError::ArgsTooLarge { size, limit } => {
                assert!(size > 150);
                assert_eq!(limit, 100);
            }
            _ => panic!("expected ArgsTooLarge"),
        }
    }

    #[test]
    fn allows_request_under_limits() {
        let config = ReadCostLimitConfig {
            max_request_size: 1000,
            max_args_size: 500,
        };

        let args = json!({ "value": "test" });
        validate_call_request(100, &args, &config).unwrap();
    }

    #[test]
    fn zero_limit_means_unlimited() {
        let config = ReadCostLimitConfig {
            max_request_size: 0,
            max_args_size: 0,
        };

        let large_args = json!({ "value": "x".repeat(10000) });
        validate_call_request(100000, &large_args, &config).unwrap();
    }

    #[test]
    fn http_status_codes_are_correct() {
        let request_too_large = CostError::RequestTooLarge {
            size: 1000,
            limit: 500,
        };
        assert_eq!(
            request_too_large.http_status(),
            axum::http::StatusCode::PAYLOAD_TOO_LARGE
        );

        let args_too_large = CostError::ArgsTooLarge {
            size: 1000,
            limit: 500,
        };
        assert_eq!(
            args_too_large.http_status(),
            axum::http::StatusCode::BAD_REQUEST
        );
    }
}
