//! API-key auth + per-key rate limiting, as one middleware layer over the data
//! routes. Keys are presented as `Authorization: Bearer <key>` or `x-api-key`,
//! and only their SHA-256 hash is ever compared against the database.

use std::sync::atomic::Ordering;

use axum::extract::{Request, State};
use axum::http::HeaderMap;
use axum::middleware::Next;
use axum::response::Response;
use sha2::{Digest, Sha256};

use crate::error::{ApiError, ApiResult};
use crate::state::AppState;

/// SHA-256 hex of an API key. Used both here and by the key-generation script.
pub fn hash_key(key: &str) -> String {
    let mut h = Sha256::new();
    h.update(key.as_bytes());
    hex::encode(h.finalize())
}

fn extract_key(headers: &HeaderMap) -> Option<String> {
    if let Some(v) = headers.get("x-api-key").and_then(|v| v.to_str().ok()) {
        return Some(v.to_string());
    }
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|s| s.to_string())
}

pub async fn auth_and_rate_limit(
    State(state): State<AppState>,
    headers: HeaderMap,
    req: Request,
    next: Next,
) -> ApiResult<Response> {
    state.http_requests.fetch_add(1, Ordering::Relaxed);

    let (identity, limit) = match extract_key(&headers) {
        Some(key) => {
            let hash = hash_key(&key);
            let row: Option<(bool, i32)> = sqlx::query_as(
                "SELECT revoked, rate_limit_per_min FROM api_keys WHERE key_hash = $1",
            )
            .bind(&hash)
            .fetch_optional(&state.pool)
            .await?;
            match row {
                Some((false, limit)) => (format!("key:{hash}"), limit),
                Some((true, _)) => return Err(ApiError::unauthorized("API key revoked")),
                None => return Err(ApiError::unauthorized("invalid API key")),
            }
        }
        None => {
            if state.require_auth {
                return Err(ApiError::unauthorized("missing API key"));
            }
            ("anon".to_string(), state.anon_rate_limit)
        }
    };

    if !state.limiter.check(&identity, limit) {
        return Err(ApiError::too_many_requests());
    }

    Ok(next.run(req).await)
}
