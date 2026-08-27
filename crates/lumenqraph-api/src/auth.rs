//! API-key auth + per-key rate limiting, as one middleware layer over the data
//! routes. Keys are presented as `Authorization: Bearer <key>` or `x-api-key`,
//! and only their SHA-256 hash is ever compared against the database.
//! Anonymous requests are rate limited per client IP address.

use std::net::SocketAddr;
use std::sync::atomic::Ordering;

use axum::extract::{ConnectInfo, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tracing::warn;

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
        let v = v.trim();
        if !v.is_empty() {
            return Some(v.to_string());
        }
    }
    // RFC 7235 §2.1: the auth-scheme is case-insensitive. Split on the first
    // space so any extra spaces in the token are preserved, then trim both ends.
    let raw = headers.get("authorization").and_then(|v| v.to_str().ok())?;
    let raw = raw.trim();
    let (scheme, rest) = raw.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = rest.trim();
    if token.is_empty() {
        return None;
    }
    Some(token.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderName, HeaderValue};

    fn make_headers(name: &str, value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
        h
    }

    #[test]
    fn bearer_scheme_is_case_insensitive() {
        for scheme in ["Bearer", "bearer", "BEARER", "bEaReR"] {
            let h = make_headers("authorization", &format!("{scheme} mytoken"));
            assert_eq!(
                extract_key(&h).as_deref(),
                Some("mytoken"),
                "failed for scheme {scheme:?}"
            );
        }
    }

    #[test]
    fn surrounding_whitespace_is_trimmed() {
        let h = make_headers("authorization", "  Bearer   mytoken  ");
        assert_eq!(extract_key(&h).as_deref(), Some("mytoken"));
    }

    #[test]
    fn missing_token_returns_none() {
        assert_eq!(extract_key(&make_headers("authorization", "Bearer ")), None);
        assert_eq!(extract_key(&make_headers("authorization", "Bearer")), None);
    }

    #[test]
    fn x_api_key_is_extracted() {
        let h = make_headers("x-api-key", "myapikey");
        assert_eq!(extract_key(&h).as_deref(), Some("myapikey"));
    }

    #[test]
    fn absent_auth_returns_none() {
        assert_eq!(extract_key(&HeaderMap::new()), None);
    }
}

/// Extract the client IP address, respecting X-Forwarded-For headers only when
/// behind a trusted proxy (controlled by RATE_LIMIT_TRUST_XFF environment variable).
fn extract_client_ip(headers: &HeaderMap, socket_addr: Option<SocketAddr>) -> String {
    let trust_xff = std::env::var("RATE_LIMIT_TRUST_XFF")
        .ok()
        .map(|v| matches!(v.trim().to_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);

    if trust_xff {
        if let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
            if let Some(ip) = xff.split(',').next().map(|s| s.trim()) {
                return ip.to_string();
            }
        }
        if let Some(forwarded) = headers.get("forwarded").and_then(|v| v.to_str().ok()) {
            if let Some(start) = forwarded.find("for=") {
                let rest = &forwarded[start + 4..];
                if let Some(end) = rest.find([';', ',']) {
                    return rest[..end].trim_matches('"').to_string();
                } else {
                    return rest.trim_matches('"').to_string();
                }
            }
        }
    }

    socket_addr
        .map(|addr| addr.ip().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

async fn log_audit_event(
    pool: &PgPool,
    key_hash_prefix: &str,
    route: &str,
    method: &str,
    status_code: u16,
) {
    let truncated_prefix = key_hash_prefix.chars().take(8).collect::<String>();
    if let Err(e) = sqlx::query(
        "INSERT INTO audit_log (key_hash_prefix, route, http_method, status_code)
         VALUES ($1, $2, $3, $4)"
    )
    .bind(&truncated_prefix)
    .bind(route)
    .bind(method)
    .bind(status_code as i32)
    .execute(pool)
    .await
    {
        warn!(error = %e, "failed to log audit event");
    }
}

pub async fn auth_and_rate_limit(
    State(state): State<AppState>,
    ConnectInfo(socket_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    req: Request,
    next: Next,
) -> ApiResult<Response> {
    state.http_requests.fetch_add(1, Ordering::Relaxed);

    let method = req.method().to_string();
    let uri = req.uri().to_string();
    let route = uri.split('?').next().unwrap_or("").to_string();

    let (identity, limit, is_authenticated) = match extract_key(&headers) {
        Some(key) => {
            let hash = hash_key(&key);
            let row: Option<(bool, i32)> = sqlx::query_as(
                "SELECT revoked, rate_limit_per_min FROM api_keys WHERE key_hash = $1",
            )
            .bind(&hash)
            .fetch_optional(&state.pool)
            .await?;
            match row {
                Some((false, limit)) => (format!("key:{hash}"), limit, true),
                Some((true, _)) => {
                    log_audit_event(&state.pool, &hash, &route, &method, 401).await;
                    return Err(ApiError::unauthorized("API key revoked"))
                },
                None => {
                    log_audit_event(&state.pool, &hash, &route, &method, 401).await;
                    return Err(ApiError::unauthorized("invalid API key"))
                },
            }
        }
        None => {
            if state.require_auth {
                return Err(ApiError::unauthorized("missing API key"));
            }
            let client_ip = extract_client_ip(&headers, Some(socket_addr));
            (format!("anon:{client_ip}"), state.anon_rate_limit, false)
        }
    };

    let rl_status = state.limiter.check(&identity, limit);
    if !rl_status.allowed {
        let mut response = (StatusCode::TOO_MANY_REQUESTS, crate::error::rate_limit_error()).into_response();

        // Add rate limit headers
        if let Some(retry_after) = rl_status.retry_after_secs {
            response.headers_mut().insert(
                "Retry-After",
                retry_after.to_string().parse().unwrap_or_else(|_| "60".parse().unwrap()),
            );
        }
        response.headers_mut().insert(
            "X-RateLimit-Limit",
            limit.to_string().parse().unwrap_or_else(|_| "0".parse().unwrap()),
        );
        response.headers_mut().insert(
            "X-RateLimit-Remaining",
            rl_status.tokens_remaining.to_string().parse().unwrap_or_else(|_| "0".parse().unwrap()),
        );

        if is_authenticated {
            let hash_prefix = identity.split(':').nth(1).unwrap_or("unknown");
            log_audit_event(&state.pool, hash_prefix, &route, &method, 429).await;
        }

        return Ok(response);
    }

    let response = next.run(req).await;
    let status = response.status().as_u16();

    if is_authenticated {
        let hash_prefix = identity.split(':').nth(1).unwrap_or("unknown");
        log_audit_event(&state.pool, hash_prefix, &route, &method, status).await;
    }

    Ok(response)
}

/// Middleware for expensive RPC-backed routes that hit upstream Soroban RPC.
/// These routes use a separate, tighter rate limit to prevent exhaustion of
/// shared RPC quota. Optionally requires authentication even when the main
/// API doesn't, providing additional protection for expensive operations.
pub async fn rpc_auth_and_rate_limit(
    State(state): State<AppState>,
    headers: HeaderMap,
    req: Request,
    next: Next,
) -> ApiResult<Response> {
    state.http_requests.fetch_add(1, Ordering::Relaxed);

    let method = req.method().to_string();
    let uri = req.uri().to_string();
    let route = uri.split('?').next().unwrap_or("").to_string();

    let (identity, limit, is_authenticated) = match extract_key(&headers) {
        Some(key) => {
            let hash = hash_key(&key);
            let row: Option<(bool, i32)> = sqlx::query_as(
                "SELECT revoked, rate_limit_per_min FROM api_keys WHERE key_hash = $1",
            )
            .bind(&hash)
            .fetch_optional(&state.pool)
            .await?;
            match row {
                Some((false, limit)) => (format!("key:{hash}"), limit, true),
                Some((true, _)) => {
                    log_audit_event(&state.pool, &hash, &route, &method, 401).await;
                    return Err(ApiError::unauthorized("API key revoked"))
                },
                None => {
                    log_audit_event(&state.pool, &hash, &route, &method, 401).await;
                    return Err(ApiError::unauthorized("invalid API key"))
                },
            }
        }
        None => {
            if state.rpc_require_auth {
                return Err(ApiError::unauthorized(
                    "RPC routes require API key; missing or invalid key",
                ));
            }
            ("anon".to_string(), state.rpc_anon_rate_limit, false)
        }
    };

    if !state.rpc_limiter.check(&identity, limit).allowed {
        if is_authenticated {
            let hash_prefix = identity.split(':').nth(1).unwrap_or("unknown");
            log_audit_event(&state.pool, hash_prefix, &route, &method, 429).await;
        }
        return Err(ApiError::too_many_requests());
    }

    let response = next.run(req).await;
    let status = response.status().as_u16();

    if is_authenticated {
        let hash_prefix = identity.split(':').nth(1).unwrap_or("unknown");
        log_audit_event(&state.pool, hash_prefix, &route, &method, status).await;
    }

    Ok(response)
}

/// Per-IP concurrency limiter middleware. Rejects requests when a single IP
/// has too many in-flight requests, preventing slowloris-style attacks.
pub async fn concurrency_limit(
    State(state): State<AppState>,
    ConnectInfo(socket_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    mut req: Request,
    next: Next,
) -> Response {
    let client_ip = extract_client_ip(&headers, Some(socket_addr));
    let status = state.concurrency_limiter.acquire(&client_ip, state.max_concurrent_per_ip);

    if !status.allowed {
        let body = json!({
            "code": "rate_limited",
            "error": format!(
                "too many concurrent requests from this IP (limit: {})",
                status.limit
            )
        });
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response();
    }

    // Insert the client IP into extensions so release middleware can access it.
    req.extensions_mut().insert(client_ip.clone());

    let response = next.run(req).await;

    // Release the slot after request completes.
    state.concurrency_limiter.release(&client_ip);

    response
}

// ---- HTTP-level integration tests ----------------------------------------
//
// These tests boot the real Axum router against a live Postgres instance and
// drive requests with reqwest. They verify auth, rate-limiting, and the error
// envelope without mocking any middleware.
//
// Run with:
//   cargo test -p lumenqraph-api -- --ignored --test-threads=1
//
// --test-threads=1 is required: each test drops and recreates the public
// schema, which would race with any parallel test.

#[cfg(test)]
mod integration_tests {
    use std::net::SocketAddr;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;

    use sqlx::postgres::PgPoolOptions;
    use sqlx::PgPool;

    use super::hash_key;
    use crate::rate_limit::RateLimiter;
    use crate::routes;
    use crate::rpc::RpcClient;
    use crate::specs::SpecCache;
    use crate::state::AppState;

    // ---- test fixtures ----

    async fn db_pool() -> PgPool {
        let url =
            std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .expect("connect to test database");
        for stmt in ["DROP SCHEMA public CASCADE", "CREATE SCHEMA public"] {
            sqlx::query(stmt)
                .execute(&pool)
                .await
                .expect("reset schema");
        }
        sqlx::migrate!("../../migrations")
            .run(&pool)
            .await
            .expect("run migrations");
        pool
    }

    fn make_state(pool: PgPool, require_auth: bool, anon_rate: i32) -> AppState {
        use crate::concurrency_limit::ConcurrencyLimiter;
        use crate::metrics_middleware::MetricsCollector;
        use crate::call_cache::CallCache;
        use crate::read_cost_limit::ReadCostLimitConfig;

        AppState {
            pool,
            require_auth,
            anon_rate_limit: anon_rate,
            limiter: Arc::new(RateLimiter::new()),
            http_requests: Arc::new(AtomicU64::new(0)),
            rpc: RpcClient::new("http://127.0.0.1:26657", 30),
            specs: Arc::new(SpecCache::new()),
            mounts: Arc::new(vec![]),
            rpc_limiter: Arc::new(RateLimiter::new()),
            rpc_require_auth: false,
            rpc_anon_rate_limit: 100,
            metrics: Arc::new(MetricsCollector::new()),
            call_cache: Arc::new(CallCache::new(100, 5)),
            build_info: Arc::new(crate::state::BuildInfo {
                version: "test".to_string(),
                commit: "test".to_string(),
                build_time: "test".to_string(),
            }),
            concurrency_limiter: Arc::new(ConcurrencyLimiter::new()),
            max_concurrent_per_ip: 100,
            read_cost_limit_config: ReadCostLimitConfig::default(),
        }
    }

    async fn spawn_server(state: AppState) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let app = routes::router(state)
            .into_make_service_with_connect_info::<SocketAddr>();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        format!("http://{addr}")
    }

    async fn insert_api_key(pool: &PgPool, key: &str, revoked: bool) {
        let hash = hash_key(key);
        sqlx::query(
            "INSERT INTO api_keys (key_hash, revoked, rate_limit_per_min, created_at)
             VALUES ($1, $2, 100, NOW())",
        )
        .bind(&hash)
        .bind(revoked)
        .execute(pool)
        .await
        .expect("insert api key");
    }

    // ---- tests ----

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn health_is_public_even_when_auth_required() {
        let pool = db_pool().await;
        let base = spawn_server(make_state(pool, true, 60)).await;
        let res = reqwest::get(format!("{base}/health")).await.unwrap();
        assert_eq!(res.status(), 200);
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn metrics_is_public_even_when_auth_required() {
        let pool = db_pool().await;
        let base = spawn_server(make_state(pool, true, 60)).await;
        let res = reqwest::get(format!("{base}/metrics")).await.unwrap();
        assert_eq!(res.status(), 200);
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn anon_request_allowed_when_auth_not_required() {
        let pool = db_pool().await;
        let base = spawn_server(make_state(pool, false, 60)).await;
        let res = reqwest::get(format!("{base}/contracts")).await.unwrap();
        assert_eq!(res.status(), 200);
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn anon_request_blocked_when_auth_required() {
        let pool = db_pool().await;
        let base = spawn_server(make_state(pool, true, 60)).await;
        let res = reqwest::get(format!("{base}/contracts")).await.unwrap();
        assert_eq!(res.status(), 401);
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn valid_key_via_x_api_key_header_allows_request() {
        let pool = db_pool().await;
        insert_api_key(&pool, "good-key", false).await;
        let base = spawn_server(make_state(pool, true, 60)).await;
        let client = reqwest::Client::new();
        let res = client
            .get(format!("{base}/contracts"))
            .header("x-api-key", "good-key")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn valid_key_via_bearer_header_allows_request() {
        let pool = db_pool().await;
        insert_api_key(&pool, "bearer-key", false).await;
        let base = spawn_server(make_state(pool, true, 60)).await;
        let client = reqwest::Client::new();
        let res = client
            .get(format!("{base}/contracts"))
            .header("Authorization", "Bearer bearer-key")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn revoked_key_returns_401() {
        let pool = db_pool().await;
        insert_api_key(&pool, "revoked-key", true).await;
        let base = spawn_server(make_state(pool, true, 60)).await;
        let client = reqwest::Client::new();
        let res = client
            .get(format!("{base}/contracts"))
            .header("x-api-key", "revoked-key")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 401);
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn unknown_key_returns_401() {
        let pool = db_pool().await;
        let base = spawn_server(make_state(pool, true, 60)).await;
        let client = reqwest::Client::new();
        let res = client
            .get(format!("{base}/contracts"))
            .header("x-api-key", "completely-unknown-key")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 401);
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn rate_limit_blocks_excess_requests() {
        let pool = db_pool().await;
        // anon_rate=2: first two requests succeed, third hits the bucket limit.
        let base = spawn_server(make_state(pool, false, 2)).await;
        let client = reqwest::Client::new();
        for _ in 0..2 {
            let res = client
                .get(format!("{base}/contracts"))
                .send()
                .await
                .unwrap();
            assert_eq!(res.status(), 200, "first two requests must succeed");
        }
        let res = client
            .get(format!("{base}/contracts"))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 429, "third request must be rate-limited");
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn error_responses_have_json_error_envelope() {
        let pool = db_pool().await;
        let base = spawn_server(make_state(pool, true, 60)).await;
        let res = reqwest::get(format!("{base}/contracts")).await.unwrap();
        assert_eq!(res.status(), 401);
        let body: serde_json::Value = res.json().await.unwrap();
        assert!(
            body.get("error").is_some(),
            "error responses must carry an 'error' field: {body}"
        );
        assert!(
            body["error"].is_string(),
            "error field must be a string: {body}"
        );
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn rate_limit_response_has_error_envelope() {
        let pool = db_pool().await;
        let base = spawn_server(make_state(pool, false, 1)).await;
        let client = reqwest::Client::new();
        // Exhaust the single token.
        client.get(format!("{base}/contracts")).send().await.unwrap();
        let res = client
            .get(format!("{base}/contracts"))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 429);
        let body: serde_json::Value = res.json().await.unwrap();
        assert!(body.get("error").is_some(), "429 must have error envelope");
    }
}
