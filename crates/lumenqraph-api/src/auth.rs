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
use crate::key_cache::CachedKey;
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

/// Configuration for IP extraction, built once at startup (via
/// `IpConfig::from_env`) and stored in `AppState` — or constructed inline for
/// the few places that still read env directly.
///
/// Separating config from request handling lets unit tests pass explicit values
/// without touching the environment and avoids an `env::var` call per request.
#[derive(Clone, Debug)]
pub struct IpConfig {
    /// Number of trusted reverse-proxy hops sitting in front of the API
    /// (controlled by `TRUSTED_PROXY_HOPS`, default 1).
    ///
    /// With `hops = 1`, the rightmost entry in `X-Forwarded-For` that was
    /// **not** added by the trusted proxy is the client IP. Proxies append on
    /// the right, so the rightmost address is the one the trusted proxy saw.
    ///
    /// With `hops = 0`, `X-Forwarded-For` is not trusted at all and the
    /// TCP-level peer address is used instead.
    pub trusted_proxy_hops: usize,
    /// Platform-specific single-value header that already contains the real
    /// client IP (e.g. `Fly-Client-IP`, `CF-Connecting-IP`, `X-Real-IP`).
    /// When set, this header is preferred over XFF.
    pub platform_header: Option<String>,
}

impl IpConfig {
    /// Read configuration from the environment. Called once at startup.
    pub fn from_env() -> Self {
        let trusted_proxy_hops = std::env::var("TRUSTED_PROXY_HOPS")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(1);

        let platform_header = std::env::var("CLIENT_IP_HEADER").ok().and_then(|v| {
            let v = v.trim().to_lowercase();
            if v.is_empty() { None } else { Some(v) }
        });

        IpConfig {
            trusted_proxy_hops,
            platform_header,
        }
    }
}

/// Extract the real client IP address from an incoming request (#428).
///
/// Resolution order:
///
/// 1. **Platform header** (`CLIENT_IP_HEADER` env var, e.g. `fly-client-ip`,
///    `cf-connecting-ip`, `x-real-ip`). These single-value headers are set by
///    the platform and cannot be spoofed by a browser/client because the proxy
///    strips/overwrites them. Use this when your platform guarantees it.
///
/// 2. **`X-Forwarded-For` — rightmost-trusted-hop strategy** (#428).
///    Proxies *append* the peer address to XFF, so the rightmost entry is the
///    one the outermost trusted proxy actually saw. We skip `hops` entries from
///    the right (one per trusted proxy layer) to land on the real client IP.
///    With `hops = 1` (default): `X-Forwarded-For: 1.2.3.4, 5.6.7.8` yields
///    `5.6.7.8` — the attacker's leftmost spoofed entries are ignored.
///    With `hops = 0`: XFF is not consulted.
///
/// 3. **TCP peer address** — used when no proxy configuration is trusted.
pub fn extract_client_ip(headers: &HeaderMap, socket_addr: Option<SocketAddr>, cfg: &IpConfig) -> String {
    // 1. Platform-specific single-value header (Fly, Cloudflare, nginx real_ip…).
    if let Some(ref header_name) = cfg.platform_header {
        if let Some(ip) = headers
            .get(header_name.as_str())
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
        {
            return ip;
        }
    }

    // 2. X-Forwarded-For with rightmost-hop strategy (#428).
    if cfg.trusted_proxy_hops > 0 {
        if let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
            let parts: Vec<&str> = xff.split(',').map(|s| s.trim()).collect();
            // `parts.len() - hops` gives the index of the real client entry.
            // If there are fewer entries than hops (misconfigured / no proxy),
            // fall back to the leftmost entry rather than panicking.
            if !parts.is_empty() {
                let idx = parts.len().saturating_sub(cfg.trusted_proxy_hops);
                let ip = parts[idx].to_string();
                if !ip.is_empty() {
                    return ip;
                }
            }
        }
    }

    // 3. TCP peer address.
    socket_addr
        .map(|addr| addr.ip().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Perform a key lookup, consulting the in-process LRU cache first (#430).
///
/// Returns `Ok((identity, limit, is_authenticated))` on success, or an
/// `ApiError` that should be returned to the caller directly.
async fn resolve_key(
    state: &AppState,
    key: &str,
    route: &str,
    method: &str,
) -> Result<(String, i32, bool), ApiError> {
    let hash = hash_key(key);

    // --- Cache look-up (#430) ---
    if let Some(cached) = state.key_cache.get(&hash) {
        return match cached {
            CachedKey::Valid { rate_limit_per_min } => {
                Ok((format!("key:{hash}"), rate_limit_per_min, true))
            }
            CachedKey::Revoked => {
                log_audit_event(&state.pool, &hash, route, method, 401).await;
                Err(ApiError::unauthorized("API key revoked"))
            }
            CachedKey::NotFound => {
                log_audit_event(&state.pool, &hash, route, method, 401).await;
                Err(ApiError::unauthorized("invalid API key"))
            }
        };
    }

    // --- DB look-up on cache miss (#430) ---
    let row: Option<(bool, i32)> = sqlx::query_as(
        "SELECT revoked, rate_limit_per_min FROM api_keys WHERE key_hash = $1",
    )
    .bind(&hash)
    .fetch_optional(&state.pool)
    .await?;

    match row {
        Some((false, limit)) => {
            state.key_cache.insert_valid(&hash, limit);
            Ok((format!("key:{hash}"), limit, true))
        }
        Some((true, _)) => {
            state.key_cache.insert_revoked(&hash);
            log_audit_event(&state.pool, &hash, route, method, 401).await;
            Err(ApiError::unauthorized("API key revoked"))
        }
        None => {
            state.key_cache.insert_not_found(&hash);
            log_audit_event(&state.pool, &hash, route, method, 401).await;
            Err(ApiError::unauthorized("invalid API key"))
        }
    }
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

/// Build a `429 Too Many Requests` response with the standard rate-limit
/// headers (`Retry-After`, `X-RateLimit-Limit`, `X-RateLimit-Remaining`).
///
/// This is the single shared helper for all three middlewares (#429), so the
/// header set is consistent across every rate-limited path.
fn rate_limited_response(limit: i32, retry_after_secs: Option<u64>, tokens_remaining: i32) -> Response {
    let mut response = (
        StatusCode::TOO_MANY_REQUESTS,
        crate::error::rate_limit_error(),
    )
        .into_response();

    if let Some(secs) = retry_after_secs {
        if let Ok(val) = secs.to_string().parse() {
            response.headers_mut().insert("Retry-After", val);
        }
    }
    if let Ok(val) = limit.to_string().parse() {
        response.headers_mut().insert("X-RateLimit-Limit", val);
    }
    if let Ok(val) = tokens_remaining.to_string().parse() {
        response.headers_mut().insert("X-RateLimit-Remaining", val);
    }

    response
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
        Some(key) => resolve_key(&state, &key, &route, &method).await?,
        None => {
            if state.require_auth {
                return Err(ApiError::unauthorized("missing API key"));
            }
            let client_ip = extract_client_ip(&headers, Some(socket_addr), &state.ip_config);
            (format!("anon:{client_ip}"), state.anon_rate_limit, false)
        }
    };

    let rl_status = state.limiter.check(&identity, limit);
    if !rl_status.allowed {
        if is_authenticated {
            let hash_prefix = identity.split(':').nth(1).unwrap_or("unknown");
            log_audit_event(&state.pool, hash_prefix, &route, &method, 429).await;
        }
        return Ok(rate_limited_response(limit, rl_status.retry_after_secs, rl_status.tokens_remaining));
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
///
/// Anonymous callers are now keyed per-IP (`anon:{ip}`) rather than sharing a
/// single global `"anon"` bucket — preventing a single client from exhausting
/// the /call and /simulate budget for everyone (#429).
pub async fn rpc_auth_and_rate_limit(
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
        Some(key) => resolve_key(&state, &key, &route, &method).await?,
        None => {
            if state.rpc_require_auth {
                return Err(ApiError::unauthorized(
                    "RPC routes require API key; missing or invalid key",
                ));
            }
            // #429: key per-IP so each client has an independent budget.
            let client_ip = extract_client_ip(&headers, Some(socket_addr), &state.ip_config);
            (format!("anon:{client_ip}"), state.rpc_anon_rate_limit, false)
        }
    };

    let rl_status = state.rpc_limiter.check(&identity, limit);
    if !rl_status.allowed {
        if is_authenticated {
            let hash_prefix = identity.split(':').nth(1).unwrap_or("unknown");
            log_audit_event(&state.pool, hash_prefix, &route, &method, 429).await;
        }
        // #429: use the shared helper so RPC-route 429s also carry Retry-After
        // and X-RateLimit-* headers.
        return Ok(rate_limited_response(limit, rl_status.retry_after_secs, rl_status.tokens_remaining));
    }

    let response = next.run(req).await;
    let status = response.status().as_u16();

    if is_authenticated {
        let hash_prefix = identity.split(':').nth(1).unwrap_or("unknown");
        log_audit_event(&state.pool, hash_prefix, &route, &method, status).await;
    }

    Ok(response)
}

/// Middleware for webhook subscription creation (POST /webhooks).
/// Middleware for webhook subscription creation (POST /webhooks).
/// Uses a separate rate limiter with a lower limit for anonymous callers
/// to prevent unbounded subscription creation.
///
/// Injects `CallerKeyHash` into request extensions when the caller is
/// authenticated, so `create_webhook` can set `owner_key_hash` (#421).
pub async fn webhook_auth_and_rate_limit(
    State(state): State<AppState>,
    ConnectInfo(socket_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    mut req: Request,
    next: Next,
) -> ApiResult<Response> {
    state.http_requests.fetch_add(1, Ordering::Relaxed);

    let method = req.method().to_string();
    let uri = req.uri().to_string();
    let route = uri.split('?').next().unwrap_or("").to_string();

    let (identity, limit, is_authenticated) = match extract_key(&headers) {
        Some(key) => resolve_key(&state, &key, &route, &method).await?,
        None => {
            if state.require_auth {
                return Err(ApiError::unauthorized("missing API key"));
            }
            let client_ip = extract_client_ip(&headers, Some(socket_addr), &state.ip_config);
            (format!("anon:{client_ip}"), state.webhook_anon_rate_limit, false)
        }
    };

    let rl_status = state.webhook_limiter.check(&identity, limit);
    if !rl_status.allowed {
        if is_authenticated {
            let hash_prefix = identity.split(':').nth(1).unwrap_or("unknown");
            log_audit_event(&state.pool, hash_prefix, &route, &method, 429).await;
        }
        return Ok(rate_limited_response(limit, rl_status.retry_after_secs, rl_status.tokens_remaining));
    }

    // Inject the authenticated caller's key hash so create_webhook can set
    // owner_key_hash on the new subscription (#421).
    if let Some(hash) = key_hash_opt {
        req.extensions_mut().insert(CallerKeyHash(hash));
    }

    let response = next.run(req).await;
    let status = response.status().as_u16();

    if is_authenticated {
        let hash_prefix = identity.split(':').nth(1).unwrap_or("unknown");
        log_audit_event(&state.pool, hash_prefix, &route, &method, status).await;
    }

    Ok(response)
}

/// Middleware for webhook management routes (GET/DELETE/PATCH /webhooks and
/// related sub-routes). **Always** requires a valid API key, regardless of the
/// `REQUIRE_API_KEY` setting (#420).
///
/// The rationale: `REQUIRE_API_KEY=false` is a "public data reads are allowed"
/// flag. It was never intended to allow anonymous callers to enumerate, delete,
/// or hijack webhook subscriptions. Webhook management is inherently a
/// privileged, mutating operation and must always be authenticated.
///
/// Injects the caller's SHA-256 key hash as an Axum extension
/// (`CallerKeyHash`) so that webhook handlers can scope their queries to the
/// authenticated key (#421).
pub async fn webhook_manage_auth_and_rate_limit(
    State(state): State<AppState>,
    ConnectInfo(_socket_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    mut req: Request,
    next: Next,
) -> ApiResult<Response> {
    state.http_requests.fetch_add(1, Ordering::Relaxed);

    let method = req.method().to_string();
    let uri = req.uri().to_string();
    let route = uri.split('?').next().unwrap_or("").to_string();

    // Webhook management routes ALWAYS require a valid API key.
    // We intentionally do not check `state.require_auth` here — anonymous
    // callers cannot list or modify webhook subscriptions on any deployment.
    let key = extract_key(&headers).ok_or_else(|| ApiError::unauthorized(
        "webhook management routes require an API key regardless of REQUIRE_API_KEY"
    ))?;

    let hash = hash_key(&key);
    let row: Option<(bool, i32)> = sqlx::query_as(
        "SELECT revoked, rate_limit_per_min FROM api_keys WHERE key_hash = $1",
    )
    .bind(&hash)
    .fetch_optional(&state.pool)
    .await?;
    let limit = match row {
        Some((false, limit)) => limit,
        Some((true, _)) => {
            log_audit_event(&state.pool, &hash, &route, &method, 401).await;
            return Err(ApiError::unauthorized("API key revoked"));
        }
        None => {
            log_audit_event(&state.pool, &hash, &route, &method, 401).await;
            return Err(ApiError::unauthorized("invalid API key"));
        }
    };

    let identity = format!("key:{hash}");
    let rl_status = state.limiter.check(&identity, limit);
    if !rl_status.allowed {
        let mut response = (StatusCode::TOO_MANY_REQUESTS, crate::error::rate_limit_error()).into_response();
        if let Some(retry_after) = rl_status.retry_after_secs {
            response.headers_mut().insert(
                "Retry-After",
                retry_after.to_string().parse().unwrap_or_else(|_| "60".parse().unwrap()),
            );
        }
        log_audit_event(&state.pool, &hash, &route, &method, 429).await;
        return Ok(response);
    }

    // Inject the caller's key hash into request extensions so webhook handlers
    // can scope their queries (#421). Handlers extract it with
    // `Extension::<CallerKeyHash>`.
    req.extensions_mut().insert(CallerKeyHash(hash.clone()));

    let response = next.run(req).await;
    let status = response.status().as_u16();
    log_audit_event(&state.pool, &hash, &route, &method, status).await;

    Ok(response)
}

/// The SHA-256 key hash of the authenticated caller, injected into request
/// extensions by [`webhook_manage_auth_and_rate_limit`] for use by webhook
/// handlers to scope queries (#421).
#[derive(Clone)]
pub struct CallerKeyHash(pub String);

/// Per-IP concurrency limiter middleware. Rejects requests when a single IP
/// has too many in-flight requests, preventing slowloris-style attacks.
pub async fn concurrency_limit(
    State(state): State<AppState>,
    ConnectInfo(socket_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    mut req: Request,
    next: Next,
) -> Response {
    let client_ip = extract_client_ip(&headers, Some(socket_addr), &state.ip_config);
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

    fn cfg(hops: usize) -> IpConfig {
        IpConfig {
            trusted_proxy_hops: hops,
            platform_header: None,
        }
    }

    fn cfg_with_platform(hops: usize, header: &str) -> IpConfig {
        IpConfig {
            trusted_proxy_hops: hops,
            platform_header: Some(header.to_lowercase()),
        }
    }

    // ── extract_key ──────────────────────────────────────────────────────────

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

    // ── extract_client_ip — #428 ─────────────────────────────────────────────

    /// With one trusted hop, the rightmost XFF entry (not the leftmost) is used.
    /// This prevents spoofing via `X-Forwarded-For: 1.2.3.4, <real-ip>`.
    #[test]
    fn xff_rightmost_hop_is_used_not_leftmost() {
        let h = make_headers("x-forwarded-for", "1.2.3.4, 5.6.7.8");
        // With hops=1: rightmost entry is 5.6.7.8 (added by the proxy).
        // The client the proxy saw is one position to the left: 5.6.7.8.
        // Wait — with hops=1 we take parts.len()-1 = index 1 = "5.6.7.8".
        // The attacker's "1.2.3.4" is ignored.
        assert_eq!(extract_client_ip(&h, None, &cfg(1)), "5.6.7.8");
    }

    #[test]
    fn single_entry_xff_with_one_hop() {
        let h = make_headers("x-forwarded-for", "1.2.3.4");
        // Only one entry; saturating_sub(1) = 0, so we take that entry.
        assert_eq!(extract_client_ip(&h, None, &cfg(1)), "1.2.3.4");
    }

    #[test]
    fn zero_hops_ignores_xff() {
        let h = make_headers("x-forwarded-for", "1.2.3.4, 5.6.7.8");
        // With hops=0 XFF is not consulted; falls back to socket (None → "unknown").
        assert_eq!(extract_client_ip(&h, None, &cfg(0)), "unknown");
    }

    #[test]
    fn platform_header_takes_priority_over_xff() {
        let mut h = HeaderMap::new();
        h.insert(
            HeaderName::from_bytes(b"fly-client-ip").unwrap(),
            HeaderValue::from_str("9.9.9.9").unwrap(),
        );
        h.insert(
            HeaderName::from_bytes(b"x-forwarded-for").unwrap(),
            HeaderValue::from_str("1.2.3.4, 5.6.7.8").unwrap(),
        );
        let ip = extract_client_ip(&h, None, &cfg_with_platform(1, "fly-client-ip"));
        assert_eq!(ip, "9.9.9.9");
    }

    #[test]
    fn falls_back_to_socket_when_no_headers() {
        use std::net::{IpAddr, Ipv4Addr};
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 4321);
        assert_eq!(
            extract_client_ip(&HeaderMap::new(), Some(addr), &cfg(1)),
            "10.0.0.1"
        );
    }


    #[test]
    fn spoofed_leftmost_entries_do_not_change_bucket() {
        // An attacker sends many different leftmost entries.
        // With hops=1, the rightmost (proxy-appended) entry is always used.
        let cfg = cfg(1);
        let addrs = ["10.0.0.1, 5.6.7.8", "10.0.0.2, 5.6.7.8", "10.0.0.3, 5.6.7.8"];
        for xff in addrs {
            let h = make_headers("x-forwarded-for", xff);
            assert_eq!(
                extract_client_ip(&h, None, &cfg),
                "5.6.7.8",
                "rightmost entry must be stable regardless of spoofed leftmost: {xff}"
            );
        }
    }


/// Per-client-IP rate limiting for sibling-instance mounts (#442).
///
/// API keys belong to the mounted upstream, not to this instance, so every
/// caller is limited by IP here; the upstream still applies its own auth and
/// per-key limits. The resolved IP is handed to the proxy handler, which
/// appends it to `X-Forwarded-For` so the upstream sees the real client
/// rather than this proxy's single address.
pub async fn proxy_rate_limit(
    State(state): State<AppState>,
    ConnectInfo(socket_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    mut req: Request,
    next: Next,
) -> ApiResult<Response> {
    state.http_requests.fetch_add(1, Ordering::Relaxed);

    let client_ip = extract_client_ip(&headers, Some(socket_addr), &state.ip_config);
    let rl_status = state
        .proxy_limiter
        .check(&format!("proxy:{client_ip}"), state.config.proxy.rate_limit_per_min);
    if !rl_status.allowed {
        return Err(ApiError::too_many_requests(rl_status.retry_after_secs));
    }

    req.extensions_mut()
        .insert(crate::routes::proxy::ClientIp(client_ip));
    Ok(next.run(req).await)
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
    use crate::auth::IpConfig;
    use crate::key_cache::KeyCache;
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
            readyz_lag_threshold: 100,
            readyz_max_age_secs: 120,
            health_max_lag_ledgers: 100,
            health_max_stale_secs: 120,
            metrics_require_auth: false,
            proxy_limiter: Arc::new(RateLimiter::new()),
            config: Arc::new(crate::config::ApiConfig::test_default()),
            key_cache: Arc::new(KeyCache::new(256)),
            ip_config: IpConfig { trusted_proxy_hops: 0, platform_header: None },
            audit_tx: None,
            audit_dropped: Arc::new(AtomicU64::new(0)),
            shutdown: tokio_util::sync::CancellationToken::new(),
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

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn rate_limit_response_has_retry_after_header() {
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
        let retry_after = res
            .headers()
            .get("retry-after")
            .expect("429 responses must carry a Retry-After header")
            .to_str()
            .unwrap();
        assert!(
            retry_after.parse::<u64>().is_ok(),
            "Retry-After must be an integer number of seconds, got {retry_after:?}"
        );
    }
}
