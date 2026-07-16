//! Instance mounts: serve *sibling* Lumenqraph instances under a path prefix
//! of this one.
//!
//! A Lumenqraph deployment indexes exactly one network, but one origin can
//! front several deployments: `INSTANCE_MOUNTS=testnet=http://127.0.0.1:8081`
//! makes this API reverse-proxy everything under `/testnet` to that sibling —
//! same origin, no CORS, one public URL. This is how the hosted demo serves
//! mainnet at `/` and testnet at `/testnet` from a single free-tier container.
//!
//! `/health` advertises the mounts, so a client (the explorer) can discover
//! the sibling networks without any configuration. Mount names must not
//! collide with API routes; naming them after networks (`testnet`) is the
//! convention. The upstream applies its own auth and rate limiting — these
//! routes are registered outside this instance's middleware on purpose.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::extract::Request;
use axum::http::{HeaderName, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::json;

/// Cap forwarded request bodies; the API's own payloads are far smaller.
const MAX_BODY_BYTES: usize = 10 * 1024 * 1024;

/// Hop-by-hop headers describe one connection, not the message — forwarding
/// them corrupts the proxied exchange. `host`/`content-length` are recomputed.
const SKIP_HEADERS: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
];

/// Parse `INSTANCE_MOUNTS` (comma-separated `name=url`). Bad entries are
/// skipped with a warning — a typo shouldn't take the whole API down.
pub fn mounts_from_env() -> Vec<(String, String)> {
    let raw = std::env::var("INSTANCE_MOUNTS").unwrap_or_default();
    raw.split(',')
        .filter(|s| !s.trim().is_empty())
        .filter_map(|entry| {
            let (name, url) = entry.split_once('=')?;
            let (name, url) = (name.trim(), url.trim().trim_end_matches('/'));
            if name.is_empty()
                || url.is_empty()
                || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
            {
                tracing::warn!(entry, "ignoring malformed INSTANCE_MOUNTS entry");
                return None;
            }
            Some((name.to_string(), url.to_string()))
        })
        .collect()
}

/// Forward one request to `upstream`, with the `/{name}` prefix stripped.
pub async fn proxy(
    client: Arc<reqwest::Client>,
    upstream: Arc<String>,
    prefix: Arc<String>,
    req: Request,
) -> Response {
    // "/testnet/contracts" -> "/contracts"; "/testnet" -> "/".
    let path = req.uri().path();
    let rest = path.strip_prefix(prefix.as_str()).unwrap_or(path);
    let rest = if rest.is_empty() { "/" } else { rest };
    let url = match req.uri().query() {
        Some(q) => format!("{upstream}{rest}?{q}"),
        None => format!("{upstream}{rest}"),
    };

    let method = req.method().clone();
    let headers = req.headers().clone();
    let body = match to_bytes(req.into_body(), MAX_BODY_BYTES).await {
        Ok(b) => b,
        Err(_) => return (StatusCode::PAYLOAD_TOO_LARGE, "request body too large").into_response(),
    };

    let mut out = client.request(method, &url);
    for (name, value) in &headers {
        if !skip(name) {
            out = out.header(name, value);
        }
    }

    let resp = match out.body(body).send().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, url, "mounted instance unreachable");
            return (
                StatusCode::BAD_GATEWAY,
                axum::Json(json!({ "error": "mounted instance unreachable" })),
            )
                .into_response();
        }
    };

    let status = resp.status();
    let resp_headers = resp.headers().clone();
    let bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(error = %e, url, "mounted instance response failed mid-body");
            return (
                StatusCode::BAD_GATEWAY,
                axum::Json(json!({ "error": "mounted instance response failed" })),
            )
                .into_response();
        }
    };

    let mut builder = Response::builder().status(status);
    for (name, value) in &resp_headers {
        if !skip(name) {
            builder = builder.header(name, value);
        }
    }
    builder
        .body(Body::from(bytes))
        .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
}

fn skip(name: &HeaderName) -> bool {
    SKIP_HEADERS.contains(&name.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_env<T>(value: &str, f: impl FnOnce() -> T) -> T {
        std::env::set_var("INSTANCE_MOUNTS", value);
        let out = f();
        std::env::remove_var("INSTANCE_MOUNTS");
        out
    }

    #[test]
    fn parses_mounts_and_skips_junk() {
        let mounts = with_env(
            "testnet=http://127.0.0.1:8081/, ,bad entry,futurenet=http://x:1",
            mounts_from_env,
        );
        assert_eq!(
            mounts,
            vec![
                ("testnet".into(), "http://127.0.0.1:8081".into()),
                ("futurenet".into(), "http://x:1".into()),
            ]
        );
    }

    #[test]
    fn empty_env_means_no_mounts() {
        assert!(with_env("", mounts_from_env).is_empty());
    }
}
