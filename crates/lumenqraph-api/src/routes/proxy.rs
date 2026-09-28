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
//! convention.
//!
//! The upstream applies its own auth and per-key rate limiting. This instance
//! additionally applies its per-IP concurrency cap and a per-IP rate limit
//! (`auth::proxy_rate_limit`), and appends the client IP to
//! `X-Forwarded-For` so the upstream can limit per client instead of seeing
//! every request come from the proxy. Upstreams should set
//! `RATE_LIMIT_TRUST_XFF=true` and so trust exactly one hop: the right-most
//! `X-Forwarded-For` entry, which is the one this proxy appended (#442).

use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::extract::{ConnectInfo, Request};
use axum::http::{HeaderMap, HeaderName, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use serde_json::json;

use crate::config::ProxyConfig;

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

/// The caller's IP as resolved by `auth::proxy_rate_limit` (honouring
/// `RATE_LIMIT_TRUST_XFF`), passed to the proxy handler as a request extension.
#[derive(Debug, Clone)]
pub struct ClientIp(pub String);

/// Parse an `INSTANCE_MOUNTS` value (comma-separated `name=url`). Bad entries
/// are skipped with a warning — a typo shouldn't take the whole API down.
pub fn parse_mounts(raw: &str) -> Vec<(String, String)> {
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

/// The shared upstream client: bounded connect and total timeouts so a slow or
/// hung upstream can't tie up proxy tasks indefinitely, and a bounded idle pool.
pub fn build_client(cfg: &ProxyConfig) -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(cfg.connect_timeout())
        .timeout(cfg.timeout())
        .pool_max_idle_per_host(cfg.pool_max_idle_per_host)
        .build()
        .expect("failed to build proxy HTTP client")
}

struct Mount {
    client: reqwest::Client,
    upstream: String,
    /// `/{name}`.
    prefix: String,
    max_response_bytes: usize,
}

/// Routes `/{name}` and `/{name}/*rest` for every mount. No middleware is
/// attached here; `routes::router` layers concurrency + rate limiting on top.
pub fn mount_router(mounts: &[(String, String)], cfg: &ProxyConfig) -> Router {
    let client = build_client(cfg);
    let mut router = Router::new();
    for (name, upstream) in mounts {
        let mount = Arc::new(Mount {
            client: client.clone(),
            upstream: upstream.clone(),
            prefix: format!("/{name}"),
            max_response_bytes: cfg.max_response_bytes,
        });
        let handler = move |req: Request| {
            let mount = Arc::clone(&mount);
            async move { proxy(mount, req).await }
        };
        router = router
            .route(&format!("/{name}"), any(handler.clone()))
            .route(&format!("/{name}/*rest"), any(handler));
    }
    router
}

/// Forward one request to the mount's upstream, with the `/{name}` prefix stripped.
async fn proxy(mount: Arc<Mount>, req: Request) -> Response {
    // "/testnet/contracts" -> "/contracts"; "/testnet" -> "/".
    let path = req.uri().path();
    let rest = path.strip_prefix(mount.prefix.as_str()).unwrap_or(path);
    let rest = if rest.is_empty() { "/" } else { rest };
    let url = match req.uri().query() {
        Some(q) => format!("{}{rest}?{q}", mount.upstream),
        None => format!("{}{rest}", mount.upstream),
    };

    let client_ip = req
        .extensions()
        .get::<ClientIp>()
        .map(|ip| ip.0.clone())
        .or_else(|| {
            req.extensions()
                .get::<ConnectInfo<SocketAddr>>()
                .map(|ConnectInfo(addr)| addr.ip().to_string())
        });
    let method = req.method().clone();
    let headers = req.headers().clone();
    let body = match to_bytes(req.into_body(), MAX_BODY_BYTES).await {
        Ok(b) => b,
        Err(_) => return (StatusCode::PAYLOAD_TOO_LARGE, "request body too large").into_response(),
    };

    let mut out = mount.client.request(method, &url);
    for (name, value) in &headers {
        if !skip(name) && name != "x-forwarded-for" {
            out = out.header(name, value);
        }
    }
    if let Some(xff) = forwarded_for(&headers, client_ip.as_deref()) {
        out = out.header("x-forwarded-for", xff);
    }

    let mut resp = match out.body(body).send().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, url, "mounted instance unreachable");
            return upstream_error(&e, "mounted instance unreachable");
        }
    };

    let status = resp.status();
    let resp_headers = resp.headers().clone();

    // Cap the buffered response: reject up front on a declared oversize
    // Content-Length, and while reading for chunked/undeclared bodies.
    let max = mount.max_response_bytes;
    if resp.content_length().is_some_and(|len| len > max as u64) {
        tracing::warn!(url, max, "mounted instance response too large");
        return gateway_error(StatusCode::BAD_GATEWAY, "bad_gateway", "mounted instance response too large");
    }
    let mut bytes = Vec::with_capacity(resp.content_length().unwrap_or(0) as usize);
    loop {
        match resp.chunk().await {
            Ok(Some(chunk)) => {
                if bytes.len() + chunk.len() > max {
                    tracing::warn!(url, max, "mounted instance response too large");
                    return gateway_error(
                        StatusCode::BAD_GATEWAY,
                        "bad_gateway",
                        "mounted instance response too large",
                    );
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok(None) => break,
            Err(e) => {
                tracing::warn!(error = %e, url, "mounted instance response failed mid-body");
                return upstream_error(&e, "mounted instance response failed");
            }
        }
    }

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

/// The `X-Forwarded-For` value to send upstream: any incoming entries with the
/// client IP appended. `None` when there is nothing to send.
fn forwarded_for(headers: &HeaderMap, client_ip: Option<&str>) -> Option<String> {
    let existing: Vec<&str> = headers
        .get_all("x-forwarded-for")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .collect();
    let mut parts = existing;
    if let Some(ip) = client_ip {
        parts.push(ip);
    }
    (!parts.is_empty()).then(|| parts.join(", "))
}

/// `504` for a timed-out upstream, `502` for anything else.
fn upstream_error(e: &reqwest::Error, message: &str) -> Response {
    if e.is_timeout() {
        gateway_error(StatusCode::GATEWAY_TIMEOUT, "gateway_timeout", "mounted instance timed out")
    } else {
        gateway_error(StatusCode::BAD_GATEWAY, "bad_gateway", message)
    }
}

fn gateway_error(status: StatusCode, code: &str, message: &str) -> Response {
    (status, axum::Json(json!({ "code": code, "error": message }))).into_response()
}

fn skip(name: &HeaderName) -> bool {
    SKIP_HEADERS.contains(&name.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use axum::extract::Path;
    use serde_json::Value;

    #[test]
    fn parses_mounts_and_skips_junk() {
        let mounts = parse_mounts("testnet=http://127.0.0.1:8081/, ,bad entry,futurenet=http://x:1");
        assert_eq!(
            mounts,
            vec![
                ("testnet".into(), "http://127.0.0.1:8081".into()),
                ("futurenet".into(), "http://x:1".into()),
            ]
        );
    }

    #[test]
    fn empty_value_means_no_mounts() {
        assert!(parse_mounts("").is_empty());
    }

    #[test]
    fn forwarded_for_appends_client_ip() {
        let mut headers = HeaderMap::new();
        assert_eq!(forwarded_for(&headers, None), None);
        assert_eq!(forwarded_for(&headers, Some("10.0.0.1")).as_deref(), Some("10.0.0.1"));
        headers.insert("x-forwarded-for", "203.0.113.9".parse().unwrap());
        assert_eq!(
            forwarded_for(&headers, Some("10.0.0.1")).as_deref(),
            Some("203.0.113.9, 10.0.0.1")
        );
    }

    // ---- end-to-end: a real proxy in front of a mock upstream (#443) ----

    /// Echo the request as the upstream saw it.
    async fn echo(req: Request) -> axum::Json<Value> {
        let method = req.method().to_string();
        let path = req.uri().path().to_string();
        let query = req.uri().query().map(str::to_string);
        let headers: serde_json::Map<String, Value> = req
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), Value::String(v.to_str().unwrap_or("").to_string())))
            .collect();
        let body = to_bytes(req.into_body(), usize::MAX).await.unwrap();
        axum::Json(json!({
            "method": method,
            "path": path,
            "query": query,
            "headers": headers,
            "body": String::from_utf8_lossy(&body),
        }))
    }

    fn mock_upstream() -> Router {
        Router::new()
            .route(
                "/status/:code",
                any(|Path(code): Path<u16>| async move {
                    (
                        StatusCode::from_u16(code).unwrap(),
                        format!("upstream says {code}"),
                    )
                }),
            )
            .route(
                "/slow",
                any(|| async {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    "too late"
                }),
            )
            .route("/big", any(|| async { "x".repeat(4096) }))
            .fallback(echo)
    }

    async fn serve(router: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .ok();
        });
        format!("http://{addr}")
    }

    /// Start a mock upstream and a proxy mounting it at `/testnet`; returns
    /// the proxy's base URL.
    async fn proxy_to_mock(cfg: ProxyConfig) -> String {
        let upstream = serve(mock_upstream()).await;
        serve(mount_router(&[("testnet".into(), upstream)], &cfg)).await
    }

    async fn get_json(resp: reqwest::Response) -> Value {
        serde_json::from_str(&resp.text().await.unwrap()).unwrap()
    }

    #[tokio::test]
    async fn get_strips_prefix_and_keeps_query() {
        let base = proxy_to_mock(ProxyConfig::default()).await;
        let resp = reqwest::get(format!("{base}/testnet/contracts?limit=5")).await.unwrap();
        assert_eq!(resp.status(), 200);
        let seen = get_json(resp).await;
        assert_eq!(seen["method"], "GET");
        assert_eq!(seen["path"], "/contracts");
        assert_eq!(seen["query"], "limit=5");
    }

    #[tokio::test]
    async fn mount_root_maps_to_upstream_root() {
        let base = proxy_to_mock(ProxyConfig::default()).await;
        let seen = get_json(reqwest::get(format!("{base}/testnet")).await.unwrap()).await;
        assert_eq!(seen["path"], "/");
        assert_eq!(seen["query"], Value::Null);
    }

    #[tokio::test]
    async fn forwards_method_body_and_headers_but_strips_hop_by_hop() {
        let base = proxy_to_mock(ProxyConfig::default()).await;
        let resp = reqwest::Client::new()
            .post(format!("{base}/testnet/webhooks"))
            .header("x-custom", "abc")
            .header("authorization", "Bearer k")
            .header("proxy-authorization", "Basic secret")
            .header("keep-alive", "timeout=5")
            .body("hello")
            .send()
            .await
            .unwrap();
        let seen = get_json(resp).await;
        assert_eq!(seen["method"], "POST");
        assert_eq!(seen["path"], "/webhooks");
        assert_eq!(seen["body"], "hello");
        assert_eq!(seen["headers"]["x-custom"], "abc");
        assert_eq!(seen["headers"]["authorization"], "Bearer k");
        assert!(seen["headers"].get("proxy-authorization").is_none());
        assert!(seen["headers"].get("keep-alive").is_none());
    }

    #[tokio::test]
    async fn appends_client_ip_to_x_forwarded_for() {
        let base = proxy_to_mock(ProxyConfig::default()).await;
        let resp = reqwest::Client::new()
            .get(format!("{base}/testnet/anything"))
            .header("x-forwarded-for", "203.0.113.9")
            .send()
            .await
            .unwrap();
        let seen = get_json(resp).await;
        assert_eq!(seen["headers"]["x-forwarded-for"], "203.0.113.9, 127.0.0.1");
    }

    #[tokio::test]
    async fn upstream_error_status_and_body_pass_through() {
        let base = proxy_to_mock(ProxyConfig::default()).await;
        for code in [404u16, 418, 503] {
            let resp = reqwest::get(format!("{base}/testnet/status/{code}")).await.unwrap();
            assert_eq!(resp.status().as_u16(), code);
            assert_eq!(resp.text().await.unwrap(), format!("upstream says {code}"));
        }
    }

    #[tokio::test]
    async fn unreachable_upstream_yields_502() {
        // Grab a free port, then close it so nothing is listening there.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);

        let base = serve(mount_router(&[("testnet".into(), dead)], &ProxyConfig::default())).await;
        let resp = reqwest::get(format!("{base}/testnet/contracts")).await.unwrap();
        assert_eq!(resp.status(), 502);
        assert_eq!(get_json(resp).await["code"], "bad_gateway");
    }

    #[tokio::test]
    async fn hung_upstream_yields_504_after_timeout() {
        let cfg = ProxyConfig {
            timeout_secs: 1,
            ..ProxyConfig::default()
        };
        let base = proxy_to_mock(cfg).await;
        let resp = reqwest::get(format!("{base}/testnet/slow")).await.unwrap();
        assert_eq!(resp.status(), 504);
        assert_eq!(get_json(resp).await["code"], "gateway_timeout");
    }

    #[tokio::test]
    async fn oversized_upstream_response_yields_502() {
        let cfg = ProxyConfig {
            max_response_bytes: 1024,
            ..ProxyConfig::default()
        };
        let base = proxy_to_mock(cfg).await;
        let resp = reqwest::get(format!("{base}/testnet/big")).await.unwrap();
        assert_eq!(resp.status(), 502);
    }
}
