//! Lumenqraph MCP — a [Model Context Protocol](https://modelcontextprotocol.io)
//! server that gives any AI agent (Claude Desktop/Code, or any MCP client)
//! typed, self-describing access to Soroban contracts.
//!
//! It reuses the same Postgres the API reads and the same read-layer encoder the
//! API calls, exposing eight tools: `list_contracts`, `get_contract_interface`,
//! `get_contract_upgrades`, `get_contract_state`, `get_contract_data`,
//! `query_events`, `call_contract`, and `simulate_call`. Because the interface
//! and argument types come from each
//! contract's on-chain spec, an agent can *discover* what a contract does and
//! call it correctly — with zero hand-written schema.
//!
//! Transport is newline-delimited JSON-RPC 2.0 over stdio (the standard MCP
//! stdio transport). Logs go to stderr so stdout stays a clean protocol channel.
//!
//! Wire it into an MCP client (e.g. Claude Desktop) as a command server:
//!   { "command": "lumenqraph-mcp", "env": { "DATABASE_URL": "…", "RPC_URL": "…" } }

mod rpc;
mod tools;

use anyhow::Context;
use axum::{
    body::Body,
    extract::State as AxumState,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::{
    collections::HashSet,
    str::FromStr,
    sync::{Arc, Mutex},
};
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::{info, warn};

use rpc::RpcClient;

/// Latest MCP protocol revision we default to when a client sends none.
const DEFAULT_PROTOCOL_VERSION: &str = "2024-11-05";
const SUPPORTED_PROTOCOL_VERSIONS: &[&str] = &["2024-11-05", "2025-03-26", "2025-06-18"];

fn db_setting<T: FromStr>(service_key: &str, common_key: &str, default: T) -> anyhow::Result<T>
where
    T::Err: std::fmt::Display,
{
    let key = if std::env::var(service_key)
        .ok()
        .is_some_and(|value| !value.trim().is_empty())
    {
        service_key
    } else {
        common_key
    };
    match std::env::var(key) {
        Ok(value) if !value.trim().is_empty() => value
            .trim()
            .parse()
            .map_err(|error| anyhow::anyhow!("invalid {key}={value:?}: {error}")),
        _ => Ok(default),
    }
}

#[derive(Clone)]
pub struct State {
    pub pool: PgPool,
    pub rpc: RpcClient,
    pub rpc_limiter: rpc::RpcRateLimiter,
    pub auth_token: Option<String>,
    /// HTTP transport only: live `Mcp-Session-Id` values issued by this server.
    /// `None` for stdio (no session management). The Mutex is cheap to hold for
    /// the duration of a request and lets `serve_io` tests skip it entirely.
    pub sessions: Option<Arc<Mutex<HashSet<String>>>>,
}

impl State {
    /// Issue a fresh session id, store it, and return it. Returns `None` when
    /// this state is for stdio (no session tracking).
    fn issue_session(&self) -> Option<String> {
        let sessions = self.sessions.as_ref()?;
        let id = uuid::Uuid::new_v4().to_string();
        sessions
            .lock()
            .expect("sessions mutex poisoned")
            .insert(id.clone());
        Some(id)
    }

    /// Return `true` if `id` matches a previously issued session id.
    /// Always `true` when this state is for stdio (no session tracking).
    fn session_valid(&self, id: &str) -> bool {
        match &self.sessions {
            None => true,
            Some(sessions) => sessions
                .lock()
                .expect("sessions mutex poisoned")
                .contains(id),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();

    // healthcheck: probe /healthz of the streamable-HTTP transport and exit
    // 0/1 (#469). Only meaningful when MCP_HTTP_BIND is set; a stdio server
    // has no listener to probe.
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        match std::env::var("MCP_HTTP_BIND") {
            Ok(bind) => lumenqraph_core::healthcheck::run_and_exit(&bind, "/healthz"),
            Err(_) => {
                eprintln!("healthcheck failed: MCP_HTTP_BIND is not set (stdio mode has no HTTP listener)");
                std::process::exit(1);
            }
        }
    }
    // IMPORTANT: log to stderr — stdout is the JSON-RPC channel.
    lumenqraph_core::telemetry::init_tracing_stderr();

    let database_url = std::env::var("DATABASE_URL").context("missing DATABASE_URL")?;
    let rpc_url = std::env::var("RPC_URL")
        .unwrap_or_else(|_| "https://soroban-testnet.stellar.org".to_string());
    let rpc_timeout_secs: u64 = std::env::var("RPC_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(30);
    let rpc_rate_limit_per_min: usize = std::env::var("MCP_RPC_RATE_LIMIT_PER_MIN")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(60);
    let auth_token = std::env::var("MCP_AUTH_TOKEN")
        .ok()
        .map(|token| token.trim().to_string())
        .filter(|token| !token.is_empty());
    let http_bind = std::env::var("MCP_HTTP_BIND").ok();
    let allow_anon = std::env::var("MCP_HTTP_ALLOW_ANONYMOUS")
        .ok()
        .map(|v| v.trim().eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if http_bind.is_some() {
        validate_http_auth(auth_token.as_deref(), allow_anon)?;
        if allow_anon && auth_token.is_none() {
            warn!(
                "MCP HTTP transport is starting WITHOUT authentication \
                 (MCP_HTTP_ALLOW_ANONYMOUS=true). Bind to localhost and \
                 restrict network access."
            );
        }
    }

    // Validate CONTRACT_IDS at startup so a misconfigured address is caught
    // immediately rather than silently ignored.
    lumenqraph_core::parse_contract_ids(
        &std::env::var("CONTRACT_IDS").unwrap_or_default(),
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    let db = lumenqraph_core::db::DbConfig {
        max_connections: db_setting("MCP_DATABASE_MAX_CONNECTIONS", "DATABASE_MAX_CONNECTIONS", 5u32)?,
        min_connections: db_setting("MCP_DATABASE_MIN_CONNECTIONS", "DATABASE_MIN_CONNECTIONS", 0u32)?,
        acquire_timeout_secs: db_setting("MCP_DATABASE_ACQUIRE_TIMEOUT_SECS", "DATABASE_ACQUIRE_TIMEOUT_SECS", 30u64)?,
        idle_timeout_secs: db_setting("MCP_DATABASE_IDLE_TIMEOUT_SECS", "DATABASE_IDLE_TIMEOUT_SECS", 600u64)?,
        connect_retries: db_setting("MCP_DATABASE_CONNECT_RETRIES", "DATABASE_CONNECT_RETRIES", 30u32)?,
    };
    let pool = lumenqraph_core::db::connect_with_retry(&database_url, &db).await?;

    let state = State {
        pool,
        rpc: RpcClient::new(rpc_url, rpc_timeout_secs),
        rpc_limiter: rpc::RpcRateLimiter::new(rpc_rate_limit_per_min),
        auth_token,
        sessions: http_bind
            .as_ref()
            .map(|_| Arc::new(Mutex::new(HashSet::new()))),
    };

    if let Some(bind) = http_bind {
        let http_config = HttpConfig::from_env();
        return serve_http(state, bind, http_config).await;
    }

    info!("lumenqraph MCP server ready (stdio)");
    serve(state).await
}

fn validate_http_auth(auth_token: Option<&str>, allow_anonymous: bool) -> anyhow::Result<()> {
    if auth_token.is_some_and(|token| !token.trim().is_empty()) || allow_anonymous {
        return Ok(());
    }
    Err(anyhow::anyhow!(
        "MCP_HTTP_BIND is set but MCP_AUTH_TOKEN is unset. The HTTP \
         transport is unauthenticated by default — anyone who can reach \
         the port can call tools and read the index. Set MCP_AUTH_TOKEN, \
         or set MCP_HTTP_ALLOW_ANONYMOUS=true to accept this risk and \
         start anyway."
    ))
}

/// HTTP transport security configuration, parsed from the environment once at
/// startup (#622).
#[derive(Clone)]
struct HttpConfig {
    /// Allowlist of `Origin` headers that may call `/mcp`. An empty set means
    /// "reject any request that carries an Origin header". Requests with no
    /// `Origin` header (CLI clients) are always allowed — Origin is set by
    /// browsers only.
    allowed_origins: HashSet<String>,
    /// When true, skip Origin validation entirely. Opt-in escape hatch for
    /// operators behind a reverse proxy that strips/normalizes Origin.
    allow_any_origin: bool,
}

impl HttpConfig {
    fn from_env() -> Self {
        let allow_any_origin = std::env::var("MCP_ALLOW_ANY_ORIGIN")
            .ok()
            .map(|v| v.trim().eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        let allowed_origins: HashSet<String> = std::env::var("MCP_ALLOWED_ORIGINS")
            .ok()
            .map(|raw| {
                raw.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();

        if allow_any_origin {
            warn!(
                "MCP_ALLOW_ANY_ORIGIN=true — Origin header validation is \
                 DISABLED. This is only safe behind an authenticating reverse \
                 proxy."
            );
        } else if allowed_origins.is_empty() {
            info!(
                "MCP_ALLOWED_ORIGINS is unset; browsers carrying any Origin \
                 header will be rejected with 403 (DNS-rebinding protection). \
                 CLI clients without an Origin header are unaffected."
            );
        } else {
            info!(origins = ?allowed_origins, "MCP HTTP Origin allowlist");
        }

        HttpConfig {
            allowed_origins,
            allow_any_origin,
        }
    }

    /// Test-only constructor (bypasses env var parsing).
    #[cfg(test)]
    fn for_testing(allowed_origins: &[&str], allow_any_origin: bool) -> Self {
        HttpConfig {
            allowed_origins: allowed_origins.iter().map(|s| s.to_string()).collect(),
            allow_any_origin,
        }
    }

    /// Validate an `Origin` header value. Returns `Ok(())` if the request may
    /// proceed, or `Err(reason)` if it must be rejected with 403.
    fn validate_origin(&self, origin: Option<&str>) -> Result<(), String> {
        if self.allow_any_origin {
            return Ok(());
        }
        // No Origin header → not a browser request (CLI, curl, etc.). Allow.
        let origin = match origin {
            None => return Ok(()),
            Some(o) => o.trim(),
        };
        if origin.is_empty() {
            return Ok(());
        }
        if self.allowed_origins.contains(origin) {
            return Ok(());
        }
        Err(format!("origin not allowed: {origin}"))
    }
}

/// The stdio JSON-RPC loop: read a message per line, dispatch, write responses.
async fn serve(state: State) -> anyhow::Result<()> {
    serve_io(state, tokio::io::stdin(), tokio::io::stdout()).await
}

async fn serve_http(state: State, bind: String, http_config: HttpConfig) -> anyhow::Result<()> {
    let app = Router::new()
        // The MCP endpoint accepts POST (JSON-RPC) and DELETE (session teardown).
        // GET returns 405 — we do not implement a server-initiated SSE stream
        // (see handle_get_mcp).
        .route(
            "/mcp",
            post(handle_http_request).delete(handle_delete_mcp).get(handle_get_mcp),
        )
        .route("/healthz", get(|| async { "ok" }))
        .with_state(state)
        .layer(axum::middleware::from_fn_with_state(
            http_config,
            origin_guard,
        ));

    // Default to 127.0.0.1 when the operator did not specify a host. The MCP
    // spec for Streamable HTTP recommends binding to localhost to prevent
    // DNS-rebinding attacks from browsers (#622). We only rewrite bare port
    // forms like ":8090" — explicit 0.0.0.0 binds are honoured (the operator
    // asked for them).
    let bind = normalize_bind(&bind);

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    info!(bind = %bind, "lumenqraph MCP server ready (streamable HTTP)");
    axum::serve(listener, app).await?;
    Ok(())
}

/// Rewrite a bare `:port` or `port` bind to `127.0.0.1:port`. If the operator
/// passed an explicit host (including `0.0.0.0`), leave it alone — they
/// opted in.
fn normalize_bind(bind: &str) -> String {
    let trimmed = bind.trim();
    // Already has a host portion (e.g. "127.0.0.1:8090", "0.0.0.0:8090", "[::1]:8090").
    if trimmed.contains(':') && !trimmed.starts_with(':') {
        return trimmed.to_string();
    }
    // Bare port forms: ":8090" or "8090".
    let port = trimmed.trim_start_matches(':');
    format!("127.0.0.1:{port}")
}

/// axum middleware that rejects browser requests whose `Origin` header is not
/// in the configured allowlist (#622). Returns 403 with an empty JSON-RPC
/// error body (no id) per the MCP spec.
async fn origin_guard(
    AxumState(http_config): AxumState<HttpConfig>,
    headers: HeaderMap,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if let Err(reason) = http_config.validate_origin(headers.get("origin").and_then(|v| v.to_str().ok())) {
        warn!(reason = %reason, "rejecting MCP request: Origin validation failed");
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "error": { "code": -32000, "message": "Forbidden: Origin not allowed" }
        });
        return (
            StatusCode::FORBIDDEN,
            [("content-type", "application/json")],
            Json(body),
        )
            .into_response();
    }
    next.run(request).await
}

/// `GET /mcp` — the MCP spec says the server MUST either open an SSE stream or
/// return 405 Method Not Allowed. We do not implement server-initiated
/// streaming; return 405 so spec-compliant clients fall back to POST-only.
async fn handle_get_mcp() -> Response {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        [("allow", "POST, DELETE")],
        "GET /mcp is not supported; this server does not serve an SSE stream. Use POST.",
    )
        .into_response()
}

/// `DELETE /mcp` — tear down an MCP session. Per spec, the server MAY respond
/// with 405; we honour it so clients can clean up. Responds 404 if the session
/// id is unknown, 200 if it was dropped, and 400 if the header is missing.
async fn handle_delete_mcp(
    AxumState(state): AxumState<State>,
    headers: HeaderMap,
) -> Response {
    let Some(sessions) = &state.sessions else {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    };
    let Some(id) = headers
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        return (
            StatusCode::BAD_REQUEST,
            "Mcp-Session-Id header required",
        )
            .into_response();
    };
    let dropped = sessions
        .lock()
        .expect("sessions mutex poisoned")
        .remove(id);
    if dropped {
        StatusCode::OK.into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

async fn handle_http_request(
    AxumState(state): AxumState<State>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid JSON-RPC body").into_response(),
    };

    let mut msg: Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid JSON body").into_response(),
    };

    if let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
        msg["authorization"] = Value::String(auth.to_string());
    }

    let method = msg
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();

    // The MCP spec requires clients to send MCP-Protocol-Version on all
    // requests after initialize. We accept any of the supported versions
    // here; an unknown version (post-initialize) yields 400.
    let client_version = headers
        .get("mcp-protocol-version")
        .and_then(|v| v.to_str().ok())
        .map(str::trim);

    let authenticated = state.auth_token.as_ref().map_or(true, |token| {
        msg.get("authorization")
            .and_then(Value::as_str)
            .map(|value| value.strip_prefix("Bearer ").unwrap_or(value).trim() == token)
            .unwrap_or(false)
    });

    let is_initialize = method == "initialize";
    // initialize always issues a session id and must not require one.
    if !is_initialize {
        // Validate the session id if this server tracks sessions.
        if state.sessions.is_some() {
            match headers
                .get("mcp-session-id")
                .and_then(|v| v.to_str().ok())
                .map(str::trim)
            {
                Some(id) if !id.is_empty() => {
                    if !state.session_valid(id) {
                        return (
                            StatusCode::NOT_FOUND,
                            Json(json!({
                                "jsonrpc": "2.0",
                                "error": { "code": -32000, "message": "Unknown or expired Mcp-Session-Id" }
                            })),
                        )
                            .into_response();
                    }
                }
                _ => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({
                            "jsonrpc": "2.0",
                            "error": { "code": -32000, "message": "Mcp-Session-Id header required" }
                        })),
                    )
                        .into_response();
                }
            }
        }
        // Validate the protocol version header (post-initialize only).
        if let Some(v) = client_version {
            if !SUPPORTED_PROTOCOL_VERSIONS.contains(&v) && !v.is_empty() {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "jsonrpc": "2.0",
                        "error": { "code": -32602, "message": format!("unsupported MCP-Protocol-Version: {v}") }
                    })),
                )
                    .into_response();
            }
        }
    }

    let (resp, _newly_auth) = match handle(&state, msg, authenticated).await {
        Some((resp, newly_auth)) => (resp, newly_auth),
        None => return StatusCode::NO_CONTENT.into_response(),
    };

    // On a successful initialize, issue and stash a Mcp-Session-Id and echo
    // the negotiated protocol version in the response headers.
    let successful_initialize = is_initialize && resp.get("result").is_some();
    let protocol_version = resp
        .get("result")
        .and_then(|result| result.get("protocolVersion"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let mut response = Json(resp).into_response();
    if successful_initialize {
        if let Some(version) = protocol_version {
            if let Ok(hv) = axum::http::HeaderValue::from_str(&version) {
                response
                    .headers_mut()
                    .insert("mcp-protocol-version", hv);
            }
        }
        if let Some(id) = state.issue_session() {
            if let Ok(hv) =
                axum::http::HeaderValue::from_str(&id).map_err(|e| warn!(error = %e, "session id not a valid header value"))
            {
                response
                    .headers_mut()
                    .insert("mcp-session-id", hv);
            }
        }
    }
    response
}

/// Protocol loop over any `AsyncRead` / `AsyncWrite` pair (stdin/stdout in
/// production, in-memory duplex streams in tests).
pub(crate) async fn serve_io<R, W>(state: State, reader: R, writer: W) -> anyhow::Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut lines = BufReader::new(reader).lines();
    let mut out = writer;
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let msg: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                let err = error_response(Value::Null, -32700, &format!("parse error: {e}"));
                write_to(&mut out, &err).await?;
                continue;
            }
        };
        let authenticated = state
            .auth_token
            .as_ref()
            .map_or(true, |token| check_auth(&msg, token));
        if let Some((response, _)) = handle(&state, msg, authenticated).await {
            write_to(&mut out, &response).await?;
        }
    }
    Ok(())
}

async fn write_to<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    value: &Value,
) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt as _;
    writer.write_all(value.to_string().as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

fn check_auth(msg: &Value, expected: &str) -> bool {
    let candidate = msg
        .get("authorization")
        .or_else(|| msg.get("Authorization"))
        .or_else(|| {
            msg.get("params").and_then(|p| {
                p.get("authorization")
                    .or_else(|| p.get("Authorization"))
                    .or_else(|| p.get("authToken"))
                    .or_else(|| p.get("auth_token"))
            })
        })
        .and_then(Value::as_str);

    if let Some(token) = candidate {
        let clean = token.strip_prefix("Bearer ").unwrap_or(token).trim();
        clean == expected
    } else {
        false
    }
}

/// Dispatch one JSON-RPC message. Returns `(response, newly_authenticated)` where response is `None` for notifications (no id).
async fn handle(state: &State, msg: Value, authenticated: bool) -> Option<(Value, bool)> {
    let id = msg.get("id").cloned();
    let method = msg
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let is_request = id.is_some();

    if method == "initialize" {
        if let Some(expected_token) = &state.auth_token {
            if !check_auth(&msg, expected_token) {
                return Some((
                    error_response(
                        id.unwrap_or(Value::Null),
                        -32001,
                        "Unauthorized: invalid or missing MCP_AUTH_TOKEN in Authorization field",
                    ),
                    false,
                ));
            }
        }
        return Some((result_response(id, initialize_result(&msg)), true));
    }

    if method.starts_with("notifications/") {
        return None;
    }

    if !authenticated {
        return if is_request {
            Some((
                error_response(
                    id.unwrap_or(Value::Null),
                    -32001,
                    "Unauthorized: authentication required",
                ),
                false,
            ))
        } else {
            None
        };
    }

    let response = match method {
        "ping" => Some(result_response(id, json!({}))),
        "tools/list" => Some(result_response(
            id,
            json!({ "tools": tools::definitions() }),
        )),
        "tools/call" => Some(handle_tools_call(state, id, &msg).await),
        _ if is_request => Some(error_response(
            id.unwrap_or(Value::Null),
            -32601,
            &format!("method not found: {method}"),
        )),
        _ => None,
    };

    response.map(|resp| (resp, false))
}

fn initialize_result(msg: &Value) -> Value {
    let requested = msg
        .get("params")
        .and_then(|p| p.get("protocolVersion"))
        .and_then(Value::as_str)
        .unwrap_or(DEFAULT_PROTOCOL_VERSION);
    let protocol_version = if SUPPORTED_PROTOCOL_VERSIONS.contains(&requested) {
        requested
    } else {
        SUPPORTED_PROTOCOL_VERSIONS.last().copied().unwrap_or(DEFAULT_PROTOCOL_VERSION)
    };
    json!({
        "protocolVersion": protocol_version,
        "capabilities": { "tools": {} },
        "serverInfo": { "name": "lumenqraph-mcp", "version": env!("CARGO_PKG_VERSION") },
        "instructions": "Typed, self-describing access to Soroban contracts. Start with \
                         list_contracts, then get_contract_interface to discover a contract's \
                         functions/events, then query_events or call_contract."
    })
}

async fn handle_tools_call(state: &State, id: Option<Value>, msg: &Value) -> Value {
    let params = msg.get("params").cloned().unwrap_or(Value::Null);
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let empty = json!({});
    let args = params.get("arguments").unwrap_or(&empty);

    // MCP convention: tool failures are results with isError:true (so the agent
    // can read the message), not JSON-RPC protocol errors.
    match tools::call(state, name, args).await {
        Ok(payload) => result_response(id, tool_content(&payload, false)),
        Err(e) => result_response(id, tool_content(&json!({ "error": e.to_string() }), true)),
    }
}

fn tool_content(payload: &Value, is_error: bool) -> Value {
    let text = serde_json::to_string_pretty(payload).unwrap_or_else(|_| payload.to_string());
    let mut out = json!({
        "content": [ { "type": "text", "text": text } ],
        "isError": is_error,
    });
    if !payload.is_null() {
        out["structuredContent"] = payload.clone();
    }
    out
}

fn result_response(id: Option<Value>, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id.unwrap_or(Value::Null), "result": result })
}

fn error_response(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initialize_negotiates_supported_protocol_versions() {
        let supported = json!({ "params": { "protocolVersion": "2025-06-18" } });
        assert_eq!(initialize_result(&supported)["protocolVersion"], "2025-06-18");

        let unsupported = json!({ "params": { "protocolVersion": "2027-01-01" } });
        assert_eq!(initialize_result(&unsupported)["protocolVersion"], "2025-06-18");

        assert_eq!(
            initialize_result(&json!({}))["protocolVersion"],
            DEFAULT_PROTOCOL_VERSION
        );
    }

    #[test]
    fn initialize_advertises_tools_capability() {
        let r = initialize_result(&json!({}));
        assert!(r["capabilities"]["tools"].is_object());
        assert_eq!(r["serverInfo"]["name"], "lumenqraph-mcp");
    }

    #[test]
    fn all_tools_are_defined_with_schemas() {
        let defs = tools::definitions();
        let names: Vec<&str> = defs
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert_eq!(
            names,
            vec![
                "list_contracts",
                "get_contract_interface",
                "get_contract_upgrades",
                "get_contract_state",
                "get_contract_data",
                "query_events",
                "get_event",
                "get_transaction_events",
                "get_contract_stats",
                "list_contract_functions",
                "call_contract",
                "simulate_call",
                "query_transfers",
                "diff_contract_interface",
                "query_swaps",
                "query_nft_events",
                "query_liquidity_events",
            ]
        );
        for t in defs.as_array().unwrap() {
            assert_eq!(
                t["inputSchema"]["type"], "object",
                "each tool needs an input schema"
            );
            assert_eq!(
                t["outputSchema"]["type"], "object",
                "each tool needs an output schema"
            );
            assert!(!t["description"].as_str().unwrap().is_empty());
        }
    }

    #[test]
    fn tool_content_marks_errors() {
        let ok = tool_content(&json!({ "x": 1 }), false);
        assert_eq!(ok["isError"], false);
        assert_eq!(ok["content"][0]["type"], "text");
        assert_eq!(ok["structuredContent"]["x"], 1);
        assert_eq!(tool_content(&json!({}), true)["isError"], true);
    }

    #[test]
    fn responses_are_well_formed_json_rpc() {
        assert_eq!(
            result_response(Some(json!(7)), json!("ok")),
            json!({ "jsonrpc": "2.0", "id": 7, "result": "ok" })
        );
        let err = error_response(json!(3), -32601, "nope");
        assert_eq!(err["error"]["code"], -32601);
        assert_eq!(err["id"], 3);
    }

    // ── handshake shape ───────────────────────────────────────────────────

    #[tokio::test]
    async fn handle_returns_none_for_notifications() {
        // Notifications have no `id` — the server must not reply.
        let msg = json!({ "jsonrpc": "2.0", "method": "notifications/initialized" });
        // Build a fake State — connect_lazy so no real DB is needed.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://test:test@localhost/test")
            .unwrap();
        let state = State {
            pool,
            rpc: RpcClient::new("http://127.0.0.1:0", 30),
            rpc_limiter: rpc::RpcRateLimiter::new(60),
            auth_token: None,
            sessions: None,
        };
        let resp = handle(&state, msg, true).await;
        assert!(resp.is_none(), "notifications should not produce a response");
    }

    #[tokio::test]
    async fn handle_returns_error_for_unknown_method() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://test:test@localhost/test")
            .unwrap();
        let state = State {
            pool,
            rpc: RpcClient::new("http://127.0.0.1:0", 30),
            rpc_limiter: rpc::RpcRateLimiter::new(60),
            auth_token: None,
            sessions: None,
        };
        let msg = json!({ "jsonrpc": "2.0", "id": 1, "method": "unknown/method" });
        let (resp, _) = handle(&state, msg, true).await.unwrap();
        assert_eq!(resp["error"]["code"], -32601);
        assert!(resp["error"]["message"].as_str().unwrap().contains("unknown/method"));
    }

    #[tokio::test]
    async fn handle_ping_returns_empty_result() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://test:test@localhost/test")
            .unwrap();
        let state = State {
            pool,
            rpc: RpcClient::new("http://127.0.0.1:0", 30),
            rpc_limiter: rpc::RpcRateLimiter::new(60),
            auth_token: None,
            sessions: None,
        };
        let msg = json!({ "jsonrpc": "2.0", "id": 42, "method": "ping" });
        let (resp, _) = handle(&state, msg, true).await.unwrap();
        assert_eq!(resp["id"], 42);
        assert!(resp["result"].is_object());
    }

    // ── authentication checks ─────────────────────────────────────────────

    #[tokio::test]
    async fn initialize_rejects_unauthenticated_request_when_token_is_configured() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://test:test@localhost/test")
            .unwrap();
        let state = State {
            pool,
            rpc: RpcClient::new("http://127.0.0.1:0", 30),
            rpc_limiter: rpc::RpcRateLimiter::new(60),
            auth_token: Some("secret123".to_string()),
            sessions: None,
        };
        let msg = json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {} });
        let (resp, is_auth) = handle(&state, msg, false).await.unwrap();
        assert!(!is_auth);
        assert_eq!(resp["error"]["code"], -32001);

        let ping_msg = json!({ "jsonrpc": "2.0", "id": 2, "method": "ping" });
        let (ping_resp, _) = handle(&state, ping_msg, false).await.unwrap();
        assert_eq!(ping_resp["error"]["code"], -32001);
    }

    #[tokio::test]
    async fn initialize_accepts_valid_authorization_token() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://test:test@localhost/test")
            .unwrap();
        let state = State {
            pool,
            rpc: RpcClient::new("http://127.0.0.1:0", 30),
            rpc_limiter: rpc::RpcRateLimiter::new(60),
            auth_token: Some("secret123".to_string()),
            sessions: None,
        };
        let msg = json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": { "authorization": "Bearer secret123" }
        });
        let (resp, is_auth) = handle(&state, msg, false).await.unwrap();
        assert!(is_auth);
        assert!(resp["result"]["capabilities"]["tools"].is_object());
    }

    // ── tools/list shape ──────────────────────────────────────────────────

    #[tokio::test]
    async fn tools_list_response_is_well_formed() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://test:test@localhost/test")
            .unwrap();
        let state = State {
            pool,
            rpc: RpcClient::new("http://127.0.0.1:0", 30),
            rpc_limiter: rpc::RpcRateLimiter::new(60),
            auth_token: None,
            sessions: None,
        };
        let msg = json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" });
        let (resp, _) = handle(&state, msg, true).await.unwrap();
        assert_eq!(resp["id"], 2);
        let tools = resp["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 17, "all declared MCP tools must be present");
        for tool in tools {
            // Every tool must have a non-empty name, description, and an object schema.
            assert!(!tool["name"].as_str().unwrap_or("").is_empty());
            assert!(!tool["description"].as_str().unwrap_or("").is_empty());
            assert_eq!(
                tool["inputSchema"]["type"],
                "object",
                "tool {:?} must declare an object inputSchema",
                tool["name"]
            );
        }
    }

    // ── each tool's required-argument validation ──────────────────────────

    /// Assert that calling a tool without its required arguments yields an
    /// `isError: true` result (MCP convention) mentioning the missing field.
    async fn assert_missing_arg_error(tool_name: &str, args: Value, expected_in_msg: &str) {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://test:test@localhost/test")
            .unwrap();
        let state = State {
            pool,
            rpc: RpcClient::new("http://127.0.0.1:0", 30),
            rpc_limiter: rpc::RpcRateLimiter::new(60),
            auth_token: None,
            sessions: None,
        };
        let msg = json!({
            "jsonrpc": "2.0", "id": 1,
            "method": "tools/call",
            "params": { "name": tool_name, "arguments": args }
        });
        let (resp, _) = handle(&state, msg, true).await.unwrap();
        // The result is always present (MCP errors are results with isError).
        let result = &resp["result"];
        assert_eq!(
            result["isError"], true,
            "tool {tool_name:?} with missing arg should set isError"
        );
        let text = result["content"][0]["text"].as_str().unwrap_or("");
        assert!(
            text.contains(expected_in_msg),
            "tool {tool_name:?}: expected {expected_in_msg:?} in error text, got: {text}"
        );
    }

    #[tokio::test]
    async fn get_contract_interface_requires_contract_id() {
        assert_missing_arg_error("get_contract_interface", json!({}), "contract_id").await;
    }

    #[tokio::test]
    async fn get_contract_upgrades_requires_contract_id() {
        assert_missing_arg_error("get_contract_upgrades", json!({}), "contract_id").await;
    }

    #[tokio::test]
    async fn get_contract_state_requires_contract_id() {
        assert_missing_arg_error("get_contract_state", json!({}), "contract_id").await;
    }

    #[tokio::test]
    async fn get_contract_data_requires_contract_id() {
        assert_missing_arg_error("get_contract_data", json!({}), "contract_id").await;
    }

    #[tokio::test]
    async fn query_events_requires_contract_id() {
        assert_missing_arg_error("query_events", json!({}), "contract_id").await;
    }

    #[tokio::test]
    async fn call_contract_requires_contract_id_and_function() {
        assert_missing_arg_error("call_contract", json!({}), "contract_id").await;
        assert_missing_arg_error(
            "call_contract",
            json!({ "contract_id": "C1" }),
            "function",
        )
        .await;
    }

    #[tokio::test]
    async fn simulate_call_requires_contract_id_and_function() {
        assert_missing_arg_error("simulate_call", json!({}), "contract_id").await;
        assert_missing_arg_error(
            "simulate_call",
            json!({ "contract_id": "C1" }),
            "function",
        )
        .await;
    }

    #[tokio::test]
    async fn unknown_tool_name_returns_is_error() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://test:test@localhost/test")
            .unwrap();
        let state = State {
            pool,
            rpc: RpcClient::new("http://127.0.0.1:0", 30),
            rpc_limiter: rpc::RpcRateLimiter::new(60),
            auth_token: None,
            sessions: None,
        };
        let msg = json!({
            "jsonrpc": "2.0", "id": 5,
            "method": "tools/call",
            "params": { "name": "no_such_tool", "arguments": {} }
        });
        let (resp, _) = handle(&state, msg, true).await.unwrap();
        assert_eq!(resp["result"]["isError"], true);
        let text = resp["result"]["content"][0]["text"].as_str().unwrap_or("");
        assert!(
            text.contains("no_such_tool"),
            "error should mention the unknown tool name: {text}"
        );
    }

    // ── DB-backed tools/call tests ─────────────────────────────────────────
    //
    // These call list_contracts, query_events, etc. against an isolated test
    // schema. Marked #[ignore] and run via `make test-db`.

    async fn fixture_state() -> State {
        let url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL");
        let schema = format!("test_{}", uuid::Uuid::new_v4().simple());

        let admin = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap();
        sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
            .execute(&admin)
            .await
            .unwrap();
        admin.close().await;

        let option = format!("-c search_path={schema},public");
        let sep = if url.contains('?') { "&" } else { "?" };
        let schema_url = format!("{url}{sep}options={}", percent_encode(&option));
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&schema_url)
            .await
            .unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

        State {
            pool,
            rpc: RpcClient::new("http://127.0.0.1:0", 30),
            rpc_limiter: rpc::RpcRateLimiter::new(60),
            auth_token: None,
            sessions: None,
        }
    }

    fn percent_encode(s: &str) -> String {
        s.chars()
            .flat_map(|c| match c {
                'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => vec![c],
                c => format!("%{:02X}", c as u32).chars().collect(),
            })
            .collect()
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn list_contracts_returns_empty_on_fresh_db() {
        let state = fixture_state().await;
        let result = tools::call(&state, "list_contracts", &json!({}))
            .await
            .unwrap();
        assert_eq!(result["contracts"], json!([]), "fresh DB has no contracts");
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn query_events_returns_empty_for_unknown_contract() {
        let state = fixture_state().await;
        let result = tools::call(
            &state,
            "query_events",
            &json!({ "contract_id": "CNOPE", "limit": 5 }),
        )
        .await
        .unwrap();
        assert_eq!(result["count"], 0);
        assert_eq!(result["events"], json!([]));
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn get_contract_interface_errors_for_unindexed_contract() {
        let state = fixture_state().await;
        let err = tools::call(
            &state,
            "get_contract_interface",
            &json!({ "contract_id": "CNOPE" }),
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("CNOPE"),
            "error should mention the contract id: {err}"
        );
    }
}

/// Integration tests for the JSON-RPC protocol layer (`serve_io`).
///
/// These tests drive the MCP server through the full stdio round-trip using an
/// in-process `tokio::io::duplex` stream pair, covering:
///  - The initialize → tools/list → tools/call handshake
///  - Malformed JSON input (parse error -32700)
///  - Unknown method (method-not-found error -32601)
///  - Notifications (no response expected)
///  - Missing required tool argument (isError result)
///
/// No real database or RPC server is required; the pool is created with
/// `connect_lazy` so no network calls are made before the tests exercise
/// validation paths.
#[cfg(test)]
mod protocol_tests {
    use super::*;

    /// Build a `State` that does not need a live database.
    fn lazy_state() -> State {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://test:test@localhost/test")
            .expect("connect_lazy");
        State {
            pool,
            rpc: RpcClient::new("http://127.0.0.1:0", 30),
            rpc_limiter: rpc::RpcRateLimiter::new(60),
            auth_token: None,
            sessions: None,
        }
    }

    /// Feed `input` (newline-delimited JSON-RPC messages) through `serve_io`
    /// and return all response lines as parsed `serde_json::Value`s.
    ///
    /// Uses a simplex stream: we write all input into one half of a duplex,
    /// close the write end (signalling EOF), run serve_io against it, then
    /// collect all output from the output half of a second duplex.
    async fn run_rpc(input: &str) -> Vec<Value> {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let state = lazy_state();

        // Build the input side: a DuplexStream where we write the test messages
        // then drop the write half to signal EOF.
        let (mut write_half, read_half) = tokio::io::duplex(64 * 1024);
        write_half.write_all(input.as_bytes()).await.expect("write input");
        drop(write_half); // EOF for the server reader

        // The output side: a DuplexStream where the server writes responses.
        let (out_write_half, mut out_read_half) = tokio::io::duplex(64 * 1024);

        // Run serve_io to completion; it will exit when the reader hits EOF.
        serve_io(state, read_half, out_write_half)
            .await
            .expect("serve_io should not fail");

        // Read all bytes written by serve_io.
        let mut buf = Vec::new();
        out_read_half.read_to_end(&mut buf).await.expect("read output");
        let output = String::from_utf8(buf).expect("utf8 output");

        output
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).unwrap_or_else(|e| {
                panic!("server produced invalid JSON: {e}\nline: {l}")
            }))
            .collect()
    }

    // ── initialize handshake ──────────────────────────────────────────────

    #[tokio::test]
    async fn initialize_round_trip() {
        let msgs = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{}}}
"#;
        let responses = run_rpc(msgs).await;
        assert_eq!(responses.len(), 1, "exactly one response to initialize");
        let r = &responses[0];
        assert_eq!(r["id"], 1);
        assert_eq!(r["result"]["protocolVersion"], "2024-11-05");
        assert_eq!(r["result"]["serverInfo"]["name"], "lumenqraph-mcp");
        assert!(r["result"]["capabilities"]["tools"].is_object());
    }

    // ── tools/list ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn tools_list_round_trip() {
        let msgs = r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}
"#;
        let responses = run_rpc(msgs).await;
        assert_eq!(responses.len(), 1);
        let r = &responses[0];
        assert_eq!(r["id"], 2);
        let tools = r["result"]["tools"].as_array().expect("tools array");
        assert!(!tools.is_empty(), "at least one tool must be declared");
        for tool in tools {
            assert!(!tool["name"].as_str().unwrap_or("").is_empty());
            assert_eq!(tool["inputSchema"]["type"], "object");
        }
    }

    // ── full initialize → tools/list → tools/call round-trip ─────────────

    #[tokio::test]
    async fn full_handshake_round_trip() {
        // Three messages: initialize, tools/list, and a tools/call that will
        // fail with isError (no DB) but still produce a well-formed response.
        let msgs = concat!(
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{}}}"#, "\n",
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#, "\n",
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_contracts","arguments":{}}}"#, "\n",
        );
        let responses = run_rpc(msgs).await;
        assert_eq!(responses.len(), 3, "one response per request");

        assert_eq!(responses[0]["id"], 1, "init id");
        assert!(responses[0]["result"]["protocolVersion"].is_string());

        assert_eq!(responses[1]["id"], 2, "tools/list id");
        assert!(responses[1]["result"]["tools"].is_array());

        assert_eq!(responses[2]["id"], 3, "tools/call id");
        // Either a real result or an isError — both are valid without a DB.
        let result = &responses[2]["result"];
        assert!(result.is_object(), "tools/call always produces a result object");
    }

    // ── error cases ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn malformed_json_returns_parse_error() {
        let msgs = "not valid json at all\n";
        let responses = run_rpc(msgs).await;
        assert_eq!(responses.len(), 1);
        let r = &responses[0];
        // id must be null/absent for a parse error (we can't know the request id).
        assert_eq!(r["error"]["code"], -32700);
        assert!(
            r["error"]["message"].as_str().unwrap_or("").contains("parse error"),
            "message should say parse error: {:?}", r["error"]["message"]
        );
    }

    #[tokio::test]
    async fn unknown_method_returns_method_not_found() {
        let msgs = r#"{"jsonrpc":"2.0","id":9,"method":"no/such/method"}
"#;
        let responses = run_rpc(msgs).await;
        assert_eq!(responses.len(), 1);
        let r = &responses[0];
        assert_eq!(r["id"], 9);
        assert_eq!(r["error"]["code"], -32601);
        assert!(
            r["error"]["message"].as_str().unwrap_or("").contains("no/such/method"),
            "error should mention the method: {:?}", r["error"]["message"]
        );
    }

    #[tokio::test]
    async fn notification_produces_no_response() {
        // Notifications have no `id`; the server must not reply.
        let msgs = r#"{"jsonrpc":"2.0","method":"notifications/initialized"}
"#;
        let responses = run_rpc(msgs).await;
        assert_eq!(responses.len(), 0, "notifications must not produce a response");
    }

    #[tokio::test]
    async fn empty_lines_are_ignored() {
        let msgs = "\n\n   \n";
        let responses = run_rpc(msgs).await;
        assert_eq!(responses.len(), 0, "blank lines produce no output");
    }

    #[tokio::test]
    async fn ping_returns_empty_result() {
        let msgs = r#"{"jsonrpc":"2.0","id":42,"method":"ping"}
"#;
        let responses = run_rpc(msgs).await;
        assert_eq!(responses.len(), 1);
        let r = &responses[0];
        assert_eq!(r["id"], 42);
        assert!(r["result"].is_object());
        assert!(r.get("error").is_none());
    }

    #[tokio::test]
    async fn missing_required_arg_returns_is_error() {
        // get_contract_interface requires contract_id; omitting it must produce
        // isError: true (MCP convention — tool errors are results, not protocol errors).
        let msgs = r#"{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"get_contract_interface","arguments":{}}}
"#;
        let responses = run_rpc(msgs).await;
        assert_eq!(responses.len(), 1);
        let r = &responses[0];
        assert_eq!(r["id"], 5);
        let result = &r["result"];
        assert_eq!(result["isError"], true, "missing arg must set isError");
        let text = result["content"][0]["text"].as_str().unwrap_or("");
        assert!(
            text.contains("contract_id"),
            "error text should mention 'contract_id': {text}"
        );
    }

    #[tokio::test]
    async fn unknown_tool_name_returns_is_error_via_protocol() {
        let msgs = r#"{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"does_not_exist","arguments":{}}}
"#;
        let responses = run_rpc(msgs).await;
        assert_eq!(responses.len(), 1);
        let r = &responses[0];
        assert_eq!(r["id"], 7);
        assert_eq!(r["result"]["isError"], true);
        let text = r["result"]["content"][0]["text"].as_str().unwrap_or("");
        assert!(
            text.contains("does_not_exist"),
            "error should mention the unknown tool: {text}"
        );
    }

    #[tokio::test]
    async fn multiple_messages_get_independent_responses() {
        // Two well-formed requests: both must produce a response, in order.
        let msgs = concat!(
            r#"{"jsonrpc":"2.0","id":10,"method":"ping"}"#, "\n",
            r#"{"jsonrpc":"2.0","id":11,"method":"ping"}"#, "\n",
        );
        let responses = run_rpc(msgs).await;
        assert_eq!(responses.len(), 2);
        assert_eq!(responses[0]["id"], 10);
        assert_eq!(responses[1]["id"], 11);
    }
}

/// #219 — CONTRACT_IDS startup validation in lumenqraph-mcp.
///
/// The MCP server calls `lumenqraph_core::parse_contract_ids` at startup and
/// propagates the error so the process refuses to start on a misconfigured
/// address. These tests exercise the same validation logic directly, without
/// needing a live Postgres connection or stdio pipe, to ensure the guard
/// never silently regresses.
#[cfg(test)]
mod contract_ids_startup_validation {
    #[test]
    fn rejects_g_strkey_account_address() {
        // A G… strkey is a Stellar account address, not a Soroban contract.
        // A G-strkey accidentally placed in CONTRACT_IDS must be caught here.
        let raw = "GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3BEJD4";
        let err = lumenqraph_core::parse_contract_ids(raw).unwrap_err();
        assert!(
            err.contains("invalid CONTRACT_ID"),
            "error should mention invalid CONTRACT_ID: {err}"
        );
        assert!(
            err.contains("GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3BEJD4"),
            "error should quote the bad id: {err}"
        );
    }

    #[test]
    fn rejects_garbage_string() {
        let raw = "not-a-contract-id";
        let err = lumenqraph_core::parse_contract_ids(raw).unwrap_err();
        assert!(
            err.contains("invalid CONTRACT_ID"),
            "garbage string should be rejected: {err}"
        );
    }

    #[test]
    fn rejects_too_many_contract_ids() {
        // getEvents supports at most 25 IDs; the parser enforces this.
        let single = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC";
        let raw = std::iter::repeat(single).take(26).collect::<Vec<_>>().join(",");
        let err = lumenqraph_core::parse_contract_ids(&raw).unwrap_err();
        assert!(
            err.contains("26"),
            "error should mention the count 26: {err}"
        );
    }

    #[test]
    fn accepts_empty_string() {
        // Empty CONTRACT_IDS means "index all" — must not be an error.
        let ids = lumenqraph_core::parse_contract_ids("").unwrap();
        assert!(ids.is_empty(), "empty string should yield zero IDs");
    }

    #[test]
    fn accepts_valid_c_strkey() {
        let raw = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC";
        let ids = lumenqraph_core::parse_contract_ids(raw).unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], raw);
    }

    #[test]
    fn mixed_valid_and_invalid_is_rejected() {
        let raw = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC,GAIH3ULLFQ4DGSECF2AR555KZ4KNDGEKN4AFI4SU2M7B43MGK3BEJD4";
        let err = lumenqraph_core::parse_contract_ids(raw).unwrap_err();
        assert!(
            err.contains("invalid CONTRACT_ID"),
            "a G-strkey mixed with a valid C-strkey should be rejected: {err}"
        );
    }
}

/// Tests for the HTTP transport security hardening from #622:
///   - Origin header validation (DNS-rebinding protection)
///   - normalize_bind() defaulting bare ports to 127.0.0.1
///   - Mcp-Session-Id issuance and validation
#[cfg(test)]
mod http_security_tests {
    use super::*;

    #[test]
    fn http_auth_requires_token_unless_anonymous_is_explicitly_allowed() {
        assert!(validate_http_auth(None, false).is_err());
        assert!(validate_http_auth(Some("  "), false).is_err());
        assert!(validate_http_auth(None, true).is_ok());
        assert!(validate_http_auth(Some("secret"), false).is_ok());
    }

    // ── Origin header validation ──────────────────────────────────────────

    #[test]
    fn no_origin_header_is_allowed_regardless_of_allowlist() {
        let config = HttpConfig::for_testing(&[], false);
        assert!(
            config.validate_origin(None).is_ok(),
            "requests with no Origin (CLI/curl) must always be allowed"
        );
    }

    #[test]
    fn empty_origin_is_allowed() {
        let config = HttpConfig::for_testing(&[], false);
        assert!(
            config.validate_origin(Some("")).is_ok(),
            "empty Origin string must not be rejected"
        );
    }

    #[test]
    fn origin_in_allowlist_is_accepted() {
        let config = HttpConfig::for_testing(&["https://app.example.com"], false);
        assert!(
            config
                .validate_origin(Some("https://app.example.com"))
                .is_ok(),
            "an Origin that is exactly in the allowlist must be accepted"
        );
    }

    #[test]
    fn foreign_origin_is_rejected_when_allowlist_is_empty() {
        let config = HttpConfig::for_testing(&[], false);
        let result = config.validate_origin(Some("https://evil.example.com"));
        assert!(
            result.is_err(),
            "a foreign Origin must be rejected when allowlist is empty (DNS-rebinding protection)"
        );
    }

    #[test]
    fn foreign_origin_is_rejected_even_when_allowlist_has_other_entries() {
        let config = HttpConfig::for_testing(&["https://safe.example.com"], false);
        let result = config.validate_origin(Some("https://evil.example.com"));
        assert!(
            result.is_err(),
            "a foreign Origin not in the allowlist must be rejected"
        );
    }

    #[test]
    fn allow_any_origin_bypasses_validation() {
        let config = HttpConfig::for_testing(&[], true);
        assert!(
            config
                .validate_origin(Some("https://anything.attacker.example"))
                .is_ok(),
            "MCP_ALLOW_ANY_ORIGIN=true must bypass Origin checks"
        );
    }

    #[test]
    fn origin_matching_is_exact_not_substring() {
        // Ensure "https://safe.example.com.evil.example.com" doesn't sneak past
        // by matching on a substring.
        let config =
            HttpConfig::for_testing(&["https://safe.example.com"], false);
        let result = config.validate_origin(Some(
            "https://safe.example.com.evil.example.com",
        ));
        assert!(
            result.is_err(),
            "Origin matching must be exact, not a substring match"
        );
    }

    // ── normalize_bind ────────────────────────────────────────────────────

    #[test]
    fn bare_colon_port_defaults_to_localhost() {
        assert_eq!(normalize_bind(":8090"), "127.0.0.1:8090");
    }

    #[test]
    fn bare_port_number_defaults_to_localhost() {
        assert_eq!(normalize_bind("8090"), "127.0.0.1:8090");
    }

    #[test]
    fn explicit_0_0_0_0_is_preserved() {
        assert_eq!(normalize_bind("0.0.0.0:8090"), "0.0.0.0:8090");
    }

    #[test]
    fn explicit_127_0_0_1_is_preserved() {
        assert_eq!(normalize_bind("127.0.0.1:8090"), "127.0.0.1:8090");
    }

    // ── session id management ─────────────────────────────────────────────

    async fn http_state_with_sessions() -> State {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://test:test@localhost/test")
            .expect("connect_lazy");
        State {
            pool,
            rpc: RpcClient::new("http://127.0.0.1:0", 30),
            rpc_limiter: rpc::RpcRateLimiter::new(60),
            auth_token: None,
            sessions: Some(Arc::new(Mutex::new(HashSet::new()))),
        }
    }

    #[tokio::test]
    async fn issue_session_returns_unique_ids() {
        let state = http_state_with_sessions().await;
        let id1 = state.issue_session().expect("should issue a session id");
        let id2 = state.issue_session().expect("should issue a session id");
        assert_ne!(id1, id2, "each session id must be unique");
    }

    #[tokio::test]
    async fn issued_session_is_valid() {
        let state = http_state_with_sessions().await;
        let id = state.issue_session().expect("should issue a session id");
        assert!(
            state.session_valid(&id),
            "a freshly issued session id must be recognised as valid"
        );
    }

    #[tokio::test]
    async fn unknown_session_is_invalid() {
        let state = http_state_with_sessions().await;
        assert!(
            !state.session_valid("not-a-real-session-id"),
            "an unknown session id must not be accepted"
        );
    }

    #[tokio::test]
    async fn deleted_session_is_invalid() {
        let state = http_state_with_sessions().await;
        let id = state.issue_session().expect("should issue a session id");
        let dropped = state
            .sessions
            .as_ref()
            .unwrap()
            .lock()
            .expect("poisoned")
            .remove(&id);
        assert!(dropped, "session id should have been in the set");
        assert!(
            !state.session_valid(&id),
            "a deleted session id must not be accepted"
        );
    }

    #[tokio::test]
    async fn stdio_state_session_valid_always_true() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://test:test@localhost/test")
            .expect("connect_lazy");
        // sessions: None is the stdio mode; session_valid must not reject.
        let state = State {
            pool,
            rpc: RpcClient::new("http://127.0.0.1:0", 30),
            rpc_limiter: rpc::RpcRateLimiter::new(60),
            auth_token: None,
            sessions: None,
        };
        assert!(
            state.session_valid("any-id-at-all"),
            "stdio state must not enforce session ids"
        );
    }
}

/// End-to-end tests for the Streamable HTTP transport (#622): spin up the
/// actual axum `Router` and drive it with `tower::ServiceExt::oneshot` to
/// verify the full request/response cycle, including middleware.
#[cfg(test)]
mod http_e2e_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    /// Build the same `Router` used by `serve_http`, wrapped with the Origin
    /// guard middleware, for in-process testing.
    fn test_router(http_config: HttpConfig) -> (Router, State) {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://test:test@localhost/test")
            .expect("connect_lazy");
        let state = State {
            pool,
            rpc: RpcClient::new("http://127.0.0.1:0", 30),
            rpc_limiter: rpc::RpcRateLimiter::new(60),
            auth_token: None,
            sessions: Some(Arc::new(Mutex::new(HashSet::new()))),
        };
        let app = Router::new()
            .route(
                "/mcp",
                post(handle_http_request)
                    .delete(handle_delete_mcp)
                    .get(handle_get_mcp),
            )
            .route("/healthz", get(|| async { "ok" }))
            .with_state(state.clone())
            .layer(axum::middleware::from_fn_with_state(
                http_config,
                origin_guard,
            ));
        (app, state)
    }

    async fn body_text(resp: axum::response::Response) -> String {
        let bytes = resp
            .into_body()
            .collect()
            .await
            .expect("collect body")
            .to_bytes();
        String::from_utf8(bytes.to_vec()).unwrap_or_default()
    }

    fn json_rpc(request: &str) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("content-type", "application/json")
            .body(Body::from(request.to_string()))
            .expect("build request")
    }

    // ── GET /mcp ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn get_mcp_returns_405_method_not_allowed() {
        // The spec says the server MUST either open an SSE stream OR return
        // 405. We do not implement server-initiated streaming → 405.
        let (app, _state) = test_router(HttpConfig::for_testing(&[], false));
        let resp = app
            .oneshot(Request::builder().method("GET").uri("/mcp").body(Body::empty()).unwrap())
            .await
            .expect("router");
        assert_eq!(
            resp.status(),
            StatusCode::METHOD_NOT_ALLOWED,
            "GET /mcp must return 405 (we do not serve an SSE stream)"
        );
        // The Allow header should advertise what IS supported.
        let allow = resp
            .headers()
            .get("allow")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        assert!(
            allow.contains("POST"),
            "Allow header should advertise POST: {allow}"
        );
    }

    // ── Origin validation ─────────────────────────────────────────────────

    #[tokio::test]
    async fn foreign_origin_is_rejected_with_403_before_handler_runs() {
        // A browser carrying an Origin not in the allowlist must be rejected
        // with 403 before the request reaches the JSON-RPC handler.
        let (app, _state) = test_router(HttpConfig::for_testing(&[], false));
        let req = Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("origin", "https://evil.example.com")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#,
            ))
            .unwrap();
        let resp = app.oneshot(req).await.expect("router");
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "a foreign Origin must be rejected with 403"
        );
        // The body should be a JSON-RPC error with no id (per spec).
        let body = body_text(resp).await;
        assert!(
            body.contains("Origin not allowed") || body.contains("Forbidden"),
            "403 body should explain the rejection: {body}"
        );
    }

    #[tokio::test]
    async fn allowed_origin_is_accepted() {
        let (app, _state) =
            test_router(HttpConfig::for_testing(&["https://app.example.com"], false));
        let req = Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("origin", "https://app.example.com")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#,
            ))
            .unwrap();
        let resp = app.oneshot(req).await.expect("router");
        assert_ne!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "an allowlisted Origin must NOT be rejected"
        );
    }

    #[tokio::test]
    async fn no_origin_header_is_allowed_for_cli_clients() {
        // curl / a CLI MCP client sends no Origin header; must pass through.
        let (app, _state) = test_router(HttpConfig::for_testing(&[], false));
        let req = json_rpc(r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#);
        let resp = app.oneshot(req).await.expect("router");
        assert_ne!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "requests without an Origin header must pass the origin guard"
        );
    }

    // ── Mcp-Session-Id ────────────────────────────────────────────────────

    #[tokio::test]
    async fn initialize_response_issues_mcp_session_id_header() {
        let (app, _state) = test_router(HttpConfig::for_testing(&[], false));
        let resp = app
            .oneshot(json_rpc(
                r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#,
            ))
            .await
            .expect("router");
        assert!(
            resp.status().is_success(),
            "initialize must succeed: {}",
            resp.status()
        );
        let session_id = resp
            .headers()
            .get("mcp-session-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        assert!(
            session_id.is_some(),
            "a successful initialize must issue an Mcp-Session-Id header"
        );
        assert_eq!(
            resp.headers()
                .get("mcp-protocol-version")
                .and_then(|v| v.to_str().ok()),
            Some(DEFAULT_PROTOCOL_VERSION),
            "initialize must echo the negotiated protocol version in a header"
        );
        let id = session_id.unwrap();
        // Must be visible ASCII (0x21–0x7E) per spec; our UUIDs satisfy this.
        assert!(
            id.chars().all(|c| (0x21u8..=0x7Eu8).contains(&(c as u8))),
            "Mcp-Session-Id must be visible ASCII only: {id:?}"
        );
        assert!(!id.is_empty(), "session id must not be empty");
    }

    #[tokio::test]
    async fn post_initialize_request_without_session_id_is_400() {
        let (app, _state) = test_router(HttpConfig::for_testing(&[], false));
        // Skip initialize; go straight to ping. The server tracks sessions
        // (it's an HTTP state), so a missing Mcp-Session-Id must yield 400.
        let resp = app
            .oneshot(json_rpc(r#"{"jsonrpc":"2.0","id":2,"method":"ping"}"#))
            .await
            .expect("router");
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "post-initialize requests without an Mcp-Session-Id must return 400"
        );
    }

    #[tokio::test]
    async fn post_initialize_request_with_unknown_session_id_is_404() {
        let (app, _state) = test_router(HttpConfig::for_testing(&[], false));
        let req = Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("content-type", "application/json")
            .header("mcp-session-id", "never-issued-by-this-server")
            .body(Body::from(r#"{"jsonrpc":"2.0","id":2,"method":"ping"}"#))
            .unwrap();
        let resp = app.oneshot(req).await.expect("router");
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "an unknown/expired Mcp-Session-Id must return 404"
        );
    }

    #[tokio::test]
    async fn full_initialize_then_ping_round_trip_with_session_id() {
        let (app, state) = test_router(HttpConfig::for_testing(&[], false));

        // 1. initialize → captures session id from response header.
        let init_resp = app
            .clone()
            .oneshot(json_rpc(
                r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#,
            ))
            .await
            .expect("router");
        let session_id = init_resp
            .headers()
            .get("mcp-session-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .expect("initialize must issue a session id");
        // The session id must be registered in the server's session set.
        assert!(
            state.session_valid(&session_id),
            "issued session id must be valid in the server state"
        );

        // 2. ping with the session id must succeed (200 with a result).
        let ping_req = Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("content-type", "application/json")
            .header("mcp-session-id", &session_id)
            .body(Body::from(r#"{"jsonrpc":"2.0","id":2,"method":"ping"}"#))
            .unwrap();
        let ping_resp = app.oneshot(ping_req).await.expect("router");
        assert!(
            ping_resp.status().is_success(),
            "ping with a valid session id must succeed: {}",
            ping_resp.status()
        );
        let body = body_text(ping_resp).await;
        assert!(
            body.contains(r#""id":2"#),
            "ping response should echo id 2: {body}"
        );
    }

    // ── DELETE /mcp ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn delete_without_session_id_header_is_400() {
        let (app, _state) = test_router(HttpConfig::for_testing(&[], false));
        let req = Request::builder()
            .method("DELETE")
            .uri("/mcp")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.expect("router");
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "DELETE without Mcp-Session-Id must return 400"
        );
    }

    #[tokio::test]
    async fn delete_unknown_session_id_is_404() {
        let (app, _state) = test_router(HttpConfig::for_testing(&[], false));
        let req = Request::builder()
            .method("DELETE")
            .uri("/mcp")
            .header("mcp-session-id", "never-issued")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.expect("router");
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "DELETE of an unknown session id must return 404"
        );
    }

    #[tokio::test]
    async fn delete_valid_session_id_is_200_and_invalidates_it() {
        let (app, state) = test_router(HttpConfig::for_testing(&[], false));
        // Issue a real session id by calling issue_session directly.
        let id = state.issue_session().expect("issue session");
        let req = Request::builder()
            .method("DELETE")
            .uri("/mcp")
            .header("mcp-session-id", &id)
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.expect("router");
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "DELETE of a valid session id must return 200"
        );
        assert!(
            !state.session_valid(&id),
            "after DELETE, the session id must no longer be valid"
        );
    }

    // ── MCP-Protocol-Version header ───────────────────────────────────────

    #[tokio::test]
    async fn unsupported_protocol_version_header_post_initialize_is_400() {
        let (app, _state) = test_router(HttpConfig::for_testing(&[], false));
        // First issue a session via initialize (no version check on init).
        let init_resp = app
            .clone()
            .oneshot(json_rpc(
                r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#,
            ))
            .await
            .expect("router");
        let session_id = init_resp
            .headers()
            .get("mcp-session-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .expect("session id");

        // Now send a ping with a bogus protocol version → 400.
        let req = Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("content-type", "application/json")
            .header("mcp-session-id", &session_id)
            .header("mcp-protocol-version", "2099-99-99")
            .body(Body::from(r#"{"jsonrpc":"2.0","id":2,"method":"ping"}"#))
            .unwrap();
        let resp = app.oneshot(req).await.expect("router");
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "an unsupported MCP-Protocol-Version must return 400"
        );
    }
}
