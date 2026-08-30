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
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use rpc::RpcClient;

/// Latest MCP protocol revision we default to when a client sends none.
const DEFAULT_PROTOCOL_VERSION: &str = "2024-11-05";

#[derive(Clone)]
pub struct State {
    pub pool: PgPool,
    pub rpc: RpcClient,
    pub auth_token: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();
    // IMPORTANT: log to stderr — stdout is the JSON-RPC channel.
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer().with_writer(std::io::stderr))
        .init();

    let database_url = std::env::var("DATABASE_URL").context("missing DATABASE_URL")?;
    let rpc_url = std::env::var("RPC_URL")
        .unwrap_or_else(|_| "https://soroban-testnet.stellar.org".to_string());
    let rpc_timeout_secs: u64 = std::env::var("RPC_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(30);

    let auth_token = std::env::var("MCP_AUTH_TOKEN")
        .ok()
        .filter(|s| !s.trim().is_empty());

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .context("failed to connect to Postgres")?;
    let state = State {
        pool,
        rpc: RpcClient::new(rpc_url, rpc_timeout_secs),
        auth_token,
    };

    info!("lumenqraph MCP server ready (stdio)");
    serve(state).await
}

/// The stdio JSON-RPC loop: read a message per line, dispatch, write responses.
async fn serve(state: State) -> anyhow::Result<()> {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();
    let mut authenticated = state.auth_token.is_none();

    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let msg: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                let err = error_response(Value::Null, -32700, &format!("parse error: {e}"));
                write(&mut stdout, &err).await?;
                continue;
            }
        };
        if let Some((response, newly_authenticated)) = handle(&state, msg, authenticated).await {
            if newly_authenticated {
                authenticated = true;
            }
            write(&mut stdout, &response).await?;
        }
    }
    Ok(())
}

async fn write(stdout: &mut tokio::io::Stdout, value: &Value) -> anyhow::Result<()> {
    stdout.write_all(value.to_string().as_bytes()).await?;
    stdout.write_all(b"\n").await?;
    stdout.flush().await?;
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
    let protocol_version = msg
        .get("params")
        .and_then(|p| p.get("protocolVersion"))
        .and_then(Value::as_str)
        .unwrap_or(DEFAULT_PROTOCOL_VERSION);
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
    json!({
        "content": [ { "type": "text", "text": text } ],
        "isError": is_error,
    })
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
    fn initialize_echoes_client_protocol_version() {
        let req = json!({ "params": { "protocolVersion": "2025-06-18" } });
        assert_eq!(initialize_result(&req)["protocolVersion"], "2025-06-18");
        // Falls back when the client omits it.
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
                "each tool needs a schema"
            );
            assert!(!t["description"].as_str().unwrap().is_empty());
        }
    }

    #[test]
    fn tool_content_marks_errors() {
        let ok = tool_content(&json!({ "x": 1 }), false);
        assert_eq!(ok["isError"], false);
        assert_eq!(ok["content"][0]["type"], "text");
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
            auth_token: None,
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
            auth_token: None,
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
            auth_token: None,
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
            auth_token: Some("secret123".to_string()),
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
            auth_token: Some("secret123".to_string()),
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
            auth_token: None,
        };
        let msg = json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" });
        let (resp, _) = handle(&state, msg, true).await.unwrap();
        assert_eq!(resp["id"], 2);
        let tools = resp["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 13, "all thirteen tools must be declared");
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
            auth_token: None,
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
            auth_token: None,
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
            auth_token: None,
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
