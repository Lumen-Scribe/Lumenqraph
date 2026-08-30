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
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use rpc::RpcClient;

/// Latest MCP protocol revision we default to when a client sends none.
const DEFAULT_PROTOCOL_VERSION: &str = "2024-11-05";

#[derive(Clone)]
pub struct State {
    pub pool: PgPool,
    pub rpc: RpcClient,
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

    // Validate CONTRACT_IDS at startup so a misconfigured address is caught
    // immediately rather than silently ignored.
    lumenqraph_core::parse_contract_ids(
        &std::env::var("CONTRACT_IDS").unwrap_or_default(),
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .context("failed to connect to Postgres")?;
    let state = State {
        pool,
        rpc: RpcClient::new(rpc_url, rpc_timeout_secs),
    };

    info!("lumenqraph MCP server ready (stdio)");
    serve(state).await
}

/// The stdio JSON-RPC loop: read a message per line, dispatch, write responses.
async fn serve(state: State) -> anyhow::Result<()> {
    serve_io(state, tokio::io::stdin(), tokio::io::stdout()).await
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
        if let Some(response) = handle(&state, msg).await {
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

/// Dispatch one JSON-RPC message. Returns `None` for notifications (no id).
async fn handle(state: &State, msg: Value) -> Option<Value> {
    let id = msg.get("id").cloned();
    let method = msg
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let is_request = id.is_some();

    match method {
        "initialize" => Some(result_response(id, initialize_result(&msg))),
        "ping" => Some(result_response(id, json!({}))),
        "tools/list" => Some(result_response(
            id,
            json!({ "tools": tools::definitions() }),
        )),
        "tools/call" => Some(handle_tools_call(state, id, &msg).await),
        // Notifications (initialized, cancelled, …) get no response.
        _ if method.starts_with("notifications/") => None,
        _ if is_request => Some(error_response(
            id.unwrap_or(Value::Null),
            -32601,
            &format!("method not found: {method}"),
        )),
        _ => None,
    }
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
        };
        let resp = handle(&state, msg).await;
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
        };
        let msg = json!({ "jsonrpc": "2.0", "id": 1, "method": "unknown/method" });
        let resp = handle(&state, msg).await.unwrap();
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
        };
        let msg = json!({ "jsonrpc": "2.0", "id": 42, "method": "ping" });
        let resp = handle(&state, msg).await.unwrap();
        assert_eq!(resp["id"], 42);
        assert!(resp["result"].is_object());
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
        };
        let msg = json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" });
        let resp = handle(&state, msg).await.unwrap();
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
        };
        let msg = json!({
            "jsonrpc": "2.0", "id": 1,
            "method": "tools/call",
            "params": { "name": tool_name, "arguments": args }
        });
        let resp = handle(&state, msg).await.unwrap();
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
        };
        let msg = json!({
            "jsonrpc": "2.0", "id": 5,
            "method": "tools/call",
            "params": { "name": "no_such_tool", "arguments": {} }
        });
        let resp = handle(&state, msg).await.unwrap();
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
