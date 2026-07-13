//! Lumenqraph MCP — a [Model Context Protocol](https://modelcontextprotocol.io)
//! server that gives any AI agent (Claude Desktop/Code, or any MCP client)
//! typed, self-describing access to Soroban contracts.
//!
//! It reuses the same Postgres the API reads and the same read-layer encoder the
//! API calls, exposing five tools: `list_contracts`, `get_contract_interface`,
//! `get_contract_state`, `query_events`, and `call_contract`. Because the types
//! come from each contract's on-chain spec, an agent can *discover* what a
//! contract does and call it correctly — with zero hand-written schema.
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

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .context("failed to connect to Postgres")?;
    let state = State {
        pool,
        rpc: RpcClient::new(rpc_url),
    };

    info!("lumenqraph MCP server ready (stdio)");
    serve(state).await
}

/// The stdio JSON-RPC loop: read a message per line, dispatch, write responses.
async fn serve(state: State) -> anyhow::Result<()> {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();
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
        if let Some(response) = handle(&state, msg).await {
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
                "get_contract_state",
                "query_events",
                "call_contract"
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
}
