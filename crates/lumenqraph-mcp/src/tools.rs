//! The tool catalogue exposed to MCP clients. Each tool is backed by the same
//! Postgres the API reads and the same read-layer encoder the API calls, so an
//! agent gets typed, self-describing access to every indexed Soroban contract.

use lumenqraph_core::read;
use lumenqraph_core::{Contract, EventRow};
use serde_json::{json, Value};

use crate::rpc::SimOutcome;
use crate::State;

/// JSON-Schema tool definitions returned by `tools/list`.
pub fn definitions() -> Value {
    json!([
        {
            "name": "list_contracts",
            "description": "List Soroban contracts the indexer has seen events for, with per-contract event counts and ledger ranges.",
            "inputSchema": { "type": "object", "properties": {}, "additionalProperties": false }
        },
        {
            "name": "get_contract_interface",
            "description": "Get a contract's decoded on-chain interface (functions with typed inputs/outputs, event schemas, and user-defined types), parsed from its deployed WASM. Use this to discover what a contract can do before calling it.",
            "inputSchema": {
                "type": "object",
                "properties": { "contract_id": { "type": "string", "description": "Contract id (C...)" } },
                "required": ["contract_id"], "additionalProperties": false
            }
        },
        {
            "name": "query_events",
            "description": "Query recent indexed events for a contract, newest first. Each event includes decoded topics/value and, when available, a named+typed 'enriched' record.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "contract_id": { "type": "string", "description": "Contract id (C...)" },
                    "event_name": { "type": "string", "description": "Optional event name filter, e.g. 'transfer'" },
                    "limit": { "type": "integer", "description": "Max events (1-200, default 20)" }
                },
                "required": ["contract_id"], "additionalProperties": false
            }
        },
        {
            "name": "call_contract",
            "description": "Invoke a contract's view function READ-ONLY (via RPC simulation) and return a typed result. Arguments are type-checked against the contract's on-chain spec. Discover callable functions with get_contract_interface first.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "contract_id": { "type": "string", "description": "Contract id (C...)" },
                    "function": { "type": "string", "description": "Function name to invoke" },
                    "args": { "description": "Arguments as an object keyed by parameter name, or a positional array" }
                },
                "required": ["contract_id", "function"], "additionalProperties": false
            }
        }
    ])
}

/// Execute a tool call. Returns the JSON payload to hand back as text content.
/// `Err` is a tool-level error (surfaced to the agent as `isError: true`).
pub async fn call(state: &State, name: &str, args: &Value) -> anyhow::Result<Value> {
    match name {
        "list_contracts" => list_contracts(state).await,
        "get_contract_interface" => get_interface(state, str_arg(args, "contract_id")?).await,
        "query_events" => {
            query_events(
                state,
                str_arg(args, "contract_id")?,
                args.get("event_name").and_then(Value::as_str),
                args.get("limit").and_then(Value::as_i64),
            )
            .await
        }
        "call_contract" => {
            call_contract(
                state,
                str_arg(args, "contract_id")?,
                str_arg(args, "function")?,
                args.get("args").cloned().unwrap_or(Value::Null),
            )
            .await
        }
        other => anyhow::bail!("unknown tool {other:?}"),
    }
}

fn str_arg<'a>(args: &'a Value, key: &str) -> anyhow::Result<&'a str> {
    args.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("missing string argument {key:?}"))
}

async fn list_contracts(state: &State) -> anyhow::Result<Value> {
    let rows: Vec<Contract> = sqlx::query_as(
        "SELECT contract_id, count(*)::bigint AS event_count,
                min(ledger) AS first_seen_ledger, max(ledger) AS last_seen_ledger
         FROM events GROUP BY contract_id ORDER BY event_count DESC LIMIT 200",
    )
    .fetch_all(&state.pool)
    .await?;
    Ok(json!({ "contracts": rows }))
}

async fn get_interface(state: &State, contract_id: &str) -> anyhow::Result<Value> {
    let row: Option<(sqlx::types::Json<Value>, bool)> =
        sqlx::query_as("SELECT interface, has_events FROM contract_specs WHERE contract_id = $1")
            .bind(contract_id)
            .fetch_optional(&state.pool)
            .await?;
    match row {
        Some((interface, has_events)) => Ok(json!({
            "contract_id": contract_id, "has_events": has_events, "interface": interface.0,
        })),
        None => anyhow::bail!(
            "no interface indexed for {contract_id} yet (the indexer fetches it on first \
             sighting; Stellar Asset Contracts have no callable spec)"
        ),
    }
}

async fn query_events(
    state: &State,
    contract_id: &str,
    event_name: Option<&str>,
    limit: Option<i64>,
) -> anyhow::Result<Value> {
    let limit = limit.unwrap_or(20).clamp(1, 200);
    let events: Vec<EventRow> = sqlx::query_as(
        "SELECT event_id, contract_id, ledger, ledger_closed_at, event_type,
                topics, decoded_topics, event_name, value, decoded_value,
                enriched, tx_hash, in_successful_call, paging_token, created_at
         FROM events
         WHERE contract_id = $1 AND ($2::text IS NULL OR event_name = $2)
         ORDER BY ledger DESC, event_id DESC LIMIT $3",
    )
    .bind(contract_id)
    .bind(event_name)
    .bind(limit)
    .fetch_all(&state.pool)
    .await?;
    Ok(json!({ "contract_id": contract_id, "count": events.len(), "events": events }))
}

async fn call_contract(
    state: &State,
    contract_id: &str,
    function: &str,
    args: Value,
) -> anyhow::Result<Value> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT spec_section FROM contract_specs WHERE contract_id = $1")
            .bind(contract_id)
            .fetch_optional(&state.pool)
            .await?;
    let hex_section = row.map(|r| r.0).filter(|s| !s.is_empty()).ok_or_else(|| {
        anyhow::anyhow!("no interface indexed for {contract_id}; cannot type-check the call")
    })?;
    let section = hex::decode(&hex_section)?;

    let call = read::encode_call(&section, contract_id, function, &args, None)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    match state.rpc.simulate(&call.tx_xdr).await? {
        SimOutcome::Ok {
            result_xdr,
            latest_ledger,
        } => Ok(json!({
            "contract_id": contract_id,
            "function": function,
            "result": read::decode_result(&result_xdr, &call.output_type),
            "simulated_at_ledger": latest_ledger,
        })),
        SimOutcome::Error(msg) => anyhow::bail!("simulation failed: {msg}"),
    }
}
