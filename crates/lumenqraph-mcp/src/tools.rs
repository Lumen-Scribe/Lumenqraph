//! The tool catalogue exposed to MCP clients. Each tool is backed by the same
//! Postgres the API reads and the same read-layer encoder the API calls, so an
//! agent gets typed, self-describing access to every indexed Soroban contract.

use lumenqraph_core::{read, Contract, ContractSpec, EventRow};
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
            "name": "get_contract_upgrades",
            "description": "Get a contract's interface history: every version of its on-chain interface the indexer has observed, newest first, with a semantic diff against the previous version (functions/events/types added, removed, or changed) and whether that change was breaking. Soroban contracts are upgradable in place, so use this to answer 'has this contract changed?', 'what changed and when?', or 'is it safe to keep calling it?'.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "contract_id": { "type": "string", "description": "Contract id (C...)" },
                    "limit": { "type": "integer", "description": "How many versions, newest first (1-200, default 20)" }
                },
                "required": ["contract_id"], "additionalProperties": false
            }
        },
        {
            "name": "get_contract_state",
            "description": "Get a contract's current on-chain state (its decoded instance storage: admin, config, counters, …), and optionally recent historical versions. Requires the indexer's state indexing to be enabled.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "contract_id": { "type": "string", "description": "Contract id (C...)" },
                    "limit": { "type": "integer", "description": "How many versions, newest first (1-200, default 1 = current state)" }
                },
                "required": ["contract_id"], "additionalProperties": false
            }
        },
        {
            "name": "get_contract_data",
            "description": "Get a contract's per-key state: the current value of individual storage entries such as token holder balances (Balance(Address)), discovered from the contract's events. Requires the indexer's key indexing to be enabled.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "contract_id": { "type": "string", "description": "Contract id (C...)" },
                    "label": { "type": "string", "description": "Optional label filter, e.g. 'balance'" },
                    "limit": { "type": "integer", "description": "Max keys, latest value of each (1-1000, default 100)" }
                },
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
        },
        {
            "name": "simulate_call",
            "description": "Dry-run ANY contract call (including state-changing ones like transfer/deposit) WITHOUT submitting it, and preview the typed result, the events it would emit, and its resource cost. Nothing is signed or broadcast. Use this to answer 'what would happen if I called X?'.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "contract_id": { "type": "string", "description": "Contract id (C...)" },
                    "function": { "type": "string", "description": "Function name to simulate" },
                    "args": { "description": "Arguments as an object keyed by parameter name, or a positional array" },
                    "source_account": { "type": "string", "description": "Optional G... source account for the simulation" }
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
        "get_contract_upgrades" => {
            get_upgrades(
                state,
                str_arg(args, "contract_id")?,
                args.get("limit").and_then(Value::as_i64),
            )
            .await
        }
        "get_contract_state" => {
            get_state(
                state,
                str_arg(args, "contract_id")?,
                args.get("limit").and_then(Value::as_i64),
            )
            .await
        }
        "get_contract_data" => {
            get_data(
                state,
                str_arg(args, "contract_id")?,
                args.get("label").and_then(Value::as_str),
                args.get("limit").and_then(Value::as_i64),
            )
            .await
        }
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
                None,
                false,
            )
            .await
        }
        "simulate_call" => {
            call_contract(
                state,
                str_arg(args, "contract_id")?,
                str_arg(args, "function")?,
                args.get("args").cloned().unwrap_or(Value::Null),
                args.get("source_account").and_then(Value::as_str),
                true,
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

async fn get_upgrades(
    state: &State,
    contract_id: &str,
    limit: Option<i64>,
) -> anyhow::Result<Value> {
    let limit = limit.unwrap_or(20).clamp(1, 200);
    // (version, wasm_hash, previous_wasm_hash, diff, breaking, observed_at)
    type VersionRow = (
        i32,
        String,
        Option<String>,
        Option<sqlx::types::Json<Value>>,
        bool,
        chrono::DateTime<chrono::Utc>,
    );
    let rows: Vec<VersionRow> = sqlx::query_as(
        "SELECT version, wasm_hash, previous_wasm_hash, diff, breaking, observed_at
         FROM contract_spec_versions WHERE contract_id = $1
         ORDER BY version DESC LIMIT $2",
    )
    .bind(contract_id)
    .bind(limit)
    .fetch_all(&state.pool)
    .await?;
    if rows.is_empty() {
        anyhow::bail!(
            "no interface history for {contract_id} yet (the indexer records a version on first \
             sighting; Stellar Asset Contracts have no spec to track)"
        );
    }
    let versions: Vec<Value> = rows
        .into_iter()
        .map(
            |(version, wasm_hash, previous_wasm_hash, diff, breaking, observed_at)| {
                json!({
                    "version": version,
                    "wasm_hash": wasm_hash,
                    "previous_wasm_hash": previous_wasm_hash,
                    "diff": diff.map(|d| d.0),
                    "breaking": breaking,
                    "observed_at": observed_at,
                })
            },
        )
        .collect();
    // Version 1 is a baseline, not an upgrade, so a lone version 1 means "seen
    // once, never changed" — spell that out so an agent doesn't read the bare
    // baseline as a change. Counted in SQL rather than from `versions`, which
    // `limit` may have truncated.
    let upgrades: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM contract_spec_versions WHERE contract_id = $1 AND version > 1",
    )
    .bind(contract_id)
    .fetch_one(&state.pool)
    .await?;
    Ok(json!({
        "contract_id": contract_id,
        "upgrades_observed": upgrades,
        "versions": versions,
    }))
}

async fn get_state(state: &State, contract_id: &str, limit: Option<i64>) -> anyhow::Result<Value> {
    let limit = limit.unwrap_or(1).clamp(1, 200);
    let rows: Vec<(i64, sqlx::types::Json<Value>, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
        "SELECT ledger, storage, captured_at
         FROM contract_state WHERE contract_id = $1
         ORDER BY ledger DESC LIMIT $2",
    )
    .bind(contract_id)
    .bind(limit)
    .fetch_all(&state.pool)
    .await?;
    if rows.is_empty() {
        anyhow::bail!(
            "no state snapshots for {contract_id} (state indexing may be disabled on the indexer)"
        );
    }
    let versions: Vec<Value> = rows
        .into_iter()
        .map(|(ledger, storage, captured_at)| {
            json!({ "ledger": ledger, "storage": storage.0, "captured_at": captured_at })
        })
        .collect();
    Ok(json!({ "contract_id": contract_id, "count": versions.len(), "versions": versions }))
}

async fn get_data(
    state: &State,
    contract_id: &str,
    label: Option<&str>,
    limit: Option<i64>,
) -> anyhow::Result<Value> {
    let limit = limit.unwrap_or(100).clamp(1, 1000);
    // (key_hash, key, durability, ledger, value, label)
    type DataRow = (
        String,
        sqlx::types::Json<Value>,
        String,
        i64,
        sqlx::types::Json<Value>,
        Option<String>,
    );
    let rows: Vec<DataRow> = sqlx::query_as(
        "SELECT key_hash, key, durability, ledger, value, label FROM (
             SELECT DISTINCT ON (key_hash)
                    key_hash, key, durability, ledger, value, label
             FROM contract_data
             WHERE contract_id = $1 AND ($2::text IS NULL OR label = $2)
             ORDER BY key_hash, ledger DESC
         ) latest
         ORDER BY ledger DESC LIMIT $3",
    )
    .bind(contract_id)
    .bind(label)
    .bind(limit)
    .fetch_all(&state.pool)
    .await?;
    if rows.is_empty() {
        anyhow::bail!(
            "no per-key data snapshots for {contract_id} (key indexing may be disabled on the indexer)"
        );
    }
    let keys: Vec<Value> = rows
        .into_iter()
        .map(|(key_hash, key, durability, ledger, value, label)| {
            json!({
                "key_hash": key_hash, "key": key.0, "durability": durability,
                "ledger": ledger, "value": value.0, "label": label,
            })
        })
        .collect();
    Ok(json!({ "contract_id": contract_id, "count": keys.len(), "keys": keys }))
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

/// Backs both `call_contract` (view read) and `simulate_call` (full preview).
/// When `preview` is set, the emitted events and resource cost are included.
async fn call_contract(
    state: &State,
    contract_id: &str,
    function: &str,
    args: Value,
    source_account: Option<&str>,
    preview: bool,
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

    let call = read::encode_call(&section, contract_id, function, &args, source_account)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    match state.rpc.simulate(&call.tx_xdr).await? {
        SimOutcome::Ok {
            result_xdr,
            events,
            min_resource_fee,
            latest_ledger,
        } => {
            let mut out = json!({
                "contract_id": contract_id,
                "function": function,
                "result": read::decode_result(&result_xdr, &call.output_type),
                "simulated_at_ledger": latest_ledger,
            });
            if preview {
                let spec = ContractSpec::from_spec_xdr(&section);
                out["events"] = json!(read::decode_events(&events, contract_id, spec.as_ref()));
                out["min_resource_fee"] = json!(min_resource_fee);
            }
            Ok(out)
        }
        SimOutcome::Error(msg) => anyhow::bail!("simulation failed: {msg}"),
    }
}
