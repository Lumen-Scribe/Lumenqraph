//! The tool catalogue exposed to MCP clients. Each tool is backed by the same
//! Postgres the API reads and the same read-layer encoder the API calls, so an
//! agent gets typed, self-describing access to every indexed Soroban contract.

use lumenqraph_core::{read, AmmSwap, Contract, ContractSpec, EventRow, LiquidityEvent, NftEvent, SpecDiff, TokenTransfer};
use serde_json::{json, Value};
use std::sync::Arc;

use crate::rpc::SimOutcome;
use crate::State;

/// Cached spec for reuse across diff calculations.
struct CachedSpec {
    parsed: Option<Arc<ContractSpec>>,
}

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
        },
        {
            "name": "query_transfers",
            "description": "Query materialized token transfers for a contract, newest first. Each transfer includes from/to addresses and amount. Optionally filter by sender or recipient address.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "contract_id": { "type": "string", "description": "Contract id (C...)" },
                    "from": { "type": "string", "description": "Optional sender address filter" },
                    "to": { "type": "string", "description": "Optional recipient address filter" },
                    "limit": { "type": "integer", "description": "Max transfers (1-200, default 20)" }
                },
                "required": ["contract_id"], "additionalProperties": false
            }
        },
        {
            "name": "diff_contract_interface",
            "description": "Compute what changed between any two versions of a contract's interface. Useful for understanding breaking changes between non-consecutive versions without assembling a chain of diffs.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "contract_id": { "type": "string", "description": "Contract id (C...)" },
                    "from": { "type": "integer", "description": "Starting version" },
                    "to": { "type": "integer", "description": "Ending version" }
                },
                "required": ["contract_id", "from", "to"], "additionalProperties": false
            }
        },
        {
            "name": "query_swaps",
            "description": "Query materialized AMM swap events for a contract, newest first. Each record includes sender, sell/buy token addresses, and sell/buy amounts. Optionally filter by sender, sell token, or buy token.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "contract_id": { "type": "string", "description": "Contract id (C...)" },
                    "sender": { "type": "string", "description": "Optional sender address filter" },
                    "sell_token": { "type": "string", "description": "Optional sell token address filter" },
                    "buy_token": { "type": "string", "description": "Optional buy token address filter" },
                    "limit": { "type": "integer", "description": "Max swaps (1-200, default 20)" }
                },
                "required": ["contract_id"], "additionalProperties": false
            }
        },
        {
            "name": "query_nft_events",
            "description": "Query materialized NFT events (mint/transfer/burn) for a contract, newest first. Optionally filter by event kind, from/to address, or token id.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "contract_id": { "type": "string", "description": "Contract id (C...)" },
                    "kind": { "type": "string", "description": "Optional kind filter: mint | transfer | burn" },
                    "from": { "type": "string", "description": "Optional sender address filter" },
                    "to": { "type": "string", "description": "Optional recipient address filter" },
                    "token_id": { "type": "string", "description": "Optional token id filter" },
                    "limit": { "type": "integer", "description": "Max events (1-200, default 20)" }
                },
                "required": ["contract_id"], "additionalProperties": false
            }
        },
        {
            "name": "query_liquidity_events",
            "description": "Query materialized liquidity events (add/remove) for a contract, newest first. Optionally filter by event kind or provider address.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "contract_id": { "type": "string", "description": "Contract id (C...)" },
                    "kind": { "type": "string", "description": "Optional kind filter: add | remove" },
                    "provider": { "type": "string", "description": "Optional liquidity provider address filter" },
                    "limit": { "type": "integer", "description": "Max events (1-200, default 20)" }
                },
                "required": ["contract_id"], "additionalProperties": false
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
        "query_transfers" => {
            query_transfers(
                state,
                str_arg(args, "contract_id")?,
                args.get("from").and_then(Value::as_str),
                args.get("to").and_then(Value::as_str),
                args.get("limit").and_then(Value::as_i64),
            )
            .await
        }
        "diff_contract_interface" => {
            diff_contract_interface(
                state,
                str_arg(args, "contract_id")?,
                args.get("from").and_then(Value::as_i64),
                args.get("to").and_then(Value::as_i64),
            )
            .await
        }
        "query_swaps" => {
            query_swaps(
                state,
                str_arg(args, "contract_id")?,
                args.get("sender").and_then(Value::as_str),
                args.get("sell_token").and_then(Value::as_str),
                args.get("buy_token").and_then(Value::as_str),
                args.get("limit").and_then(Value::as_i64),
            )
            .await
        }
        "query_nft_events" => {
            query_nft_events(
                state,
                str_arg(args, "contract_id")?,
                args.get("kind").and_then(Value::as_str),
                args.get("from").and_then(Value::as_str),
                args.get("to").and_then(Value::as_str),
                args.get("token_id").and_then(Value::as_str),
                args.get("limit").and_then(Value::as_i64),
            )
            .await
        }
        "query_liquidity_events" => {
            query_liquidity_events(
                state,
                str_arg(args, "contract_id")?,
                args.get("kind").and_then(Value::as_str),
                args.get("provider").and_then(Value::as_str),
                args.get("limit").and_then(Value::as_i64),
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
        "SELECT contract_id, event_count, first_seen_ledger, last_seen_ledger
         FROM contract_summaries
         WHERE event_count > 0
         ORDER BY event_count DESC
         LIMIT 200",
    )
    .fetch_all(&state.pool)
    .await?;
    Ok(json!({ "contracts": rows }))
}

fn validate_contract_id(contract_id: &str) -> anyhow::Result<()> {
    if !lumenqraph_core::is_valid_contract_id(contract_id) {
        anyhow::bail!("invalid contract id {contract_id:?}: expected a C… strkey");
    }
    Ok(())
}

async fn get_interface(state: &State, contract_id: &str) -> anyhow::Result<Value> {
    validate_contract_id(contract_id)?;
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
    validate_contract_id(contract_id)?;
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
    validate_contract_id(contract_id)?;
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
    validate_contract_id(contract_id)?;
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
    validate_contract_id(contract_id)?;
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

async fn query_transfers(
    state: &State,
    contract_id: &str,
    from: Option<&str>,
    to: Option<&str>,
    limit: Option<i64>,
) -> anyhow::Result<Value> {
    validate_contract_id(contract_id)?;
    let limit = limit.unwrap_or(20).clamp(1, 200);
    let transfers: Vec<TokenTransfer> = sqlx::query_as(
        "SELECT event_id, contract_id, from_addr, to_addr, amount, kind, ledger, ledger_closed_at
         FROM token_transfers
         WHERE contract_id = $1
           AND ($2::text IS NULL OR from_addr = $2)
           AND ($3::text IS NULL OR to_addr = $3)
         ORDER BY ledger DESC, event_id DESC
         LIMIT $4",
    )
    .bind(contract_id)
    .bind(from)
    .bind(to)
    .bind(limit)
    .fetch_all(&state.pool)
    .await?;
    Ok(json!({
        "contract_id": contract_id,
        "count": transfers.len(),
        "transfers": transfers,
    }))
}

async fn query_swaps(
    state: &State,
    contract_id: &str,
    sender: Option<&str>,
    sell_token: Option<&str>,
    buy_token: Option<&str>,
    limit: Option<i64>,
) -> anyhow::Result<Value> {
    validate_contract_id(contract_id)?;
    let limit = limit.unwrap_or(20).clamp(1, 200);
    let swaps: Vec<lumenqraph_core::AmmSwap> = sqlx::query_as(
        "SELECT event_id, contract_id, sender, sell_token, buy_token,
                sell_amount, buy_amount, raw_event_name, ledger, ledger_closed_at
         FROM amm_swaps
         WHERE contract_id = $1
           AND ($2::text IS NULL OR sender = $2)
           AND ($3::text IS NULL OR sell_token = $3)
           AND ($4::text IS NULL OR buy_token = $4)
         ORDER BY ledger DESC, event_id DESC
         LIMIT $5",
    )
    .bind(contract_id)
    .bind(sender)
    .bind(sell_token)
    .bind(buy_token)
    .bind(limit)
    .fetch_all(&state.pool)
    .await?;
    Ok(json!({
        "contract_id": contract_id,
        "count": swaps.len(),
        "swaps": swaps,
    }))
}

async fn query_nft_events(
    state: &State,
    contract_id: &str,
    kind: Option<&str>,
    from: Option<&str>,
    to: Option<&str>,
    token_id: Option<&str>,
    limit: Option<i64>,
) -> anyhow::Result<Value> {
    validate_contract_id(contract_id)?;
    if let Some(k) = kind {
        if !matches!(k, "mint" | "transfer" | "burn") {
            anyhow::bail!("kind must be one of: mint, transfer, burn");
        }
    }
    let limit = limit.unwrap_or(20).clamp(1, 200);
    let events: Vec<lumenqraph_core::NftEvent> = sqlx::query_as(
        "SELECT event_id, contract_id, event_kind, from_addr, to_addr,
                token_id, ledger, ledger_closed_at
         FROM nft_events
         WHERE contract_id = $1
           AND ($2::text IS NULL OR event_kind = $2)
           AND ($3::text IS NULL OR from_addr = $3)
           AND ($4::text IS NULL OR to_addr = $4)
           AND ($5::text IS NULL OR token_id = $5)
         ORDER BY ledger DESC, event_id DESC
         LIMIT $6",
    )
    .bind(contract_id)
    .bind(kind)
    .bind(from)
    .bind(to)
    .bind(token_id)
    .bind(limit)
    .fetch_all(&state.pool)
    .await?;
    Ok(json!({
        "contract_id": contract_id,
        "count": events.len(),
        "nft_events": events,
    }))
}

async fn query_liquidity_events(
    state: &State,
    contract_id: &str,
    kind: Option<&str>,
    provider: Option<&str>,
    limit: Option<i64>,
) -> anyhow::Result<Value> {
    validate_contract_id(contract_id)?;
    if let Some(k) = kind {
        if !matches!(k, "add" | "remove") {
            anyhow::bail!("kind must be one of: add, remove");
        }
    }
    let limit = limit.unwrap_or(20).clamp(1, 200);
    let events: Vec<lumenqraph_core::LiquidityEvent> = sqlx::query_as(
        "SELECT event_id, contract_id, event_kind, provider, amount_a, amount_b,
                shares, raw_event_name, extra_amounts, ledger, ledger_closed_at
         FROM liquidity_events
         WHERE contract_id = $1
           AND ($2::text IS NULL OR event_kind = $2)
           AND ($3::text IS NULL OR provider = $3)
         ORDER BY ledger DESC, event_id DESC
         LIMIT $4",
    )
    .bind(contract_id)
    .bind(kind)
    .bind(provider)
    .bind(limit)
    .fetch_all(&state.pool)
    .await?;
    Ok(json!({
        "contract_id": contract_id,
        "count": events.len(),
        "liquidity_events": events,
    }))
}

async fn diff_contract_interface(
    state: &State,
    contract_id: &str,
    from: Option<i64>,
    to: Option<i64>,
) -> anyhow::Result<Value> {
    validate_contract_id(contract_id)?;
    let from = from.ok_or_else(|| anyhow::anyhow!("missing required argument 'from'"))?;
    let to = to.ok_or_else(|| anyhow::anyhow!("missing required argument 'to'"))?;

    if from < 1 || to < 1 {
        anyhow::bail!("version numbers must be >= 1");
    }
    if from == to {
        anyhow::bail!("`from` and `to` are the same version; nothing to diff");
    }

    let old_spec = load_spec_at_version(state, contract_id, from).await?;
    let new_spec = load_spec_at_version(state, contract_id, to).await?;

    let diff = SpecDiff::between(old_spec.as_ref(), new_spec.as_ref());

    Ok(json!({
        "contract_id": contract_id,
        "from": from,
        "to": to,
        "diff": diff.to_json(),
    }))
}

async fn load_spec_at_version(
    state: &State,
    contract_id: &str,
    version: i64,
) -> anyhow::Result<Arc<ContractSpec>> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT spec_section FROM contract_spec_versions
         WHERE contract_id = $1 AND version = $2",
    )
    .bind(contract_id)
    .bind(version)
    .fetch_optional(&state.pool)
    .await?;

    let hex_section = row
        .map(|r| r.0)
        .ok_or_else(|| anyhow::anyhow!("no version {version} recorded for this contract"))?;

    if hex_section.is_empty() {
        anyhow::bail!("spec section for version {version} is empty");
    }

    let section = hex::decode(&hex_section)?;
    let spec = ContractSpec::from_spec_xdr(&section)
        .ok_or_else(|| anyhow::anyhow!("spec section for version {version} contains invalid XDR or has no entries"))?;
    Ok(Arc::new(spec))
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
    validate_contract_id(contract_id)?;
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
    // Parsed spec: names UDT values in the result (and enriches preview events).
    let spec = ContractSpec::from_spec_xdr(&section);

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
                "result": read::decode_result(&result_xdr, &call, spec.as_ref()),
                "simulated_at_ledger": latest_ledger,
            });
            if preview {
                out["events"] = json!(read::decode_events(&events, contract_id, spec.as_ref()));
                out["min_resource_fee"] = json!(min_resource_fee);
            }
            Ok(out)
        }
        SimOutcome::Error(msg) => {
            // Log the full upstream detail server-side; only return a concise,
            // sanitised copy to the MCP client (see issue #154).
            tracing::warn!(rpc_error = %msg, "contract simulation failed");
            anyhow::bail!(
                "simulation failed: {}",
                lumenqraph_core::sanitize::sanitize_simulation_error(&msg)
            )
        }
    }
}
