//! `GET /contracts` — the set of contracts we've seen events for, with counts.
//! Derived on the fly from the events table so it can never drift from reality.
//!
//! `GET /contracts/:id/interface` — the contract's decoded on-chain interface
//! (functions, events, and user-defined types), parsed from its deployed WASM.

use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::{DateTime, Utc};
use lumenqraph_core::Contract;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::types::Json as SqlxJson;

use crate::error::{ApiError, ApiResult};
use crate::state::AppState;

pub async fn list_contracts(State(state): State<AppState>) -> ApiResult<Json<Vec<Contract>>> {
    let contracts: Vec<Contract> = sqlx::query_as(
        "SELECT contract_id,
                count(*)::bigint       AS event_count,
                min(ledger)            AS first_seen_ledger,
                max(ledger)            AS last_seen_ledger
         FROM events
         GROUP BY contract_id
         ORDER BY event_count DESC",
    )
    .fetch_all(&state.pool)
    .await?;

    Ok(Json(contracts))
}

pub async fn contract_interface(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
) -> ApiResult<Json<Value>> {
    let row: Option<(SqlxJson<Value>, bool, DateTime<Utc>)> = sqlx::query_as(
        "SELECT interface, has_events, fetched_at
         FROM contract_specs WHERE contract_id = $1",
    )
    .bind(&contract_id)
    .fetch_optional(&state.pool)
    .await?;

    match row {
        Some((interface, has_events, fetched_at)) => Ok(Json(json!({
            "contract_id": contract_id,
            "has_events": has_events,
            "fetched_at": fetched_at,
            "interface": interface.0,
        }))),
        None => Err(ApiError::not_found(
            "no on-chain interface indexed for this contract yet",
        )),
    }
}

#[derive(Deserialize)]
pub struct StateQuery {
    /// How many versions to return, newest first (1 = current state only).
    #[serde(default = "default_state_limit")]
    limit: i64,
}

fn default_state_limit() -> i64 {
    1
}

/// `GET /contracts/:id/state` — versioned snapshots of a contract's instance
/// storage, newest first. `limit=1` (default) is the current state.
pub async fn contract_state(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Query(q): Query<StateQuery>,
) -> ApiResult<Json<Value>> {
    let limit = q.limit.clamp(1, 200);
    let rows: Vec<(i64, SqlxJson<Value>, DateTime<Utc>)> = sqlx::query_as(
        "SELECT ledger, storage, captured_at
         FROM contract_state WHERE contract_id = $1
         ORDER BY ledger DESC LIMIT $2",
    )
    .bind(&contract_id)
    .bind(limit)
    .fetch_all(&state.pool)
    .await?;

    if rows.is_empty() {
        return Err(ApiError::not_found(
            "no state snapshots for this contract (state indexing may be disabled, \
             or the contract hasn't been active since it was enabled)",
        ));
    }

    let versions: Vec<Value> = rows
        .into_iter()
        .map(|(ledger, storage, captured_at)| {
            json!({ "ledger": ledger, "storage": storage.0, "captured_at": captured_at })
        })
        .collect();
    Ok(Json(json!({
        "contract_id": contract_id,
        "count": versions.len(),
        "versions": versions,
    })))
}

#[derive(Deserialize)]
pub struct DataQuery {
    /// Filter to a discovery label, e.g. `balance`.
    label: Option<String>,
    /// Max keys to return (latest value of each), default 100.
    #[serde(default = "default_data_limit")]
    limit: i64,
}

fn default_data_limit() -> i64 {
    100
}

/// One `contract_data` row as selected below: (key_hash, key, durability,
/// ledger, value, label, captured_at).
type DataRow = (
    String,
    SqlxJson<Value>,
    String,
    i64,
    SqlxJson<Value>,
    Option<String>,
    DateTime<Utc>,
);

/// `GET /contracts/:id/data` — the current value of every *per-key* entry
/// snapshotted for this contract (e.g. every tracked holder balance), one row
/// per key (its latest snapshot). Requires the indexer's key indexing.
pub async fn contract_data(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Query(q): Query<DataQuery>,
) -> ApiResult<Json<Value>> {
    let limit = q.limit.clamp(1, 1000);
    // DISTINCT ON gives the newest row per key_hash; the outer query orders and
    // bounds the set of keys returned.
    let rows: Vec<DataRow> = sqlx::query_as(
        "SELECT key_hash, key, durability, ledger, value, label, captured_at FROM (
                 SELECT DISTINCT ON (key_hash)
                        key_hash, key, durability, ledger, value, label, captured_at
                 FROM contract_data
                 WHERE contract_id = $1 AND ($2::text IS NULL OR label = $2)
                 ORDER BY key_hash, ledger DESC
             ) latest
             ORDER BY ledger DESC
             LIMIT $3",
    )
    .bind(&contract_id)
    .bind(&q.label)
    .bind(limit)
    .fetch_all(&state.pool)
    .await?;

    if rows.is_empty() {
        return Err(ApiError::not_found(
            "no per-key data snapshots for this contract (key indexing may be disabled, \
             or no tracked keys have been active since it was enabled)",
        ));
    }

    let keys: Vec<Value> = rows
        .into_iter()
        .map(
            |(key_hash, key, durability, ledger, value, label, captured_at)| {
                json!({
                    "key_hash": key_hash,
                    "key": key.0,
                    "durability": durability,
                    "ledger": ledger,
                    "value": value.0,
                    "label": label,
                    "captured_at": captured_at,
                })
            },
        )
        .collect();
    Ok(Json(json!({
        "contract_id": contract_id,
        "count": keys.len(),
        "keys": keys,
    })))
}

#[derive(Deserialize)]
pub struct DataHistoryQuery {
    /// How many versions to return, newest first.
    #[serde(default = "default_state_limit")]
    limit: i64,
}

/// `GET /contracts/:id/data/:key_hash` — the version history of a single
/// per-key entry (e.g. one holder's balance over time), newest first.
pub async fn contract_data_key(
    State(state): State<AppState>,
    Path((contract_id, key_hash)): Path<(String, String)>,
    Query(q): Query<DataHistoryQuery>,
) -> ApiResult<Json<Value>> {
    let limit = q.limit.clamp(1, 500);
    // (key, durability, ledger, value, label, captured_at)
    type HistRow = (
        SqlxJson<Value>,
        String,
        i64,
        SqlxJson<Value>,
        Option<String>,
        DateTime<Utc>,
    );
    let rows: Vec<HistRow> = sqlx::query_as(
        "SELECT key, durability, ledger, value, label, captured_at
             FROM contract_data
             WHERE contract_id = $1 AND key_hash = $2
             ORDER BY ledger DESC LIMIT $3",
    )
    .bind(&contract_id)
    .bind(&key_hash)
    .bind(limit)
    .fetch_all(&state.pool)
    .await?;

    if rows.is_empty() {
        return Err(ApiError::not_found("no data snapshots for this key"));
    }

    // Key and durability are constant across a key's history; take them once.
    let key = rows[0].0 .0.clone();
    let durability = rows[0].1.clone();
    let label = rows[0].4.clone();
    let versions: Vec<Value> = rows
        .into_iter()
        .map(|(_, _, ledger, value, _, captured_at)| {
            json!({ "ledger": ledger, "value": value.0, "captured_at": captured_at })
        })
        .collect();
    Ok(Json(json!({
        "contract_id": contract_id,
        "key_hash": key_hash,
        "key": key,
        "durability": durability,
        "label": label,
        "count": versions.len(),
        "versions": versions,
    })))
}
