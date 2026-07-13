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
