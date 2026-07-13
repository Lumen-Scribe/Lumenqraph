//! `GET /contracts` — the set of contracts we've seen events for, with counts.
//! Derived on the fly from the events table so it can never drift from reality.
//!
//! `GET /contracts/:id/interface` — the contract's decoded on-chain interface
//! (functions, events, and user-defined types), parsed from its deployed WASM.

use axum::extract::{Path, State};
use axum::Json;
use chrono::{DateTime, Utc};
use lumenqraph_core::Contract;
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
