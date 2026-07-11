//! `GET /contracts/:contract_id/events` — most-recent events for a contract,
//! newest first, with limit/offset pagination and an optional `event_name`
//! filter. Each row includes both raw base64 XDR and decoded JSON.

use axum::extract::{Path, Query, State};
use axum::Json;
use lumenqraph_core::EventRow;
use serde::Deserialize;

use crate::error::ApiResult;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct EventsQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    #[serde(default)]
    offset: i64,
    /// Optional filter, e.g. `?event_name=transfer`.
    event_name: Option<String>,
}

fn default_limit() -> i64 {
    50
}

pub async fn list_events(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Query(q): Query<EventsQuery>,
) -> ApiResult<Json<Vec<EventRow>>> {
    let limit = q.limit.clamp(1, 1000);
    let offset = q.offset.max(0);

    let events: Vec<EventRow> = sqlx::query_as(
        "SELECT event_id, contract_id, ledger, ledger_closed_at, event_type,
                topics, decoded_topics, event_name, value, decoded_value,
                tx_hash, in_successful_call, paging_token, created_at
         FROM events
         WHERE contract_id = $1
           AND ($2::text IS NULL OR event_name = $2)
         ORDER BY ledger DESC, event_id DESC
         LIMIT $3 OFFSET $4",
    )
    .bind(&contract_id)
    .bind(&q.event_name)
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.pool)
    .await?;

    Ok(Json(events))
}
