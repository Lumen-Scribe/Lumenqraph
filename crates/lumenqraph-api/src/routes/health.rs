//! `GET /health` — indexing freshness and lag vs the chain tip.

use axum::extract::State;
use axum::Json;
use serde_json::{json, Value};

use crate::error::ApiResult;
use crate::state::AppState;

pub async fn health(State(state): State<AppState>) -> ApiResult<Json<Value>> {
    let status: Option<(i64, i64, i64, i64, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
        "SELECT last_processed_ledger, chain_tip_ledger, events_ingested_total, errors_total, updated_at
         FROM indexer_cursor WHERE id = 1",
    )
    .fetch_optional(&state.pool)
    .await?;

    let Some((last, tip, ingested, errors, updated_at)) = status else {
        return Ok(Json(json!({ "status": "starting" })));
    };

    let lag_ledgers = (tip - last).max(0);
    let secs_since_update = (chrono::Utc::now() - updated_at).num_seconds();
    // Stale if the cursor hasn't advanced recently, or we're far behind.
    let healthy = secs_since_update <= 120 && lag_ledgers < 100;

    Ok(Json(json!({
        "status": if healthy { "ok" } else { "degraded" },
        "last_processed_ledger": last,
        "chain_tip_ledger": tip,
        "lag_ledgers": lag_ledgers,
        "seconds_since_cursor_update": secs_since_update,
        "events_ingested_total": ingested,
        "errors_total": errors,
    })))
}
