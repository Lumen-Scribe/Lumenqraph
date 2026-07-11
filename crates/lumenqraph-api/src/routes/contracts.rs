//! `GET /contracts` — the set of contracts we've seen events for, with counts.
//! Derived on the fly from the events table so it can never drift from reality.

use axum::extract::State;
use axum::Json;
use lumenqraph_core::Contract;

use crate::error::ApiResult;
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
