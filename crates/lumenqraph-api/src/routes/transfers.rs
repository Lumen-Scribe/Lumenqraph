//! `GET /contracts/:contract_id/transfers` — the materialized token-transfer
//! view for a contract (from/to/amount), newest first. Optional `?from=`/`?to=`
//! address filters.

use axum::extract::{Path, Query, State};
use axum::Json;
use lumenqraph_core::TokenTransfer;
use serde::Deserialize;

use crate::error::ApiResult;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct TransfersQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    #[serde(default)]
    offset: i64,
    from: Option<String>,
    to: Option<String>,
}

fn default_limit() -> i64 {
    50
}

pub async fn list_transfers(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Query(q): Query<TransfersQuery>,
) -> ApiResult<Json<Vec<TokenTransfer>>> {
    let limit = q.limit.clamp(1, 1000);
    let offset = q.offset.max(0);

    let transfers: Vec<TokenTransfer> = sqlx::query_as(
        "SELECT event_id, contract_id, from_addr, to_addr, amount, ledger, ledger_closed_at
         FROM token_transfers
         WHERE contract_id = $1
           AND ($2::text IS NULL OR from_addr = $2)
           AND ($3::text IS NULL OR to_addr = $3)
         ORDER BY ledger DESC, event_id DESC
         LIMIT $4 OFFSET $5",
    )
    .bind(&contract_id)
    .bind(&q.from)
    .bind(&q.to)
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.pool)
    .await?;

    Ok(Json(transfers))
}
