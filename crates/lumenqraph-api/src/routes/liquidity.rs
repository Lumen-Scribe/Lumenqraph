//! `GET /contracts/:contract_id/liquidity` — materialized liquidity event view
//! for a contract (add/remove), newest first.
//! Optional filters: `?kind=` (add|remove), `?provider=`.
//!
//! Supports both offset and cursor pagination via `after=`.

use axum::extract::{Path, Query, State};
use axum::Json;
use lumenqraph_core::LiquidityEvent;
use serde::{Deserialize, Serialize};

use crate::error::{ApiError, ApiResult};
use crate::pagination;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct LiquidityQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    #[serde(default)]
    offset: i64,
    /// Opaque cursor from a previous response's `next_cursor`.
    after: Option<String>,
    /// Filter by event kind: "add" | "remove".
    kind: Option<String>,
    provider: Option<String>,
}

fn default_limit() -> i64 {
    50
}

#[derive(Serialize)]
pub struct LiquidityResponse {
    pub data: Vec<LiquidityEvent>,
    /// Whether there are more results available.
    pub has_more: bool,
    /// Opaque cursor to fetch the next page. Null if this is the last page.
    pub next_cursor: Option<String>,
}

pub async fn list_liquidity_events(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Query(q): Query<LiquidityQuery>,
) -> ApiResult<Json<LiquidityResponse>> {
    if !lumenqraph_core::is_valid_contract_id(&contract_id) {
        return Err(ApiError::bad_request("invalid contract id"));
    }
    // Validate kind filter early.
    if let Some(ref kind) = q.kind {
        if !matches!(kind.as_str(), "add" | "remove") {
            return Err(ApiError::bad_request("kind must be one of: add, remove"));
        }
    }
    let limit = q.limit.clamp(1, 1000);

    let events: Vec<LiquidityEvent> = if let Some(ref cursor) = q.after {
        let page_config = pagination::PaginationConfig::new(limit, Some(cursor))
            .map_err(|e| ApiError::bad_request(format!("invalid cursor: {e}")))?;
        sqlx::query_as(
            "SELECT event_id, contract_id, event_kind, provider, amount_a, amount_b,
                    shares, raw_event_name, extra_amounts, ledger, ledger_closed_at
             FROM liquidity_events
             WHERE contract_id = $1
               AND ($2::text IS NULL OR event_kind = $2)
               AND ($3::text IS NULL OR provider = $3)
               AND ($4::bigint IS NULL OR ledger < $4 OR (ledger = $4 AND event_id < $5))
             ORDER BY ledger DESC, event_id DESC
             LIMIT $6",
        )
        .bind(&contract_id)
        .bind(&q.kind)
        .bind(&q.provider)
        .bind(page_config.after_ledger)
        .bind(page_config.after_event_id)
        .bind(limit + 1)
        .fetch_all(&state.pool)
        .await?
    } else {
        let offset = q.offset.max(0);
        sqlx::query_as(
            "SELECT event_id, contract_id, event_kind, provider, amount_a, amount_b,
                    shares, raw_event_name, extra_amounts, ledger, ledger_closed_at
             FROM liquidity_events
             WHERE contract_id = $1
               AND ($2::text IS NULL OR event_kind = $2)
               AND ($3::text IS NULL OR provider = $3)
             ORDER BY ledger DESC, event_id DESC
             LIMIT $4 OFFSET $5",
        )
        .bind(&contract_id)
        .bind(&q.kind)
        .bind(&q.provider)
        .bind(limit)
        .bind(offset)
        .fetch_all(&state.pool)
        .await?
    };

    let (has_next_page, result_events) = if q.after.is_some() && events.len() as i64 > limit {
        let mut trimmed = events;
        trimmed.truncate(limit as usize);
        (true, trimmed)
    } else {
        (false, events)
    };

    let next_cursor = if has_next_page {
        result_events
            .last()
            .map(|l| pagination::encode_cursor(l.ledger, &l.event_id))
    } else {
        None
    };

    Ok(Json(LiquidityResponse {
        data: result_events,
        has_more: has_next_page,
        next_cursor,
    }))
}
