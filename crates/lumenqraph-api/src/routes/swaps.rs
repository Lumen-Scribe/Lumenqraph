//! `GET /contracts/:contract_id/swaps` — materialized AMM swap view for a
//! contract (sender/sell_token/buy_token/amounts), newest first.
//! Optional filters: `?sender=`, `?sell_token=`, `?buy_token=`.
//!
//! Supports both offset (deprecated for large result sets) and cursor
//! pagination via `after=` (opaque cursor from a previous response).

use axum::extract::{Path, Query, State};
use axum::Json;
use lumenqraph_core::AmmSwap;
use serde::{Deserialize, Serialize};

use crate::error::{ApiError, ApiResult};
use crate::pagination;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct SwapsQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    #[serde(default)]
    offset: i64,
    /// Opaque cursor from a previous response's `next_cursor`.
    after: Option<String>,
    sender: Option<String>,
    sell_token: Option<String>,
    buy_token: Option<String>,
}

fn default_limit() -> i64 {
    50
}

#[derive(Serialize)]
pub struct SwapsResponse {
    pub data: Vec<AmmSwap>,
    /// Whether there are more results available.
    pub has_more: bool,
    /// Opaque cursor to fetch the next page. Null if this is the last page.
    pub next_cursor: Option<String>,
}

pub async fn list_swaps(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Query(q): Query<SwapsQuery>,
) -> ApiResult<Json<SwapsResponse>> {
    if !lumenqraph_core::is_valid_contract_id(&contract_id) {
        return Err(ApiError::bad_request("invalid contract id"));
    }
    let limit = q.limit.clamp(1, 1000);

    let swaps: Vec<AmmSwap> = if let Some(ref cursor) = q.after {
        let page_config = pagination::PaginationConfig::new(limit, Some(cursor))
            .map_err(|e| ApiError::bad_request(format!("invalid cursor: {e}")))?;
        sqlx::query_as(
            "SELECT event_id, contract_id, sender, sell_token, buy_token,
                    sell_amount, buy_amount, raw_event_name, ledger, ledger_closed_at
             FROM amm_swaps
             WHERE contract_id = $1
               AND ($2::text IS NULL OR sender = $2)
               AND ($3::text IS NULL OR sell_token = $3)
               AND ($4::text IS NULL OR buy_token = $4)
               AND ($5::bigint IS NULL OR ledger < $5 OR (ledger = $5 AND event_id < $6))
             ORDER BY ledger DESC, event_id DESC
             LIMIT $7",
        )
        .bind(&contract_id)
        .bind(&q.sender)
        .bind(&q.sell_token)
        .bind(&q.buy_token)
        .bind(page_config.after_ledger)
        .bind(page_config.after_event_id)
        .bind(limit + 1)
        .fetch_all(&state.pool)
        .await?
    } else {
        let offset = q.offset.max(0);
        sqlx::query_as(
            "SELECT event_id, contract_id, sender, sell_token, buy_token,
                    sell_amount, buy_amount, raw_event_name, ledger, ledger_closed_at
             FROM amm_swaps
             WHERE contract_id = $1
               AND ($2::text IS NULL OR sender = $2)
               AND ($3::text IS NULL OR sell_token = $3)
               AND ($4::text IS NULL OR buy_token = $4)
             ORDER BY ledger DESC, event_id DESC
             LIMIT $5 OFFSET $6",
        )
        .bind(&contract_id)
        .bind(&q.sender)
        .bind(&q.sell_token)
        .bind(&q.buy_token)
        .bind(limit)
        .bind(offset)
        .fetch_all(&state.pool)
        .await?
    };

    let (has_next_page, result_swaps) = if q.after.is_some() && swaps.len() as i64 > limit {
        let mut trimmed = swaps;
        trimmed.truncate(limit as usize);
        (true, trimmed)
    } else {
        (false, swaps)
    };

    Ok(Json(SwapsResponse {
        data: result_swaps,
        has_more: has_next_page,
        next_cursor: if has_next_page {
            result_swaps
                .last()
                .map(|s| pagination::encode_cursor(s.ledger, &s.event_id))
        } else {
            None
        },
    }))
}
