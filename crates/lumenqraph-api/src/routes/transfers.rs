//! `GET /contracts/:contract_id/transfers` — the materialized token-transfer
//! view for a contract (from/to/amount), newest first. Optional `?from=`/`?to=`
//! address filters.
//!
//! Supports both offset (deprecated for large result sets) and cursor pagination.
//! For large result sets, cursor pagination is strongly recommended as it has
//! constant-time per-page performance, whereas offset pagination degrades linearly.

use axum::extract::{Path, Query, State};
use axum::Json;
use lumenqraph_core::TokenTransfer;
use serde::{Deserialize, Serialize};

use crate::error::{ApiError, ApiResult};
use crate::pagination;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct TransfersQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    #[serde(default)]
    offset: i64,
    /// Opaque cursor from a previous response's `next_cursor`.
    after: Option<String>,
    from: Option<String>,
    to: Option<String>,
}

fn default_limit() -> i64 {
    50
}

#[derive(Serialize)]
pub struct TransfersResponse {
    /// The transfer rows in the result set.
    pub data: Vec<TokenTransfer>,
    /// Whether there are more results available.
    pub has_more: bool,
    /// Opaque cursor to fetch the next page. Null if this is the last page.
    pub next_cursor: Option<String>,
}

pub async fn list_transfers(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Query(q): Query<TransfersQuery>,
) -> ApiResult<Json<TransfersResponse>> {
    if !lumenqraph_core::is_valid_contract_id(&contract_id) {
        return Err(ApiError::bad_request("invalid contract id"));
    }
    let limit = q.limit.clamp(1, 1000);

    // If cursor is provided, use keyset pagination; otherwise fall back to offset.
    let transfers: Vec<TokenTransfer> = if let Some(ref cursor) = q.after {
        let page_config = pagination::PaginationConfig::new(limit, Some(cursor))
            .map_err(|e| ApiError::bad_request(format!("invalid cursor: {e}")))?;
        sqlx::query_as(
            "SELECT event_id, contract_id, from_addr, to_addr, amount, kind, ledger, ledger_closed_at
             FROM token_transfers
             WHERE contract_id = $1
               AND ($2::text IS NULL OR from_addr = $2)
               AND ($3::text IS NULL OR to_addr = $3)
               AND ($4::bigint IS NULL OR ledger < $4 OR (ledger = $4 AND event_id < $5))
             ORDER BY ledger DESC, event_id DESC
             LIMIT $6",
        )
        .bind(&contract_id)
        .bind(&q.from)
        .bind(&q.to)
        .bind(page_config.after_ledger)
        .bind(page_config.after_event_id)
        .bind(limit + 1)
        .fetch_all(&state.pool)
        .await?
    } else {
        // Backward compatibility: use offset pagination if no cursor provided
        let offset = q.offset.max(0);
        sqlx::query_as(
            "SELECT event_id, contract_id, from_addr, to_addr, amount, kind, ledger, ledger_closed_at
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
        .await?
    };

    // Determine if there's a next page and extract the cursor.
    let (has_next_page, result_transfers) = if q.after.is_some() && transfers.len() as i64 > limit {
        let mut trimmed = transfers;
        trimmed.truncate(limit as usize);
        (true, trimmed)
    } else {
        (false, transfers)
    };

    Ok(Json(TransfersResponse {
        data: result_transfers,
        has_more: has_next_page,
        next_cursor: if has_next_page {
            result_transfers
                .last()
                .map(|t| pagination::encode_cursor(t.ledger, &t.event_id))
        } else {
            None
        },
    }))
}
