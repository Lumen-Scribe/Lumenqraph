//! `GET /contracts/:contract_id/nfts` — materialized NFT event view for a
//! contract (mint/transfer/burn), newest first.
//! Optional filters: `?kind=` (mint|transfer|burn), `?from=`, `?to=`, `?token_id=`.
//!
//! Supports both offset and cursor pagination via `after=`.

use axum::extract::{Path, Query, State};
use axum::Json;
use lumenqraph_core::NftEvent;
use serde::{Deserialize, Serialize};

use crate::error::{ApiError, ApiResult};
use crate::pagination;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct NftsQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    #[serde(default)]
    offset: i64,
    /// Opaque cursor from a previous response's `next_cursor`.
    after: Option<String>,
    /// Filter by event kind: "mint" | "transfer" | "burn".
    kind: Option<String>,
    from: Option<String>,
    to: Option<String>,
    token_id: Option<String>,
}

fn default_limit() -> i64 {
    50
}

#[derive(Serialize)]
pub struct NftsResponse {
    pub data: Vec<NftEvent>,
    /// Whether there are more results available.
    pub has_more: bool,
    /// Opaque cursor to fetch the next page. Null if this is the last page.
    pub next_cursor: Option<String>,
}

pub async fn list_nft_events(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Query(q): Query<NftsQuery>,
) -> ApiResult<Json<NftsResponse>> {
    if !lumenqraph_core::is_valid_contract_id(&contract_id) {
        return Err(ApiError::bad_request("invalid contract id"));
    }
    // Validate kind filter early.
    if let Some(ref kind) = q.kind {
        if !matches!(kind.as_str(), "mint" | "transfer" | "burn") {
            return Err(ApiError::bad_request(
                "kind must be one of: mint, transfer, burn",
            ));
        }
    }
    let limit = q.limit.clamp(1, 1000);

    let events: Vec<NftEvent> = if let Some(ref cursor) = q.after {
        let page_config = pagination::PaginationConfig::new(limit, Some(cursor))
            .map_err(|e| ApiError::bad_request(format!("invalid cursor: {e}")))?;
        sqlx::query_as(
            "SELECT event_id, contract_id, event_kind, from_addr, to_addr,
                    token_id, ledger, ledger_closed_at
             FROM nft_events
             WHERE contract_id = $1
               AND ($2::text IS NULL OR event_kind = $2)
               AND ($3::text IS NULL OR from_addr = $3)
               AND ($4::text IS NULL OR to_addr = $4)
               AND ($5::text IS NULL OR token_id = $5)
               AND ($6::bigint IS NULL OR ledger < $6 OR (ledger = $6 AND event_id < $7))
             ORDER BY ledger DESC, event_id DESC
             LIMIT $8",
        )
        .bind(&contract_id)
        .bind(&q.kind)
        .bind(&q.from)
        .bind(&q.to)
        .bind(&q.token_id)
        .bind(page_config.after_ledger)
        .bind(page_config.after_event_id)
        .bind(limit + 1)
        .fetch_all(&state.pool)
        .await?
    } else {
        let offset = q.offset.max(0);
        sqlx::query_as(
            "SELECT event_id, contract_id, event_kind, from_addr, to_addr,
                    token_id, ledger, ledger_closed_at
             FROM nft_events
             WHERE contract_id = $1
               AND ($2::text IS NULL OR event_kind = $2)
               AND ($3::text IS NULL OR from_addr = $3)
               AND ($4::text IS NULL OR to_addr = $4)
               AND ($5::text IS NULL OR token_id = $5)
             ORDER BY ledger DESC, event_id DESC
             LIMIT $6 OFFSET $7",
        )
        .bind(&contract_id)
        .bind(&q.kind)
        .bind(&q.from)
        .bind(&q.to)
        .bind(&q.token_id)
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
            .map(|n| pagination::encode_cursor(n.ledger, &n.event_id))
    } else {
        None
    };

    Ok(Json(NftsResponse {
        data: result_events,
        has_more: has_next_page,
        next_cursor,
    }))
}
