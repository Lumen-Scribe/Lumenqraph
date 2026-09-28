//! `GET /contracts/:contract_id/transfers` — the materialized token-transfer
//! view for a contract (from/to/amount), newest first. Optional `?from=`/`?to=`
//! address filters.
//!
//! Keyset pagination via `after=` (the `next_cursor` of the previous page).
//! `offset` is deprecated, capped at [`crate::pagination::MAX_OFFSET`], and
//! answered with a `Deprecation` header.

use axum::extract::{Query, State};
use axum::Json;
use lumenqraph_core::TokenTransfer;
use serde::Deserialize;

use crate::error::{ApiError, ApiResult};
use crate::extract::ValidContractId;
use crate::pagination::{LedgerCursor, Page, PageRequest};
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

/// Response envelope for `GET /contracts/:contract_id/transfers`.
pub type TransfersResponse = Page<TokenTransfer>;

pub async fn list_transfers(
    State(state): State<AppState>,
    ValidContractId(contract_id): ValidContractId,
    Query(q): Query<TransfersQuery>,
) -> ApiResult<Json<TransfersResponse>> {
    let page = PageRequest::<LedgerCursor>::parse(q.limit, 1000, q.offset, q.after.as_deref())?;
    let (after_ledger, after_event_id) = match page.after {
        Some(ref c) => (Some(c.ledger), Some(c.event_id.as_str())),
        None => (None, None),
    };

    let transfers: Vec<TokenTransfer> = sqlx::query_as(
        "SELECT event_id, contract_id, from_addr, to_addr, amount, kind, ledger, ledger_closed_at
         FROM token_transfers
         WHERE contract_id = $1
           AND ($2::text IS NULL OR from_addr = $2)
           AND ($3::text IS NULL OR to_addr = $3)
           AND ($4::bigint IS NULL OR ledger < $4 OR (ledger = $4 AND event_id < $5))
         ORDER BY ledger DESC, event_id DESC
         LIMIT $6 OFFSET $7",
    )
    .bind(&contract_id)
    .bind(&q.from)
    .bind(&q.to)
    .bind(after_ledger)
    .bind(after_event_id)
    .bind(page.fetch_limit())
    .bind(page.offset)
    .fetch_all(&state.pool)
    .await?;

    Ok(page.finish(transfers, |t| LedgerCursor::new(t.ledger, &t.event_id)))
}
