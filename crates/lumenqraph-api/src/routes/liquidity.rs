//! `GET /contracts/:contract_id/liquidity` — materialized liquidity event view
//! for a contract (add/remove), newest first.
//! Optional filters: `?kind=` (add|remove), `?provider=`.
//!
//! Keyset pagination via `after=` (the `next_cursor` of the previous page).
//! `offset` is deprecated, capped at [`crate::pagination::MAX_OFFSET`], and
//! answered with a `Deprecation` header.

use axum::extract::{Query, State};
use axum::Json;
use lumenqraph_core::LiquidityEvent;
use serde::Deserialize;

use crate::error::{ApiError, ApiResult};
use crate::extract::ValidContractId;
use crate::pagination::{LedgerCursor, Page, PageRequest};
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

/// Response envelope for `GET /contracts/:contract_id/liquidity`.
pub type LiquidityResponse = Page<LiquidityEvent>;

pub async fn list_liquidity_events(
    State(state): State<AppState>,
    ValidContractId(contract_id): ValidContractId,
    Query(q): Query<LiquidityQuery>,
) -> ApiResult<Json<LiquidityResponse>> {
    // Validate kind filter early.
    if let Some(ref kind) = q.kind {
        if !matches!(kind.as_str(), "add" | "remove") {
            return Err(ApiError::bad_request("kind must be one of: add, remove"));
        }
    }
    let page = PageRequest::<LedgerCursor>::parse(q.limit, 1000, q.offset, q.after.as_deref())?;
    let (after_ledger, after_event_id) = match page.after {
        Some(ref c) => (Some(c.ledger), Some(c.event_id.as_str())),
        None => (None, None),
    };

    let rows: Vec<LiquidityEvent> = sqlx::query_as(
        "SELECT event_id, contract_id, event_kind, provider, amount_a, amount_b,
                shares, raw_event_name, extra_amounts, ledger, ledger_closed_at
         FROM liquidity_events
         WHERE contract_id = $1
           AND ($2::text IS NULL OR event_kind = $2)
           AND ($3::text IS NULL OR provider = $3)
           AND ($4::bigint IS NULL OR ledger < $4 OR (ledger = $4 AND event_id < $5))
         ORDER BY ledger DESC, event_id DESC
         LIMIT $6 OFFSET $7",
    )
    .bind(&contract_id)
    .bind(&q.kind)
    .bind(&q.provider)
    .bind(after_ledger)
    .bind(after_event_id)
    .bind(page.fetch_limit())
    .bind(page.offset)
    .fetch_all(&state.pool)
    .await?;

    Ok(page.finish(rows, |r| LedgerCursor::new(r.ledger, &r.event_id)))
}
