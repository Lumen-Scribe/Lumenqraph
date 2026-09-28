//! `GET /contracts/:contract_id/swaps` — materialized AMM swap view for a
//! contract (sender/sell_token/buy_token/amounts), newest first.
//! Optional filters: `?sender=`, `?sell_token=`, `?buy_token=`.
//!
//! Keyset pagination via `after=` (the `next_cursor` of the previous page).
//! `offset` is deprecated, capped at [`crate::pagination::MAX_OFFSET`], and
//! answered with a `Deprecation` header.

use axum::extract::{Query, State};
use axum::Json;
use lumenqraph_core::AmmSwap;
use serde::Deserialize;

use crate::error::{ApiError, ApiResult};
use crate::extract::ValidContractId;
use crate::pagination::{LedgerCursor, Page, PageRequest};
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

/// Response envelope for `GET /contracts/:contract_id/swaps`.
pub type SwapsResponse = Page<AmmSwap>;

pub async fn list_swaps(
    State(state): State<AppState>,
    ValidContractId(contract_id): ValidContractId,
    Query(q): Query<SwapsQuery>,
) -> ApiResult<Json<SwapsResponse>> {
    let page = PageRequest::<LedgerCursor>::parse(q.limit, 1000, q.offset, q.after.as_deref())?;
    let (after_ledger, after_event_id) = match page.after {
        Some(ref c) => (Some(c.ledger), Some(c.event_id.as_str())),
        None => (None, None),
    };

    let rows: Vec<AmmSwap> = sqlx::query_as(
        "SELECT event_id, contract_id, sender, sell_token, buy_token,
                sell_amount, buy_amount, raw_event_name, ledger, ledger_closed_at
         FROM amm_swaps
         WHERE contract_id = $1
           AND ($2::text IS NULL OR sender = $2)
           AND ($3::text IS NULL OR sell_token = $3)
           AND ($4::text IS NULL OR buy_token = $4)
           AND ($5::bigint IS NULL OR ledger < $5 OR (ledger = $5 AND event_id < $6))
         ORDER BY ledger DESC, event_id DESC
         LIMIT $7 OFFSET $8",
    )
    .bind(&contract_id)
    .bind(&q.sender)
    .bind(&q.sell_token)
    .bind(&q.buy_token)
    .bind(after_ledger)
    .bind(after_event_id)
    .bind(page.fetch_limit())
    .bind(page.offset)
    .fetch_all(&state.pool)
    .await?;

    Ok(page.finish(rows, |r| LedgerCursor::new(r.ledger, &r.event_id)))
}
