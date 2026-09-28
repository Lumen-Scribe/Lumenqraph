//! `GET /contracts/:contract_id/nfts` — materialized NFT event view for a
//! contract (mint/transfer/burn), newest first.
//! Optional filters: `?kind=` (mint|transfer|burn), `?from=`, `?to=`, `?token_id=`.
//!
//! Keyset pagination via `after=` (the `next_cursor` of the previous page).
//! `offset` is deprecated, capped at [`crate::pagination::MAX_OFFSET`], and
//! answered with a `Deprecation` header.

use axum::extract::{Query, State};
use axum::Json;
use lumenqraph_core::NftEvent;
use serde::Deserialize;

use crate::error::{ApiError, ApiResult};
use crate::extract::ValidContractId;
use crate::pagination::{LedgerCursor, Page, PageRequest};
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

/// Response envelope for `GET /contracts/:contract_id/nfts`.
pub type NftsResponse = Page<NftEvent>;

pub async fn list_nft_events(
    State(state): State<AppState>,
    ValidContractId(contract_id): ValidContractId,
    Query(q): Query<NftsQuery>,
) -> ApiResult<Json<NftsResponse>> {
    // Validate kind filter early.
    if let Some(ref kind) = q.kind {
        if !matches!(kind.as_str(), "mint" | "transfer" | "burn") {
            return Err(ApiError::bad_request(
                "kind must be one of: mint, transfer, burn",
            ));
        }
    }
    let page = PageRequest::<LedgerCursor>::parse(q.limit, 1000, q.offset, q.after.as_deref())?;
    let (after_ledger, after_event_id) = match page.after {
        Some(ref c) => (Some(c.ledger), Some(c.event_id.as_str())),
        None => (None, None),
    };

    let rows: Vec<NftEvent> = sqlx::query_as(
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
         LIMIT $8 OFFSET $9",
    )
    .bind(&contract_id)
    .bind(&q.kind)
    .bind(&q.from)
    .bind(&q.to)
    .bind(&q.token_id)
    .bind(after_ledger)
    .bind(after_event_id)
    .bind(page.fetch_limit())
    .bind(page.offset)
    .fetch_all(&state.pool)
    .await?;

    Ok(page.finish(rows, |r| LedgerCursor::new(r.ledger, &r.event_id)))
}
