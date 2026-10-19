//! `GET /contracts/:contract_id/events` — most-recent events for a contract,
//! newest first, with keyset (cursor) pagination and an optional `event_name`
//! filter. Each row includes both raw base64 XDR and decoded JSON.
//!
//! Cursor pagination is strongly recommended for production use. Offset pagination
//! is deprecated due to linear performance degradation with large offsets and will
//! be removed in a future version. Offsets are capped at
//! [`crate::pagination::MAX_OFFSET`] and answered with a `Deprecation` header;
//! use cursor pagination for deeper pages.

use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::DateTime;
use lumenqraph_core::EventRow;
use serde::{Deserialize, Serialize};

use crate::error::{ApiError, ApiResult};
use crate::extract::ValidContractId;
use crate::pagination::{LedgerCursor, Page, PageRequest};
use crate::state::AppState;

#[derive(Deserialize)]
pub struct EventsQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    #[serde(default)]
    offset: i64,
    /// Opaque cursor from a previous response's `next_cursor`.
    after: Option<String>,
    /// Optional filter, e.g. `?event_name=transfer`.
    event_name: Option<String>,
    /// Optional ledger range filter: minimum ledger (inclusive).
    from_ledger: Option<i64>,
    /// Optional ledger range filter: maximum ledger (inclusive).
    to_ledger: Option<i64>,
    /// Optional time range filter: minimum timestamp (RFC3339).
    since: Option<String>,
    /// Optional time range filter: maximum timestamp (RFC3339).
    until: Option<String>,
    /// Optional topic filter: filter by decoded_topics[0] value (e.g., topic0=transfer).
    topic0: Option<String>,
    /// Optional topic filter: filter by decoded_topics[1] value.
    topic1: Option<String>,
    /// Optional topic filter: filter by decoded_topics[2] value.
    topic2: Option<String>,
    /// Optional topic filter: filter by decoded_topics[3] value.
    topic3: Option<String>,
    /// Optional parameter filter: filter by parameter name:value (e.g., from=GXXXX).
    param: Option<String>,
    /// Optional filter to return only events from successful contract calls (default: false, returns all).
    successful_only: Option<bool>,
}

fn default_limit() -> i64 {
    50
}

/// Response envelope for `GET /contracts/:contract_id/events`.
pub type EventsResponse = Page<EventRow>;

pub async fn list_events(
    State(state): State<AppState>,
    ValidContractId(contract_id): ValidContractId,
    Query(q): Query<EventsQuery>,
) -> ApiResult<Json<EventsResponse>> {
    let limit = q.limit.clamp(1, 1000);

    let page = PageRequest::<LedgerCursor>::parse(q.limit, 1000, q.offset, q.after.as_deref())?;
    let (after_ledger, after_event_id) = match page.after {
        Some(ref c) => (Some(c.ledger), Some(c.event_id.as_str())),
        None => (None, None),
    };

    // Validate and parse time range if provided
    let since_datetime: Option<DateTime<chrono::Utc>> = if let Some(ref since) = q.since {
        Some(
            DateTime::parse_from_rfc3339(since)
                .map_err(|_| ApiError::bad_request("Invalid 'since' timestamp format (RFC3339)"))?
                .with_timezone(&chrono::Utc),
        )
    } else {
        None
    };

    let until_datetime: Option<DateTime<chrono::Utc>> = if let Some(ref until) = q.until {
        Some(
            DateTime::parse_from_rfc3339(until)
                .map_err(|_| ApiError::bad_request("Invalid 'until' timestamp format (RFC3339)"))?
                .with_timezone(&chrono::Utc),
        )
    } else {
        None
    };

    // Validate time range consistency
    if let (Some(since), Some(until)) = (since_datetime, until_datetime) {
        if since > until {
            return Err(ApiError::bad_request("'since' must be before 'until'"));
        }
    }

    // Validate ledger range consistency
    if let (Some(from_ledger), Some(to_ledger)) = (q.from_ledger, q.to_ledger) {
        if from_ledger > to_ledger {
            return Err(ApiError::bad_request("'from_ledger' must be <= 'to_ledger'"));
        }
    }

    let events: Vec<EventRow> = sqlx::query_as(
        "SELECT event_id, contract_id, ledger, ledger_closed_at, event_type,
                topics, decoded_topics, event_name, value, decoded_value,
                enriched, tx_hash, in_successful_call, paging_token, created_at
         FROM events
         WHERE contract_id = $1
           AND ($2::text IS NULL OR event_name = $2)
           AND ($3::bigint IS NULL OR ledger >= $3)
           AND ($4::bigint IS NULL OR ledger <= $4)
           AND ($5::timestamp IS NULL OR ledger_closed_at >= $5)
           AND ($6::timestamp IS NULL OR ledger_closed_at <= $6)
           -- Topic filtering: decoded_topics is a JSONB array. To filter by position,
           -- we use JSONB containment (@>) with null padding. For example, to match
           -- decoded_topics[1], we build an array [null, value] and check containment.
           -- This works because JSONB containment checks if the right side is a subset
           -- of the left side, preserving position for array elements.
           AND ($7::text IS NULL OR decoded_topics @> jsonb_build_array($7::jsonb))
           AND ($8::text IS NULL OR decoded_topics @> jsonb_build_array(jsonb_null::jsonb, $8::jsonb))
           AND ($9::text IS NULL OR decoded_topics @> jsonb_build_array(jsonb_null::jsonb, jsonb_null::jsonb, $9::jsonb))
           AND ($10::text IS NULL OR decoded_topics @> jsonb_build_array(jsonb_null::jsonb, jsonb_null::jsonb, jsonb_null::jsonb, $10::jsonb))
           AND ($11::text IS NULL OR enriched @> ($11::jsonb))
           AND ($15::boolean IS NULL OR in_successful_call = $15)
           AND ($12::bigint IS NULL OR ledger < $12 OR (ledger = $12 AND event_id < $13))
         ORDER BY ledger DESC, event_id DESC
         LIMIT $14 OFFSET $16",
    )
    .bind(&contract_id)
    .bind(&q.event_name)
    .bind(q.from_ledger)
    .bind(q.to_ledger)
    .bind(since_datetime)
    .bind(until_datetime)
    .bind(&q.topic0)
    .bind(&q.topic1)
    .bind(&q.topic2)
    .bind(&q.topic3)
    .bind(&q.param)
    .bind(after_ledger)
    .bind(after_event_id)
    .bind(page.fetch_limit())
    .bind(q.successful_only)
    .bind(page.offset)
    .fetch_all(&state.pool)
    .await?;

    Ok(page.finish(events, |e| LedgerCursor::new(e.ledger, &e.event_id)))
}

/// `GET /events/:event_id` — fetch a single event by its unique id.
///
/// Returns the full [`EventRow`] (raw XDR, decoded JSON, and enriched record)
/// or `404` when no event with that id has been indexed.
pub async fn get_event(
    State(state): State<AppState>,
    Path(event_id): Path<String>,
) -> ApiResult<Json<EventRow>> {
    let row: Option<EventRow> = sqlx::query_as(
        "SELECT event_id, contract_id, ledger, ledger_closed_at, event_type,
                topics, decoded_topics, event_name, value, decoded_value,
                enriched, tx_hash, in_successful_call, paging_token, created_at
         FROM events
         WHERE event_id = $1",
    )
    .bind(&event_id)
    .fetch_optional(&state.pool)
    .await?;

    match row {
        Some(event) => Ok(Json(event)),
        None => Err(ApiError::not_found(format!(
            "no event found with id '{event_id}'"
        ))),
    }
}

/// `GET /transactions/:tx_hash/events` — all indexed events for a transaction.
///
/// Returns events ordered by `(ledger ASC, event_id ASC)` — the same order
/// they were emitted on-chain. Paginates with the same keyset `after` cursor
/// as the other event lists: pass the previous response's `next_cursor` to
/// fetch the rest of a transaction with more events than `limit`.
///
/// `tx_hash` must be 64 hexadecimal characters and is matched case-insensitively
/// (it is normalised to lowercase); anything else is rejected with `400`.
pub async fn transaction_events(
    State(state): State<AppState>,
    Path(tx_hash): Path<String>,
    Query(q): Query<TxEventsQuery>,
) -> ApiResult<Json<TxEventsResponse>> {
    let tx_hash = normalize_tx_hash(&tx_hash)?;
    let page = PageRequest::<LedgerCursor>::parse(
        q.limit.unwrap_or(100),
        1000,
        0,
        q.after.as_deref(),
    )?;
    let page = page_transaction_events(&state.pool, &tx_hash, &page, q.successful_only).await?;
    Ok(Json(TxEventsResponse {
        tx_hash,
        count: page.data.len(),
        has_more: page.has_more,
        next_cursor: page.next_cursor,
        data: page.data,
    }))
}

/// One keyset page of a transaction's events, ordered `(ledger, event_id)` ascending.
async fn page_transaction_events(
    pool: &sqlx::PgPool,
    tx_hash: &str,
    request: &PageRequest<LedgerCursor>,
    successful_only: Option<bool>,
) -> ApiResult<Page<EventRow>> {
    let (after_ledger, after_event_id) = match request.after {
        Some(ref c) => (Some(c.ledger), Some(c.event_id.as_str())),
        None => (None, None),
    };

    let events: Vec<EventRow> = sqlx::query_as(
        "SELECT event_id, contract_id, ledger, ledger_closed_at, event_type,
                topics, decoded_topics, event_name, value, decoded_value,
                enriched, tx_hash, in_successful_call, paging_token, created_at
         FROM events
         WHERE tx_hash = $1
           AND ($2::boolean IS NULL OR in_successful_call = $2)
           AND ($3::bigint IS NULL OR ledger > $3 OR (ledger = $3 AND event_id > $4))
         ORDER BY ledger ASC, event_id ASC
         LIMIT $5",
    )
    .bind(tx_hash)
    .bind(successful_only)
    .bind(after_ledger)
    .bind(after_event_id)
    .bind(request.fetch_limit())
    .fetch_all(pool)
    .await?;

    Ok(request.finish(events, |e| LedgerCursor::new(e.ledger, &e.event_id)))
}

/// Validate a transaction hash and normalise it to lowercase hex.
fn normalize_tx_hash(raw: &str) -> ApiResult<String> {
    if raw.len() == 64 && raw.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(raw.to_ascii_lowercase())
    } else {
        Err(ApiError::bad_request(
            "tx_hash must be 64 hexadecimal characters",
        ))
    }
}

#[derive(Deserialize)]
pub struct TxEventsQuery {
    /// Maximum events to return (1–1000, default 100).
    limit: Option<i64>,
    /// Opaque cursor from a previous response's `next_cursor`.
    after: Option<String>,
    /// Optional filter to return only events from successful contract calls (default: false, returns all).
    successful_only: Option<bool>,
}

#[derive(Serialize)]
pub struct TxEventsResponse {
    /// The transaction hash that was queried.
    pub tx_hash: String,
    /// Number of events in this response.
    pub count: usize,
    /// Whether there are more results available (if limit was reached).
    pub has_more: bool,
    /// Opaque cursor to pass as `after` for the next page; null on the last page.
    pub next_cursor: Option<String>,
    /// The event rows, ordered by ledger and event_id ascending.
    pub data: Vec<EventRow>,
}

#[cfg(test)]
mod tests {
    //! The DB-backed test needs a throwaway Postgres:
    //!
    //!   TEST_DATABASE_URL=postgres://…/lumenqraph \
    //!     cargo test -p lumenqraph-api -- --ignored
    //!
    //! Unit tests run without one.

    use super::*;

    #[test]
    fn tx_hash_accepts_64_hex_and_lowercases() {
        let upper = "ABCDEF0123456789abcdef0123456789ABCDEF0123456789abcdef0123456789";
        assert_eq!(
            normalize_tx_hash(upper).unwrap(),
            upper.to_ascii_lowercase()
        );
    }

    #[test]
    fn tx_hash_rejects_wrong_length_and_non_hex() {
        assert!(normalize_tx_hash("abc").is_err());
        assert!(normalize_tx_hash(&"a".repeat(65)).is_err());
        assert!(normalize_tx_hash(&format!("{}g", "a".repeat(63))).is_err());
        assert!(normalize_tx_hash(&"z".repeat(64)).is_err());
    }

    async fn fixture() -> sqlx::PgPool {
        let url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .expect("connect to test database");
        for stmt in ["DROP SCHEMA public CASCADE", "CREATE SCHEMA public"] {
            sqlx::query(stmt).execute(&pool).await.expect("reset schema");
        }
        sqlx::migrate!("../../migrations")
            .run(&pool)
            .await
            .expect("run migrations");
        pool
    }

    #[tokio::test]
    #[ignore = "needs TEST_DATABASE_URL"]
    async fn paging_a_large_transaction_returns_every_event_once() {
        const TOTAL: i64 = 1_205;
        let pool = fixture().await;
        let tx = "a".repeat(64);
        sqlx::query(
            "INSERT INTO events (event_id, contract_id, ledger, ledger_closed_at, event_type,
                                 topics, event_name, value, tx_hash, in_successful_call, paging_token)
             SELECT lpad(i::text, 8, '0'), 'C1', 100 + i / 50, now(), 'contract', '[]', 'transfer',
                    'v', $1, true, lpad(i::text, 8, '0')
             FROM generate_series(1, $2) AS i",
        )
        .bind(&tx)
        .bind(TOTAL)
        .execute(&pool)
        .await
        .expect("insert events");

        let mut seen = Vec::new();
        let mut after = None;
        loop {
            let request = PageRequest::<LedgerCursor>::parse(
                1000,
                1000,
                0,
                after.as_deref(),
            )
            .unwrap();
            let page = page_transaction_events(&pool, &tx, &request, None)
                .await
                .unwrap();
            seen.extend(page.data.iter().map(|e| e.event_id.clone()));
            match page.next_cursor {
                Some(next) => after = Some(next),
                None => break,
            }
        }

        assert_eq!(seen.len() as i64, TOTAL, "every event returned");
        let mut unique = seen.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), seen.len(), "no event returned twice");
    }
}
