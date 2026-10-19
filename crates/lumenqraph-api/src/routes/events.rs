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
    /// Optional topic filter: match decoded_topics[0]. A bare value such as
    /// `topic0=transfer` is a string; a value starting with `"`, `[` or `{` must be
    /// JSON, e.g. `topic0=%22transfer%22`.
    topic0: Option<String>,
    /// Optional topic filter: match decoded_topics[1]. Same format as `topic0`.
    topic1: Option<String>,
    /// Optional topic filter: match decoded_topics[2]. Same format as `topic0`.
    topic2: Option<String>,
    /// Optional topic filter: match decoded_topics[3]. Same format as `topic0`.
    topic3: Option<String>,
    /// Optional parameter filter as `name:value`, e.g. `param=from:GXXXX`. The name
    /// is split at the first `:`, so the value may contain colons.
    param: Option<String>,
    /// Optional filter to return only events from successful contract calls (default: false, returns all).
    successful_only: Option<bool>,
}

fn default_limit() -> i64 {
    50
}

/// Parse a `topicN` filter into the JSON literal compared with `decoded_topics -> N`.
///
/// A value starting with `"`, `[` or `{` must be valid JSON. Any other value is a
/// bare string and is matched as a JSON string, so `topic0=transfer` works without
/// quoting. Numbers are strings too: `topic2=1000` matches the string `"1000"`,
/// which is how i128 amounts are encoded.
fn parse_topic_filter(raw: Option<&str>) -> ApiResult<Option<String>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let value = if raw.starts_with(['"', '[', '{']) {
        serde_json::from_str::<serde_json::Value>(raw)
            .map_err(|_| ApiError::bad_request("topic filter is not valid JSON"))?
    } else {
        serde_json::Value::String(raw.to_string())
    };
    Ok(Some(value.to_string()))
}

/// Parse the `param` filter (`name:value`) into a JSON object for `enriched @>`.
fn parse_param_filter(raw: Option<&str>) -> ApiResult<Option<String>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let (name, value) = raw
        .split_once(':')
        .filter(|(name, _)| !name.is_empty())
        .ok_or_else(|| {
            ApiError::bad_request("param must be in the form name:value, e.g. param=from:GABC...")
        })?;
    let mut object = serde_json::Map::new();
    object.insert(name.to_string(), serde_json::Value::String(value.to_string()));
    Ok(Some(serde_json::Value::Object(object).to_string()))
}

/// Response envelope for `GET /contracts/:contract_id/events`.
pub type EventsResponse = Page<EventRow>;

pub async fn list_events(
    State(state): State<AppState>,
    ValidContractId(contract_id): ValidContractId,
    Query(q): Query<EventsQuery>,
) -> ApiResult<Json<EventsResponse>> {
    let limit = q.limit.clamp(1, 1000);

    let topic0 = parse_topic_filter(q.topic0.as_deref())?;
    let topic1 = parse_topic_filter(q.topic1.as_deref())?;
    let topic2 = parse_topic_filter(q.topic2.as_deref())?;
    let topic3 = parse_topic_filter(q.topic3.as_deref())?;
    let param = parse_param_filter(q.param.as_deref())?;

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
           -- Topic filtering matches the value at an exact position: `-> n` reads
           -- decoded_topics[n] as jsonb, so `topic1` only matches index 1.
           AND ($7::text IS NULL OR (decoded_topics -> 0) = $7::jsonb)
           AND ($8::text IS NULL OR (decoded_topics -> 1) = $8::jsonb)
           AND ($9::text IS NULL OR (decoded_topics -> 2) = $9::jsonb)
           AND ($10::text IS NULL OR (decoded_topics -> 3) = $10::jsonb)
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
    .bind(&topic0)
    .bind(&topic1)
    .bind(&topic2)
    .bind(&topic3)
    .bind(&param)
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
/// they were emitted on-chain. Supports an optional `limit` query parameter
/// (1–1000, default 100).
pub async fn transaction_events(
    State(state): State<AppState>,
    Path(tx_hash): Path<String>,
    Query(q): Query<TxEventsQuery>,
) -> ApiResult<Json<TxEventsResponse>> {
    let limit = q.limit.unwrap_or(100).clamp(1, 1000);

    let events: Vec<EventRow> = sqlx::query_as(
        "SELECT event_id, contract_id, ledger, ledger_closed_at, event_type,
                topics, decoded_topics, event_name, value, decoded_value,
                enriched, tx_hash, in_successful_call, paging_token, created_at
         FROM events
         WHERE tx_hash = $1
           AND ($3::boolean IS NULL OR in_successful_call = $3)
         ORDER BY ledger ASC, event_id ASC
         LIMIT $2",
    )
    .bind(&tx_hash)
    .bind(limit + 1)
    .bind(q.successful_only)
    .fetch_all(&state.pool)
    .await?;

    let has_more = events.len() as i64 > limit;
    let result_events = if has_more {
        events.into_iter().take(limit as usize).collect()
    } else {
        events
    };

    Ok(Json(TxEventsResponse {
        tx_hash,
        count: result_events.len(),
        has_more,
        data: result_events,
    }))
}

#[derive(Deserialize)]
pub struct TxEventsQuery {
    /// Maximum events to return (1–1000, default 100).
    limit: Option<i64>,
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
    /// The event rows, ordered by ledger and event_id ascending.
    pub data: Vec<EventRow>,
}

#[cfg(test)]
mod tests {
    //! Needs a migrated throwaway database. Run with:
    //!
    //!   TEST_DATABASE_URL=postgres://… cargo test -p lumenqraph-api -- --ignored

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::get;
    use axum::Router;
    use serde_json::Value;
    use sqlx::PgPool;
    use tower::ServiceExt;

    use super::list_events;
    use crate::state::test_state;

    const CONTRACT: &str = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM";
    const EVENT_ID: &str = "api-test-list-events-topics";

    async fn count_events(app: &Router, query: &str) -> usize {
        let req = Request::builder()
            .method("GET")
            .uri(format!("/contracts/{CONTRACT}/events?{query}"))
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "query `{query}` must not fail");

        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        body["data"].as_array().expect("`data` array").len()
    }

    #[tokio::test]
    #[ignore]
    async fn list_events_returns_200_with_and_without_topic_filters() {
        let url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL");
        let pool = PgPool::connect(&url).await.unwrap();

        sqlx::query("DELETE FROM events WHERE event_id = $1")
            .bind(EVENT_ID)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO events
                (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
                 decoded_topics, event_name, value, tx_hash, in_successful_call, paging_token)
             VALUES ($1, $2, 1, now(), 'contract', '[]',
                     '[\"transfer\", \"GABC\"]', 'transfer', '', 'tx', true, 'p1')",
        )
        .bind(EVENT_ID)
        .bind(CONTRACT)
        .execute(&pool)
        .await
        .unwrap();

        let app = Router::new()
            .route("/contracts/:contract_id/events", get(list_events))
            .with_state(test_state(pool.clone()));

        assert_eq!(count_events(&app, "").await, 1);
        assert_eq!(count_events(&app, "topic0=%22transfer%22").await, 1);
        assert_eq!(count_events(&app, "topic1=%22GABC%22").await, 1);
        assert_eq!(count_events(&app, "topic1=%22NOPE%22").await, 0);
        assert_eq!(count_events(&app, "topic2=%22GABC%22").await, 0);

        sqlx::query("DELETE FROM events WHERE event_id = $1")
            .bind(EVENT_ID)
            .execute(&pool)
            .await
            .unwrap();
    }
}
