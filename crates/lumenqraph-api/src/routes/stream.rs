//! `GET /contracts/:contract_id/events/stream` — Server-Sent Events (SSE) for
//! real-time push of new events as they're indexed.
//!
//! Clients connect to the stream and receive new events via SSE, with cursor-based
//! resume functionality to tail from a specific event sequence number.

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures::stream::{self, Stream};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::convert::Infallible;
use std::time::Duration;
use tokio::time::{interval, sleep};
use tracing::info;

use crate::error::{ApiError, ApiResult};
use crate::state::AppState;

#[derive(Deserialize)]
pub struct StreamQuery {
    /// Optional filter by event name (e.g., `?event_name=transfer`).
    event_name: Option<String>,
    /// Optional cursor to resume from. Events with ledger > cursor will be sent.
    cursor: Option<i64>,
    /// Optional composite cursor to resume from within a ledger. Events with
    /// `(ledger, event_id) > (cursor_ledger, cursor_event_id)` will be sent.
    cursor_event_id: Option<String>,
    /// Poll interval in seconds for checking new events (default: 5).
    #[serde(default = "default_poll_interval")]
    poll_interval: u64,
}

fn default_poll_interval() -> u64 {
    5
}

#[derive(Serialize, Debug)]
struct StreamEvent {
    /// Ledger number where the event occurred.
    pub ledger: i64,
    /// Event ID for deduplication.
    pub event_id: String,
    /// The actual event data.
    pub data: serde_json::Value,
}

/// Composite keyset cursor `(ledger, event_id)` used to resume the stream.
#[derive(Clone, Debug)]
struct Cursor {
    ledger: i64,
    event_id: String,
}

/// Parse a `Last-Event-ID` header value of the form `"<ledger>:<event_id>"`.
fn parse_last_event_id(value: &str) -> Option<Cursor> {
    let (ledger, event_id) = value.split_once(':')?;
    let ledger = ledger.trim().parse::<i64>().ok()?;
    Some(Cursor {
        ledger,
        event_id: event_id.to_string(),
    })
}

pub async fn stream_events(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Query(q): Query<StreamQuery>,
    headers: HeaderMap,
) -> ApiResult<Sse<impl Stream<Item = Result<Event, Infallible>>>> {
    // Validate poll interval (min 1 second, max 60 seconds)
    let poll_secs = q.poll_interval.clamp(1, 60);

    // Resume cursor precedence: the standard `Last-Event-ID` header (sent
    // automatically by `EventSource` on reconnect) takes priority over the
    // explicit `?cursor` query params. When neither is present, start from the
    // current head so a fresh stream tails new events instead of replaying the
    // entire history from ledger 0.
    let initial_cursor = if let Some(cursor) = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(parse_last_event_id)
    {
        cursor
    } else {
        match (q.cursor, q.cursor_event_id.clone()) {
            (Some(ledger), Some(event_id)) => Cursor { ledger, event_id },
            (Some(ledger), None) => Cursor {
                ledger,
                event_id: String::new(),
            },
            _ => current_head(&state, &contract_id).await?,
        }
    };

    info!(
        contract_id = %contract_id,
        event_name = ?q.event_name,
        cursor_ledger = initial_cursor.ledger,
        cursor_event_id = %initial_cursor.event_id,
        poll_secs,
        "starting event stream"
    );

    // Create the stream. The unfold state carries a buffer of already-fetched
    // rows so a single DB query can serve up to its batch size of events.
    let stream = stream::unfold(
        (
            state,
            contract_id,
            q.event_name,
            initial_cursor,
            VecDeque::<StreamEvent>::new(),
        ),
        move |(state, contract_id, event_name, mut cursor, mut buffer)| async move {
            loop {
                // Emit any buffered events before issuing another query.
                if let Some(event) = buffer.pop_front() {
                    cursor = Cursor {
                        ledger: event.ledger,
                        event_id: event.event_id.clone(),
                    };
                    let event_json = serde_json::to_string(&event).ok()?;
                    let sse = Event::default()
                        .id(format!("{}:{}", event.ledger, event.event_id))
                        .data(event_json);
                    return Some((
                        Ok(sse),
                        (state, contract_id, event_name, cursor, buffer),
                    ));
                }

                match fetch_new_events(&state, &contract_id, &event_name, &cursor).await {
                    Ok(events) => {
                        if events.is_empty() {
                            // No new events, wait before polling again
                            sleep(Duration::from_secs(poll_secs)).await;
                        } else {
                            buffer = VecDeque::from(events);
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "error fetching events");
                        return Some((
                            Ok(Event::default().comment(format!("error: {}", e))),
                            (state, contract_id, event_name, cursor, buffer),
                        ));
                    }
                }
            }
        },
    );

    // Emit a keep-alive comment at least every 15 s so idle streams aren't
    // closed by proxies/load balancers with 60–100 s idle timeouts.
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

/// Resolve the current head of the event log for a contract, used as the
/// starting cursor for a fresh stream with no resume position.
async fn current_head(state: &AppState, contract_id: &str) -> ApiResult<Cursor> {
    let row: Option<(i64, String)> = sqlx::query_as(
        "SELECT ledger, event_id
         FROM events
         WHERE contract_id = $1
         ORDER BY ledger DESC, event_id DESC
         LIMIT 1",
    )
    .bind(contract_id)
    .fetch_optional(&state.pool)
    .await?;

    Ok(row
        .map(|(ledger, event_id)| Cursor { ledger, event_id })
        .unwrap_or(Cursor {
            ledger: 0,
            event_id: String::new(),
        }))
}

async fn fetch_new_events(
    state: &AppState,
    contract_id: &str,
    event_name: &Option<String>,
    cursor: &Cursor,
) -> ApiResult<Vec<StreamEvent>> {
    // Query for new events after the composite `(ledger, event_id)` cursor.
    let rows: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT ledger, event_id,
                json_build_object(
                    'event_id', event_id,
                    'contract_id', contract_id,
                    'ledger', ledger,
                    'ledger_closed_at', ledger_closed_at,
                    'event_type', event_type,
                    'topics', topics,
                    'decoded_topics', decoded_topics,
                    'event_name', event_name,
                    'value', value,
                    'decoded_value', decoded_value,
                    'enriched', enriched,
                    'tx_hash', tx_hash,
                    'in_successful_call', in_successful_call,
                    'paging_token', paging_token,
                    'created_at', created_at
                )::text as event_data
         FROM events
         WHERE contract_id = $1
           AND ($2::text IS NULL OR event_name = $2)
           AND (ledger, event_id) > ($3, $4)
         ORDER BY ledger ASC, event_id ASC
         LIMIT 100",
    )
    .bind(contract_id)
    .bind(event_name)
    .bind(cursor.ledger)
    .bind(&cursor.event_id)
    .fetch_all(&state.pool)
    .await?;

    let mut events = Vec::new();
    for (ledger, event_id, event_data) in rows {
        if let Ok(data) = serde_json::from_str(&event_data) {
            events.push(StreamEvent {
                ledger,
                event_id,
                data,
            });
        }
    }

    Ok(events)
}
