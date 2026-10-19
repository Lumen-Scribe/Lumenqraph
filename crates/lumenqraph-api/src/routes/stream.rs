//! `GET /contracts/:contract_id/events/stream` — Server-Sent Events (SSE) for
//! real-time push of new events as they're indexed.
//!
//! Clients connect to the stream and receive new events via SSE, with cursor-based
//! resume functionality to tail from a specific event sequence number.

use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, Sse};
use futures::stream::{self, Stream};
use serde::{Deserialize, Serialize};
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

pub async fn stream_events(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Query(q): Query<StreamQuery>,
) -> ApiResult<Sse<impl Stream<Item = Result<Event, Infallible>>>> {
    // Validate poll interval (min 1 second, max 60 seconds)
    let poll_secs = q.poll_interval.clamp(1, 60);

    info!(
        contract_id = %contract_id,
        event_name = ?q.event_name,
        cursor = ?q.cursor,
        poll_secs,
        "starting event stream"
    );

    // Create the stream
    let stream = stream::unfold(
        (state, contract_id, q.event_name, q.cursor.unwrap_or(0)),
        move |(state, contract_id, event_name, mut last_ledger)| async move {
            loop {
                match fetch_new_events(&state, &contract_id, &event_name, last_ledger).await {
                    Ok(events) => {
                        for event in events {
                            let event_json = serde_json::to_string(&event).ok()?;
                            last_ledger = event.ledger;
                            return Some((
                                Ok(Event::default().data(event_json)),
                                (state.clone(), contract_id.clone(), event_name.clone(), last_ledger),
                            ));
                        }
                        // No new events, wait before polling again
                        sleep(Duration::from_secs(poll_secs)).await;
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "error fetching events");
                        return Some((
                            Ok(Event::default().comment(format!("error: {}", e))),
                            (state.clone(), contract_id.clone(), event_name.clone(), last_ledger),
                        ));
                    }
                }
            }
        },
    );

    Ok(Sse::new(stream))
}

async fn fetch_new_events(
    state: &AppState,
    contract_id: &str,
    event_name: &Option<String>,
    cursor: i64,
) -> ApiResult<Vec<StreamEvent>> {
    // Query for new events since the cursor ledger
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
           AND ledger > $3
         ORDER BY ledger ASC, event_id ASC
         LIMIT 100",
    )
    .bind(contract_id)
    .bind(event_name)
    .bind(cursor)
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
