//! `GET /contracts/:contract_id/events/stream` — Server-Sent Events (SSE) for
//! real-time push of new events as they're indexed.
//!
//! Clients connect to the stream and receive new events via SSE, with cursor-based
//! resume functionality to tail from a specific event sequence number.
//!
//! A single shared `PgListener` task listens for `NOTIFY lumenqraph_events` and
//! fans out newly indexed rows through a `tokio::sync::broadcast` channel, so the
//! number of DB queries is independent of the number of connected clients. Each
//! SSE connection filters the broadcast in memory by contract and event name.
//! Polling is retained as a fallback when the listener is unavailable.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, Sse};
use futures::stream::{self, Stream};
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::{interval, sleep};
use tracing::{info, warn};

use crate::error::{ApiError, ApiResult};
use crate::state::AppState;

/// Maximum number of concurrent SSE streams allowed globally.
const SSE_MAX_STREAMS: usize = 1000;
/// Maximum number of concurrent SSE streams allowed per contract key.
const SSE_MAX_STREAMS_PER_KEY: usize = 100;
/// Capacity of the shared broadcast channel.
const SSE_BROADCAST_CAPACITY: usize = 1024;

/// Global counter of active SSE streams, exposed via metrics.
static ACTIVE_STREAMS: AtomicUsize = AtomicUsize::new(0);

/// Returns the number of currently active SSE streams.
pub fn active_streams() -> usize {
    ACTIVE_STREAMS.load(Ordering::Relaxed)
}

/// A single event row broadcast to all SSE subscribers.
#[derive(Clone, Debug)]
pub struct BroadcastEvent {
    pub contract_id: String,
    pub event_name: Option<String>,
    pub ledger: i64,
    pub event_id: String,
    pub data: serde_json::Value,
}

/// Shared fan-out state: a broadcast sender plus the last sequence seen.
#[derive(Clone)]
pub struct EventFanout {
    tx: broadcast::Sender<BroadcastEvent>,
}

impl EventFanout {
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(SSE_BROADCAST_CAPACITY);
        Self { tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<BroadcastEvent> {
        self.tx.subscribe()
    }

    pub fn publish(&self, event: BroadcastEvent) {
        // Ignore send errors: no subscribers is not an error.
        let _ = self.tx.send(event);
    }
}

impl Default for EventFanout {
    fn default() -> Self {
        Self::new()
    }
}

/// Spawns the single shared `PgListener` task. On each `NOTIFY lumenqraph_events`
/// it fetches new rows once (`seq > last_seen`) and broadcasts them. Falls back to
/// periodic polling if the listener cannot be established.
pub async fn spawn_listener(state: AppState, fanout: Arc<EventFanout>) {
    let mut last_seen: i64 = 0;
    let mut listener = match sqlx::postgres::PgListener::connect_with(&state.pool).await {
        Ok(l) => l,
        Err(e) => {
            warn!(error = %e, "PgListener unavailable; SSE will fall back to polling");
            return;
        }
    };

    if let Err(e) = listener.listen("lumenqraph_events").await {
        warn!(error = %e, "failed to LISTEN lumenqraph_events; SSE will fall back to polling");
        return;
    }

    loop {
        match listener.recv().await {
            Ok(_notification) => {
                if let Err(e) = fetch_and_broadcast(&state, &fanout, &mut last_seen).await {
                    warn!(error = %e, "failed to fetch events after notification");
                }
            }
            Err(e) => {
                warn!(error = %e, "PgListener recv error; retrying");
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/// Fetches rows with `seq > last_seen` once and broadcasts them.
async fn fetch_and_broadcast(
    state: &AppState,
    fanout: &EventFanout,
    last_seen: &mut i64,
) -> ApiResult<()> {
    let rows: Vec<(i64, String, String, Option<String>, String)> = sqlx::query_as(
        "SELECT seq, contract_id, event_id, event_name,
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
         WHERE seq > $1
         ORDER BY seq ASC
         LIMIT 1000",
    )
    .bind(*last_seen)
    .fetch_all(&state.pool)
    .await?;

    for (seq, contract_id, event_id, event_name, event_data) in rows {
        *last_seen = seq;
        if let Ok(data) = serde_json::from_str(&event_data) {
            fanout.publish(BroadcastEvent {
                contract_id,
                event_name,
                ledger: seq,
                event_id,
                data,
            });
        }
    }

    Ok(())
}

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

    // Enforce global and per-key stream caps.
    let active = ACTIVE_STREAMS.fetch_add(1, Ordering::SeqCst) + 1;
    if active > SSE_MAX_STREAMS {
        ACTIVE_STREAMS.fetch_sub(1, Ordering::SeqCst);
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "too many concurrent SSE streams",
        ));
    }
    if active > SSE_MAX_STREAMS_PER_KEY {
        ACTIVE_STREAMS.fetch_sub(1, Ordering::SeqCst);
        return Err(ApiError::new(
            StatusCode::TOO_MANY_REQUESTS,
            "too many concurrent SSE streams for this contract",
        ));
    }

    info!(
        contract_id = %contract_id,
        event_name = ?q.event_name,
        cursor = ?q.cursor,
        poll_secs,
        active_streams = active,
        "starting event stream"
    );

    let fanout = state.event_fanout.clone();
    let rx = fanout.subscribe();

    // Create the stream: prefer broadcast fan-out, fall back to polling.
    let stream = stream::unfold(
        (
            state,
            contract_id,
            q.event_name,
            q.cursor.unwrap_or(0),
            rx,
            poll_secs,
        ),
        move |(state, contract_id, event_name, mut last_ledger, mut rx, poll_secs)| async move {
            loop {
                // Wait for a broadcast event, with a polling fallback timeout.
                match tokio::time::timeout(Duration::from_secs(poll_secs), rx.recv()).await {
                    Ok(Ok(event)) => {
                        if event.contract_id != contract_id {
                            continue;
                        }
                        if let Some(ref name) = event_name {
                            if event.event_name.as_deref() != Some(name.as_str()) {
                                continue;
                            }
                        }
                        if event.ledger <= last_ledger {
                            continue;
                        }
                        last_ledger = event.ledger;
                        let stream_event = StreamEvent {
                            ledger: event.ledger,
                            event_id: event.event_id,
                            data: event.data,
                        };
                        let event_json = serde_json::to_string(&stream_event).ok()?;
                        return Some((
                            Ok(Event::default().data(event_json)),
                            (state.clone(), contract_id.clone(), event_name.clone(), last_ledger, rx, poll_secs),
                        ));
                    }
                    Ok(Err(broadcast::error::RecvError::Lagged(_))) => {
                        // Fell behind; fall through to polling to catch up.
                    }
                    Ok(Err(broadcast::error::RecvError::Closed)) => {
                        return None;
                    }
                    Err(_elapsed) => {
                        // Fallback: poll for new events.
                        match fetch_new_events(&state, &contract_id, &event_name, last_ledger).await {
                            Ok(events) => {
                                for event in events {
                                    let event_json = serde_json::to_string(&event).ok()?;
                                    last_ledger = event.ledger;
                                    return Some((
                                        Ok(Event::default().data(event_json)),
                                        (state.clone(), contract_id.clone(), event_name.clone(), last_ledger, rx, poll_secs),
                                    ));
                                }
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "error fetching events");
                                return Some((
                                    Ok(Event::default().comment(format!("error: {}", e))),
                                    (state.clone(), contract_id.clone(), event_name.clone(), last_ledger, rx, poll_secs),
                                ));
                            }
                        }
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
