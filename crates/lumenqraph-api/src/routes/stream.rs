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

use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::Extension;
use futures::stream::{self, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio::time::{interval, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::error::ApiResult;
use crate::extract::ValidContractId;
use crate::request_id::RequestId;
use crate::state::AppState;

/// Maximum number of concurrent SSE streams allowed globally.
const SSE_MAX_STREAMS: usize = 1000;
/// Maximum number of concurrent SSE streams allowed per contract key.
const SSE_MAX_STREAMS_PER_KEY: usize = 100;
/// Capacity of the shared broadcast channel.
const SSE_BROADCAST_CAPACITY: usize = 1024;
/// Rows fetched per catch-up query on the listener.
const CATCH_UP_BATCH: i64 = 1000;
/// Reconnect delay for the shared listener: starts at the minimum and doubles
/// up to the maximum. A connection that stays up for `LISTENER_STABLE_AFTER`
/// resets it.
const LISTENER_MIN_BACKOFF: Duration = Duration::from_secs(1);
const LISTENER_MAX_BACKOFF: Duration = Duration::from_secs(30);
const LISTENER_STABLE_AFTER: Duration = Duration::from_secs(30);

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

/// Shared fan-out: one broadcast channel that every SSE connection subscribes to,
/// plus health counters for the listener feeding it.
pub struct EventFanout {
    tx: broadcast::Sender<BroadcastEvent>,
    /// True while the shared listener holds a `LISTEN` connection. While it is
    /// false, SSE connections poll Postgres themselves so no events are missed.
    listener_up: AtomicBool,
    /// Messages dropped by slow subscribers (`RecvError::Lagged`), summed.
    lagged: AtomicU64,
}

impl EventFanout {
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(SSE_BROADCAST_CAPACITY);
        Self {
            tx,
            listener_up: AtomicBool::new(false),
            lagged: AtomicU64::new(0),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<BroadcastEvent> {
        self.tx.subscribe()
    }

    pub fn publish(&self, event: BroadcastEvent) {
        // Ignore send errors: no subscribers is not an error.
        let _ = self.tx.send(event);
    }

    pub fn set_listener_up(&self, up: bool) {
        self.listener_up.store(up, Ordering::Relaxed);
    }

    pub fn listener_up(&self) -> bool {
        self.listener_up.load(Ordering::Relaxed)
    }

    fn record_lag(&self, dropped: u64) {
        self.lagged.fetch_add(dropped, Ordering::Relaxed);
    }

    /// Total messages dropped by lagging subscribers since startup.
    pub fn lagged_total(&self) -> u64 {
        self.lagged.load(Ordering::Relaxed)
    }

    /// Number of SSE connections currently subscribed.
    pub fn receivers(&self) -> usize {
        self.tx.receiver_count()
    }
}

impl Default for EventFanout {
    fn default() -> Self {
        Self::new()
    }
}

/// Starts the single shared listener task for the process.
pub fn spawn_listener(state: AppState, fanout: Arc<EventFanout>) {
    tokio::spawn(run_listener(state, fanout));
}

/// Keeps one `LISTEN lumenqraph_events` connection open, reconnecting with
/// backoff. Each notification triggers a catch-up read of `seq > last_seen`.
async fn run_listener(state: AppState, fanout: Arc<EventFanout>) {
    // `None` until the first successful LISTEN, so the first catch-up starts at the head.
    let mut last_seen: Option<i64> = None;
    let mut backoff = LISTENER_MIN_BACKOFF;
    loop {
        let started = Instant::now();
        let result = listen_once(&state, &fanout, &mut last_seen).await;
        fanout.set_listener_up(false);
        if let Err(e) = result {
            warn!(error = %e, "SSE listener lost; SSE clients fall back to polling");
        }
        if started.elapsed() >= LISTENER_STABLE_AFTER {
            backoff = LISTENER_MIN_BACKOFF;
        }
        sleep(backoff).await;
        backoff = (backoff * 2).min(LISTENER_MAX_BACKOFF);
    }
}

/// One listener session. Returns only when the connection fails.
async fn listen_once(
    state: &AppState,
    fanout: &EventFanout,
    last_seen: &mut Option<i64>,
) -> ApiResult<()> {
    let mut listener = sqlx::postgres::PgListener::connect_with(&state.pool).await?;
    listener.listen("lumenqraph_events").await?;
    let mut cursor = match *last_seen {
        Some(seq) => seq,
        None => head_seq(state).await?,
    };
    fanout.set_listener_up(true);
    // Events committed while we were disconnected are picked up here.
    catch_up(state, fanout, &mut cursor).await?;
    *last_seen = Some(cursor);
    loop {
        listener.recv().await?;
        catch_up(state, fanout, &mut cursor).await?;
        *last_seen = Some(cursor);
    }
}

/// Highest committed `seq`, or 0 for an empty log.
async fn head_seq(state: &AppState) -> ApiResult<i64> {
    let head: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(seq), 0) FROM events")
        .fetch_one(&state.pool)
        .await?;
    Ok(head)
}

/// Broadcast every row after `last_seen`, in batches, until none are left.
async fn catch_up(state: &AppState, fanout: &EventFanout, last_seen: &mut i64) -> ApiResult<()> {
    loop {
        let fetched = fetch_and_broadcast(state, fanout, last_seen).await?;
        if fetched < CATCH_UP_BATCH as usize {
            return Ok(());
        }
    }
}

/// Fetch up to `CATCH_UP_BATCH` rows with `seq > last_seen` and broadcast them.
/// Returns how many rows were read.
async fn fetch_and_broadcast(
    state: &AppState,
    fanout: &EventFanout,
    last_seen: &mut i64,
) -> ApiResult<usize> {
    let rows: Vec<(i64, i64, String, String, Option<String>, String)> = sqlx::query_as(
        "SELECT seq, ledger, contract_id, event_id, event_name,
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
         LIMIT $2",
    )
    .bind(*last_seen)
    .bind(CATCH_UP_BATCH)
    .fetch_all(&state.pool)
    .await?;

    let fetched = rows.len();
    for (seq, ledger, contract_id, event_id, event_name, event_data) in rows {
        *last_seen = seq;
        if let Ok(data) = serde_json::from_str(&event_data) {
            fanout.publish(BroadcastEvent {
                contract_id,
                event_name,
                ledger,
                event_id,
                data,
            });
        }
    }

    Ok(fetched)
}

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

/// Per-connection state carried through the `stream::unfold` loop.
struct StreamState {
    state: AppState,
    contract_id: String,
    event_name: Option<String>,
    cursor: Cursor,
    buffer: VecDeque<StreamEvent>,
    rx: broadcast::Receiver<BroadcastEvent>,
    fanout: Arc<EventFanout>,
    poll_secs: u64,
    last_ledger: i64,
    request_id: String,
}

impl StreamState {
    /// Advance the cursor to the given event.
    fn advance(&mut self, event: &StreamEvent) {
        self.cursor = Cursor {
            ledger: event.ledger,
            event_id: event.event_id.clone(),
        };
        self.last_ledger = event.ledger;
    }

    /// Build an SSE event with the `id:` field set for `Last-Event-ID` resume.
    fn sse_event(event: &StreamEvent) -> Option<Event> {
        let event_json = serde_json::to_string(event).ok()?;
        Some(
            Event::default()
                .id(format!("{}:{}", event.ledger, event.event_id))
                .data(event_json),
        )
    }
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

/// Build the SSE `error` frame sent when a poll fails. It carries only a generic
/// code and the request ID; driver error text is logged server-side and never
/// reaches the client.
fn error_frame_data(request_id: &str) -> String {
    json!({ "code": "internal_error", "request_id": request_id }).to_string()
}

/// Log a failed poll, back off for one poll interval so a broken database is not
/// retried in a hot loop, and return the sanitized error frame.
async fn fetch_error_event(e: &crate::error::ApiError, request_id: &str, poll_secs: u64) -> Event {
    tracing::error!(error = %e, request_id, "error fetching events");
    sleep(Duration::from_secs(poll_secs)).await;
    Event::default()
        .event("error")
        .data(error_frame_data(request_id))
}

pub async fn stream_events(
    State(state): State<AppState>,
    ValidContractId(contract_id): ValidContractId,
    Query(q): Query<StreamQuery>,
    headers: HeaderMap,
    request_id: Option<Extension<RequestId>>,
) -> ApiResult<Sse<impl Stream<Item = Result<Event, Infallible>>>> {
    let request_id = request_id
        .map(|Extension(id)| id.0.to_string())
        .unwrap_or_default();
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
        active_streams = active_streams(),
        "starting event stream"
    );

    // Subscribe to the shared fan-out before constructing the stream so no
    // events published after this point are missed.
    let rx = state.event_fanout.subscribe();
    let shutdown = state.shutdown.clone();
    let last_ledger = initial_cursor.ledger;

    let initial = StreamState {
        state,
        contract_id,
        event_name: q.event_name,
        cursor: initial_cursor,
        buffer: VecDeque::new(),
        rx,
        fanout,
        poll_secs,
        last_ledger,
        request_id,
    };

    // Create the stream. The unfold state carries a buffer of already-fetched
    // rows so a single DB query can serve up to its batch size of events.
    let stream = stream::unfold(initial, move |mut st| async move {
        loop {
            // Emit any buffered events before issuing another query.
            if let Some(event) = st.buffer.pop_front() {
                st.advance(&event);
                let sse = StreamState::sse_event(&event)?;
                return Some((Ok(sse), st));
            }

            // Wait for a broadcast notification, with a polling timeout as a
            // fallback when the listener is unavailable or the receiver lags.
            match tokio::time::timeout(Duration::from_secs(st.poll_secs), st.rx.recv()).await {
                Ok(Ok(broadcast_event)) => {
                    // Filter the broadcast in memory by contract and event name.
                    if broadcast_event.contract_id != st.contract_id {
                        continue;
                    }
                    if let Some(ref name) = st.event_name {
                        if broadcast_event.event_name.as_deref() != Some(name.as_str()) {
                            continue;
                        }
                    }
                    let event = StreamEvent {
                        ledger: broadcast_event.ledger,
                        event_id: broadcast_event.event_id,
                        data: broadcast_event.data,
                    };
                    // Skip events already delivered (resume / duplicate notify).
                    if (event.ledger, event.event_id.as_str())
                        <= (st.cursor.ledger, st.cursor.event_id.as_str())
                    {
                        continue;
                    }
                    st.advance(&event);
                    let sse = StreamState::sse_event(&event)?;
                    return Some((Ok(sse), st));
                }
                Ok(Err(broadcast::error::RecvError::Lagged(dropped))) => {
                    // Fell behind; fall through to polling to catch up.
                    st.fanout.record_lag(dropped);
                    match fetch_new_events(
                        &st.state,
                        &st.contract_id,
                        &st.event_name,
                        &st.cursor,
                    )
                    .await
                    {
                        Ok(events) => {
                            if events.is_empty() {
                                sleep(Duration::from_secs(st.poll_secs)).await;
                            } else {
                                st.buffer = VecDeque::from(events);
                            }
                        }
                        Err(e) => {
                            let frame =
                                fetch_error_event(&e, &st.request_id, st.poll_secs).await;
                            return Some((Ok(frame), st));
                        }
                    }
                }
                Ok(Err(broadcast::error::RecvError::Closed)) => {
                    // The fan-out was dropped; end the stream.
                    return None;
                }
                Err(_elapsed) => {
                    // Quiet period. While the shared listener is up it wakes us
                    // on every new event, so idle streams issue no queries.
                    if st.fanout.listener_up() {
                        continue;
                    }
                    // Fallback: poll for new events.
                    match fetch_new_events(
                        &st.state,
                        &st.contract_id,
                        &st.event_name,
                        &st.cursor,
                    )
                    .await
                    {
                        Ok(events) => {
                            if events.is_empty() {
                                sleep(Duration::from_secs(st.poll_secs)).await;
                            } else {
                                st.buffer = VecDeque::from(events);
                            }
                        }
                        Err(e) => {
                            let frame =
                                fetch_error_event(&e, &st.request_id, st.poll_secs).await;
                            return Some((Ok(frame), st));
                        }
                    }
                }
            }
        }
    });

    let stream = end_on_shutdown(stream, shutdown);

    // Emit a keep-alive comment at least every 15 s so idle streams aren't
    // closed by proxies/load balancers with 60–100 s idle timeouts.
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

/// Ends an SSE stream once the server starts shutting down. A final
/// `event: shutdown` frame is sent first so the client reconnects, with
/// `Last-Event-ID`, to another instance. Without this, open streams keep
/// graceful shutdown waiting until the hard timeout (#571).
fn end_on_shutdown<S>(
    events: S,
    shutdown: CancellationToken,
) -> impl Stream<Item = Result<Event, Infallible>>
where
    S: Stream<Item = Result<Event, Infallible>>,
{
    events
        .take_until(async move { shutdown.cancelled().await })
        .chain(stream::once(async {
            Ok(Event::default()
                .event("shutdown")
                .data(json!({ "reason": "server_shutdown" }).to_string()))
        }))
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

#[cfg(test)]
mod tests {
    use super::*;

    /// #570 — SSE error frames must not leak driver or infrastructure error text.
    #[test]
    fn error_frame_carries_only_code_and_request_id() {
        let data: serde_json::Value =
            serde_json::from_str(&error_frame_data("req-123")).unwrap();
        assert_eq!(data["code"], "internal_error");
        assert_eq!(data["request_id"], "req-123");
        assert_eq!(data.as_object().unwrap().len(), 2);
    }

    /// #571 — an open stream ends with a single shutdown frame once the token is
    /// cancelled, instead of holding the server open.
    #[tokio::test]
    async fn stream_ends_with_one_shutdown_frame_when_cancelled() {
        let shutdown = CancellationToken::new();
        let open_stream = stream::pending::<Result<Event, Infallible>>();
        let mut ended = Box::pin(end_on_shutdown(open_stream, shutdown.clone()));

        shutdown.cancel();

        let frames: Vec<_> = tokio::time::timeout(Duration::from_secs(1), ended.collect())
            .await
            .expect("stream should end promptly after cancellation");
        assert_eq!(frames.len(), 1);
    }

    #[test]
    fn error_frame_never_contains_driver_error_text() {
        let driver_error = "connection to server at 10.0.3.4 failed: relation \"events\" does not exist";
        let data = error_frame_data("req-456");
        assert!(!data.contains("10.0.3.4"));
        assert!(!data.contains("relation"));
        assert!(!data.contains(driver_error));
    }
}
