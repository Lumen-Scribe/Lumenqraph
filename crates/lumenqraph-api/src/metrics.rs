//! `GET /metrics` — Prometheus text exposition for the API service.

use std::sync::atomic::Ordering;

use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;

use crate::error::ApiResult;
use crate::metrics_middleware::escape_label_value;
use crate::state::AppState;

pub async fn metrics(State(state): State<AppState>) -> ApiResult<impl IntoResponse> {
    let total_events: (i64,) = sqlx::query_as("SELECT count(*) FROM events")
        .fetch_one(&state.pool)
        .await?;

    let requests = state.http_requests.load(Ordering::Relaxed);
    let audit_dropped = state.audit_dropped.load(Ordering::Relaxed);
    let rate_limit_backend_errors = crate::rate_limit::backend_error_count();

    let cache_hits = state.call_cache.hits();
    let cache_misses = state.call_cache.misses();
    let cache_evictions = state.call_cache.evictions();
    let cache_size = state.call_cache.size();

    let sse_active_streams = crate::routes::stream::active_streams();
    let sse_listener_up = u8::from(state.event_fanout.listener_up());
    let sse_receivers = state.event_fanout.receivers();
    let sse_lagged = state.event_fanout.lagged_total();

    let mut body = format!(
        "# HELP lumenqraph_api_events_stored Current number of stored events\n\
         # TYPE lumenqraph_api_events_stored gauge\n\
         lumenqraph_api_events_stored {events}\n\
         # HELP lumenqraph_api_rate_limit_backend_errors_total Redis rate-limit backend errors (each applies the configured fail mode)\n\
         # TYPE lumenqraph_api_rate_limit_backend_errors_total counter\n\
         lumenqraph_api_rate_limit_backend_errors_total {rate_limit_backend_errors}\n\
         # HELP lumenqraph_api_requests_total API requests served\n\
         # TYPE lumenqraph_api_requests_total counter\n\
         lumenqraph_api_requests_total {requests}\n\
         # HELP lumenqraph_api_audit_dropped_total Audit events dropped because the writer queue was full\n\
         # TYPE lumenqraph_api_audit_dropped_total counter\n\
         lumenqraph_api_audit_dropped_total {audit_dropped}\n\
         # HELP lumenqraph_api_call_cache_hits_total Total cache hits for /call results\n\
         # TYPE lumenqraph_api_call_cache_hits_total counter\n\
         lumenqraph_api_call_cache_hits_total {cache_hits}\n\
         # HELP lumenqraph_api_call_cache_misses_total Total cache misses for /call results\n\
         # TYPE lumenqraph_api_call_cache_misses_total counter\n\
         lumenqraph_api_call_cache_misses_total {cache_misses}\n\
         # HELP lumenqraph_api_call_cache_evictions_total Total entries evicted from /call cache\n\
         # TYPE lumenqraph_api_call_cache_evictions_total counter\n\
         lumenqraph_api_call_cache_evictions_total {cache_evictions}\n\
         # HELP lumenqraph_api_call_cache_size Current number of entries in /call cache\n\
         # TYPE lumenqraph_api_call_cache_size gauge\n\
         lumenqraph_api_call_cache_size {cache_size}\n\
         # HELP lumenqraph_api_sse_active_streams Currently open SSE streams\n\
         # TYPE lumenqraph_api_sse_active_streams gauge\n\
         lumenqraph_api_sse_active_streams {sse_active_streams}\n\
         # HELP lumenqraph_api_sse_listener_up 1 while the shared LISTEN connection is connected\n\
         # TYPE lumenqraph_api_sse_listener_up gauge\n\
         lumenqraph_api_sse_listener_up {sse_listener_up}\n\
         # HELP lumenqraph_api_sse_broadcast_receivers SSE connections subscribed to the fan-out\n\
         # TYPE lumenqraph_api_sse_broadcast_receivers gauge\n\
         lumenqraph_api_sse_broadcast_receivers {sse_receivers}\n\
         # HELP lumenqraph_api_sse_broadcast_lagged_total Events dropped by SSE subscribers that fell behind\n\
         # TYPE lumenqraph_api_sse_broadcast_lagged_total counter\n\
         lumenqraph_api_sse_broadcast_lagged_total {sse_lagged}\n",
        events = total_events.0,
        requests = requests,
        cache_hits = cache_hits,
        cache_misses = cache_misses,
        cache_evictions = cache_evictions,
        cache_size = cache_size,
        sse_active_streams = sse_active_streams,
        sse_listener_up = sse_listener_up,
        sse_receivers = sse_receivers,
        sse_lagged = sse_lagged,
    );

    body.push_str(
        "# HELP lumenqraph_http_request_duration_seconds Per-route HTTP request latency\n",
    );
    body.push_str("# TYPE lumenqraph_http_request_duration_seconds histogram\n");
    {
        let series = state.metrics.series.read();
        for (route, by_method) in series.iter() {
            for (method, metrics) in by_method.iter() {
                let hist = metrics.latency.snapshot();
                let labels = format!(
                    "route=\"{}\",method=\"{}\"",
                    escape_label_value(route),
                    escape_label_value(method)
                );
                for (bound, cumulative) in hist.cumulative() {
                    body.push_str(&format!(
                        "lumenqraph_http_request_duration_seconds_bucket{{{labels},le=\"{bound}\"}} {cumulative}\n"
                    ));
                }
                body.push_str(&format!(
                    "lumenqraph_http_request_duration_seconds_bucket{{{labels},le=\"+Inf\"}} {}\n",
                    hist.count
                ));
                body.push_str(&format!(
                    "lumenqraph_http_request_duration_seconds_sum{{{labels}}} {}\n",
                    hist.sum_seconds
                ));
                body.push_str(&format!(
                    "lumenqraph_http_request_duration_seconds_count{{{labels}}} {}\n",
                    hist.count
                ));
            }
        }
    }

    Ok(([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body))
}
