//! `GET /metrics` — Prometheus text exposition. Indexer numbers come from the
//! status row the indexer maintains; API numbers from in-process counters.

use std::sync::atomic::Ordering;

use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;

use crate::error::ApiResult;
use crate::state::AppState;

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64) * p / 100.0).ceil() as usize;
    sorted[idx.saturating_sub(1)]
}

pub async fn metrics(State(state): State<AppState>) -> ApiResult<impl IntoResponse> {
    let status: Option<(i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64)> = sqlx::query_as(
        "SELECT last_processed_ledger, chain_tip_ledger, events_ingested_total, errors_total,
                events_enriched_total, events_not_enriched_total, spec_fetch_failures_total,
                rpc_calls_total, rpc_errors_total, rpc_errors_32001_total, consecutive_errors
         FROM indexer_cursor WHERE id = 1",
    )
    .fetch_optional(&state.pool)
    .await?;

    let total_events: (i64,) = sqlx::query_as("SELECT count(*) FROM events")
        .fetch_one(&state.pool)
        .await?;

    let (last, tip, ingested, errors, enriched, not_enriched, spec_fetch_failures,
         rpc_calls, rpc_errors, rpc_errors_32001, consecutive_errors) = status.unwrap_or((0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
    let lag = (tip - last).max(0);
    let lag_time_secs = lag * 5; // Approximate ~5 seconds per ledger
    let requests = state.http_requests.load(Ordering::Relaxed);

    let cache_hits = state.call_cache.hits();
    let cache_misses = state.call_cache.misses();
    let cache_evictions = state.call_cache.evictions();
    let cache_size = state.call_cache.size();

    let mut body = format!(
        "# HELP lumenqraph_indexer_last_processed_ledger Last ledger the indexer processed\n\
         # TYPE lumenqraph_indexer_last_processed_ledger gauge\n\
         lumenqraph_indexer_last_processed_ledger {last}\n\
         # HELP lumenqraph_indexer_chain_tip_ledger Latest ledger observed on chain\n\
         # TYPE lumenqraph_indexer_chain_tip_ledger gauge\n\
         lumenqraph_indexer_chain_tip_ledger {tip}\n\
         # HELP lumenqraph_indexer_lag_ledgers Ledgers behind the chain tip\n\
         # TYPE lumenqraph_indexer_lag_ledgers gauge\n\
         lumenqraph_indexer_lag_ledgers {lag}\n\
         # HELP lumenqraph_indexer_lag_seconds Estimated time behind the chain tip in seconds\n\
         # TYPE lumenqraph_indexer_lag_seconds gauge\n\
         lumenqraph_indexer_lag_seconds {lag_time_secs}\n\
         # HELP lumenqraph_events_total Total events stored\n\
         # TYPE lumenqraph_events_total counter\n\
         lumenqraph_events_total {events}\n\
         # HELP lumenqraph_indexer_ingested_total Events ingested by the indexer\n\
         # TYPE lumenqraph_indexer_ingested_total counter\n\
         lumenqraph_indexer_ingested_total {ingested}\n\
         # HELP lumenqraph_indexer_errors_total Indexer poll-cycle errors\n\
         # TYPE lumenqraph_indexer_errors_total counter\n\
         lumenqraph_indexer_errors_total {errors}\n\
         # HELP lumenqraph_consecutive_errors Current number of consecutive poll-cycle failures (circuit breaker gauge; resets to 0 on success)\n\
         # TYPE lumenqraph_consecutive_errors gauge\n\
         lumenqraph_consecutive_errors {consecutive_errors}\n\
         # HELP lumenqraph_events_enriched_total Events successfully enriched with spec data\n\
         # TYPE lumenqraph_events_enriched_total counter\n\
         lumenqraph_events_enriched_total {enriched}\n\
         # HELP lumenqraph_events_not_enriched_total Events without matching spec (fallback to decoded)\n\
         # TYPE lumenqraph_events_not_enriched_total counter\n\
         lumenqraph_events_not_enriched_total {not_enriched}\n\
         # HELP lumenqraph_spec_fetch_failures_total Failed attempts to fetch contract specs\n\
         # TYPE lumenqraph_spec_fetch_failures_total counter\n\
         lumenqraph_spec_fetch_failures_total {spec_fetch_failures}\n\
         # HELP lumenqraph_rpc_calls_total Total RPC method calls made\n\
         # TYPE lumenqraph_rpc_calls_total counter\n\
         lumenqraph_rpc_calls_total {rpc_calls}\n\
         # HELP lumenqraph_rpc_errors_total RPC errors encountered\n\
         # TYPE lumenqraph_rpc_errors_total counter\n\
         lumenqraph_rpc_errors_total {rpc_errors}\n\
         # HELP lumenqraph_rpc_errors_32001_total RPC -32001 processing-limit errors\n\
         # TYPE lumenqraph_rpc_errors_32001_total counter\n\
         lumenqraph_rpc_errors_32001_total {rpc_errors_32001}\n\
         # HELP lumenqraph_api_requests_total API requests served\n\
         # TYPE lumenqraph_api_requests_total counter\n\
         lumenqraph_api_requests_total {requests}\n\
         # HELP lumenqraph_call_cache_hits_total Total cache hits for /call results\n\
         # TYPE lumenqraph_call_cache_hits_total counter\n\
         lumenqraph_call_cache_hits_total {cache_hits}\n\
         # HELP lumenqraph_call_cache_misses_total Total cache misses for /call results\n\
         # TYPE lumenqraph_call_cache_misses_total counter\n\
         lumenqraph_call_cache_misses_total {cache_misses}\n\
         # HELP lumenqraph_call_cache_evictions_total Total entries evicted from /call cache\n\
         # TYPE lumenqraph_call_cache_evictions_total counter\n\
         lumenqraph_call_cache_evictions_total {cache_evictions}\n\
         # HELP lumenqraph_call_cache_size Current number of entries in /call cache\n\
         # TYPE lumenqraph_call_cache_size gauge\n\
         lumenqraph_call_cache_size {cache_size}\n",
        last = last,
        tip = tip,
        lag = lag,
        lag_time_secs = lag_time_secs,
        events = total_events.0,
        ingested = ingested,
        errors = errors,
        consecutive_errors = consecutive_errors,
        enriched = enriched,
        not_enriched = not_enriched,
        spec_fetch_failures = spec_fetch_failures,
        rpc_calls = rpc_calls,
        rpc_errors = rpc_errors,
        rpc_errors_32001 = rpc_errors_32001,
        requests = requests,
        cache_hits = cache_hits,
        cache_misses = cache_misses,
        cache_evictions = cache_evictions,
        cache_size = cache_size,
    );

    body.push_str("# HELP lumenqraph_http_request_duration_ms Per-route HTTP request latency\n");
    body.push_str("# TYPE lumenqraph_http_request_duration_ms histogram\n");
    {
        let histograms = state.metrics.histogram_buckets.read();
        for (key, samples) in histograms.iter() {
            if samples.is_empty() {
                continue;
            }
            let mut sorted = samples.clone();
            sorted.sort_unstable();

            let p50 = percentile(&sorted, 50.0);
            let p95 = percentile(&sorted, 95.0);
            let p99 = percentile(&sorted, 99.0);
            let count = sorted.len();
            let sum: u64 = sorted.iter().sum();

            body.push_str(&format!("{key}_bucket{{le=\"0.001\"}} 0\n"));
            body.push_str(&format!("{key}_bucket{{le=\"0.005\"}} 0\n"));
            body.push_str(&format!("{key}_bucket{{le=\"0.01\"}} 0\n"));
            body.push_str(&format!("{key}_bucket{{le=\"0.05\"}} 0\n"));
            body.push_str(&format!("{key}_bucket{{le=\"0.1\"}} 0\n"));
            body.push_str(&format!("{key}_bucket{{le=\"0.5\"}} 0\n"));
            body.push_str(&format!("{key}_bucket{{le=\"1.0\"}} 0\n"));
            body.push_str(&format!("{key}_bucket{{le=\"5.0\"}} 0\n"));
            body.push_str(&format!("{key}_bucket{{le=\"10.0\"}} 0\n"));
            body.push_str(&format!("{key}_bucket{{le=\"+Inf\"}} {count}\n"));
            body.push_str(&format!("{key}_count {count}\n"));
            body.push_str(&format!("{key}_sum {sum}\n"));
            body.push_str(&format!("{key}_p50 {p50}\n"));
            body.push_str(&format!("{key}_p95 {p95}\n"));
            body.push_str(&format!("{key}_p99 {p99}\n"));
        }
    }

    body.push_str("# HELP lumenqraph_http_request_status Per-route HTTP request status codes\n");
    body.push_str("# TYPE lumenqraph_http_request_status counter\n");
    {
        let counters = state.metrics.status_counters.read();
        for (key, count) in counters.iter() {
            body.push_str(&format!("{key} {count}\n"));
        }
    }

    Ok(([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body))
}
