//! `GET /metrics` — Prometheus text exposition. Indexer numbers come from the
//! status row the indexer maintains; API numbers from in-process counters.

use std::sync::atomic::Ordering;

use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;

use crate::error::ApiResult;
use crate::state::AppState;

pub async fn metrics(State(state): State<AppState>) -> ApiResult<impl IntoResponse> {
    let status: Option<(i64, i64, i64, i64)> = sqlx::query_as(
        "SELECT last_processed_ledger, chain_tip_ledger, events_ingested_total, errors_total
         FROM indexer_cursor WHERE id = 1",
    )
    .fetch_optional(&state.pool)
    .await?;

    let total_events: (i64,) = sqlx::query_as("SELECT count(*) FROM events")
        .fetch_one(&state.pool)
        .await?;

    let (last, tip, ingested, errors) = status.unwrap_or((0, 0, 0, 0));
    let lag = (tip - last).max(0);
    let requests = state.http_requests.load(Ordering::Relaxed);

    let body = format!(
        "# HELP lumenqraph_indexer_last_processed_ledger Last ledger the indexer processed\n\
         # TYPE lumenqraph_indexer_last_processed_ledger gauge\n\
         lumenqraph_indexer_last_processed_ledger {last}\n\
         # HELP lumenqraph_indexer_chain_tip_ledger Latest ledger observed on chain\n\
         # TYPE lumenqraph_indexer_chain_tip_ledger gauge\n\
         lumenqraph_indexer_chain_tip_ledger {tip}\n\
         # HELP lumenqraph_indexer_lag_ledgers Ledgers behind the chain tip\n\
         # TYPE lumenqraph_indexer_lag_ledgers gauge\n\
         lumenqraph_indexer_lag_ledgers {lag}\n\
         # HELP lumenqraph_events_total Total events stored\n\
         # TYPE lumenqraph_events_total counter\n\
         lumenqraph_events_total {events}\n\
         # HELP lumenqraph_indexer_ingested_total Events ingested by the indexer\n\
         # TYPE lumenqraph_indexer_ingested_total counter\n\
         lumenqraph_indexer_ingested_total {ingested}\n\
         # HELP lumenqraph_indexer_errors_total Indexer poll-cycle errors\n\
         # TYPE lumenqraph_indexer_errors_total counter\n\
         lumenqraph_indexer_errors_total {errors}\n\
         # HELP lumenqraph_api_requests_total API requests served\n\
         # TYPE lumenqraph_api_requests_total counter\n\
         lumenqraph_api_requests_total {requests}\n",
        last = last,
        tip = tip,
        lag = lag,
        events = total_events.0,
        ingested = ingested,
        errors = errors,
        requests = requests,
    );

    Ok(([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body))
}
