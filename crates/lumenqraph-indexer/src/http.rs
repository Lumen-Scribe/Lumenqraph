//! HTTP server for health and metrics endpoints on the indexer.

use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use chrono::Utc;
use sqlx::PgPool;
use std::net::SocketAddr;
use std::sync::Arc;
use serde_json::json;
use tracing::{error, info};

#[derive(Clone)]
pub struct HttpState {
    pub pool: Arc<PgPool>,
}

pub async fn start_http_server(pool: Arc<PgPool>, bind_addr: &str) -> anyhow::Result<()> {
    let state = HttpState { pool };

    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    let addr: SocketAddr = listener.local_addr()?;

    info!(addr = %addr, "indexer health/metrics listener started");

    let server = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async {
        // Never signals shutdown — health/metrics server runs until process dies
        std::future::pending().await
    });

    tokio::spawn(async move {
        if let Err(e) = server.await {
            error!(error = %e, "health/metrics server error");
        }
    });

    Ok(())
}

async fn health(State(state): State<HttpState>) -> impl IntoResponse {
    match get_indexer_status(&state.pool).await {
        Ok(status) => {
            let health_status = if status.is_healthy {
                "ok"
            } else {
                "degraded"
            };
            (
                axum::http::StatusCode::OK,
                Json(json!({
                    "status": health_status,
                    "last_processed_ledger": status.last_processed_ledger,
                    "chain_tip_ledger": status.chain_tip_ledger,
                    "lag_ledgers": status.lag_ledgers,
                    "seconds_since_cursor_update": status.secs_since_update,
                })),
            )
        }
        Err(_) => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "unavailable",
                "error": "failed to fetch indexer status"
            })),
        ),
    }
}

async fn metrics(State(state): State<HttpState>) -> impl IntoResponse {
    let mut body = String::new();

    match gather_metrics(&state.pool).await {
        Ok(metrics) => {
            body.push_str("# HELP lumenqraph_indexer_last_processed_ledger The last ledger processed by the indexer\n");
            body.push_str("# TYPE lumenqraph_indexer_last_processed_ledger gauge\n");
            body.push_str(&format!("lumenqraph_indexer_last_processed_ledger {}\n", metrics.last_processed_ledger));

            body.push_str("# HELP lumenqraph_indexer_chain_tip_ledger The current chain tip ledger\n");
            body.push_str("# TYPE lumenqraph_indexer_chain_tip_ledger gauge\n");
            body.push_str(&format!("lumenqraph_indexer_chain_tip_ledger {}\n", metrics.chain_tip_ledger));

            body.push_str("# HELP lumenqraph_indexer_lag_ledgers Lag between indexer and chain tip in ledgers\n");
            body.push_str("# TYPE lumenqraph_indexer_lag_ledgers gauge\n");
            body.push_str(&format!("lumenqraph_indexer_lag_ledgers {}\n", metrics.lag_ledgers));

            body.push_str("# HELP lumenqraph_indexer_seconds_since_update Seconds since last cursor update\n");
            body.push_str("# TYPE lumenqraph_indexer_seconds_since_update gauge\n");
            body.push_str(&format!("lumenqraph_indexer_seconds_since_update {}\n", metrics.secs_since_update));

            body.push_str("# HELP lumenqraph_indexer_events_ingested_total Total events ingested\n");
            body.push_str("# TYPE lumenqraph_indexer_events_ingested_total counter\n");
            body.push_str(&format!("lumenqraph_indexer_events_ingested_total {}\n", metrics.events_ingested_total));

            body.push_str("# HELP lumenqraph_indexer_errors_total Total indexer errors\n");
            body.push_str("# TYPE lumenqraph_indexer_errors_total counter\n");
            body.push_str(&format!("lumenqraph_indexer_errors_total {}\n", metrics.errors_total));

            body.push_str("# HELP lumenqraph_enrichment_rate Fraction of events successfully enriched (0.0 to 1.0)\n");
            body.push_str("# TYPE lumenqraph_enrichment_rate gauge\n");
            body.push_str(&format!("lumenqraph_enrichment_rate {}\n", metrics.enrichment_rate));
        }
        Err(e) => {
            error!(error = %e, "failed to gather indexer metrics");
        }
    }

    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}

struct IndexerStatus {
    last_processed_ledger: i64,
    chain_tip_ledger: i64,
    lag_ledgers: i64,
    secs_since_update: i64,
    is_healthy: bool,
}

async fn get_indexer_status(pool: &PgPool) -> anyhow::Result<IndexerStatus> {
    let status: Option<(i64, i64, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
        "SELECT last_processed_ledger, chain_tip_ledger, updated_at
         FROM indexer_cursor WHERE id = 1",
    )
    .fetch_optional(pool)
    .await?;

    let Some((last, tip, updated_at)) = status else {
        anyhow::bail!("indexer cursor not initialized");
    };

    let lag_ledgers = (tip - last).max(0);
    let secs_since_update = (Utc::now() - updated_at).num_seconds();

    let max_stale_secs = env_parse_i64("HEALTH_MAX_STALE_SECS", 120);
    let max_lag_ledgers = env_parse_i64("HEALTH_MAX_LAG_LEDGERS", 100);
    let is_healthy = secs_since_update <= max_stale_secs && lag_ledgers < max_lag_ledgers;

    Ok(IndexerStatus {
        last_processed_ledger: last,
        chain_tip_ledger: tip,
        lag_ledgers,
        secs_since_update,
        is_healthy,
    })
}

struct IndexerMetrics {
    last_processed_ledger: i64,
    chain_tip_ledger: i64,
    lag_ledgers: i64,
    secs_since_update: i64,
    events_ingested_total: i64,
    errors_total: i64,
    enrichment_rate: f64,
}

async fn gather_metrics(pool: &PgPool) -> anyhow::Result<IndexerMetrics> {
    let status: Option<(i64, i64, i64, i64, i64, i64, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
        "SELECT last_processed_ledger, chain_tip_ledger, events_ingested_total, errors_total, events_enriched_total, events_not_enriched_total, updated_at
         FROM indexer_cursor WHERE id = 1",
    )
    .fetch_optional(pool)
    .await?;

    let Some((last, tip, events, errors, enriched, not_enriched, updated_at)) = status else {
        anyhow::bail!("indexer cursor not initialized");
    };

    let lag_ledgers = (tip - last).max(0);
    let secs_since_update = (Utc::now() - updated_at).num_seconds();

    // Calculate enrichment rate
    let total_enriched = enriched + not_enriched;
    let enrichment_rate = if total_enriched > 0 {
        enriched as f64 / total_enriched as f64
    } else {
        1.0 // Default to 1.0 (100%) if no events yet
    };

    Ok(IndexerMetrics {
        last_processed_ledger: last,
        chain_tip_ledger: tip,
        lag_ledgers,
        secs_since_update,
        events_ingested_total: events,
        errors_total: errors,
        enrichment_rate,
    })
}

fn env_parse_i64(key: &str, default: i64) -> i64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}
