//! Webhook metrics — backlog depth, delivery rate, and failure statistics.
//!
//! Exposes a `/metrics` endpoint on its own port for Prometheus scraping.

use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router, Server};
use chrono::Utc;
use serde_json::json;
use sqlx::PgPool;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info};

#[derive(Clone)]
pub struct MetricsState {
    pub pool: Arc<PgPool>,
}

pub async fn start_metrics_server(pool: Arc<PgPool>, bind_addr: &str) -> anyhow::Result<()> {
    let state = MetricsState { pool };

    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    let addr: SocketAddr = listener.local_addr()?;

    info!(addr = %addr, "webhook metrics listener started");

    let server = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(async {
        // Never signals shutdown — metrics server runs until process dies
        std::future::pending().await
    });

    tokio::spawn(async move {
        if let Err(e) = server.await {
            error!(error = %e, "metrics server error");
        }
    });

    Ok(())
}

async fn health(State(state): State<MetricsState>) -> impl IntoResponse {
    match gather_metrics(&state.pool).await {
        Ok(metrics) => {
            let health_status = if metrics.pending_count < 1000 {
                "ok"
            } else {
                "degraded"
            };
            (
                axum::http::StatusCode::OK,
                Json(json!({
                    "status": health_status,
                    "pending_count": metrics.pending_count,
                    "oldest_pending_age_seconds": metrics.oldest_pending_age_secs,
                    "failed_count": metrics.failed_count,
                })),
            )
        }
        Err(_) => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "unavailable",
                "error": "failed to fetch webhook metrics"
            })),
        ),
    }
}

async fn metrics(State(state): State<MetricsState>) -> impl IntoResponse {
    let mut body = String::new();

    match gather_metrics(&state.pool).await {
        Ok(metrics) => {
            body.push_str("# HELP lumenqraph_webhook_pending_backlog Pending webhook deliveries waiting to be sent\n");
            body.push_str("# TYPE lumenqraph_webhook_pending_backlog gauge\n");
            body.push_str(&format!("lumenqraph_webhook_pending_backlog {}\n", metrics.pending_count));

            body.push_str("# HELP lumenqraph_webhook_oldest_pending_age_seconds Age of oldest pending webhook delivery\n");
            body.push_str("# TYPE lumenqraph_webhook_oldest_pending_age_seconds gauge\n");
            body.push_str(&format!(
                "lumenqraph_webhook_oldest_pending_age_seconds {}\n",
                metrics.oldest_pending_age_secs
            ));

            body.push_str("# HELP lumenqraph_webhook_failed_total Terminal failed webhook deliveries\n");
            body.push_str("# TYPE lumenqraph_webhook_failed_total counter\n");
            body.push_str(&format!(
                "lumenqraph_webhook_failed_total {}\n",
                metrics.failed_count
            ));

            body.push_str("# HELP lumenqraph_webhook_delivered_total Total delivered webhook deliveries\n");
            body.push_str("# TYPE lumenqraph_webhook_delivered_total counter\n");
            body.push_str(&format!(
                "lumenqraph_webhook_delivered_total {}\n",
                metrics.delivered_count
            ));

            body.push_str("# HELP lumenqraph_webhook_delivered_last_min Last minute delivered webhooks\n");
            body.push_str("# TYPE lumenqraph_webhook_delivered_last_min gauge\n");
            body.push_str(&format!(
                "lumenqraph_webhook_delivered_last_min {}\n",
                metrics.delivered_last_min
            ));

            body.push_str("# HELP lumenqraph_webhook_failed_last_min Last minute failed webhooks\n");
            body.push_str("# TYPE lumenqraph_webhook_failed_last_min gauge\n");
            body.push_str(&format!(
                "lumenqraph_webhook_failed_last_min {}\n",
                metrics.failed_last_min
            ));
        }
        Err(e) => {
            error!(error = %e, "failed to gather webhook metrics");
        }
    }

    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}

struct WebhookMetrics {
    pending_count: i64,
    oldest_pending_age_secs: i64,
    failed_count: i64,
    delivered_count: i64,
    delivered_last_min: i64,
    failed_last_min: i64,
}

async fn gather_metrics(pool: &PgPool) -> anyhow::Result<WebhookMetrics> {
    let pending: (i64, Option<i64>) = sqlx::query_as(
        "SELECT COUNT(*), EXTRACT(EPOCH FROM (now() - min(created_at)))::bigint
         FROM webhook_deliveries WHERE status = 'pending'",
    )
    .fetch_one(pool)
    .await?;

    let failed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM webhook_deliveries WHERE status = 'failed'",
    )
    .fetch_one(pool)
    .await?;

    let delivered: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM webhook_deliveries WHERE status = 'delivered'",
    )
    .fetch_one(pool)
    .await?;

    let now = Utc::now();
    let one_min_ago = now - chrono::Duration::minutes(1);

    let delivered_last_min: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM webhook_deliveries
         WHERE status = 'delivered' AND delivered_at >= $1",
    )
    .bind(one_min_ago)
    .fetch_one(pool)
    .await?;

    let failed_last_min: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM webhook_deliveries
         WHERE status = 'failed' AND created_at >= $1",
    )
    .bind(one_min_ago)
    .fetch_one(pool)
    .await?;

    Ok(WebhookMetrics {
        pending_count: pending.0,
        oldest_pending_age_secs: pending.1.unwrap_or(0),
        failed_count: failed,
        delivered_count: delivered,
        delivered_last_min,
        failed_last_min,
    })
}
