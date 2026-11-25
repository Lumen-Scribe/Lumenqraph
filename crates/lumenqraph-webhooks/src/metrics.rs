//! Webhook metrics — backlog depth, delivery rate, and failure statistics.
//!
//! Exposes a `/metrics` endpoint on its own port for Prometheus scraping.

use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::json;
use sqlx::PgPool;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{error, info};

use crate::dispatcher::{
    DELIVERIES_DELIVERED_TOTAL, DELIVERIES_FAILED_TOTAL, DELIVERIES_RETRIED_TOTAL,
    DELIVERY_ATTEMPTS_TOTAL, DELIVERY_LATENCY_LAST_SECONDS, DELIVERY_LATENCY_SECONDS_TOTAL,
    PENDING_DELIVERIES, QUEUE_AGE_OLDEST_SECONDS,
};

/// Default bind address for the webhook metrics listener.
///
/// Loopback-only by default so operational telemetry (backlog depth, failure
/// rates) is not exposed on hosts with public interfaces. Set
/// `WEBHOOKS_METRICS_BIND_ADDR` to `0.0.0.0:9091` (or a private network
/// address) when Prometheus scrapes from another host.
pub const DEFAULT_METRICS_BIND_ADDR: &str = "127.0.0.1:9091";

#[derive(Clone)]
pub struct MetricsState {
    pub pool: Arc<PgPool>,
    /// Optional bearer token required on `/metrics` when set.
    pub auth_token: Option<Arc<String>>,
}

impl MetricsState {
    /// Returns true when the request is authorized to read `/metrics`.
    ///
    /// When no token is configured the endpoint is open (relying on the
    /// loopback-by-default bind for protection).
    fn authorized(&self, headers: &HeaderMap) -> bool {
        let Some(expected) = self.auth_token.as_deref() else {
            return true;
        };
        let Some(value) = headers.get(header::AUTHORIZATION) else {
            return false;
        };
        let Ok(value) = value.to_str() else {
            return false;
        };
        let Some(token) = value.strip_prefix("Bearer ") else {
            return false;
        };
        constant_time_eq(token.as_bytes(), expected.as_bytes())
    }
}

/// Constant-time byte comparison to avoid leaking the token via timing.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

pub async fn start_metrics_server(pool: Arc<PgPool>, bind_addr: &str) -> anyhow::Result<()> {
    start_metrics_server_with_token(pool, bind_addr, None).await
}

/// Start the metrics listener, optionally requiring a bearer token on `/metrics`.
pub async fn start_metrics_server_with_token(
    pool: Arc<PgPool>,
    bind_addr: &str,
    auth_token: Option<String>,
) -> anyhow::Result<()> {
    let state = MetricsState {
        pool,
        auth_token: auth_token.map(Arc::new),
    };

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

async fn metrics(State(state): State<MetricsState>, headers: HeaderMap) -> impl IntoResponse {
    if !state.authorized(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer")],
            String::from("unauthorized\n"),
        );
    }

    let mut body = String::new();

    // Emitted unconditionally, straight from the atomic the dispatcher refreshes
    // each tick, so queue depth stays visible to Prometheus even when the
    // scrape-time queries below fail (e.g. the database is briefly unreachable).
    body.push_str("# HELP lumenqraph_webhooks_pending_deliveries Webhook deliveries in 'pending' state, refreshed each dispatcher tick\n");
    body.push_str("# TYPE lumenqraph_webhooks_pending_deliveries gauge\n");
    body.push_str(&format!(
        "lumenqraph_webhooks_pending_deliveries {}\n",
        PENDING_DELIVERIES.load(Ordering::Relaxed)
    ));
    body.push_str("# HELP lumenqraph_webhooks_deliveries_pruned_total Delivery rows removed by retention since start\n");
    body.push_str("# TYPE lumenqraph_webhooks_deliveries_pruned_total counter\n");
    body.push_str(&format!(
        "lumenqraph_webhooks_deliveries_pruned_total {}\n",
        crate::prune::DELIVERIES_PRUNED.load(Ordering::Relaxed)
    ));
    body.push_str("# HELP lumenqraph_webhooks_pending_expired_total Pending deliveries expired for inactive subscriptions since start\n");
    body.push_str("# TYPE lumenqraph_webhooks_pending_expired_total counter\n");
    body.push_str(&format!(
        "lumenqraph_webhooks_pending_expired_total {}\n",
        crate::prune::PENDING_DELIVERIES_EXPIRED.load(Ordering::Relaxed)
    ));

    // Delivery latency and queue age, maintained by the dispatcher worker pool.
    // Emitted unconditionally so slow-subscriber impact stays visible even if
    // the scrape-time queries below fail.
    body.push_str("# HELP lumenqraph_webhooks_delivery_attempts_total Delivery attempts made by the worker pool since start\n");
    body.push_str("# TYPE lumenqraph_webhooks_delivery_attempts_total counter\n");
    body.push_str(&format!(
        "lumenqraph_webhooks_delivery_attempts_total {}\n",
        DELIVERY_ATTEMPTS_TOTAL.load(Ordering::Relaxed)
    ));

    body.push_str("# HELP lumenqraph_webhook_deliveries_total Delivery outcomes recorded by the worker pool since start\n");
    body.push_str("# TYPE lumenqraph_webhook_deliveries_total counter\n");
    for (result, counter) in [
        ("delivered", &DELIVERIES_DELIVERED_TOTAL),
        ("failed", &DELIVERIES_FAILED_TOTAL),
        ("retried", &DELIVERIES_RETRIED_TOTAL),
    ] {
        body.push_str(&format!(
            "lumenqraph_webhook_deliveries_total{{result=\"{result}\"}} {}\n",
            counter.load(Ordering::Relaxed)
        ));
    }

    body.push_str("# HELP lumenqraph_webhook_delivery_latency_seconds_total Cumulative time spent in delivery attempts since start\n");
    body.push_str("# TYPE lumenqraph_webhook_delivery_latency_seconds_total counter\n");
    body.push_str(&format!(
        "lumenqraph_webhooks_delivery_latency_seconds_total {}\n",
        DELIVERY_LATENCY_SECONDS_TOTAL.load(Ordering::Relaxed)
    ));

    body.push_str("# HELP lumenqraph_webhooks_delivery_latency_last_seconds Duration of the most recent delivery attempt\n");
    body.push_str("# TYPE lumenqraph_webhooks_delivery_latency_last_seconds gauge\n");
    body.push_str(&format!(
        "lumenqraph_webhooks_delivery_latency_last_seconds {}\n",
        DELIVERY_LATENCY_LAST_SECONDS.load(Ordering::Relaxed)
    ));

    body.push_str("# HELP lumenqraph_webhooks_oldest_pending_age_seconds Age of the oldest pending delivery, refreshed each dispatcher tick\n");
    body.push_str("# TYPE lumenqraph_webhooks_oldest_pending_age_seconds gauge\n");
    body.push_str(&format!(
        "lumenqraph_webhooks_oldest_pending_age_seconds {}\n",
        QUEUE_AGE_OLDEST_SECONDS.load(Ordering::Relaxed)
    ));

    match gather_metrics(&state.pool).await {
        Ok(metrics) => {
            // Keep the dispatcher-maintained gauge in sync with the scrape-time
            // value so it stays accurate even between dispatcher ticks.
            QUEUE_AGE_OLDEST_SECONDS
                .store(metrics.oldest_pending_age_secs as f64, Ordering::Relaxed);

            body.push_str(
                "# HELP lumenqraph_webhooks_failed_deliveries Retained terminal failed webhook deliveries\n",
            );
            body.push_str("# TYPE lumenqraph_webhooks_failed_deliveries gauge\n");
            body.push_str(&format!(
                "lumenqraph_webhooks_failed_deliveries {}\n",
                metrics.failed_count
            ));

            body.push_str(
                "# HELP lumenqraph_webhooks_delivered_deliveries Retained delivered webhook deliveries\n",
            );
            body.push_str("# TYPE lumenqraph_webhooks_delivered_deliveries gauge\n");
            body.push_str(&format!(
                "lumenqraph_webhooks_delivered_deliveries {}\n",
                metrics.delivered_count
            ));
        }
        Err(e) => {
            error!(error = %e, "failed to gather webhook metrics");
        }
    }

    // Exports lumenqraph_db_pool_connections, lumenqraph_db_pool_acquire_timeouts_total,
    // and lumenqraph_db_pool_acquire_duration_seconds.
    lumenqraph_core::db::render_pool_metrics(&mut body, &state.pool);

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
}

struct WebhookMetrics {
    pending_count: i64,
    oldest_pending_age_secs: i64,
    failed_count: i64,
    delivered_count: i64,
}

async fn gather_metrics(pool: &PgPool) -> anyhow::Result<WebhookMetrics> {
    let pending: (i64, Option<i64>) = sqlx::query_as(
        "SELECT COUNT(*), EXTRACT(EPOCH FROM (now() - min(created_at)))::bigint
         FROM webhook_deliveries WHERE status = 'pending'",
    )
    .fetch_one(pool)
    .await?;

    let failed: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM webhook_deliveries WHERE status = 'failed'")
            .fetch_one(pool)
            .await?;

    let delivered: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM webhook_deliveries WHERE status = 'delivered'")
            .fetch_one(pool)
            .await?;

    Ok(WebhookMetrics {
        pending_count: pending.0,
        oldest_pending_age_secs: pending.1.unwrap_or(0),
        failed_count: failed,
        delivered_count: delivered,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state_with_token(token: Option<&str>) -> MetricsState {
        // The pool is never touched by `authorized`, so a lazy pool is fine.
        let pool =
            PgPool::connect_lazy("postgres://localhost/unused").expect("lazy pool construction");
        MetricsState {
            pool: Arc::new(pool),
            auth_token: token.map(|t| Arc::new(t.to_string())),
        }
    }

    #[test]
    fn default_bind_addr_is_loopback() {
        let addr: SocketAddr = DEFAULT_METRICS_BIND_ADDR.parse().expect("valid addr");
        assert!(
            addr.ip().is_loopback(),
            "default metrics bind address must be loopback-only, got {addr}"
        );
    }

    #[tokio::test]
    async fn no_token_configured_allows_requests() {
        let state = state_with_token(None);
        assert!(state.authorized(&HeaderMap::new()));
    }

    #[tokio::test]
    async fn token_configured_rejects_missing_or_wrong_header() {
        let state = state_with_token(Some("s3cret"));
        assert!(!state.authorized(&HeaderMap::new()));

        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, "Bearer wrong".parse().unwrap());
        assert!(!state.authorized(&headers));

        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, "Basic s3cret".parse().unwrap());
        assert!(!state.authorized(&headers));
    }

    #[tokio::test]
    async fn token_configured_accepts_matching_bearer() {
        let state = state_with_token(Some("s3cret"));
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, "Bearer s3cret".parse().unwrap());
        assert!(state.authorized(&headers));
    }
}
