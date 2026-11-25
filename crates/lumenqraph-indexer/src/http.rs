//! HTTP server for health and metrics endpoints on the indexer.
//!
//! Endpoints (#756):
//! - `/livez`  — liveness: 200 while the process and this listener are up. Never
//!   touches the database.
//! - `/readyz` — readiness: 503 whenever the indexer is degraded (stale cursor,
//!   ledger lag, webhook queue depth, DB failure). Always strict.
//! - `/health` — same status body as `/readyz`; 503 while degraded when
//!   `INDEXER_HEALTH_STRICT=true` (default), 200 with `"status": "degraded"` in
//!   the body otherwise (legacy behaviour).
//! - `/metrics` — Prometheus scrape endpoint.

use axum::extract::State;
use axum::http::header;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use chrono::Utc;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info};

use crate::specs::SpecCache;

/// Thresholds and strictness for the indexer's health endpoints, parsed once at
/// startup from the environment (#756).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HealthConfig {
    /// When `true` (default), `/health` answers 503 while degraded so
    /// orchestrators (Docker `HEALTHCHECK`, Kubernetes probes, Fly checks) can
    /// detect a stuck indexer — they only look at the status code. When `false`,
    /// `/health` keeps the legacy behaviour of always answering 200 with
    /// `"status": "degraded"` in the body. `/readyz` is strict regardless.
    /// Controlled by `INDEXER_HEALTH_STRICT`.
    pub strict: bool,
    /// Seconds since the cursor last advanced (`progress_at`) after which the
    /// indexer counts as stale/stuck. `HEALTH_MAX_STALE_SECS` (default 120).
    pub max_stale_secs: i64,
    /// Ledger lag (`chain_tip_ledger - last_processed_ledger`) at which the
    /// indexer counts as degraded. `HEALTH_MAX_LAG_LEDGERS` (default 100).
    pub max_lag_ledgers: i64,
    /// Pending webhook deliveries (queue depth) at which the indexer counts as
    /// degraded. `HEALTH_MAX_PENDING_DELIVERIES` (default 5000, matching the
    /// `LumenqraphWebhookBacklogCritical` alert).
    pub max_pending_deliveries: i64,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            strict: true,
            max_stale_secs: 120,
            max_lag_ledgers: 100,
            max_pending_deliveries: 5000,
        }
    }
}

impl HealthConfig {
    /// Read the health settings from the environment, falling back to the
    /// defaults above for unset or unparseable values.
    pub fn from_env() -> Self {
        Self {
            strict: env_parse_bool("INDEXER_HEALTH_STRICT", true),
            max_stale_secs: env_parse_i64("HEALTH_MAX_STALE_SECS", 120),
            max_lag_ledgers: env_parse_i64("HEALTH_MAX_LAG_LEDGERS", 100),
            max_pending_deliveries: env_parse_i64("HEALTH_MAX_PENDING_DELIVERIES", 5000),
        }
    }
}

#[derive(Clone)]
pub struct HttpState {
    pub pool: Arc<PgPool>,
    /// Reference to the live spec cache so its size can be reported in metrics.
    pub spec_cache: Arc<SpecCache>,
    /// Optional bearer token required for `/metrics` (#605). When `None`, the
    /// endpoint is unauthenticated (rely on binding to a private address).
    pub metrics_bearer_token: Option<Arc<String>>,
    /// Health thresholds/strictness shared by `/health` and `/readyz` (#756).
    pub health: HealthConfig,
}

/// Reads the optional metrics bearer token from the environment.
///
/// `METRICS_BEARER_TOKEN` takes precedence; for parity with the API's
/// `METRICS_REQUIRE_API_KEY` semantics, `METRICS_API_KEY` is also accepted.
/// Empty values are treated as "no auth configured".
pub fn metrics_bearer_token_from_env() -> Option<Arc<String>> {
    for key in ["METRICS_BEARER_TOKEN", "METRICS_API_KEY"] {
        if let Ok(v) = std::env::var(key) {
            let v = v.trim();
            if !v.is_empty() {
                return Some(Arc::new(v.to_string()));
            }
        }
    }
    None
}

pub async fn start_http_server(
    pool: Arc<PgPool>,
    spec_cache: Arc<SpecCache>,
    bind_addr: &str,
) -> anyhow::Result<()> {
    let state = HttpState {
        pool,
        spec_cache,
        metrics_bearer_token: metrics_bearer_token_from_env(),
        health: HealthConfig::from_env(),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
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

/// Liveness probe: 200 as long as the process and its HTTP listener are up.
/// Deliberately never touches the database — a wedged poller or an unreachable
/// Postgres must not make an orchestrator kill the process; that is what
/// `/health` and `/readyz` signal (#756).
async fn livez() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({ "status": "ok" })))
}

/// `/health` reports the indexer status. With `INDEXER_HEALTH_STRICT=true`
/// (the default) it answers 503 while degraded, so orchestrators that only
/// look at the status code (Docker `HEALTHCHECK`, Kubernetes probes, Fly
/// checks) can detect and restart a stuck indexer (#756). The JSON body is the
/// same either way.
async fn health(State(state): State<HttpState>) -> impl IntoResponse {
    status_response(&state, state.health.strict).await
}

/// `/readyz` is the strict readiness probe: 503 whenever the indexer is
/// degraded (stale cursor, ledger lag, webhook queue depth) or the database is
/// unreachable, regardless of `INDEXER_HEALTH_STRICT` (#756).
async fn readyz(State(state): State<HttpState>) -> impl IntoResponse {
    status_response(&state, true).await
}

/// Shared response for `/health` and `/readyz`. A database failure is always
/// 503 (`"unavailable"`); a degraded indexer is 503 only when `strict`.
async fn status_response(state: &HttpState, strict: bool) -> (StatusCode, Json<Value>) {
    match get_indexer_status(&state.pool, &state.health).await {
        Ok(status) => (
            health_status_code(status.is_healthy, strict),
            Json(health_body(&status)),
        ),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "unavailable",
                "error": "failed to fetch indexer status"
            })),
        ),
    }
}

/// Status code for a health/readiness response: 503 when the indexer is
/// degraded and the endpoint is strict, 200 otherwise (#756). Pure so the
/// decision can be unit tested without a database.
fn health_status_code(is_healthy: bool, strict: bool) -> StatusCode {
    if is_healthy || !strict {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// JSON body for `/health` and `/readyz`. The fields are unchanged from the
/// pre-#756 body; `seconds_since_progress`, `pending_deliveries` and
/// `degraded_reasons` are added so a 503 is self-explanatory.
fn health_body(status: &IndexerStatus) -> Value {
    json!({
        "status": if status.is_healthy { "ok" } else { "degraded" },
        "last_processed_ledger": status.last_processed_ledger,
        "chain_tip_ledger": status.chain_tip_ledger,
        "lag_ledgers": status.lag_ledgers,
        "seconds_since_cursor_update": status.secs_since_update,
        // null when the cursor has never advanced (stuck poller, #757).
        "seconds_since_progress": status.secs_since_progress,
        "pending_deliveries": status.pending_deliveries,
        "degraded_reasons": status.reasons,
        "unrecovered_missed_ranges": status.unrecovered_missed_ranges,
        "recoverable_missed_ranges": status.recoverable_missed_ranges,
        "unrecoverable_missed_ranges": status.unrecoverable_missed_ranges,
    })
}

async fn metrics(
    State(state): State<HttpState>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    if let Some(expected) = state.metrics_bearer_token.as_deref() {
        if !bearer_token_matches(&headers, expected) {
            return (
                axum::http::StatusCode::UNAUTHORIZED,
                [(header::WWW_AUTHENTICATE, "Bearer")],
                "unauthorized".to_string(),
            );
        }
    }

    let mut body = String::new();

    match gather_metrics(&state.pool).await {
        Ok(metrics) => {
            body.push_str("# HELP lumenqraph_indexer_last_processed_ledger The last ledger processed by the indexer\n");
            body.push_str("# TYPE lumenqraph_indexer_last_processed_ledger gauge\n");
            body.push_str(&format!(
                "lumenqraph_indexer_last_processed_ledger {}\n",
                metrics.last_processed_ledger
            ));

            body.push_str(
                "# HELP lumenqraph_indexer_chain_tip_ledger The current chain tip ledger\n",
            );
            body.push_str("# TYPE lumenqraph_indexer_chain_tip_ledger gauge\n");
            body.push_str(&format!(
                "lumenqraph_indexer_chain_tip_ledger {}\n",
                metrics.chain_tip_ledger
            ));

            body.push_str("# HELP lumenqraph_indexer_lag_ledgers Lag between indexer and chain tip in ledgers\n");
            body.push_str("# TYPE lumenqraph_indexer_lag_ledgers gauge\n");
            body.push_str(&format!(
                "lumenqraph_indexer_lag_ledgers {}\n",
                metrics.lag_ledgers
            ));

            body.push_str("# HELP lumenqraph_indexer_lag_seconds Estimated time behind the chain tip at five seconds per ledger\n");
            body.push_str("# TYPE lumenqraph_indexer_lag_seconds gauge\n");
            body.push_str(&format!(
                "lumenqraph_indexer_lag_seconds {}\n",
                metrics.lag_ledgers.saturating_mul(5)
            ));

            body.push_str(
                "# HELP lumenqraph_indexer_seconds_since_update Seconds since last cursor update\n",
            );
            body.push_str("# TYPE lumenqraph_indexer_seconds_since_update gauge\n");
            body.push_str(&format!(
                "lumenqraph_indexer_seconds_since_update {}\n",
                metrics.secs_since_update
            ));

            // Seconds since the cursor last ADVANCED (progress_at). Unlike
            // seconds_since_update, counter writes do not refresh it, so a
            // stuck poller shows a growing value here (#756, #757). Emitted
            // only once the cursor has advanced at least once.
            if let Some(secs_since_progress) = metrics.secs_since_progress {
                body.push_str("# HELP lumenqraph_indexer_seconds_since_progress Seconds since the cursor last advanced (progress_at), not since the last counter write\n");
                body.push_str("# TYPE lumenqraph_indexer_seconds_since_progress gauge\n");
                body.push_str(&format!(
                    "lumenqraph_indexer_seconds_since_progress {secs_since_progress}\n"
                ));
            }

            body.push_str(
                "# HELP lumenqraph_indexer_events_ingested_total Total events ingested\n",
            );
            body.push_str("# TYPE lumenqraph_indexer_events_ingested_total counter\n");
            body.push_str(&format!(
                "lumenqraph_indexer_events_ingested_total {}\n",
                metrics.events_ingested_total
            ));

            body.push_str("# HELP lumenqraph_indexer_errors_total Total indexer errors\n");
            body.push_str("# TYPE lumenqraph_indexer_errors_total counter\n");
            body.push_str(&format!(
                "lumenqraph_indexer_errors_total {}\n",
                metrics.errors_total
            ));

            body.push_str("# HELP lumenqraph_indexer_enrichment_rate Fraction of events successfully enriched (0.0 to 1.0)\n");
            body.push_str("# TYPE lumenqraph_indexer_enrichment_rate gauge\n");
            body.push_str(&format!(
                "lumenqraph_indexer_enrichment_rate {}\n",
                metrics.enrichment_rate
            ));

            // Spec cache size gauge (issue #268).
            let cache_size = state.spec_cache.cache_size();
            body.push_str("# HELP lumenqraph_indexer_spec_cache_size Current number of entries in the in-memory spec cache\n");
            body.push_str("# TYPE lumenqraph_indexer_spec_cache_size gauge\n");
            body.push_str(&format!(
                "lumenqraph_indexer_spec_cache_size {cache_size}\n"
            ));

            // RPC failover counter (#398).
            body.push_str("# HELP lumenqraph_indexer_rpc_failovers_total Total number of RPC endpoint failovers\n");
            body.push_str("# TYPE lumenqraph_indexer_rpc_failovers_total counter\n");
            body.push_str(&format!(
                "lumenqraph_indexer_rpc_failovers_total {}\n",
                metrics.rpc_failovers_total
            ));

            // Timestamp parse error counter (#399).
            body.push_str("# HELP lumenqraph_indexer_timestamp_parse_errors_total Total ledgerClosedAt parse failures (never stored as Utc::now())\n");
            body.push_str("# TYPE lumenqraph_indexer_timestamp_parse_errors_total counter\n");
            body.push_str(&format!(
                "lumenqraph_indexer_timestamp_parse_errors_total {}\n",
                metrics.timestamp_parse_errors_total
            ));

            body.push_str("# HELP lumenqraph_indexer_rpc_oldest_ledger Oldest ledger currently available from the RPC provider\n");
            body.push_str("# TYPE lumenqraph_indexer_rpc_oldest_ledger gauge\n");
            body.push_str(&format!(
                "lumenqraph_indexer_rpc_oldest_ledger {}\n",
                crate::poller::rpc_oldest_ledger()
            ));
            body.push_str("# HELP lumenqraph_indexer_open_missed_ranges Number of ledger ranges not yet recovered\n");
            body.push_str("# TYPE lumenqraph_indexer_open_missed_ranges gauge\n");
            body.push_str(&format!(
                "lumenqraph_indexer_open_missed_ranges {}\n",
                metrics.unrecovered_missed_ranges
            ));
            body.push_str("# HELP lumenqraph_indexer_recoverable_missed_ranges Unrecovered ranges still within RPC retention\n");
            body.push_str("# TYPE lumenqraph_indexer_recoverable_missed_ranges gauge\n");
            body.push_str(&format!(
                "lumenqraph_indexer_recoverable_missed_ranges {}\n",
                metrics.recoverable_missed_ranges
            ));
            body.push_str("# HELP lumenqraph_indexer_permanent_missed_ranges Unrecovered ranges outside RPC retention\n");
            body.push_str("# TYPE lumenqraph_indexer_permanent_missed_ranges gauge\n");
            body.push_str(&format!(
                "lumenqraph_indexer_permanent_missed_ranges {}\n",
                metrics.unrecoverable_missed_ranges
            ));
            body.push_str("# HELP lumenqraph_indexer_consecutive_errors Current number of consecutive poll-cycle failures (circuit breaker gauge; resets to 0 on success)\n");
            body.push_str("# TYPE lumenqraph_indexer_consecutive_errors gauge\n");
            body.push_str(&format!(
                "lumenqraph_indexer_consecutive_errors {}\n",
                metrics.consecutive_errors
            ));
            body.push_str("# HELP lumenqraph_indexer_events_enriched_total Events successfully enriched with spec data\n");
            body.push_str("# TYPE lumenqraph_indexer_events_enriched_total counter\n");
            body.push_str(&format!(
                "lumenqraph_indexer_events_enriched_total {}\n",
                metrics.enriched
            ));
            body.push_str("# HELP lumenqraph_indexer_events_not_enriched_total Events without matching spec (fallback to decoded)\n");
            body.push_str("# TYPE lumenqraph_indexer_events_not_enriched_total counter\n");
            body.push_str(&format!(
                "lumenqraph_indexer_events_not_enriched_total {}\n",
                metrics.not_enriched
            ));
            body.push_str("# HELP lumenqraph_indexer_spec_fetch_failures_total Failed attempts to fetch contract specs\n");
            body.push_str("# TYPE lumenqraph_indexer_spec_fetch_failures_total counter\n");
            body.push_str(&format!(
                "lumenqraph_indexer_spec_fetch_failures_total {}\n",
                metrics.spec_fetch_failures
            ));
            body.push_str(
                "# HELP lumenqraph_indexer_rpc_calls_total Total RPC method calls made\n",
            );
            body.push_str("# TYPE lumenqraph_indexer_rpc_calls_total counter\n");
            body.push_str(&format!(
                "lumenqraph_indexer_rpc_calls_total {}\n",
                metrics.rpc_calls
            ));
            body.push_str("# HELP lumenqraph_indexer_rpc_errors_total RPC errors encountered\n");
            body.push_str("# TYPE lumenqraph_indexer_rpc_errors_total counter\n");
            body.push_str(&format!(
                "lumenqraph_indexer_rpc_errors_total {}\n",
                metrics.rpc_errors
            ));
            body.push_str("# HELP lumenqraph_indexer_rpc_errors_32001_total RPC -32001 processing-limit errors\n");
            body.push_str("# TYPE lumenqraph_indexer_rpc_errors_32001_total counter\n");
            body.push_str(&format!(
                "lumenqraph_indexer_rpc_errors_32001_total {}\n",
                metrics.rpc_errors_32001
            ));
            render_process_counters(&mut body);
        }
        Err(e) => {
            error!(error = %e, "failed to gather indexer metrics");
        }
    }

    // Exports lumenqraph_db_pool_connections, lumenqraph_db_pool_acquire_timeouts_total,
    // and lumenqraph_db_pool_acquire_duration_seconds.
    lumenqraph_core::db::render_pool_metrics(&mut body, &state.pool);

    (
        axum::http::StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
}

/// Renders the process-wide counters that do not require a database round-trip.
///
/// Kept as a pure function so the rendering path can be unit tested without a
/// live `PgPool` (see `tests::metrics_render_process_counters`).
fn render_process_counters(body: &mut String) {
    crate::timing::render(body);
    crate::metrics::render(body);
    // Transfer-like events skipped by the SEP-41 projection (#600).
    body.push_str("# HELP lumenqraph_indexer_transfer_projection_skipped_total Transfer-like events skipped by the SEP-41 projection since start\n");
    body.push_str("# TYPE lumenqraph_indexer_transfer_projection_skipped_total counter\n");
    body.push_str(&format!(
        "lumenqraph_indexer_transfer_projection_skipped_total {}\n",
        crate::store::transfer_projection_skipped_total()
    ));
    // webhook_deliveries rows dropped by retention (#601).
    body.push_str("# HELP lumenqraph_indexer_webhook_deliveries_dropped_by_retention_total webhook_deliveries rows dropped by retention since start\n");
    body.push_str(
        "# TYPE lumenqraph_indexer_webhook_deliveries_dropped_by_retention_total counter\n",
    );
    body.push_str(&format!(
        "lumenqraph_indexer_webhook_deliveries_dropped_by_retention_total {}\n",
        crate::retention::webhook_deliveries_dropped_by_retention_total()
    ));
}

/// Constant-time-ish comparison of the `Authorization: Bearer <token>` header.
fn bearer_token_matches(headers: &axum::http::HeaderMap, expected: &str) -> bool {
    let Some(value) = headers.get(header::AUTHORIZATION) else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };
    let Some(token) = value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
    else {
        return false;
    };
    let token = token.trim();
    // Length check first, then byte-wise comparison without early exit.
    if token.len() != expected.len() {
        return false;
    }
    let mut diff = 0u8;
    for (a, b) in token.bytes().zip(expected.bytes()) {
        diff |= a ^ b;
    }
    diff == 0
}

struct IndexerStatus {
    last_processed_ledger: i64,
    chain_tip_ledger: i64,
    lag_ledgers: i64,
    /// Seconds since `updated_at` — the "heartbeat" every counter write bumps.
    secs_since_update: i64,
    /// Seconds since `progress_at` — the last time the cursor actually
    /// advanced. `None` means the cursor has never advanced (stuck poller),
    /// which counts as infinitely stale (#756, #757).
    secs_since_progress: Option<i64>,
    /// Pending webhook deliveries (queue depth).
    pending_deliveries: i64,
    is_healthy: bool,
    /// Human-readable reasons the indexer is degraded (empty when healthy).
    reasons: Vec<String>,
    unrecovered_missed_ranges: i64,
    recoverable_missed_ranges: i64,
    unrecoverable_missed_ranges: i64,
}

/// Pure health decision, split out from the database fetch so it can be unit
/// tested without Postgres (#756).
///
/// The indexer is healthy when ALL of these hold:
/// - the cursor advanced within `max_stale_secs` (`secs_since_progress` is
///   `Some` and <= the threshold; `None` = never advanced = stuck = degraded).
///   Freshness is deliberately measured from `progress_at`, NOT `updated_at`:
///   counter writes (`incr_errors`, ...) bump `updated_at` without indexing
///   anything, and must not mask a stuck poller (#757).
/// - ledger lag is below `max_lag_ledgers`.
/// - the webhook queue depth (pending deliveries) is below
///   `max_pending_deliveries`.
fn evaluate_health(
    lag_ledgers: i64,
    secs_since_progress: Option<i64>,
    pending_deliveries: i64,
    cfg: &HealthConfig,
) -> (bool, Vec<String>) {
    let mut reasons = Vec::new();
    match secs_since_progress {
        Some(secs) if secs > cfg.max_stale_secs => reasons.push(format!(
            "cursor stale: no progress for {secs}s (HEALTH_MAX_STALE_SECS={})",
            cfg.max_stale_secs
        )),
        // Never advanced: a fresh database or a poller whose every cycle fails.
        None => {
            reasons.push("cursor never advanced: no successful poll cycle yet (stuck?)".to_string())
        }
        _ => {}
    }
    if lag_ledgers >= cfg.max_lag_ledgers {
        reasons.push(format!(
            "lag {lag_ledgers} ledgers behind chain tip (HEALTH_MAX_LAG_LEDGERS={})",
            cfg.max_lag_ledgers
        ));
    }
    if pending_deliveries >= cfg.max_pending_deliveries {
        reasons.push(format!(
            "pending webhook deliveries {pending_deliveries} (HEALTH_MAX_PENDING_DELIVERIES={})",
            cfg.max_pending_deliveries
        ));
    }
    (reasons.is_empty(), reasons)
}

async fn get_indexer_status(pool: &PgPool, cfg: &HealthConfig) -> anyhow::Result<IndexerStatus> {
    // Column order matches the SELECT below.
    type CursorRow = (
        i64,
        i64,
        chrono::DateTime<chrono::Utc>,
        Option<chrono::DateTime<chrono::Utc>>,
    );
    let status: Option<CursorRow> = sqlx::query_as(
        "SELECT last_processed_ledger, chain_tip_ledger, updated_at, progress_at
         FROM indexer_cursor WHERE id = 1",
    )
    .fetch_optional(pool)
    .await?;

    let Some((last, tip, updated_at, progress_at)) = status else {
        anyhow::bail!("indexer cursor not initialized");
    };

    let lag_ledgers = (tip - last).max(0);
    let secs_since_update = (Utc::now() - updated_at).num_seconds();
    let secs_since_progress = progress_at.map(|p| (Utc::now() - p).num_seconds());

    // Queue depth: deliveries the indexer enqueued that the dispatcher has not
    // picked up yet. A fast-growing queue means the pipeline is backing up.
    let pending_deliveries: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM webhook_deliveries WHERE status = 'pending'")
            .fetch_one(pool)
            .await
            .unwrap_or(0);

    let (is_healthy, reasons) =
        evaluate_health(lag_ledgers, secs_since_progress, pending_deliveries, cfg);

    let (recoverable_missed_ranges, unrecoverable_missed_ranges): (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*) FILTER (WHERE recoverable), COUNT(*) FILTER (WHERE NOT recoverable)
         FROM missed_ranges WHERE recovered_at IS NULL",
    )
    .fetch_one(pool)
    .await
    .unwrap_or((0, 0));
    let unrecovered_missed_ranges = recoverable_missed_ranges + unrecoverable_missed_ranges;

    Ok(IndexerStatus {
        last_processed_ledger: last,
        chain_tip_ledger: tip,
        lag_ledgers,
        secs_since_update,
        secs_since_progress,
        pending_deliveries,
        is_healthy,
        reasons,
        unrecovered_missed_ranges,
        recoverable_missed_ranges,
        unrecoverable_missed_ranges,
    })
}

struct IndexerMetrics {
    last_processed_ledger: i64,
    chain_tip_ledger: i64,
    lag_ledgers: i64,
    secs_since_update: i64,
    /// Seconds since `progress_at`; `None` until the cursor first advances.
    secs_since_progress: Option<i64>,
    events_ingested_total: i64,
    errors_total: i64,
    enrichment_rate: f64,
    consecutive_errors: i64,
    enriched: i64,
    not_enriched: i64,
    spec_fetch_failures: i64,
    rpc_calls: i64,
    rpc_errors: i64,
    rpc_errors_32001: i64,

    /// Cumulative RPC endpoint failovers (#398).
    rpc_failovers_total: i64,
    /// Cumulative timestamp parse errors (#399).
    timestamp_parse_errors_total: i64,
    unrecovered_missed_ranges: i64,
    recoverable_missed_ranges: i64,
    unrecoverable_missed_ranges: i64,
}

#[allow(clippy::type_complexity)] // one row mirrors the indexer_cursor columns
async fn gather_metrics(pool: &PgPool) -> anyhow::Result<IndexerMetrics> {
    // Column order matches the SELECT below.
    type CursorRow = (
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        chrono::DateTime<chrono::Utc>,
        Option<chrono::DateTime<chrono::Utc>>,
    );
    let status: Option<CursorRow> = sqlx::query_as(
        "SELECT last_processed_ledger, chain_tip_ledger, events_ingested_total, errors_total, events_enriched_total, events_not_enriched_total,
                COALESCE(rpc_failovers_total, 0),
                COALESCE(timestamp_parse_errors_total, 0),
                updated_at,
                progress_at
         FROM indexer_cursor WHERE id = 1",
    )
    .fetch_optional(pool)
    .await?;

    let Some((
        last,
        tip,
        events,
        errors,
        enriched,
        not_enriched,
        rpc_failovers,
        ts_errors,
        consecutive_errors,
        spec_fetch_failures,
        rpc_calls,
        rpc_errors,
        rpc_errors_32001,
        updated_at,
        progress_at,
    )) = status
    else {
        anyhow::bail!("indexer cursor not initialized");
    };

    let lag_ledgers = (tip - last).max(0);
    let secs_since_update = (Utc::now() - updated_at).num_seconds();
    // Freshness gauge: seconds since the cursor last ADVANCED (progress_at), not
    // since the last counter write — a stuck poller must show up here (#757).
    // Not emitted until the first progress write (NULL = never advanced).
    let secs_since_progress = progress_at.map(|p| (Utc::now() - p).num_seconds());

    let total_enriched = enriched + not_enriched;
    let enrichment_rate = if total_enriched > 0 {
        enriched as f64 / total_enriched as f64
    } else {
        1.0
    };
    let (recoverable_missed_ranges, unrecoverable_missed_ranges): (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*) FILTER (WHERE recoverable), COUNT(*) FILTER (WHERE NOT recoverable)
         FROM missed_ranges WHERE recovered_at IS NULL",
    )
    .fetch_one(pool)
    .await
    .unwrap_or((0, 0));
    let unrecovered_missed_ranges = recoverable_missed_ranges + unrecoverable_missed_ranges;

    Ok(IndexerMetrics {
        last_processed_ledger: last,
        chain_tip_ledger: tip,
        lag_ledgers,
        secs_since_update,
        secs_since_progress,
        events_ingested_total: events,
        errors_total: errors,
        enrichment_rate,
        consecutive_errors,
        enriched,
        not_enriched,
        spec_fetch_failures,
        rpc_calls,
        rpc_errors,
        rpc_errors_32001,

        rpc_failovers_total: rpc_failovers,
        timestamp_parse_errors_total: ts_errors,
        unrecovered_missed_ranges,
        recoverable_missed_ranges,
        unrecoverable_missed_ranges,
    })
}

fn env_parse_i64(key: &str, default: i64) -> i64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

/// Parse a boolean env var: `1/true/yes/on` are true, `0/false/no/off` are
/// false, anything else (including unset) falls back to `default`.
fn env_parse_bool(key: &str, default: bool) -> bool {
    parse_bool(std::env::var(key).ok().as_deref(), default)
}

/// Pure core of [`env_parse_bool`], testable without touching the process env.
fn parse_bool(value: Option<&str>, default: bool) -> bool {
    match value.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
        Some("1") | Some("true") | Some("yes") | Some("on") => true,
        Some("0") | Some("false") | Some("no") | Some("off") => false,
        _ => default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn test_state(token: Option<&str>) -> HttpState {
        test_state_with(token, HealthConfig::default())
    }

    fn test_state_with(token: Option<&str>, health: HealthConfig) -> HttpState {
        // The pool is never touched by the auth check, so a lazy pool is fine.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://user:pass@localhost:5432/db")
            .expect("lazy pool");
        HttpState {
            pool: Arc::new(pool),
            spec_cache: Arc::new(SpecCache::new(16, 2)),
            metrics_bearer_token: token.map(|t| Arc::new(t.to_string())),
            health,
        }
    }

    fn app_with(state: HttpState) -> Router {
        Router::new()
            .route("/health", get(health))
            .route("/livez", get(livez))
            .route("/readyz", get(readyz))
            .route("/metrics", get(metrics))
            .with_state(state)
    }

    /// GET `uri` through the router and return (status, parsed JSON body).
    async fn get_json(app: Router, uri: &str) -> (StatusCode, Value) {
        let resp = app
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, body)
    }

    // ---- Pure decision tests (no database) for #756 ----

    #[test]
    fn health_status_code_is_503_only_when_degraded_and_strict() {
        assert_eq!(health_status_code(true, true), StatusCode::OK);
        assert_eq!(health_status_code(true, false), StatusCode::OK);
        assert_eq!(
            health_status_code(false, true),
            StatusCode::SERVICE_UNAVAILABLE
        );
        // Legacy mode: degraded still answers 200.
        assert_eq!(health_status_code(false, false), StatusCode::OK);
    }

    #[test]
    fn evaluate_health_healthy_when_fresh_close_and_queue_empty() {
        let cfg = HealthConfig::default();
        let (healthy, reasons) = evaluate_health(0, Some(5), 0, &cfg);
        assert!(healthy, "expected healthy, reasons: {reasons:?}");
        assert!(reasons.is_empty());
    }

    #[test]
    fn evaluate_health_degraded_when_cursor_stale() {
        let cfg = HealthConfig::default();
        let (healthy, reasons) = evaluate_health(0, Some(121), 0, &cfg);
        assert!(!healthy);
        assert!(reasons.iter().any(|r| r.contains("stale")), "{reasons:?}");
    }

    #[test]
    fn evaluate_health_degraded_when_never_progressed() {
        // The stuck-poller case (#757): counter writes bump updated_at but
        // progress_at stays NULL, and that must count as degraded.
        let cfg = HealthConfig::default();
        let (healthy, reasons) = evaluate_health(0, None, 0, &cfg);
        assert!(!healthy);
        assert!(
            reasons.iter().any(|r| r.contains("never advanced")),
            "{reasons:?}"
        );
    }

    #[test]
    fn evaluate_health_degraded_when_lag_too_high() {
        let cfg = HealthConfig::default();
        let (healthy, reasons) = evaluate_health(100, Some(0), 0, &cfg);
        assert!(!healthy);
        assert!(reasons.iter().any(|r| r.contains("lag")), "{reasons:?}");
    }

    #[test]
    fn evaluate_health_degraded_when_queue_depth_too_high() {
        let cfg = HealthConfig::default();
        let (healthy, reasons) = evaluate_health(0, Some(0), 5000, &cfg);
        assert!(!healthy);
        assert!(
            reasons.iter().any(|r| r.contains("pending webhook")),
            "{reasons:?}"
        );
    }

    #[test]
    fn evaluate_health_respects_custom_thresholds() {
        let cfg = HealthConfig {
            strict: true,
            max_stale_secs: 10,
            max_lag_ledgers: 5,
            max_pending_deliveries: 50,
        };
        assert!(evaluate_health(4, Some(10), 49, &cfg).0);
        assert!(!evaluate_health(5, Some(10), 49, &cfg).0);
        assert!(!evaluate_health(4, Some(11), 49, &cfg).0);
        assert!(!evaluate_health(4, Some(10), 50, &cfg).0);
    }

    #[test]
    fn parse_bool_truthy_falsy_and_default() {
        for (input, expected) in [
            ("1", true),
            ("true", true),
            (" YES ", true),
            ("on", true),
            ("0", false),
            ("false", false),
            ("No", false),
            (" off ", false),
        ] {
            assert_eq!(
                parse_bool(Some(input), !expected),
                expected,
                "input {input:?}"
            );
        }
        // Unset or unparseable falls back to the default.
        assert!(parse_bool(None, true));
        assert!(!parse_bool(None, false));
        assert!(parse_bool(Some("garbage"), true));
        assert!(!parse_bool(Some(""), false));
    }

    #[test]
    fn env_parse_i64_falls_back_to_default_on_garbage() {
        // HEALTH_MAX_STALE_SECS-style parsing: unset/garbage -> default. The
        // env var is only read here with a name no deployment sets.
        assert_eq!(env_parse_i64("LUMENQRAPH_TEST_UNSET_VAR", 120), 120);
    }

    #[test]
    fn health_body_marks_degraded_and_exposes_reasons() {
        let status = IndexerStatus {
            last_processed_ledger: 100,
            chain_tip_ledger: 200,
            lag_ledgers: 100,
            secs_since_update: 3,
            secs_since_progress: None,
            pending_deliveries: 0,
            is_healthy: false,
            reasons: vec!["cursor never advanced".to_string()],
            unrecovered_missed_ranges: 0,
            recoverable_missed_ranges: 0,
            unrecoverable_missed_ranges: 0,
        };
        let body = health_body(&status);
        assert_eq!(body["status"], "degraded");
        assert_eq!(body["lag_ledgers"], 100);
        assert_eq!(body["seconds_since_cursor_update"], 3);
        // never progressed -> null, not a misleading small number (#757)
        assert!(body["seconds_since_progress"].is_null());
        assert_eq!(body["degraded_reasons"][0], "cursor never advanced");

        let ok = IndexerStatus {
            is_healthy: true,
            reasons: vec![],
            secs_since_progress: Some(2),
            ..status
        };
        let body = health_body(&ok);
        assert_eq!(body["status"], "ok");
        assert_eq!(body["seconds_since_progress"], 2);
    }

    // ---- Route tests against a dead (lazy) pool: no Postgres needed ----

    #[tokio::test]
    async fn livez_returns_200_without_any_database() {
        let app = app_with(test_state(None));
        let (status, body) = get_json(app, "/livez").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ok");
    }

    #[tokio::test]
    async fn health_returns_503_unavailable_when_db_unreachable() {
        // The lazy pool points at a closed port, so the status query fails:
        // a DB failure must be 503 even in non-strict mode (#756).
        let app = app_with(test_state(None));
        let (status, body) = get_json(app, "/health").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["status"], "unavailable");
    }

    #[tokio::test]
    async fn readyz_returns_503_when_db_unreachable() {
        let app = app_with(test_state(None));
        let (status, body) = get_json(app, "/readyz").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["status"], "unavailable");
    }

    #[tokio::test]
    async fn health_db_failure_is_503_even_when_strict_disabled() {
        let non_strict = HealthConfig {
            strict: false,
            ..HealthConfig::default()
        };
        let app = app_with(test_state_with(None, non_strict));
        let (status, _) = get_json(app, "/health").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    /// End-to-end probe exactly like the `healthcheck` subcommand: a real TCP
    /// listener serving the router, probed with the dependency-free helper from
    /// `lumenqraph-core`. `/livez` must pass (process up), `/health` with a dead
    /// DB must fail with 503 (#756).
    #[tokio::test]
    async fn healthcheck_probe_accepts_livez_and_rejects_degraded_health() {
        let app = app_with(test_state(None));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app.into_make_service())
                .await
                .unwrap();
        });
        let addr = addr.to_string();
        let livez = tokio::task::spawn_blocking({
            let addr = addr.clone();
            move || lumenqraph_core::healthcheck::check(&addr, "/livez")
        })
        .await
        .unwrap();
        assert!(livez.is_ok(), "livez must be 2xx: {livez:?}");
        let health = tokio::task::spawn_blocking(move || {
            lumenqraph_core::healthcheck::check(&addr, "/health")
        })
        .await
        .unwrap();
        assert!(
            health.is_err(),
            "health with dead DB must be non-2xx: {health:?}"
        );
    }

    #[tokio::test]
    async fn metrics_rejects_missing_token_when_configured() {
        let app = app_with(test_state(Some("s3cret")));
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn metrics_rejects_wrong_token_when_configured() {
        let app = app_with(test_state(Some("s3cret")));
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .header(header::AUTHORIZATION, "Bearer nope")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn bearer_token_matches_accepts_correct_token() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(header::AUTHORIZATION, "Bearer s3cret".parse().unwrap());
        assert!(bearer_token_matches(&headers, "s3cret"));
        assert!(!bearer_token_matches(&headers, "other"));
    }

    // ---- Postgres-backed route tests for #756/#757 ----
    // Each needs TEST_DATABASE_URL and runs with `--ignored --test-threads=1`.
    // Multi-thread flavor: `TestDb::drop` blocks its thread while it cleans up.

    use lumenqraph_core::db_test::TestDb;

    fn db_state(db: &TestDb, health: HealthConfig) -> HttpState {
        HttpState {
            pool: Arc::new(db.pool().clone()),
            spec_cache: Arc::new(SpecCache::new(16, 2)),
            metrics_bearer_token: None,
            health,
        }
    }

    /// Advance the cursor once, as a successful poll cycle would.
    async fn advance_cursor(db: &TestDb, last: i64, tip: i64) {
        let snap = crate::cursor::read_snapshot(db.pool()).await.unwrap();
        crate::cursor::write_progress(db.pool(), snap.version, last, tip, 1)
            .await
            .unwrap();
    }

    /// Backdate `progress_at` by `secs` seconds, leaving `updated_at` fresh.
    async fn backdate_progress_at(db: &TestDb, secs: i64) {
        sqlx::query(
            "UPDATE indexer_cursor SET progress_at = now() - make_interval(secs => $1) WHERE id = 1",
        )
        .bind(secs as f64)
        .execute(db.pool())
        .await
        .unwrap();
    }

    /// Regression test for #756: a fresh cursor answers 200 with "ok".
    #[tokio::test(flavor = "multi_thread")]
    #[ignore] // Requires TEST_DATABASE_URL (CI runs `--ignored`)
    async fn health_returns_200_when_cursor_fresh() {
        let db = TestDb::new_with_migrations("../../migrations").await;
        advance_cursor(&db, 1000, 1000).await;

        let app = app_with(db_state(&db, HealthConfig::default()));
        let (status, body) = get_json(app, "/health").await;
        assert_eq!(status, StatusCode::OK, "body: {body}");
        assert_eq!(body["status"], "ok");
        assert_eq!(body["last_processed_ledger"], 1000);
        assert_eq!(body["lag_ledgers"], 0);
        assert_eq!(body["pending_deliveries"], 0);
        assert!(
            body["seconds_since_progress"].as_i64().unwrap() < 60,
            "progress must be fresh: {body}"
        );
    }

    /// Regression test for #756 (the original repro): a stale cursor must
    /// answer 503, not 200 — before the fix the handler always returned 200
    /// with `"status": "degraded"` in the body.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore] // Requires TEST_DATABASE_URL (CI runs `--ignored`)
    async fn health_returns_503_when_cursor_stale() {
        let db = TestDb::new_with_migrations("../../migrations").await;
        advance_cursor(&db, 1000, 1000).await;
        // Wedged poller: nothing (neither heartbeat nor progress) for an hour.
        sqlx::query(
            "UPDATE indexer_cursor SET updated_at = now() - make_interval(secs => 3600),
                                     progress_at = now() - make_interval(secs => 3600) WHERE id = 1",
        )
        .execute(db.pool())
        .await
        .unwrap();

        let app = app_with(db_state(&db, HealthConfig::default()));
        let (status, body) = get_json(app, "/health").await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "stale cursor must be 503, not 200 (issue #756); body: {body}"
        );
        assert_eq!(body["status"], "degraded");
        assert!(body["seconds_since_progress"].as_i64().unwrap() >= 3600);
        assert!(
            body["degraded_reasons"].to_string().contains("stale"),
            "body: {body}"
        );
    }

    /// #757: freshness must come from `progress_at`, not `updated_at`. Even
    /// with a fresh `updated_at` (counter writes just ran), a stale
    /// `progress_at` means the poller is not advancing → 503.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore] // Requires TEST_DATABASE_URL (CI runs `--ignored`)
    async fn health_returns_503_when_progress_stale_but_heartbeat_fresh() {
        let db = TestDb::new_with_migrations("../../migrations").await;
        advance_cursor(&db, 1000, 1000).await;
        backdate_progress_at(&db, 3600).await; // updated_at stays now()

        let app = app_with(db_state(&db, HealthConfig::default()));
        let (status, body) = get_json(app, "/health").await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "fresh heartbeat must not mask stale progress (#757); body: {body}"
        );
        assert_eq!(body["status"], "degraded");
        assert!(body["seconds_since_cursor_update"].as_i64().unwrap() < 60);
        assert!(body["seconds_since_progress"].as_i64().unwrap() >= 3600);
    }

    /// #757 stuck-poller repro through the route: every cycle fails, so error
    /// counters keep `updated_at` fresh while the cursor never advances.
    /// Before the fix this answered 200/"ok"; it must be 503.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore] // Requires TEST_DATABASE_URL (CI runs `--ignored`)
    async fn health_returns_503_when_stuck_counter_writes_only() {
        let db = TestDb::new_with_migrations("../../migrations").await;
        for i in 1..=5 {
            crate::cursor::incr_errors(db.pool()).await.unwrap();
            crate::cursor::set_consecutive_errors(db.pool(), i)
                .await
                .unwrap();
            crate::cursor::track_rpc_call(db.pool(), 10, 3, 1, 0)
                .await
                .unwrap();
        }

        let app = app_with(db_state(&db, HealthConfig::default()));
        let (status, body) = get_json(app, "/health").await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "stuck poller must be 503, not 200 (issues #756/#757); body: {body}"
        );
        assert_eq!(body["status"], "degraded");
        // Heartbeat looks fresh (counter writes), progress never happened.
        assert!(body["seconds_since_cursor_update"].as_i64().unwrap() < 60);
        assert!(body["seconds_since_progress"].is_null());
        assert!(
            body["degraded_reasons"]
                .to_string()
                .contains("never advanced"),
            "body: {body}"
        );
    }

    /// Degraded on ledger lag: tip far ahead of the cursor → 503.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore] // Requires TEST_DATABASE_URL (CI runs `--ignored`)
    async fn health_returns_503_when_lag_exceeds_threshold() {
        let db = TestDb::new_with_migrations("../../migrations").await;
        // Cursor at 1000, chain tip at 1500: lag 500 > HEALTH_MAX_LAG_LEDGERS=100.
        advance_cursor(&db, 1000, 1500).await;

        let app = app_with(db_state(&db, HealthConfig::default()));
        let (status, body) = get_json(app, "/health").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "body: {body}");
        assert_eq!(body["status"], "degraded");
        assert_eq!(body["lag_ledgers"], 500);
        assert!(
            body["degraded_reasons"].to_string().contains("lag"),
            "body: {body}"
        );
    }

    /// Degraded on queue depth: pending webhook deliveries over the threshold
    /// → 503.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore] // Requires TEST_DATABASE_URL (CI runs `--ignored`)
    async fn health_returns_503_when_webhook_queue_depth_too_high() {
        let db = TestDb::new_with_migrations("../../migrations").await;
        advance_cursor(&db, 1000, 1000).await;

        // One subscription + two events + one pending delivery each
        // (webhook_deliveries is unique per (subscription_id, event_id)).
        let sub_id: uuid::Uuid = sqlx::query_scalar(
            "INSERT INTO webhook_subscriptions (url, secret) VALUES ('https://example.com/hook', 's3cret') RETURNING id",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        for (event_id, paging) in [("evt-1", "paging-1"), ("evt-2", "paging-2")] {
            sqlx::query(
                "INSERT INTO events (event_id, contract_id, ledger, ledger_closed_at, event_type, topics, value, tx_hash, in_successful_call, paging_token)
                 VALUES ($1, 'CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4', 1000, now(), 'contract', '[]', '', 'txhash', true, $2)",
            )
            .bind(event_id)
            .bind(paging)
            .execute(db.pool())
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO webhook_deliveries (subscription_id, event_id, status) VALUES ($1, $2, 'pending')",
            )
            .bind(sub_id)
            .bind(event_id)
            .execute(db.pool())
            .await
            .unwrap();
        }

        // Threshold of 2 pending deliveries: exactly at the limit → degraded.
        let cfg = HealthConfig {
            max_pending_deliveries: 2,
            ..HealthConfig::default()
        };
        let app = app_with(db_state(&db, cfg));
        let (status, body) = get_json(app, "/health").await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "queue depth over threshold must be 503; body: {body}"
        );
        assert_eq!(body["pending_deliveries"], 2);
        assert!(
            body["degraded_reasons"]
                .to_string()
                .contains("pending webhook"),
            "body: {body}"
        );
    }

    /// With `INDEXER_HEALTH_STRICT=false`, `/health` keeps the legacy 200 +
    /// "degraded" body, but `/readyz` stays strict (503).
    #[tokio::test(flavor = "multi_thread")]
    #[ignore] // Requires TEST_DATABASE_URL (CI runs `--ignored`)
    async fn non_strict_health_returns_200_but_readyz_stays_503_when_degraded() {
        let db = TestDb::new_with_migrations("../../migrations").await;
        advance_cursor(&db, 1000, 1000).await;
        backdate_progress_at(&db, 3600).await;

        let non_strict = HealthConfig {
            strict: false,
            ..HealthConfig::default()
        };
        let app = app_with(db_state(&db, non_strict));

        let (status, body) = get_json(app.clone(), "/health").await;
        assert_eq!(
            status,
            StatusCode::OK,
            "legacy mode keeps 200; body: {body}"
        );
        assert_eq!(body["status"], "degraded");

        let (status, body) = get_json(app, "/readyz").await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "/readyz is always strict; body: {body}"
        );
        assert_eq!(body["status"], "degraded");
    }

    /// `/livez` stays 200 even while degraded: liveness must not depend on the
    /// indexer making progress (#756).
    #[tokio::test(flavor = "multi_thread")]
    #[ignore] // Requires TEST_DATABASE_URL (CI runs `--ignored`)
    async fn livez_returns_200_even_when_degraded() {
        let db = TestDb::new_with_migrations("../../migrations").await;
        advance_cursor(&db, 1000, 1000).await;
        backdate_progress_at(&db, 3600).await;

        let app = app_with(db_state(&db, HealthConfig::default()));
        let (status, body) = get_json(app, "/livez").await;
        assert_eq!(status, StatusCode::OK, "body: {body}");
        assert_eq!(body["status"], "ok");
    }

    /// `/readyz` is 200 when healthy and 503 when degraded.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore] // Requires TEST_DATABASE_URL (CI runs `--ignored`)
    async fn readyz_returns_200_when_healthy_503_when_degraded() {
        let db = TestDb::new_with_migrations("../../migrations").await;
        advance_cursor(&db, 1000, 1000).await;

        let app = app_with(db_state(&db, HealthConfig::default()));
        let (status, body) = get_json(app.clone(), "/readyz").await;
        assert_eq!(status, StatusCode::OK, "body: {body}");
        assert_eq!(body["status"], "ok");

        backdate_progress_at(&db, 3600).await;
        let app = app_with(db_state(&db, HealthConfig::default()));
        let (status, body) = get_json(app, "/readyz").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "body: {body}");
        assert_eq!(body["status"], "degraded");
    }

    #[test]
    fn metrics_render_process_counters() {
        let mut body = String::new();
        render_process_counters(&mut body);

        // Both process-wide counters must be exported with HELP/TYPE lines.
        assert!(
            body.contains("lumenqraph_indexer_transfer_projection_skipped_total"),
            "missing transfer_projection_skipped_total in metrics body:\n{body}"
        );
        assert!(
            body.contains("# TYPE lumenqraph_indexer_transfer_projection_skipped_total counter"),
            "missing TYPE line for transfer_projection_skipped_total:\n{body}"
        );
        assert!(
            body.contains("lumenqraph_indexer_webhook_deliveries_dropped_by_retention_total"),
            "missing webhook_deliveries_dropped_by_retention_total in metrics body:\n{body}"
        );
        assert!(
            body.contains(
                "# TYPE lumenqraph_indexer_webhook_deliveries_dropped_by_retention_total counter"
            ),
            "missing TYPE line for webhook_deliveries_dropped_by_retention_total:\n{body}"
        );
        assert!(
            body.contains("lumenqraph_indexer_heartbeat_timestamp_seconds"),
            "missing heartbeat_timestamp_seconds in metrics body:\n{body}"
        );
        assert!(
            body.contains("# TYPE lumenqraph_indexer_heartbeat_timestamp_seconds gauge"),
            "missing TYPE line for heartbeat_timestamp_seconds:\n{body}"
        );
    }
}
