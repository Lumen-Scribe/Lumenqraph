//! Webhook subscription management. Consumers register a URL (+ optional
//! contract/event filters) and receive an HMAC-signing `secret` once, at
//! creation. The `lumenqraph-webhooks` service does the actual delivery.

use axum::extract::{Path, Query, State};
use axum::Json;
use lumenqraph_core::WebhookSubscription;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;
use sqlx::PgPool;
use tracing::warn;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::error::{ApiError, ApiResult};
use crate::state::AppState;
use crate::url_validation;

type HmacSha256 = Hmac<Sha256>;

/// Default retention window (in days) for delivered/failed `webhook_deliveries`
/// rows. Overridable via `WEBHOOK_DELIVERY_RETENTION_DAYS`; `0` disables pruning.
pub const DEFAULT_WEBHOOK_DELIVERY_RETENTION_DAYS: i64 = 14;

/// How often the background pruner runs. Slow on purpose: pruning is a
/// housekeeping task, not a latency-sensitive path.
const WEBHOOK_DELIVERY_PRUNE_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(10 * 60);

/// Resolve the configured retention window. Returns `None` when pruning is
/// disabled (`WEBHOOK_DELIVERY_RETENTION_DAYS=0`).
fn webhook_delivery_retention_days() -> Option<i64> {
    let days = std::env::var("WEBHOOK_DELIVERY_RETENTION_DAYS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(DEFAULT_WEBHOOK_DELIVERY_RETENTION_DAYS);
    if days <= 0 {
        None
    } else {
        Some(days)
    }
}

/// Delete delivered/failed `webhook_deliveries` rows older than `retention_days`
/// in batches. `pending` rows are never touched. Returns the number of rows
/// removed.
pub async fn prune_webhook_deliveries(pool: &PgPool, retention_days: i64) -> Result<u64, sqlx::Error> {
    if retention_days <= 0 {
        return Ok(0);
    }
    let cutoff = chrono::Utc::now() - chrono::Duration::days(retention_days);
    let mut total: u64 = 0;
    loop {
        let deleted = sqlx::query(
            "DELETE FROM webhook_deliveries
             WHERE id IN (
                 SELECT id FROM webhook_deliveries
                 WHERE status IN ('delivered', 'failed')
                   AND created_at < $1
                 ORDER BY created_at
                 LIMIT 1000
             )",
        )
        .bind(cutoff)
        .execute(pool)
        .await?
        .rows_affected();
        total += deleted;
        if deleted < 1000 {
            break;
        }
    }
    Ok(total)
}

/// Spawn the slow background loop that prunes old webhook deliveries. The
/// webhooks service owns `webhook_deliveries`, so pruning lives here rather
/// than in the indexer.
pub fn spawn_webhook_delivery_pruner(pool: PgPool) {
    let Some(retention_days) = webhook_delivery_retention_days() else {
        tracing::info!("webhook delivery pruning disabled (WEBHOOK_DELIVERY_RETENTION_DAYS=0)");
        return;
    };
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(WEBHOOK_DELIVERY_PRUNE_INTERVAL);
        // Skip the immediate first tick so startup isn't blocked on a big delete.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            match prune_webhook_deliveries(&pool, retention_days).await {
                Ok(0) => {}
                Ok(n) => tracing::info!(rows = n, "pruned old webhook deliveries"),
                Err(e) => warn!(error = %e, "failed to prune webhook deliveries"),
            }
        }
    });
}

async fn log_webhook_action(
    pool: &PgPool,
    action_type: &str,
    resource_id: &str,
) {
    if let Err(e) = sqlx::query(
        "INSERT INTO audit_log (key_hash_prefix, route, http_method, status_code, action_type, resource_id)
         VALUES ('webhook', '/webhooks', 'MUTATION', 200, $1, $2)"
    )
    .bind(action_type)
    .bind(resource_id)
    .execute(pool)
    .await
    {
        warn!(error = %e, "failed to log webhook action");
    }
}

#[derive(Deserialize)]
pub struct CreateWebhook {
    url: String,
    /// `"event"` (default) or `"upgrade"`. Defaulting preserves the behaviour of
    /// every caller written before upgrade subscriptions existed.
    #[serde(default = "default_kind")]
    kind: String,
    contract_id: Option<String>,
    event_name: Option<String>,
    /// Optional backfill: "last N", a ledger number, or a timestamp (ISO-8601).
    /// Defaults to current watermark (no backfill).
    #[serde(default)]
    since: Option<String>,
}

#[derive(Deserialize)]
pub struct UpdateWebhook {
    /// Toggle active/paused state of the subscription
    active: Option<bool>,
    /// Update contract filter
    contract_id: Option<String>,
    /// Update event name filter
    event_name: Option<String>,
}

/// Response returned by `create_webhook`. Carries the one-time plaintext
/// signing secret alongside the persisted subscription. The secret is never
/// stored in plaintext and is only exposed here, at creation time.
#[derive(Serialize)]
pub struct CreatedWebhook {
    #[serde(flatten)]
    subscription: WebhookSubscription,
    /// One-time HMAC signing secret. Not retrievable after creation.
    secret: String,
}

fn default_kind() -> String {
    "event".to_string()
}

fn random_secret() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

pub async fn create_webhook(
    State(state): State<AppState>,
    Json(body): Json<CreateWebhook>,
) -> ApiResult<Json<CreatedWebhook>> {
    if state.webhook_max_subscriptions > 0 {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM webhook_subscriptions")
            .fetch_one(&state.pool)
            .await?;
        if count as usize >= state.webhook_max_subscriptions {
            return Err(ApiError::bad_request(format!(
                "maximum webhook subscriptions limit reached ({})",
                state.webhook_max_subscriptions
            )));
        }
    }

    url_validation::validate_webhook_url(&body.url)
        .map_err(|e| ApiError::bad_request(format!("invalid webhook url: {}", e)))?;

    if !matches!(body.kind.as_str(), "event" | "upgrade") {
        return Err(ApiError::bad_request(format!(
            "unknown kind `{}`; expected `event` or `upgrade`",
            body.kind
        )));
    }
    if body.kind == "upgrade" && body.event_name.is_some() {
        return Err(ApiError::bad_request(
            "event_name does not apply to an `upgrade` subscription; \
             use contract_id to watch one contract, or omit it to watch all",
        ));
    }
    let secret = random_secret();

    let starting_seq = if let Some(ref since) = body.since {
        calculate_starting_seq(&state.pool, since).await?
    } else {
        0
    };

    let encryption_key = std::env::var("WEBHOOK_ENCRYPTION_KEY")
        .unwrap_or_else(|_| "default-key-for-testing".to_string());

    let sub: WebhookSubscription = sqlx::query_as(
        "INSERT INTO webhook_subscriptions (url, kind, contract_id, event_name, encrypted_secret, starting_seq)
         VALUES ($1, $2, $3, $4, pgp_sym_encrypt($5, $6), $7)
         RETURNING id, url, kind, contract_id, event_name, active, created_at",
    )
    .bind(&body.url)
    .bind(&body.kind)
    .bind(&body.contract_id)
    .bind(&body.event_name)
    .bind(&secret)
    .bind(&encryption_key)
    .bind(starting_seq)
    .fetch_one(&state.pool)
    .await?;

    log_webhook_action(&state.pool, "webhook_create", &sub.id.to_string()).await;

    // Return the secret in the response (this is the only time it's exposed).
    Ok(Json(CreatedWebhook {
        subscription: sub,
        secret,
    }))
}

async fn calculate_starting_seq(pool: &sqlx::PgPool, since: &str) -> ApiResult<i64> {
    if since.starts_with("last ") {
        let count_str = since.strip_prefix("last ").unwrap_or("0");
        let count: i64 = count_str.parse()
            .map_err(|_| ApiError::bad_request("invalid 'last N' format; expected 'last <number>'"))?;
        let current_max: i64 = sqlx::query_scalar("SELECT COALESCE(max(seq), 0) FROM events")
            .fetch_one(pool)
            .await?;
        Ok((current_max - count).max(0))
    } else if let Ok(ledger) = since.parse::<i64>() {
        let seq: Option<i64> = sqlx::query_scalar(
            "SELECT seq FROM events WHERE ledger >= $1 ORDER BY seq ASC LIMIT 1",
        )
        .bind(ledger)
        .fetch_optional(pool)
        .await?;
        Ok(seq.map(|s| s - 1).unwrap_or(0))
    } else if let Ok(ts) = chrono::DateTime::parse_from_rfc3339(since) {
        let ts_utc = ts.with_timezone(&chrono::Utc);
        let seq: Option<i64> = sqlx::query_scalar(
            "SELECT seq FROM events WHERE ledger_closed_at >= $1 ORDER BY seq ASC LIMIT 1",
        )
        .bind(ts_utc)
        .fetch_optional(pool)
        .await?;
        Ok(seq.map(|s| s - 1).unwrap_or(0))
    } else {
        Err(ApiError::bad_request(
            "invalid 'since' format; expected 'last N', a ledger number, or an ISO-8601 timestamp",
        ))
    }
}

#[derive(Deserialize)]
pub struct ListWebhooksQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}

pub async fn list_webhooks(
    State(state): State<AppState>,
    Query(params): Query<ListWebhooksQuery>,
) -> ApiResult<Json<Value>> {
    let limit = params.limit.unwrap_or(50).min(200);
    let offset = params.offset.unwrap_or(0);

    let rows: Vec<WebhookSubscription> = sqlx::query_as(
        "SELECT id, url, kind, contract_id, event_name, active, created_at
         FROM webhook_subscriptions
         ORDER BY created_at DESC
         LIMIT $1 OFFSET $2",
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.pool)
    .await?;

    Ok(Json(json!({ "webhooks": rows, "count": rows.len() })))
}

pub async fn delete_webhook(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<Value>> {
    let deleted = sqlx::query(
        "DELETE FROM webhook_subscriptions WHERE id = $1",
    )
    .bind(id)
    .execute(&state.pool)
    .await?
    .rows_affected();

    if deleted == 0 {
        return Err(ApiError::not_found("webhook subscription not found"));
    }

    log_webhook_action(&state.pool, "webhook_delete", &id.to_string()).await;
    Ok(Json(json!({ "deleted": true })))
}

pub async fn update_webhook(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateWebhook>,
) -> ApiResult<Json<WebhookSubscription>> {
    // Verify it exists first.
    let exists: Option<bool> = sqlx::query_scalar(
        "SELECT active FROM webhook_subscriptions WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?;

    if exists.is_none() {
        return Err(ApiError::not_found("webhook subscription not found"));
    }

    let sub: WebhookSubscription = sqlx::query_as(
        "UPDATE webhook_subscriptions
         SET active       = COALESCE($2, active),
             contract_id  = CASE WHEN $3::text IS NOT NULL THEN $3 ELSE contract_id END,
             event_name   = CASE WHEN $4::text IS NOT NULL THEN $4 ELSE event_name END
         WHERE id = $1
         RETURNING id, url, kind, contract_id, event_name, active, created_at",
    )
    .bind(id)
    .bind(body.active)
    .bind(body.contract_id)
    .bind(body.event_name)
    .fetch_one(&state.pool)
    .await?;

    log_webhook_action(&state.pool, "webhook_update", &id.to_string()).await;
    Ok(Json(sub))
}

#[derive(Deserialize)]
pub struct ListDeliveriesQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}

pub async fn list_webhook_deliveries(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(params): Query<ListDeliveriesQuery>,
) -> ApiResult<Json<Value>> {
    // Verify subscription exists.
    let exists: Option<bool> = sqlx::query_scalar(
        "SELECT active FROM webhook_subscriptions WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?;

    if exists.is_none() {
        return Err(ApiError::not_found("webhook subscription not found"));
    }

    let limit = params.limit.unwrap_or(50).min(200);
    let offset = params.offset.unwrap_or(0);

    let rows: Vec<(i64, String, i32, Option<String>, Option<chrono::DateTime<chrono::Utc>>, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
        "SELECT id, status, attempts, last_error, delivered_at, created_at
         FROM webhook_deliveries
         WHERE subscription_id = $1
         ORDER BY created_at DESC
         LIMIT $2 OFFSET $3",
    )
    .bind(id)
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.pool)
    .await?;

    let deliveries: Vec<Value> = rows.into_iter().map(|(did, status, attempts, last_error, delivered_at, created_at)| {
        json!({
            "id": did,
            "status": status,
            "attempts": attempts,
            "last_error": last_error,
            "delivered_at": delivered_at,
            "created_at": created_at,
        })
    }).collect();

    Ok(Json(json!({ "deliveries": deliveries, "count": deliveries.len() })))
}

pub async fn redrive_webhook(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<Value>> {
    // Verify subscription exists.
    let exists: Option<bool> = sqlx::query_scalar(
        "SELECT active FROM webhook_subscriptions WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?;

    if exists.is_none() {
        return Err(ApiError::not_found("webhook subscription not found"));
    }

    // Re-queue all failed deliveries for this subscription.
    let redriven = sqlx::query(
        "UPDATE webhook_deliveries
         SET status = 'pending', attempts = 0, last_error = NULL, next_attempt_at = now()
         WHERE subscription_id = $1 AND status = 'failed'",
    )
    .bind(id)
    .execute(&state.pool)
    .await?
    .rows_affected();

    log_webhook_action(&state.pool, "webhook_redrive", &id.to_string()).await;
    Ok(Json(json!({ "redriven": redriven })))
}

pub async fn reenable_webhook(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<WebhookSubscription>> {
    let sub: Option<WebhookSubscription> = sqlx::query_as(
        "UPDATE webhook_subscriptions
         SET active = true,
             consecutive_failures = 0,
             auto_disabled_at = NULL,
             auto_disabled_reason = NULL
         WHERE id = $1
         RETURNING id, url, kind, contract_id, event_name, active, created_at",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?;

    match sub {
        Some(s) => {
            log_webhook_action(&state.pool, "webhook_reenable", &id.to_string()).await;
            Ok(Json(s))
        }
        None => Err(ApiError::not_found("webhook subscription not found")),
    }
}

pub async fn rotate_webhook_secret(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<Value>> {
    let exists: Option<bool> = sqlx::query_scalar(
        "SELECT active FROM webhook_subscriptions WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?;

    if exists.is_none() {
        return Err(ApiError::not_found("webhook subscription not found"));
    }

    let new_secret = random_secret();
    let encryption_key = std::env::var("WEBHOOK_ENCRYPTION_KEY")
        .unwrap_or_else(|_| "default-key-for-testing".to_string());

    sqlx::query(
        "UPDATE webhook_subscriptions
         SET encrypted_secret = pgp_sym_encrypt($2, $3)
         WHERE id = $1",
    )
    .bind(id)
    .bind(&new_secret)
    .bind(&encryption_key)
    .execute(&state.pool)
    .await?;

    log_webhook_action(&state.pool, "webhook_rotate_secret", &id.to_string()).await;
    Ok(Json(json!({ "secret": new_secret })))
}

/// Response for `POST /webhooks/:id/test` (#427).
#[derive(Serialize)]
pub struct TestWebhookResponse {
    /// The HTTP status code the subscriber's endpoint returned, or `null` if
    /// the request could not be delivered (network error, timeout, etc.).
    pub status: Option<u16>,
    /// Round-trip latency in milliseconds.
    pub latency_ms: u64,
    /// Whether the delivery was considered successful (2xx response).
    pub success: bool,
    /// Error message when the delivery failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// `POST /webhooks/:id/test` — send a signed ping delivery to the subscriber's
/// endpoint to let them verify their signature-checking code (#427).
///
/// The request is signed with the subscription's secret exactly like a real
/// delivery. The response reports the receiver's HTTP status code and the
/// round-trip latency so integrators can confirm end-to-end delivery without
/// waiting for an on-chain event.
pub async fn test_webhook(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<TestWebhookResponse>> {
    let encryption_key = std::env::var("WEBHOOK_ENCRYPTION_KEY")
        .unwrap_or_else(|_| "default-key-for-testing".to_string());

    // Fetch the subscription and decrypt its secret in one query.
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT url, pgp_sym_decrypt(encrypted_secret, $2)
         FROM webhook_subscriptions
         WHERE id = $1",
    )
    .bind(id)
    .bind(&encryption_key)
    .fetch_optional(&state.pool)
    .await?;

    let (url, secret) = match row {
        Some(r) => r,
        None => return Err(ApiError::not_found("webhook subscription not found")),
    };

    // Validate the URL before sending (prevents SSRF against private networks).
    crate::url_validation::validate_webhook_url(&url)
        .map_err(|e| ApiError::bad_request(format!("subscription URL is invalid: {}", e)))?;

    // Build the signed ping payload.
    let payload = json!({
        "type": "ping",
        "subscription_id": id.to_string(),
        "sent_at": chrono::Utc::now().to_rfc3339(),
    });

    let body_bytes = serde_json::to_vec(&payload)
        .map_err(|e| ApiError::Internal(e.into()))?;

    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("invalid secret: {}", e)))?;
    mac.update(&body_bytes);
    let signature = hex::encode(mac.finalize().into_bytes());
    let timestamp = chrono::Utc::now().to_rfc3339();

    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|e| ApiError::Internal(e.into()))?;

    let start = std::time::Instant::now();
    let result = http
        .post(&url)
        .header("Content-Type", "application/json")
        .header("X-Lumenqraph-Signature", format!("sha256={signature}"))
        .header("X-Lumenqraph-Timestamp", timestamp)
        .header("X-Lumenqraph-Event", "ping")
        .header(
            "User-Agent",
            concat!("lumenqraph-api/", env!("CARGO_PKG_VERSION")),
        )
        .body(body_bytes)
        .send()
        .await;
    let latency_ms = start.elapsed().as_millis() as u64;

    log_webhook_action(&state.pool, "webhook_test", &id.to_string()).await;

    match result {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let success = resp.status().is_success();
            let error = if success {
                None
            } else {
                Some(format!("endpoint returned HTTP {}", status))
            };
            Ok(Json(TestWebhookResponse {
                status: Some(status),
                latency_ms,
                success,
                error,
            }))
        }
        Err(e) => Ok(Json(TestWebhookResponse {
            status: None,
            latency_ms,
            success: false,
            error: Some(e.to_string()),
        })),
    }
}
