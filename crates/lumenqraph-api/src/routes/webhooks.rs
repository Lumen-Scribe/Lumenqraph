//! Webhook subscription management. Consumers register a URL (+ optional
//! contract/event filters) and receive an HMAC-signing `secret` once, at
//! creation. The `lumenqraph-webhooks` service does the actual delivery.

use axum::extract::{Path, Query, State};
use axum::response::Response;
use axum::Json;
use lumenqraph_core::WebhookSubscription;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;
use sqlx::PgPool;
use tracing::warn;

use crate::error::{ApiError, ApiResult};
use crate::pagination::{self, IdCursor, Page, PageRequest};
use crate::state::AppState;
use crate::url_validation;

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
        let seq: Option<i64> = sqlx::query_scalar("SELECT min(seq) FROM events WHERE ledger >= $1")
            .bind(ledger)
            .fetch_optional(pool)
            .await?
            .flatten();
        Ok(seq.unwrap_or(0))
    } else {
        let ts = chrono::DateTime::parse_from_rfc3339(since)
            .map_err(|_| ApiError::bad_request("invalid timestamp format; expected ISO-8601"))?
            .with_timezone(&chrono::Utc);
        let seq: Option<i64> = sqlx::query_scalar("SELECT min(seq) FROM events WHERE ledger_closed_at >= $1")
            .bind(ts)
            .fetch_optional(pool)
            .await?
            .flatten();
        Ok(seq.unwrap_or(0))
    }
}

/// (id, url, kind, contract_id, event_name, active, created_at, auto_disabled_at, auto_disabled_reason)
type WebhookListRow = (
    Uuid,
    String,
    String,
    Option<String>,
    Option<String>,
    bool,
    chrono::DateTime<chrono::Utc>,
    Option<chrono::DateTime<chrono::Utc>>,
    Option<String>,
);

/// List subscriptions without exposing their secrets.
pub async fn list_webhooks(State(state): State<AppState>) -> ApiResult<Json<Vec<Value>>> {
    let rows: Vec<WebhookListRow> = sqlx::query_as(
        "SELECT id, url, kind, contract_id, event_name, active, created_at, auto_disabled_at, auto_disabled_reason
             FROM webhook_subscriptions ORDER BY created_at DESC",
    )
    .fetch_all(&state.pool)
    .await?;

    let out = rows
        .into_iter()
        .map(
            |(id, url, kind, contract_id, event_name, active, created_at, auto_disabled_at, auto_disabled_reason)| {
                json!({
                    "id": id,
                    "url": url,
                    "kind": kind,
                    "contract_id": contract_id,
                    "event_name": event_name,
                    "active": active,
                    "created_at": created_at,
                    "auto_disabled_at": auto_disabled_at,
                    "auto_disabled_reason": auto_disabled_reason,
                })
            },
        )
        .collect();
    Ok(Json(out))
}

pub async fn update_webhook(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateWebhook>,
) -> ApiResult<Json<Value>> {
    // Check that at least one field is being updated
    if body.active.is_none() && body.contract_id.is_none() && body.event_name.is_none() {
        return Err(ApiError::bad_request("no fields to update"));
    }

    // Validate filters if updating them
    if let Some(ref contract_id) = body.contract_id {
        if contract_id.is_empty() {
            return Err(ApiError::bad_request("contract_id cannot be empty"));
        }
    }

    // Get current subscription
    let current: (String, bool, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT kind, active, contract_id, event_name FROM webhook_subscriptions WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or_else(|| ApiError::not_found("webhook subscription not found"))?;

    let (kind, _active, cur_contract, cur_event) = current;

    // Validate kind-specific constraints
    if kind == "upgrade" && body.event_name.is_some() {
        return Err(ApiError::bad_request(
            "event_name does not apply to an `upgrade` subscription",
        ));
    }

    // Apply updates
    let updated: (Uuid, String, String, Option<String>, Option<String>, bool, chrono::DateTime<chrono::Utc>) = sqlx::query_as(
        "UPDATE webhook_subscriptions
            SET active      = COALESCE($2, active),
                contract_id = COALESCE($3, contract_id),
                event_name  = COALESCE($4, event_name)
          WHERE id = $1
          RETURNING id, url, kind, contract_id, event_name, active, created_at",
    )
    .bind(id)
    .bind(body.active)
    .bind(body.contract_id.as_ref().or(cur_contract.as_ref()))
    .bind(body.event_name.as_ref().or(cur_event.as_ref()))
    .fetch_one(&state.pool)
    .await?;

    log_webhook_action(&state.pool, "webhook_update", &id.to_string()).await;

    Ok(Json(json!({
        "id": updated.0,
        "url": updated.1,
        "kind": updated.2,
        "contract_id": updated.3,
        "event_name": updated.4,
        "active": updated.5,
        "created_at": updated.6,
    })))
}

pub async fn delete_webhook(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<Value>> {
    let result = sqlx::query("DELETE FROM webhook_subscriptions WHERE id = $1")
        .bind(id)
        .execute(&state.pool)
        .await?;

    if result.rows_affected() == 0 {
        return Err(ApiError::not_found("webhook subscription not found"));
    }

    log_webhook_action(&state.pool, "webhook_delete", &id.to_string()).await;
    Ok(Json(json!({ "deleted": true })))
}

/// Delivery history row for a webhook subscription.
#[derive(Serialize, sqlx::FromRow)]
pub struct DeliveryRow {
    id: i64,
    status: String,
    attempts: i32,
    last_error: Option<String>,
    delivered_at: Option<chrono::DateTime<chrono::Utc>>,
    created_at: chrono::DateTime<chrono::Utc>,
}

/// Per-status delivery counts, returned only with `?include_summary=true`.
#[derive(Serialize, sqlx::FromRow)]
pub struct DeliverySummary {
    total: i64,
    delivered: i64,
    failed: i64,
    pending: i64,
}

#[derive(Serialize)]
pub struct DeliveriesResponse {
    #[serde(flatten)]
    page: Page<DeliveryRow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    summary: Option<DeliverySummary>,
}

#[derive(Deserialize)]
pub struct DeliveriesQuery {
    /// Maximum number of deliveries to return (default 50, max 500).
    #[serde(default = "default_deliveries_limit")]
    limit: i64,
    /// Deprecated offset pagination (capped at `pagination::MAX_OFFSET`).
    #[serde(default)]
    offset: i64,
    /// Opaque cursor from a previous response's `next_cursor`.
    after: Option<String>,
    /// Also return per-status counts. Costs an aggregate over every delivery
    /// of the subscription, so it is opt-in.
    #[serde(default)]
    include_summary: bool,
}

fn default_deliveries_limit() -> i64 {
    50
}

/// `GET /webhooks/:id/deliveries` — delivery attempts, newest first, keyset
/// paginated on the delivery `id`.
pub async fn list_webhook_deliveries(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(q): Query<DeliveriesQuery>,
) -> ApiResult<Response> {
    let page = PageRequest::<IdCursor>::parse(q.limit, 500, q.offset, q.after.as_deref())?;

    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM webhook_subscriptions WHERE id = $1)")
            .bind(id)
            .fetch_one(&state.pool)
            .await?;
    if !exists {
        return Err(ApiError::not_found("subscription not found"));
    }

    let rows: Vec<DeliveryRow> = sqlx::query_as(
        "SELECT id, status, attempts, last_error, delivered_at, created_at
         FROM webhook_deliveries
         WHERE subscription_id = $1
           AND ($2::bigint IS NULL OR id < $2)
         ORDER BY id DESC
         LIMIT $3 OFFSET $4",
    )
    .bind(id)
    .bind(page.after.map(|c| c.0))
    .bind(page.fetch_limit())
    .bind(page.offset)
    .fetch_all(&state.pool)
    .await?;

    let summary = if q.include_summary {
        Some(
            sqlx::query_as::<_, DeliverySummary>(
                "SELECT
                   COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE status = 'delivered') AS delivered,
                   COUNT(*) FILTER (WHERE status = 'failed') AS failed,
                   COUNT(*) FILTER (WHERE status = 'pending') AS pending
                 FROM webhook_deliveries
                 WHERE subscription_id = $1",
            )
            .bind(id)
            .fetch_one(&state.pool)
            .await?,
        )
    } else {
        None
    };

    let page = page.finish(rows, |d| IdCursor(d.id));
    let offset_deprecated = page.offset_deprecated;
    Ok(pagination::respond(
        DeliveriesResponse { page, summary },
        offset_deprecated,
    ))
}

#[derive(Deserialize)]
pub struct RedriveQuery {
    since: Option<chrono::DateTime<chrono::Utc>>,
}

/// `POST /webhooks/:id/redrive` — reset failed deliveries (optionally only
/// those created at or after `since`) back to pending.
pub async fn redrive_webhook(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(query): Query<RedriveQuery>,
) -> ApiResult<Json<Value>> {
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM webhook_subscriptions WHERE id = $1)")
            .bind(id)
            .fetch_one(&state.pool)
            .await?;
    if !exists {
        return Err(ApiError::not_found("subscription not found"));
    }

    let affected = sqlx::query(
        "UPDATE webhook_deliveries
         SET status = 'pending', attempts = 0, next_attempt_at = now(), last_error = NULL
         WHERE subscription_id = $1 AND status = 'failed'
           AND ($2::timestamptz IS NULL OR created_at >= $2)",
    )
    .bind(id)
    .bind(query.since)
    .execute(&state.pool)
    .await?
    .rows_affected();

    Ok(Json(json!({ "redriven": affected })))
}

/// `POST /webhooks/:id/reenable` — clear an auto-disable and reactivate.
pub async fn reenable_webhook(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<Value>> {
    let affected = sqlx::query(
        "UPDATE webhook_subscriptions
         SET active = true, auto_disabled_at = NULL, auto_disabled_reason = NULL, consecutive_failures = 0
         WHERE id = $1 AND auto_disabled_at IS NOT NULL",
    )
    .bind(id)
    .execute(&state.pool)
    .await?
    .rows_affected();

    if affected == 0 {
        return Err(ApiError::bad_request(
            "subscription not found or not auto-disabled",
        ));
    }

    Ok(Json(json!({ "reenabled": true })))
}

#[derive(Deserialize)]
pub struct RotateSecret {
    /// Grace period in seconds during which deliveries are signed with both the
    /// previous and the new secret. Defaults to 24h.
    #[serde(default = "default_grace_seconds")]
    grace_seconds: i64,
}

fn default_grace_seconds() -> i64 {
    86_400
}

/// Rotate the signing secret for a subscription.
///
/// The previous secret is retained (encrypted) for `grace_seconds` so the
/// delivery service can sign with both secrets during the grace window; see
/// `lumenqraph-webhooks::dispatcher::send`.
pub async fn rotate_webhook_secret(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    body: Option<Json<RotateSecret>>,
) -> ApiResult<Json<Value>> {
    let grace_seconds = body
        .map(|Json(b)| b.grace_seconds)
        .unwrap_or_else(default_grace_seconds);
    if grace_seconds < 0 {
        return Err(ApiError::bad_request("grace_seconds must be non-negative"));
    }

    let encryption_key = std::env::var("WEBHOOK_ENCRYPTION_KEY")
        .unwrap_or_else(|_| "default-key-for-testing".to_string());

    let new_secret = random_secret();

    let updated: Option<(Uuid,)> = sqlx::query_as(
        "UPDATE webhook_subscriptions
            SET previous_encrypted_secret  = encrypted_secret,
                previous_secret_expires_at = now() + ($1 * interval '1 second'),
                encrypted_secret           = pgp_sym_encrypt($2, $3)
          WHERE id = $4
          RETURNING id",
    )
    .bind(grace_seconds)
    .bind(&new_secret)
    .bind(&encryption_key)
    .bind(id)
    .fetch_optional(&state.pool)
    .await?;

    let (id,) = updated.ok_or_else(|| ApiError::not_found("webhook subscription not found"))?;

    log_webhook_action(&state.pool, "webhook_rotate_secret", &id.to_string()).await;

    Ok(Json(json!({
        "id": id,
        "secret": new_secret,
        "previous_secret_expires_at": chrono::Utc::now()
            + chrono::Duration::seconds(grace_seconds),
    })))
}
