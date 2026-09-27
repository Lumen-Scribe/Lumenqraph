//! Webhook subscription management. Consumers register a URL (+ optional
//! contract/event filters) and receive an HMAC-signing `secret` once, at
//! creation. The `lumenqraph-webhooks` service does the actual delivery.
//!
//! Issue #421: Every subscription is scoped to the API key that created it.
//! `list_webhooks` only returns the caller's own subscriptions; all other
//! mutation endpoints return 404 for subscriptions owned by other keys (to
//! avoid enumeration). Existing rows with NULL owner are only accessible to
//! callers with the "admin" role (not yet implemented) — they are simply
//! unreachable from normal keys.

use axum::extract::{Path, Query, State};
use axum::Extension;
use axum::Json;
use lumenqraph_core::WebhookSubscription;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;
use sqlx::PgPool;
use tracing::warn;

use crate::auth::CallerKeyHash;
use crate::error::{ApiError, ApiResult};
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

/// Log a webhook lifecycle event to the audit log.
///
/// `key_hash_prefix` is the caller's key hash (or a fallback string); it is
/// truncated to 8 characters, matching the audit log convention used elsewhere.
async fn log_webhook_action(
    pool: &PgPool,
    key_hash_prefix: &str,
    action_type: &str,
    resource_id: &str,
) {
    let prefix = key_hash_prefix.chars().take(8).collect::<String>();
    if let Err(e) = sqlx::query(
        "INSERT INTO audit_log (key_hash_prefix, route, http_method, status_code, action_type, resource_id)
         VALUES ($1, '/webhooks', 'MUTATION', 200, $2, $3)"
    )
    .bind(&prefix)
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
    /// Toggle active/paused state of the subscription.
    active: Option<bool>,
    /// Update contract filter.
    contract_id: Option<String>,
    /// Update event name filter.
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
    // CallerKeyHash may be absent for anonymous callers (when REQUIRE_API_KEY=false).
    caller: Option<Extension<CallerKeyHash>>,
    Json(body): Json<CreateWebhook>,
) -> ApiResult<Json<CreatedWebhook>> {
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

    // Set owner_key_hash from the authenticated caller (#421). Anonymous callers
    // (no API key) create subscriptions with NULL owner, which are only accessible
    // to admin keys.
    let owner_key_hash: Option<String> = caller.map(|ext| ext.0.0.clone());

    let sub: WebhookSubscription = sqlx::query_as(
        "INSERT INTO webhook_subscriptions
             (url, kind, contract_id, event_name, encrypted_secret, starting_seq, owner_key_hash)
         VALUES ($1, $2, $3, $4, pgp_sym_encrypt($5, $6), $7, $8)
         RETURNING id, url, kind, contract_id, event_name, active, created_at",
    )
    .bind(&body.url)
    .bind(&body.kind)
    .bind(&body.contract_id)
    .bind(&body.event_name)
    .bind(&secret)
    .bind(&encryption_key)
    .bind(starting_seq)
    .bind(&owner_key_hash)
    .fetch_one(&state.pool)
    .await?;

    let key_prefix = owner_key_hash.as_deref().unwrap_or("anon");
    log_webhook_action(&state.pool, key_prefix, "webhook_create", &sub.id.to_string()).await;

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
            "SELECT min(seq) FROM events WHERE ledger >= $1"
        )
        .bind(ledger)
        .fetch_one(pool)
        .await?;
        Ok(seq.unwrap_or(0))
    } else if let Ok(ts) = since.parse::<chrono::DateTime<chrono::Utc>>() {
        let seq: Option<i64> = sqlx::query_scalar(
            "SELECT min(seq) FROM events WHERE ledger_closed_at >= $1"
        )
        .bind(ts)
        .fetch_one(pool)
        .await?;
        Ok(seq.unwrap_or(0))
    } else {
        Err(ApiError::bad_request(
            "invalid 'since' format; expected 'last N', a ledger number, or ISO-8601 timestamp",
        ))
    }
}

/// `GET /webhooks` — list the caller's own webhook subscriptions.
///
/// Scoped to `owner_key_hash = $caller` (#421). Returns only the subscriptions
/// created with the caller's API key.
pub async fn list_webhooks(
    State(state): State<AppState>,
    Extension(caller): Extension<CallerKeyHash>,
) -> ApiResult<Json<Value>> {
    let subs: Vec<WebhookSubscription> = sqlx::query_as(
        "SELECT id, url, kind, contract_id, event_name, active, created_at
         FROM webhook_subscriptions
         WHERE owner_key_hash = $1
         ORDER BY created_at DESC",
    )
    .bind(&caller.0)
    .fetch_all(&state.pool)
    .await?;

    Ok(Json(json!({
        "subscriptions": subs,
        "count": subs.len(),
    })))
}

/// `DELETE /webhooks/:id` — delete a subscription owned by the caller.
///
/// Returns 404 (not 403) when the id doesn't belong to the caller, to avoid
/// confirming the existence of other tenants' subscriptions (#421).
pub async fn delete_webhook(
    State(state): State<AppState>,
    Extension(caller): Extension<CallerKeyHash>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<Value>> {
    let deleted = sqlx::query(
        "DELETE FROM webhook_subscriptions
         WHERE id = $1 AND owner_key_hash = $2",
    )
    .bind(id)
    .bind(&caller.0)
    .execute(&state.pool)
    .await?
    .rows_affected();

    if deleted == 0 {
        return Err(ApiError::not_found("webhook subscription not found"));
    }

    log_webhook_action(&state.pool, &caller.0, "webhook_delete", &id.to_string()).await;
    Ok(Json(json!({ "deleted": true, "id": id })))
}

/// `PATCH /webhooks/:id` — update a subscription owned by the caller.
///
/// Returns 404 when the id doesn't belong to the caller (#421).
pub async fn update_webhook(
    State(state): State<AppState>,
    Extension(caller): Extension<CallerKeyHash>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateWebhook>,
) -> ApiResult<Json<WebhookSubscription>> {
    // Build a targeted UPDATE so we only touch the fields the caller provided.
    // Using separate queries keeps the SQL readable; a webhook update is rare.
    if let Some(active) = body.active {
        sqlx::query(
            "UPDATE webhook_subscriptions SET active = $1
             WHERE id = $2 AND owner_key_hash = $3",
        )
        .bind(active)
        .bind(id)
        .bind(&caller.0)
        .execute(&state.pool)
        .await?;
    }
    if body.contract_id.is_some() {
        sqlx::query(
            "UPDATE webhook_subscriptions SET contract_id = $1
             WHERE id = $2 AND owner_key_hash = $3",
        )
        .bind(&body.contract_id)
        .bind(id)
        .bind(&caller.0)
        .execute(&state.pool)
        .await?;
    }
    if body.event_name.is_some() {
        sqlx::query(
            "UPDATE webhook_subscriptions SET event_name = $1
             WHERE id = $2 AND owner_key_hash = $3",
        )
        .bind(&body.event_name)
        .bind(id)
        .bind(&caller.0)
        .execute(&state.pool)
        .await?;
    }

    // Re-fetch to verify the row exists and belongs to the caller.
    let sub: Option<WebhookSubscription> = sqlx::query_as(
        "SELECT id, url, kind, contract_id, event_name, active, created_at
         FROM webhook_subscriptions
         WHERE id = $1 AND owner_key_hash = $2",
    )
    .bind(id)
    .bind(&caller.0)
    .fetch_optional(&state.pool)
    .await?;

    match sub {
        Some(s) => {
            log_webhook_action(&state.pool, &caller.0, "webhook_update", &id.to_string()).await;
            Ok(Json(s))
        }
        None => Err(ApiError::not_found("webhook subscription not found")),
    }
}

/// Pagination query for delivery list.
#[derive(Deserialize)]
pub struct DeliveriesQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    #[serde(default)]
    offset: i64,
}

fn default_limit() -> i64 {
    50
}

/// `GET /webhooks/:id/deliveries` — list recent delivery attempts for a subscription.
///
/// Scoped: returns 404 when the subscription doesn't belong to the caller (#421).
pub async fn list_webhook_deliveries(
    State(state): State<AppState>,
    Extension(caller): Extension<CallerKeyHash>,
    Path(id): Path<Uuid>,
    Query(q): Query<DeliveriesQuery>,
) -> ApiResult<Json<Value>> {
    // Verify ownership first (returns 404 if not owned by caller).
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM webhook_subscriptions WHERE id = $1 AND owner_key_hash = $2)",
    )
    .bind(id)
    .bind(&caller.0)
    .fetch_one(&state.pool)
    .await?;

    if !exists {
        return Err(ApiError::not_found("webhook subscription not found"));
    }

    let limit = q.limit.min(200).max(1);
    let offset = q.offset.max(0);

    let deliveries: Vec<Value> = sqlx::query_as::<_, (Uuid, String, i32, Option<String>, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
        "SELECT id, status, attempt_count, error_message, created_at, updated_at
         FROM webhook_deliveries
         WHERE subscription_id = $1
         ORDER BY created_at DESC
         LIMIT $2 OFFSET $3",
    )
    .bind(id)
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.pool)
    .await?
    .into_iter()
    .map(|(did, status, attempts, error, created, updated)| {
        json!({
            "id": did,
            "status": status,
            "attempt_count": attempts,
            "error_message": error,
            "created_at": created,
            "updated_at": updated,
        })
    })
    .collect();

    Ok(Json(json!({
        "subscription_id": id,
        "deliveries": deliveries,
        "count": deliveries.len(),
    })))
}

/// `POST /webhooks/:id/redrive` — re-queue failed deliveries for retry.
///
/// Scoped: returns 404 when the subscription doesn't belong to the caller (#421).
pub async fn redrive_webhook(
    State(state): State<AppState>,
    Extension(caller): Extension<CallerKeyHash>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<Value>> {
    // Verify ownership.
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM webhook_subscriptions WHERE id = $1 AND owner_key_hash = $2)",
    )
    .bind(id)
    .bind(&caller.0)
    .fetch_one(&state.pool)
    .await?;

    if !exists {
        return Err(ApiError::not_found("webhook subscription not found"));
    }

    let requeued = sqlx::query(
        "UPDATE webhook_deliveries
         SET status = 'pending', attempt_count = 0, error_message = NULL, updated_at = NOW()
         WHERE subscription_id = $1 AND status = 'failed'",
    )
    .bind(id)
    .execute(&state.pool)
    .await?
    .rows_affected();

    log_webhook_action(&state.pool, &caller.0, "webhook_redrive", &id.to_string()).await;
    Ok(Json(json!({
        "subscription_id": id,
        "requeued": requeued,
    })))
}

/// `POST /webhooks/:id/reenable` — re-activate a paused subscription.
///
/// Scoped: returns 404 when the subscription doesn't belong to the caller (#421).
pub async fn reenable_webhook(
    State(state): State<AppState>,
    Extension(caller): Extension<CallerKeyHash>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<Value>> {
    let updated = sqlx::query(
        "UPDATE webhook_subscriptions SET active = true
         WHERE id = $1 AND owner_key_hash = $2",
    )
    .bind(id)
    .bind(&caller.0)
    .execute(&state.pool)
    .await?
    .rows_affected();

    if updated == 0 {
        return Err(ApiError::not_found("webhook subscription not found"));
    }

    log_webhook_action(&state.pool, &caller.0, "webhook_reenable", &id.to_string()).await;
    Ok(Json(json!({ "subscription_id": id, "active": true })))
}

/// `POST /webhooks/:id/rotate-secret` — rotate the HMAC signing secret.
///
/// Returns the new plaintext secret once (caller must update their receiver).
/// Scoped: returns 404 when the subscription doesn't belong to the caller (#421).
pub async fn rotate_webhook_secret(
    State(state): State<AppState>,
    Extension(caller): Extension<CallerKeyHash>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<Value>> {
    // Verify ownership.
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM webhook_subscriptions WHERE id = $1 AND owner_key_hash = $2)",
    )
    .bind(id)
    .bind(&caller.0)
    .fetch_one(&state.pool)
    .await?;

    if !exists {
        return Err(ApiError::not_found("webhook subscription not found"));
    }

    let new_secret = random_secret();
    let encryption_key = std::env::var("WEBHOOK_ENCRYPTION_KEY")
        .unwrap_or_else(|_| "default-key-for-testing".to_string());

    sqlx::query(
        "UPDATE webhook_subscriptions
         SET encrypted_secret = pgp_sym_encrypt($1, $2)
         WHERE id = $3 AND owner_key_hash = $4",
    )
    .bind(&new_secret)
    .bind(&encryption_key)
    .bind(id)
    .bind(&caller.0)
    .execute(&state.pool)
    .await?;

    log_webhook_action(&state.pool, &caller.0, "webhook_rotate_secret", &id.to_string()).await;
    Ok(Json(json!({
        "subscription_id": id,
        "secret": new_secret,
    })))
}
