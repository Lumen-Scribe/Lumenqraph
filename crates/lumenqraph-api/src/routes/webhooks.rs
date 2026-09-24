//! Webhook subscription management. Consumers register a URL (+ optional
//! contract/event filters) and receive an HMAC-signing `secret` once, at
//! creation. The `lumenqraph-webhooks` service does the actual delivery.

use axum::extract::{Path, Query, State};
use axum::Json;
use lumenqraph_core::WebhookSubscription;
use rand::RngCore;
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;
use sqlx::PgPool;
use tracing::warn;

use crate::error::{ApiError, ApiResult};
use crate::state::AppState;
use crate::url_validation;

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
) -> ApiResult<Json<WebhookSubscription>> {
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
        "INSERT INTO webhook_subscriptions (url, kind, contract_id, event_name, secret, encrypted_secret, starting_seq)
         VALUES ($1, $2, $3, $4, '[encrypted]', pgp_sym_encrypt($5, $6), $7)
         RETURNING id, url, kind, contract_id, event_name, '[encrypted]' as secret, active, created_at",
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
    
    // Return the secret in the response (this is the only time it's exposed)
    let mut response = serde_json::to_value(&sub)?;
    if let Some(obj) = response.as_object_mut() {
        obj.insert("secret".to_string(), serde_json::Value::String(secret));
    }
    
    Ok(Json(serde_json::from_value(response)?))
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
