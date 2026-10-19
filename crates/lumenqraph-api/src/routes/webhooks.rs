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
use axum::response::Response;
use axum::Json;
use lumenqraph_core::WebhookSubscription;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;
use sqlx::PgPool;
use sqlx::Row as _;
use tracing::warn;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::auth::CallerKeyHash;
use crate::error::{ApiError, ApiResult};
use crate::pagination::{self, IdCursor, Page, PageRequest};
use crate::state::AppState;
use crate::url_validation;

type HmacSha256 = Hmac<Sha256>;

/// Default retention window (in days) for delivered/failed `webhook_deliveries`
/// rows. Overridable via `WEBHOOK_DELIVERY_RETENTION_DAYS` (parsed once into
/// `ApiConfig`); `0` disables pruning.
pub const DEFAULT_WEBHOOK_DELIVERY_RETENTION_DAYS: i64 = 14;

/// How often the background pruner runs. Slow on purpose: pruning is a
/// housekeeping task, not a latency-sensitive path.
const WEBHOOK_DELIVERY_PRUNE_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(10 * 60);

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
pub fn spawn_webhook_delivery_pruner(pool: PgPool, retention_days: i64) {
    if retention_days <= 0 {
        tracing::info!("webhook delivery pruning disabled (WEBHOOK_DELIVERY_RETENTION_DAYS=0)");
        return;
    }
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
    #[serde(default)]
    contract_id: Option<String>,
    #[serde(default)]
    contract_ids: Option<Vec<String>>,
    #[serde(default)]
    event_name: Option<String>,
    #[serde(default)]
    event_names: Option<Vec<String>>,
    #[serde(default)]
    filter: Option<Value>,
    /// Optional backfill: "last N", a ledger number, or a timestamp (ISO-8601).
    /// Defaults to current watermark (no backfill).
    #[serde(default)]
    since: Option<String>,
}

/// Deserializes a field so that an absent key is `None` (leave unchanged) and
/// an explicit `null` is `Some(None)` (clear the stored value).
fn double_option<'de, D, T>(de: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(de).map(Some)
}

#[derive(Deserialize)]
pub struct UpdateWebhook {
    /// Toggle active/paused state of the subscription.
    active: Option<bool>,
    /// New delivery URL. Validated with the same SSRF rules as create.
    url: Option<String>,
    /// Update legacy single contract filter. `null` clears it.
    #[serde(default, deserialize_with = "double_option")]
    contract_id: Option<Option<String>>,
    /// Update legacy single event filter. `null` clears it.
    #[serde(default, deserialize_with = "double_option")]
    event_name: Option<Option<String>>,
    #[serde(default, deserialize_with = "double_option")]
    contract_ids: Option<Option<Vec<String>>>,
    #[serde(default, deserialize_with = "double_option")]
    event_names: Option<Option<Vec<String>>>,
    #[serde(default, deserialize_with = "double_option")]
    filter: Option<Option<Value>>,
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

    let contract_ids = normalize_contract_ids(body.contract_id.as_deref(), body.contract_ids.clone());
    let event_names = normalize_event_names(body.event_name.as_deref(), body.event_names.clone());

    if let Some(ref filter) = body.filter {
        validate_webhook_filter(filter)?;
    }

    if let Some(ref ids) = contract_ids {
        if ids.len() > 25 {
            return Err(ApiError::bad_request("contract_ids must contain at most 25 entries"));
        }
        for cid in ids {
            validate_contract_id(cid)?;
        }
    }
    if let Some(ref names) = event_names {
        if names.len() > 25 {
            return Err(ApiError::bad_request("event_names must contain at most 25 entries"));
        }
        for name in names {
            validate_event_name(name)?;
        }
    }

    let secret = random_secret();

    // #423: when `since` is given, set up per-subscription backfill.
    //
    // `backfill_start` is the seq *before* the first event we want to
    // backfill; `starting_seq` becomes the current global max (the watermark
    // at which the live stream takes over once the backfill completes).
    let (starting_seq, backfill_seq): (i64, Option<i64>) = if let Some(ref since) = body.since {
        let backfill_start = calculate_starting_seq(&state.pool, since).await?;
        let global_max: i64 =
            sqlx::query_scalar("SELECT COALESCE(max(seq), 0) FROM events")
                .fetch_one(&state.pool)
                .await?;
        // Only arm the backfill if there are actually events in the window.
        if backfill_start < global_max {
            (global_max, Some(backfill_start))
        } else {
            (global_max, None)
        }
    } else {
        (0, None)
    };

    // Read once at startup (#441); an unset key keeps the legacy test fallback
    // and is warned about in `main`.
    let encryption_key = state
        .config
        .webhook_encryption_key
        .as_deref()
        .unwrap_or("default-key-for-testing");

    // Set owner_key_hash from the authenticated caller (#421). Anonymous callers
    // (no API key) create subscriptions with NULL owner, which are only accessible
    // to admin keys.
    let owner_key_hash: Option<String> = caller.map(|ext| ext.0.0.clone());

    let sub: WebhookSubscription = sqlx::query_as(
        "INSERT INTO webhook_subscriptions
             (url, kind, contract_id, event_name, contract_ids, event_names, filter, encrypted_secret, starting_seq, owner_key_hash)
         VALUES ($1, $2, $3, $4, $5, $6, $7, pgp_sym_encrypt($8, $9), $10, $11)
         RETURNING id, url, kind, contract_id, event_name, contract_ids, event_names, filter, active, created_at",
    )
    .bind(&body.url)
    .bind(&body.kind)
    .bind(contract_ids.as_ref().and_then(|ids| ids.first().cloned()))
    .bind(event_names.as_ref().and_then(|names| names.first().cloned()))
    .bind(&contract_ids)
    .bind(&event_names)
    .bind(body.filter.as_ref().unwrap_or(&Value::Null))
    .bind(&secret)
    .bind(encryption_key)
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

/// Hard cap on `last N` backfill to prevent runaway queries.
const MAX_BACKFILL_COUNT: i64 = 100_000;

/// Validate a `contract_id` string as a Soroban contract address (C-strkey).
fn validate_contract_id(id: &str) -> Result<(), ApiError> {
    if lumenqraph_core::xdr::is_valid_contract_id(id) {
        Ok(())
    } else {
        Err(ApiError::bad_request(format!(
            "invalid contract_id `{id}`; expected a Soroban contract address (C… strkey)"
        )))
    }
}

/// Validate an `event_name` as a Soroban symbol (≤32 chars, `[a-zA-Z0-9_]`).
fn validate_event_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty() {
        return Err(ApiError::bad_request("event_name must not be empty"));
    }
    if name.len() > 32 {
        return Err(ApiError::bad_request(
            "event_name must be ≤32 characters (Soroban symbol limit)",
        ));
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(ApiError::bad_request(
            "event_name must contain only ASCII letters, digits, and underscores",
        ));
    }
    Ok(())
}

fn validate_webhook_filter(filter: &Value) -> Result<(), ApiError> {
    match filter {
        Value::Object(obj) => {
            if obj.len() > 16 {
                return Err(ApiError::bad_request("filter contains too many keys (max 16)"));
            }
            for (key, value) in obj {
                if key.len() > 64 {
                    return Err(ApiError::bad_request("filter key names must be <=64 chars"));
                }
                match value {
                    Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
                    Value::Array(arr) if arr.len() <= 8 => {}
                    Value::Object(_) => {
                        return Err(ApiError::bad_request(
                            "nested filter objects are not supported; use flat keys like topic2 or to",
                        ));
                    }
                    _ => {}
                }
            }
            Ok(())
        }
        Value::Null => Ok(()),
        _ => Err(ApiError::bad_request(
            "filter must be a JSON object or null; use keys like topic2 or to",
        )),
    }
}

fn normalize_contract_ids(
    legacy_single: Option<&str>,
    multi: Option<Vec<String>>,
) -> Option<Vec<String>> {
    let mut values = Vec::new();
    if let Some(val) = legacy_single {
        if !val.is_empty() {
            values.push(val.to_string());
        }
    }
    if let Some(mut list) = multi {
        values.append(&mut list);
    }
    let values = values.into_iter().filter(|v| !v.is_empty()).collect::<Vec<_>>();
    if values.is_empty() { None } else { Some(values) }
}

fn normalize_event_names(
    legacy_single: Option<&str>,
    multi: Option<Vec<String>>,
) -> Option<Vec<String>> {
    let mut values = Vec::new();
    if let Some(val) = legacy_single {
        if !val.is_empty() {
            values.push(val.to_string());
        }
    }
    if let Some(mut list) = multi {
        values.append(&mut list);
    }
    let values = values.into_iter().filter(|v| !v.is_empty()).collect::<Vec<_>>();
    if values.is_empty() { None } else { Some(values) }
}

async fn calculate_starting_seq(pool: &sqlx::PgPool, since: &str) -> ApiResult<i64> {
    if since.starts_with("last ") {
        let count_str = since.strip_prefix("last ").unwrap_or("0");
        let count: i64 = count_str.parse()
            .map_err(|_| ApiError::bad_request("invalid 'last N' format; expected 'last <number>'"))?;
        if count > MAX_BACKFILL_COUNT {
            return Err(ApiError::bad_request(format!(
                "since 'last N' exceeds maximum allowed backfill count ({MAX_BACKFILL_COUNT})"
            )));
        }
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
    Option<Vec<String>>,
    Option<Vec<String>>,
    Option<Value>,
    bool,
    chrono::DateTime<chrono::Utc>,
    Option<chrono::DateTime<chrono::Utc>>,
    Option<String>,
);

/// List subscriptions without exposing their secrets.
pub async fn list_webhooks(State(state): State<AppState>) -> ApiResult<Json<Vec<Value>>> {
    let rows: Vec<WebhookListRow> = sqlx::query_as(
        "SELECT id, url, kind, contract_id, event_name, contract_ids, event_names, filter, active, created_at, auto_disabled_at, auto_disabled_reason
             FROM webhook_subscriptions ORDER BY created_at DESC",
    )
    .fetch_all(&state.pool)
    .await?;

    let out = rows
        .into_iter()
        .map(
            |(id, url, kind, contract_id, event_name, contract_ids, event_names, filter, active, created_at, auto_disabled_at, auto_disabled_reason)| {
                json!({
                    "id": id,
                    "url": url,
                    "kind": kind,
                    "contract_id": contract_id,
                    "event_name": event_name,
                    "contract_ids": contract_ids,
                    "event_names": event_names,
                    "filter": filter,
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
    let touches_event_fields = body.event_name.is_some() || body.event_names.is_some();
    if body.active.is_none()
        && body.url.is_none()
        && body.contract_id.is_none()
        && !touches_event_fields
        && body.contract_ids.is_none()
        && body.filter.is_none()
    {
        return Err(ApiError::bad_request("no fields to update"));
    }

    if let Some(Some(ref filter)) = body.filter {
        validate_webhook_filter(filter)?;
    }
    if let Some(ref url) = body.url {
        url_validation::validate_webhook_url(url)
            .map_err(|e| ApiError::bad_request(format!("invalid webhook url: {}", e)))?;
    }
    if let Some(Some(ref ids)) = body.contract_ids {
        if ids.len() > 25 {
            return Err(ApiError::bad_request("contract_ids must contain at most 25 entries"));
        }
        for cid in ids {
            validate_contract_id(cid)?;
        }
    }
    if let Some(Some(ref names)) = body.event_names {
        if names.len() > 25 {
            return Err(ApiError::bad_request("event_names must contain at most 25 entries"));
        }
        for name in names {
            validate_event_name(name)?;
        }
    }

    let kind: String = sqlx::query_scalar("SELECT kind FROM webhook_subscriptions WHERE id = $1")
        .bind(id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| ApiError::not_found("webhook subscription not found"))?;
    if kind == "upgrade" && touches_event_fields {
        return Err(ApiError::bad_request(
            "event_name does not apply to an `upgrade` subscription",
        ));
    }

    // Each optional field is paired with a `set_*` flag so an explicit `null`
    // (clear) can be distinguished from an absent key (leave unchanged). COALESCE
    // alone cannot express "set to NULL".
    let (set_contract, contract_id) = split_clearable(body.contract_id);
    let (set_event, event_name) = split_clearable(body.event_name);
    let (set_contracts, contract_ids) = split_clearable(body.contract_ids);
    let (set_events, event_names) = split_clearable(body.event_names);
    let (set_filter, filter) = split_clearable(body.filter);
    // Re-enabling clears the auto-disable state so the subscription stays on.
    let reactivate = body.active == Some(true);

    let row: WebhookListRow = sqlx::query_as(
        "UPDATE webhook_subscriptions
            SET active       = COALESCE($2, active),
                url          = COALESCE($3, url),
                contract_id  = CASE WHEN $4 THEN $5 ELSE contract_id END,
                event_name   = CASE WHEN $6 THEN $7 ELSE event_name END,
                contract_ids = CASE WHEN $8 THEN $9 ELSE contract_ids END,
                event_names  = CASE WHEN $10 THEN $11 ELSE event_names END,
                filter       = CASE WHEN $12 THEN COALESCE($13, 'null'::jsonb) ELSE filter END,
                auto_disabled_at     = CASE WHEN $14 THEN NULL ELSE auto_disabled_at END,
                auto_disabled_reason = CASE WHEN $14 THEN NULL ELSE auto_disabled_reason END,
                consecutive_failures = CASE WHEN $14 THEN 0 ELSE consecutive_failures END
          WHERE id = $1
          RETURNING id, url, kind, contract_id, event_name, contract_ids, event_names, filter, active, created_at, auto_disabled_at, auto_disabled_reason",
    )
    .bind(id)
    .bind(body.active)
    .bind(body.url.as_deref())
    .bind(set_contract)
    .bind(contract_id.as_deref())
    .bind(set_event)
    .bind(event_name.as_deref())
    .bind(set_contracts)
    .bind(&contract_ids)
    .bind(set_events)
    .bind(&event_names)
    .bind(set_filter)
    .bind(&filter)
    .bind(reactivate)
    .fetch_one(&state.pool)
    .await?;

    log_webhook_action(&state.pool, "webhook_update", &id.to_string()).await;

    Ok(Json(webhook_row_json(row)))
}

/// Splits a tri-state PATCH field into `(should_set, value)`. An absent key
/// yields `(false, None)`; `null` yields `(true, None)` to clear the column.
fn split_clearable<T>(field: Option<Option<T>>) -> (bool, Option<T>) {
    match field {
        None => (false, None),
        Some(value) => (true, value),
    }
}

/// Serializes a subscription row in the same shape as `list_webhooks`.
fn webhook_row_json(row: WebhookListRow) -> Value {
    let (id, url, kind, contract_id, event_name, contract_ids, event_names, filter, active, created_at, auto_disabled_at, auto_disabled_reason) = row;
    json!({
        "id": id,
        "url": url,
        "kind": kind,
        "contract_id": contract_id,
        "event_name": event_name,
        "contract_ids": contract_ids,
        "event_names": event_names,
        "filter": filter,
        "active": active,
        "created_at": created_at,
        "auto_disabled_at": auto_disabled_at,
        "auto_disabled_reason": auto_disabled_reason,
    })
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
    last_status_code: Option<i32>,
    last_response_snippet: Option<String>,
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
        "SELECT id, status, attempts, last_error, last_status_code, last_response_snippet, delivered_at, created_at
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
    /// Maximum deliveries to reset in this call. Callers page by calling again
    /// while `has_more` is true.
    max: Option<i64>,
}

const DEFAULT_REDRIVE_BATCH: i64 = 1_000;
const MAX_REDRIVE_BATCH: i64 = 10_000;
/// Redriven deliveries are spread over this many seconds so a recovered
/// subscriber is not hit by every pending delivery at once.
const REDRIVE_JITTER_SECS: f64 = 300.0;

/// `POST /webhooks/:id/redrive` — reset up to `max` failed deliveries
/// (optionally only those created at or after `since`) back to pending.
pub async fn redrive_webhook(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(query): Query<RedriveQuery>,
) -> ApiResult<Json<Value>> {
    let batch = query.max.unwrap_or(DEFAULT_REDRIVE_BATCH);
    if !(1..=MAX_REDRIVE_BATCH).contains(&batch) {
        return Err(ApiError::bad_request(format!(
            "max must be between 1 and {}",
            MAX_REDRIVE_BATCH
        )));
    }

    let (active, auto_disabled_at): (bool, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as("SELECT active, auto_disabled_at FROM webhook_subscriptions WHERE id = $1")
            .bind(id)
            .fetch_optional(&state.pool)
            .await?
            .ok_or_else(|| ApiError::not_found("subscription not found"))?;
    if !active || auto_disabled_at.is_some() {
        return Err(ApiError::conflict(
            "subscription is inactive or auto-disabled; re-enable it before redriving",
        ));
    }

    let redriven = sqlx::query(
        "UPDATE webhook_deliveries
         SET status = 'pending',
             attempts = 0,
             next_attempt_at = now() + (random() * $4::float8) * interval '1 second',
             last_error = NULL,
             last_status_code = NULL,
             last_response_snippet = NULL
         WHERE id IN (
             SELECT id FROM webhook_deliveries
             WHERE subscription_id = $1 AND status = 'failed'
               AND ($2::timestamptz IS NULL OR created_at >= $2)
             ORDER BY id
             LIMIT $3
         )",
    )
    .bind(id)
    .bind(query.since)
    .bind(batch)
    .bind(REDRIVE_JITTER_SECS)
    .execute(&state.pool)
    .await?
    .rows_affected();

    let has_more: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM webhook_deliveries
             WHERE subscription_id = $1 AND status = 'failed'
               AND ($2::timestamptz IS NULL OR created_at >= $2)
         )",
    )
    .bind(id)
    .bind(query.since)
    .fetch_one(&state.pool)
    .await?;

    Ok(Json(json!({ "redriven": redriven, "has_more": has_more })))
}

/// `POST /webhooks/:id/reenable` — clear an auto-disable and reactivate.
pub async fn reenable_webhook(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<Value>> {
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM webhook_subscriptions WHERE id = $1)")
            .bind(id)
            .fetch_one(&state.pool)
            .await?;
    if !exists {
        return Err(ApiError::not_found("webhook subscription not found"));
    }

    let affected = sqlx::query(
        "UPDATE webhook_subscriptions
         SET active = true, auto_disabled_at = NULL, auto_disabled_reason = NULL, consecutive_failures = 0
         WHERE id = $1 AND auto_disabled_at IS NOT NULL",
    )
    .bind(id)
    .execute(&state.pool)
    .await?
    .rows_affected();

    // Not auto-disabled (or cleared concurrently): nothing to do, reported as a no-op.
    if affected == 0 {
        return Ok(Json(json!({ "reenabled": false })));
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

/// Longest grace window accepted for a secret rotation (7 days).
const MAX_GRACE_SECONDS: i64 = 7 * 86_400;

fn validate_grace_seconds(grace_seconds: i64) -> Result<(), ApiError> {
    if !(0..=MAX_GRACE_SECONDS).contains(&grace_seconds) {
        return Err(ApiError::bad_request(format!(
            "grace_seconds must be between 0 and {}",
            MAX_GRACE_SECONDS
        )));
    }
    Ok(())
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
    validate_grace_seconds(grace_seconds)?;

    // Same key source as create_webhook (#441); no public fallback is read from the environment.
    let encryption_key = state
        .config
        .webhook_encryption_key
        .as_deref()
        .unwrap_or("default-key-for-testing");

    let new_secret = random_secret();

    // Expiry is computed and returned by the database so the response matches
    // what the dispatcher will enforce.
    let updated: Option<(Uuid, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
        "UPDATE webhook_subscriptions
            SET previous_encrypted_secret  = encrypted_secret,
                previous_secret_expires_at = now() + ($1 * interval '1 second'),
                encrypted_secret           = pgp_sym_encrypt($2, $3)
          WHERE id = $4
          RETURNING id, previous_secret_expires_at",
    )
    .bind(grace_seconds)
    .bind(&new_secret)
    .bind(encryption_key)
    .bind(id)
    .fetch_optional(&state.pool)
    .await?;

    let (id, previous_expires_at) =
        updated.ok_or_else(|| ApiError::not_found("webhook subscription not found"))?;

    log_webhook_action(&state.pool, "webhook_rotate_secret", &id.to_string()).await;

    Ok(Json(json!({
        "id": id,
        "secret": new_secret,
        "previous_secret_expires_at": previous_expires_at,
    })))
}

#[cfg(test)]
mod update_tests {
    use super::*;

    #[test]
    fn absent_field_leaves_value_unchanged() {
        let body: UpdateWebhook = serde_json::from_str("{}").unwrap();
        assert!(body.contract_id.is_none());
        assert!(body.filter.is_none());
        assert_eq!(split_clearable(body.contract_id), (false, None));
    }

    #[test]
    fn explicit_null_clears_value() {
        let body: UpdateWebhook =
            serde_json::from_str(r#"{"contract_id": null, "filter": null}"#).unwrap();
        assert_eq!(body.contract_id, Some(None));
        assert_eq!(split_clearable(body.contract_id), (true, None));
        assert_eq!(split_clearable(body.filter), (true, None));
    }

    #[test]
    fn present_value_is_set() {
        let body: UpdateWebhook =
            serde_json::from_str(r#"{"contract_ids": ["CABC"], "url": "https://example.com/hook"}"#)
                .unwrap();
        assert_eq!(
            split_clearable(body.contract_ids),
            (true, Some(vec!["CABC".to_string()]))
        );
        assert_eq!(body.url.as_deref(), Some("https://example.com/hook"));
    }
}

#[cfg(test)]
mod rotate_tests {
    use super::*;

    #[test]
    fn grace_seconds_is_bounded() {
        assert!(validate_grace_seconds(0).is_ok());
        assert!(validate_grace_seconds(MAX_GRACE_SECONDS).is_ok());
        assert!(validate_grace_seconds(-1).is_err());
        assert!(validate_grace_seconds(MAX_GRACE_SECONDS + 1).is_err());
        // Would overflow `$1 * interval '1 second'` on the database side.
        assert!(validate_grace_seconds(i64::MAX).is_err());
    }
}
