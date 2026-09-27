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
use sqlx::Row as _;
use tracing::warn;

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

    // #424: validate contract_id and event_name on creation.
    if let Some(ref cid) = body.contract_id {
        validate_contract_id(cid)?;
    }
    if let Some(ref en) = body.event_name {
        validate_event_name(en)?;
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

    let encryption_key = std::env::var("WEBHOOK_ENCRYPTION_KEY")
        .unwrap_or_else(|_| "default-key-for-testing".to_string());

    let sub: WebhookSubscription = sqlx::query_as(
        "INSERT INTO webhook_subscriptions (url, kind, contract_id, event_name, encrypted_secret, starting_seq, backfill_seq)
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
    .bind(backfill_seq)
    .fetch_one(&state.pool)
    .await?;

    log_webhook_action(&state.pool, "webhook_create", &sub.id.to_string()).await;

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
        // Resolve the first seq at or after this ledger.
        let seq: Option<i64> = sqlx::query_scalar(
            "SELECT MIN(seq) FROM events WHERE ledger >= $1",
        )
        .bind(ledger)
        .fetch_one(pool)
        .await?;
        Ok(seq.map(|s| (s - 1).max(0)).unwrap_or(0))
    } else {
        // Try to parse as an ISO-8601 timestamp.
        let ts = chrono::DateTime::parse_from_rfc3339(since)
            .map_err(|_| ApiError::bad_request(
                "invalid `since` value; expected 'last N', a ledger number, or an ISO-8601 timestamp",
            ))?
            .with_timezone(&chrono::Utc);
        let seq: Option<i64> = sqlx::query_scalar(
            "SELECT MIN(seq) FROM events WHERE ledger_closed_at >= $1",
        )
        .bind(ts)
        .fetch_one(pool)
        .await?;
        Ok(seq.map(|s| (s - 1).max(0)).unwrap_or(0))
    }
}

// ── Tri-state PATCH helper ───────────────────────────────────────────────────
//
// JSON has three states for an optional field:
//   absent  → keep the current value     (Option::None on the outer Option)
//   null    → clear the filter            (Some(None))
//   "value" → set to this value           (Some(Some("value")))
//
// `serde(default)` + `serde(deserialize_with = "deserialize_option_option")`
// gives us that tri-state without pulling in an extra crate.

fn deserialize_option_option<'de, D, T>(d: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    Ok(Some(Option::<T>::deserialize(d)?))
}

#[derive(Deserialize)]
pub struct UpdateWebhookBody {
    /// Toggle active/paused state of the subscription.
    pub active: Option<bool>,
    /// Tri-state: absent = keep, null = clear, string = set.
    #[serde(default, deserialize_with = "deserialize_option_option")]
    pub contract_id: Option<Option<String>>,
    /// Tri-state: absent = keep, null = clear, string = set.
    #[serde(default, deserialize_with = "deserialize_option_option")]
    pub event_name: Option<Option<String>>,
    /// If present, update the delivery URL.
    pub url: Option<String>,
}

/// Query params for `GET /webhooks/:id/deliveries`.
#[derive(Deserialize)]
pub struct DeliveryQuery {
    /// Filter by delivery status: `pending`, `delivered`, or `failed`.
    status: Option<String>,
    /// Keyset pagination: return rows with id > after.
    after: Option<i64>,
    #[serde(default = "default_delivery_limit")]
    limit: i64,
}

fn default_delivery_limit() -> i64 {
    50
}

// ── Public handler functions ─────────────────────────────────────────────────

/// `GET /webhooks` — list all subscriptions (secrets omitted).
pub async fn list_webhooks(
    State(state): State<AppState>,
) -> ApiResult<Json<Value>> {
    let rows: Vec<(
        Uuid,
        String,
        String,
        Option<String>,
        Option<String>,
        bool,
        chrono::DateTime<chrono::Utc>,
        i32,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<String>,
        Option<i64>,
        Option<i64>,
    )> = sqlx::query_as(
        "SELECT id, url, kind, contract_id, event_name, active, created_at,
                consecutive_failures, auto_disabled_at, auto_disabled_reason,
                starting_seq, backfill_seq
         FROM webhook_subscriptions
         ORDER BY created_at DESC",
    )
    .fetch_all(&state.pool)
    .await?;

    let subs: Vec<Value> = rows
        .into_iter()
        .map(
            |(
                id,
                url,
                kind,
                contract_id,
                event_name,
                active,
                created_at,
                consecutive_failures,
                auto_disabled_at,
                auto_disabled_reason,
                starting_seq,
                backfill_seq,
            )| {
                json!({
                    "id": id,
                    "url": url,
                    "kind": kind,
                    "contract_id": contract_id,
                    "event_name": event_name,
                    "active": active,
                    "created_at": created_at,
                    "consecutive_failures": consecutive_failures,
                    "auto_disabled_at": auto_disabled_at,
                    "auto_disabled_reason": auto_disabled_reason,
                    "starting_seq": starting_seq,
                    "backfill_seq": backfill_seq,
                })
            },
        )
        .collect();

    Ok(Json(json!({ "subscriptions": subs, "count": subs.len() })))
}

/// `GET /webhooks/:id` — fetch a single subscription by id (#426).
pub async fn get_webhook(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<Value>> {
    let row: Option<(
        Uuid,
        String,
        String,
        Option<String>,
        Option<String>,
        bool,
        chrono::DateTime<chrono::Utc>,
        i32,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<String>,
        Option<i64>,
        Option<i64>,
    )> = sqlx::query_as(
        "SELECT id, url, kind, contract_id, event_name, active, created_at,
                consecutive_failures, auto_disabled_at, auto_disabled_reason,
                starting_seq, backfill_seq
         FROM webhook_subscriptions
         WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?;

    let (
        id,
        url,
        kind,
        contract_id,
        event_name,
        active,
        created_at,
        consecutive_failures,
        auto_disabled_at,
        auto_disabled_reason,
        starting_seq,
        backfill_seq,
    ) = row.ok_or_else(|| ApiError::not_found(format!("webhook subscription {id} not found")))?;

    Ok(Json(json!({
        "id": id,
        "url": url,
        "kind": kind,
        "contract_id": contract_id,
        "event_name": event_name,
        "active": active,
        "created_at": created_at,
        "consecutive_failures": consecutive_failures,
        "auto_disabled_at": auto_disabled_at,
        "auto_disabled_reason": auto_disabled_reason,
        "starting_seq": starting_seq,
        "backfill_seq": backfill_seq,
    })))
}

/// `DELETE /webhooks/:id`
pub async fn delete_webhook(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<Value>> {
    let deleted = sqlx::query("DELETE FROM webhook_subscriptions WHERE id = $1")
        .bind(id)
        .execute(&state.pool)
        .await?
        .rows_affected();

    if deleted == 0 {
        return Err(ApiError::not_found(format!(
            "webhook subscription {id} not found"
        )));
    }

    log_webhook_action(&state.pool, "webhook_delete", &id.to_string()).await;
    Ok(Json(json!({ "deleted": true, "id": id })))
}

/// `PATCH /webhooks/:id` — update url, active, contract_id, event_name.
///
/// Implements #424 (tri-state fields, validation, url update) and #425
/// (reset auto-disable fields when re-activating).
pub async fn update_webhook(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateWebhookBody>,
) -> ApiResult<Json<Value>> {
    // Validate the new URL before touching the DB.
    if let Some(ref url) = body.url {
        url_validation::validate_webhook_url(url)
            .map_err(|e| ApiError::bad_request(format!("invalid webhook url: {e}")))?;
    }

    // Validate contract_id if being set (not cleared).
    if let Some(Some(ref cid)) = body.contract_id {
        validate_contract_id(cid)?;
    }

    // Validate event_name if being set (not cleared).
    if let Some(Some(ref en)) = body.event_name {
        validate_event_name(en)?;
    }

    // Fetch current state so we know whether this is a false→true re-activation.
    let current: Option<(bool, Option<chrono::DateTime<chrono::Utc>>)> =
        sqlx::query_as("SELECT active, auto_disabled_at FROM webhook_subscriptions WHERE id = $1")
            .bind(id)
            .fetch_optional(&state.pool)
            .await?;

    let (current_active, auto_disabled_at) =
        current.ok_or_else(|| ApiError::not_found(format!("webhook subscription {id} not found")))?;

    // #425: when PATCH transitions active false → true, reset the auto-disable
    // state so the subscription isn't immediately re-disabled on the next failure.
    let reactivating = body.active == Some(true) && !current_active;

    // Build a dynamic UPDATE.  We always need to touch at least one column so
    // we use a sentinel that writes what's already there when nothing changed.
    //
    // The approach: build the SET clause fragments and bind list together.
    //
    // To keep this readable without a query-builder crate we use positional
    // parameters ($1…$N) and a running index.
    let mut set_clauses: Vec<String> = Vec::new();
    // $1 is always the id (used in WHERE).

    let mut param_idx: i32 = 2; // $1 reserved for id

    // Helper macro to push a clause and advance the index.
    macro_rules! push {
        ($col:expr) => {{
            set_clauses.push(format!("{} = ${}", $col, param_idx));
            let idx = param_idx;
            param_idx += 1;
            idx
        }};
    }

    // Collect each optional update.
    let mut url_val: Option<String> = None;
    let mut active_val: Option<bool> = None;
    let mut contract_id_val: Option<Option<String>> = None;
    let mut event_name_val: Option<Option<String>> = None;

    if let Some(ref url) = body.url {
        url_val = Some(url.clone());
        push!("url");
    }
    if let Some(active) = body.active {
        active_val = Some(active);
        push!("active");
    }
    if let Some(ref cid) = body.contract_id {
        contract_id_val = Some(cid.clone());
        push!("contract_id");
    }
    if let Some(ref en) = body.event_name {
        event_name_val = Some(en.clone());
        push!("event_name");
    }

    // #425: reset auto-disable fields when re-activating.
    let mut reset_auto_disable = false;
    if reactivating {
        reset_auto_disable = true;
        set_clauses.push("consecutive_failures = 0".to_string());
        set_clauses.push("auto_disabled_at = NULL".to_string());
        set_clauses.push("auto_disabled_reason = NULL".to_string());
    }

    if set_clauses.is_empty() {
        // Nothing to update — return current state.
        return get_webhook(State(state), Path(id)).await;
    }

    let sql = format!(
        "UPDATE webhook_subscriptions SET {} WHERE id = $1 RETURNING \
         id, url, kind, contract_id, event_name, active, created_at, \
         consecutive_failures, auto_disabled_at, auto_disabled_reason, \
         starting_seq, backfill_seq",
        set_clauses.join(", ")
    );

    // Bind all parameters dynamically.
    let mut q = sqlx::query(&sql).bind(id);
    if let Some(v) = url_val { q = q.bind(v); }
    if let Some(v) = active_val { q = q.bind(v); }
    if let Some(v) = contract_id_val { q = q.bind(v); }
    if let Some(v) = event_name_val { q = q.bind(v); }

    let row = q
        .fetch_one(&state.pool)
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => {
                ApiError::not_found(format!("webhook subscription {id} not found"))
            }
            other => other.into(),
        })?;

    let updated_id: Uuid = row.get("id");
    let url: String = row.get("url");
    let kind: String = row.get("kind");
    let contract_id: Option<String> = row.get("contract_id");
    let event_name: Option<String> = row.get("event_name");
    let active: bool = row.get("active");
    let created_at: chrono::DateTime<chrono::Utc> = row.get("created_at");
    let consecutive_failures: i32 = row.get("consecutive_failures");
    let new_auto_disabled_at: Option<chrono::DateTime<chrono::Utc>> = row.get("auto_disabled_at");
    let auto_disabled_reason: Option<String> = row.get("auto_disabled_reason");
    let starting_seq: Option<i64> = row.get("starting_seq");
    let backfill_seq: Option<i64> = row.get("backfill_seq");

    log_webhook_action(&state.pool, "webhook_update", &updated_id.to_string()).await;

    Ok(Json(json!({
        "id": updated_id,
        "url": url,
        "kind": kind,
        "contract_id": contract_id,
        "event_name": event_name,
        "active": active,
        "created_at": created_at,
        "consecutive_failures": consecutive_failures,
        "auto_disabled_at": new_auto_disabled_at,
        "auto_disabled_reason": auto_disabled_reason,
        "starting_seq": starting_seq,
        "backfill_seq": backfill_seq,
        "reactivated": reset_auto_disable,
    })))
}

/// `POST /webhooks/:id/reenable` — re-enable an auto-disabled subscription and
/// reset all failure tracking state.
pub async fn reenable_webhook(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<Value>> {
    let updated = sqlx::query(
        "UPDATE webhook_subscriptions
         SET active = true,
             consecutive_failures = 0,
             auto_disabled_at = NULL,
             auto_disabled_reason = NULL
         WHERE id = $1",
    )
    .bind(id)
    .execute(&state.pool)
    .await?
    .rows_affected();

    if updated == 0 {
        return Err(ApiError::not_found(format!(
            "webhook subscription {id} not found"
        )));
    }

    log_webhook_action(&state.pool, "webhook_reenable", &id.to_string()).await;
    Ok(Json(json!({ "reactivated": true, "id": id })))
}

/// `POST /webhooks/:id/rotate-secret` — generate a new signing secret.
///
/// The old secret is immediately invalid; callers must update their HMAC
/// verification logic before rotating.
pub async fn rotate_webhook_secret(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<Value>> {
    let new_secret = random_secret();
    let encryption_key = std::env::var("WEBHOOK_ENCRYPTION_KEY")
        .unwrap_or_else(|_| "default-key-for-testing".to_string());

    let updated = sqlx::query(
        "UPDATE webhook_subscriptions
         SET encrypted_secret = pgp_sym_encrypt($2, $3)
         WHERE id = $1",
    )
    .bind(id)
    .bind(&new_secret)
    .bind(&encryption_key)
    .execute(&state.pool)
    .await?
    .rows_affected();

    if updated == 0 {
        return Err(ApiError::not_found(format!(
            "webhook subscription {id} not found"
        )));
    }

    log_webhook_action(&state.pool, "webhook_rotate_secret", &id.to_string()).await;
    Ok(Json(json!({ "id": id, "secret": new_secret })))
}

/// `POST /webhooks/:id/redrive` — re-queue all `failed` deliveries for retry.
pub async fn redrive_webhook(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<Value>> {
    // Verify the subscription exists.
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM webhook_subscriptions WHERE id = $1)")
            .bind(id)
            .fetch_one(&state.pool)
            .await?;

    if !exists {
        return Err(ApiError::not_found(format!(
            "webhook subscription {id} not found"
        )));
    }

    let redriven = sqlx::query(
        "UPDATE webhook_deliveries
         SET status = 'pending', next_attempt_at = now(), attempts = 0, last_error = NULL
         WHERE subscription_id = $1 AND status = 'failed'",
    )
    .bind(id)
    .execute(&state.pool)
    .await?
    .rows_affected();

    log_webhook_action(&state.pool, "webhook_redrive", &id.to_string()).await;
    Ok(Json(json!({ "redriven": redriven, "id": id })))
}

/// `GET /webhooks/:id/deliveries` — list delivery history for a subscription.
///
/// Supports `?status=pending|delivered|failed` and keyset pagination via
/// `?after=<id>` (#426).
pub async fn list_webhook_deliveries(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(query): Query<DeliveryQuery>,
) -> ApiResult<Json<Value>> {
    // Verify the subscription exists.
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM webhook_subscriptions WHERE id = $1)")
            .bind(id)
            .fetch_one(&state.pool)
            .await?;

    if !exists {
        return Err(ApiError::not_found(format!(
            "webhook subscription {id} not found"
        )));
    }

    let limit = query.limit.clamp(1, 200);

    // Validate optional status filter.
    if let Some(ref s) = query.status {
        if !matches!(s.as_str(), "pending" | "delivered" | "failed") {
            return Err(ApiError::bad_request(
                "status must be one of: pending, delivered, failed",
            ));
        }
    }

    let rows: Vec<(
        i64,
        Option<String>,
        Option<i64>,
        String,
        i32,
        Option<String>,
        chrono::DateTime<chrono::Utc>,
        Option<chrono::DateTime<chrono::Utc>>,
        chrono::DateTime<chrono::Utc>,
        Option<i32>,
        Option<String>,
    )> = sqlx::query_as(
        "SELECT d.id, d.event_id, d.upgrade_id, d.status, d.attempts,
                d.last_error, d.next_attempt_at, d.delivered_at, d.created_at,
                d.last_status_code, d.last_response_snippet
         FROM webhook_deliveries d
         WHERE d.subscription_id = $1
           AND ($2::text IS NULL OR d.status = $2)
           AND ($3::bigint IS NULL OR d.id > $3)
         ORDER BY d.id ASC
         LIMIT $4",
    )
    .bind(id)
    .bind(&query.status)
    .bind(query.after)
    .bind(limit)
    .fetch_all(&state.pool)
    .await?;

    let deliveries: Vec<Value> = rows
        .into_iter()
        .map(
            |(
                row_id,
                event_id,
                upgrade_id,
                status,
                attempts,
                last_error,
                next_attempt_at,
                delivered_at,
                created_at,
                last_status_code,
                last_response_snippet,
            )| {
                json!({
                    "id": row_id,
                    "event_id": event_id,
                    "upgrade_id": upgrade_id,
                    "status": status,
                    "attempts": attempts,
                    "last_error": last_error,
                    "next_attempt_at": next_attempt_at,
                    "delivered_at": delivered_at,
                    "created_at": created_at,
                    "last_status_code": last_status_code,
                    "last_response_snippet": last_response_snippet,
                })
            },
        )
        .collect();

    let next_after = deliveries.last().and_then(|d| d["id"].as_i64());

    Ok(Json(json!({
        "deliveries": deliveries,
        "count": deliveries.len(),
        "next_after": next_after,
    })))
}
