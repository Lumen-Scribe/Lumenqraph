//! Webhook subscription management. Consumers register a URL (+ optional
//! contract/event filters) and receive an HMAC-signing `secret` once, at
//! creation. The `lumenqraph-webhooks` service does the actual delivery.

use axum::extract::{Path, State};
use axum::Json;
use lumenqraph_core::WebhookSubscription;
use rand::RngCore;
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::error::{ApiError, ApiResult};
use crate::state::AppState;

#[derive(Deserialize)]
pub struct CreateWebhook {
    url: String,
    /// `"event"` (default) or `"upgrade"`. Defaulting preserves the behaviour of
    /// every caller written before upgrade subscriptions existed.
    #[serde(default = "default_kind")]
    kind: String,
    contract_id: Option<String>,
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
    if !(body.url.starts_with("http://") || body.url.starts_with("https://")) {
        return Err(ApiError::bad_request("url must be http(s)://"));
    }
    if !matches!(body.kind.as_str(), "event" | "upgrade") {
        return Err(ApiError::bad_request(format!(
            "unknown kind `{}`; expected `event` or `upgrade`",
            body.kind
        )));
    }
    // An upgrade fires for a whole contract, not for one of its events, so an
    // event_name filter here would silently never match.
    if body.kind == "upgrade" && body.event_name.is_some() {
        return Err(ApiError::bad_request(
            "event_name does not apply to an `upgrade` subscription; \
             use contract_id to watch one contract, or omit it to watch all",
        ));
    }
    let secret = random_secret();

    let sub: WebhookSubscription = sqlx::query_as(
        "INSERT INTO webhook_subscriptions (url, kind, contract_id, event_name, secret)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING id, url, kind, contract_id, event_name, secret, active, created_at",
    )
    .bind(&body.url)
    .bind(&body.kind)
    .bind(&body.contract_id)
    .bind(&body.event_name)
    .bind(&secret)
    .fetch_one(&state.pool)
    .await?;

    Ok(Json(sub))
}

/// (id, url, kind, contract_id, event_name, active, created_at)
type WebhookListRow = (
    Uuid,
    String,
    String,
    Option<String>,
    Option<String>,
    bool,
    chrono::DateTime<chrono::Utc>,
);

/// List subscriptions without exposing their secrets.
pub async fn list_webhooks(State(state): State<AppState>) -> ApiResult<Json<Vec<Value>>> {
    let rows: Vec<WebhookListRow> = sqlx::query_as(
        "SELECT id, url, kind, contract_id, event_name, active, created_at
             FROM webhook_subscriptions ORDER BY created_at DESC",
    )
    .fetch_all(&state.pool)
    .await?;

    let out = rows
        .into_iter()
        .map(
            |(id, url, kind, contract_id, event_name, active, created_at)| {
                json!({
                    "id": id,
                    "url": url,
                    "kind": kind,
                    "contract_id": contract_id,
                    "event_name": event_name,
                    "active": active,
                    "created_at": created_at,
                })
            },
        )
        .collect();
    Ok(Json(out))
}

pub async fn delete_webhook(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<Value>> {
    let affected = sqlx::query("DELETE FROM webhook_subscriptions WHERE id = $1")
        .bind(id)
        .execute(&state.pool)
        .await?
        .rows_affected();
    if affected == 0 {
        return Err(ApiError::not_found("subscription not found"));
    }
    Ok(Json(json!({ "deleted": id })))
}
