//! Privileged API-key lifecycle management.
//!
//! The database stores only a SHA-256 hash. A plaintext key is generated in
//! this module and sent only in a create or rotate response.

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use utoipa::ToSchema;

use crate::auth::{hash_key, AdminActor};
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;

const PREFIX_LEN: usize = 12;
const MIN_PREFIX_LEN: usize = 8;
const MAX_RATE_LIMIT: i32 = 1_000_000;
const MAX_GRACE_SECONDS: i64 = 31_536_000;

/// Input accepted by `POST /admin/keys`.
#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateApiKey {
    pub name: String,
    pub tier: String,
    pub rate_limit_per_min: i32,
    /// Omitted scopes default to `read`. They are retained as key metadata for
    /// deployments that enforce scopes.
    #[serde(default)]
    pub scopes: Option<Vec<String>>,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
}

/// Optional body for `POST /admin/keys/{prefix}/rotate`.
#[derive(Debug, Deserialize, ToSchema)]
pub struct RotateApiKey {
    /// The old key remains usable for this many seconds. Defaults to immediate
    /// revocation.
    #[serde(default)]
    pub grace_seconds: Option<i64>,
}

/// Safe API-key metadata: neither a plaintext key nor a full key hash is ever
/// exposed by this type.
#[derive(Debug, Serialize, ToSchema)]
pub struct ApiKeyMetadata {
    pub key_hash_prefix: String,
    pub name: String,
    pub tier: String,
    pub rate_limit_per_min: i32,
    pub scopes: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub revoked: bool,
    pub revocation_scheduled_at: Option<DateTime<Utc>>,
}

/// Returned only from create and rotate. Store `key` immediately; it cannot be
/// recovered from the service afterwards.
#[derive(Debug, Serialize, ToSchema)]
pub struct IssuedApiKey {
    pub key: String,
    #[serde(flatten)]
    pub metadata: ApiKeyMetadata,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ApiKeyList {
    pub data: Vec<ApiKeyMetadata>,
}

#[derive(Debug, FromRow)]
struct ApiKeyRow {
    key_hash: String,
    name: String,
    tier: String,
    rate_limit_per_min: i32,
    scopes: Vec<String>,
    created_at: DateTime<Utc>,
    last_used_at: Option<DateTime<Utc>>,
    expires_at: Option<DateTime<Utc>>,
    revoked: bool,
    revocation_scheduled_at: Option<DateTime<Utc>>,
}

impl From<ApiKeyRow> for ApiKeyMetadata {
    fn from(row: ApiKeyRow) -> Self {
        Self {
            key_hash_prefix: row.key_hash.chars().take(PREFIX_LEN).collect(),
            name: row.name,
            tier: row.tier,
            rate_limit_per_min: row.rate_limit_per_min,
            scopes: row.scopes,
            created_at: row.created_at,
            last_used_at: row.last_used_at,
            expires_at: row.expires_at,
            revoked: row.revoked,
            revocation_scheduled_at: row.revocation_scheduled_at,
        }
    }
}

// `revocation_scheduled_at` is enforced by the authentication query without a
// background worker. Expose it as an effective revocation here too, so a list
// reflects a grace period that has elapsed even before any cleanup job runs.
const KEY_COLUMNS: &str = "key_hash, name, tier, rate_limit_per_min, scopes, created_at, last_used_at, expires_at, CASE WHEN revoked OR (revocation_scheduled_at IS NOT NULL AND revocation_scheduled_at <= now()) THEN true ELSE false END AS revoked, revocation_scheduled_at";

/// Create a consumer API key. Its raw value is not persisted and is only
/// returned by this response.
pub async fn create_key(
    State(state): State<AppState>,
    Extension(_actor): Extension<AdminActor>,
    Json(mut input): Json<CreateApiKey>,
) -> ApiResult<(StatusCode, Json<IssuedApiKey>)> {
    validate_create(&mut input)?;
    let plaintext = generate_key();
    let key_hash = hash_key(&plaintext);
    let scopes = input
        .scopes
        .take()
        .unwrap_or_else(|| vec!["read".to_string()]);

    let row: ApiKeyRow = sqlx::query_as(&format!(
        "INSERT INTO api_keys (key_hash, name, tier, rate_limit_per_min, scopes, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6)
         RETURNING {KEY_COLUMNS}"
    ))
    .bind(&key_hash)
    .bind(&input.name)
    .bind(&input.tier)
    .bind(input.rate_limit_per_min)
    .bind(scopes)
    .bind(input.expires_at)
    .fetch_one(&state.pool)
    .await?;

    let metadata = ApiKeyMetadata::from(row);
    audit_action(
        &state,
        "key_create",
        &metadata.key_hash_prefix,
        "/admin/keys",
        "POST",
        StatusCode::CREATED,
    )
    .await;

    Ok((
        StatusCode::CREATED,
        Json(IssuedApiKey {
            key: plaintext,
            metadata,
        }),
    ))
}

/// List key metadata without revealing any plaintext key or full hash.
pub async fn list_keys(
    State(state): State<AppState>,
    Extension(_actor): Extension<AdminActor>,
) -> ApiResult<Json<ApiKeyList>> {
    let rows: Vec<ApiKeyRow> = sqlx::query_as(&format!(
        "SELECT {KEY_COLUMNS} FROM api_keys ORDER BY created_at DESC, key_hash ASC"
    ))
    .fetch_all(&state.pool)
    .await?;
    let data: Vec<ApiKeyMetadata> = rows.into_iter().map(Into::into).collect();

    audit_action(
        &state,
        "key_list",
        "api_keys",
        "/admin/keys",
        "GET",
        StatusCode::OK,
    )
    .await;
    Ok(Json(ApiKeyList { data }))
}

/// Immediately revoke a key selected by its displayed hash prefix. This action
/// is idempotent so operators can safely retry after a network timeout.
pub async fn revoke_key(
    State(state): State<AppState>,
    Extension(_actor): Extension<AdminActor>,
    Path(prefix): Path<String>,
) -> ApiResult<Json<ApiKeyMetadata>> {
    let existing = find_by_prefix(&state, &prefix).await?;
    let row: ApiKeyRow = sqlx::query_as(&format!(
        "UPDATE api_keys
         SET revoked = true, revocation_scheduled_at = NULL
         WHERE key_hash = $1
         RETURNING {KEY_COLUMNS}"
    ))
    .bind(existing.key_hash)
    .fetch_one(&state.pool)
    .await?;
    let metadata = ApiKeyMetadata::from(row);

    audit_action(
        &state,
        "key_revoke",
        &metadata.key_hash_prefix,
        "/admin/keys/:prefix/revoke",
        "POST",
        StatusCode::OK,
    )
    .await;
    Ok(Json(metadata))
}

/// Create a replacement using the same settings as the selected key. The old
/// key is revoked immediately unless `grace_seconds` is supplied.
pub async fn rotate_key(
    State(state): State<AppState>,
    Extension(_actor): Extension<AdminActor>,
    Path(prefix): Path<String>,
    body: Option<Json<RotateApiKey>>,
) -> ApiResult<(StatusCode, Json<IssuedApiKey>)> {
    let grace_seconds = body
        .map(|Json(body)| body.grace_seconds.unwrap_or(0))
        .unwrap_or(0);
    if !(0..=MAX_GRACE_SECONDS).contains(&grace_seconds) {
        return Err(ApiError::bad_request(format!(
            "grace_seconds must be between 0 and {MAX_GRACE_SECONDS}"
        )));
    }

    let selected = find_by_prefix(&state, &prefix).await?;
    if selected.revoked {
        return Err(ApiError::bad_request(
            "cannot rotate an already revoked API key",
        ));
    }

    let plaintext = generate_key();
    let new_hash = hash_key(&plaintext);
    let mut tx = state.pool.begin().await?;

    let source: Option<ApiKeyRow> = sqlx::query_as(&format!(
        "SELECT {KEY_COLUMNS} FROM api_keys WHERE key_hash = $1 FOR UPDATE"
    ))
    .bind(&selected.key_hash)
    .fetch_optional(&mut *tx)
    .await?;
    let source = source.ok_or_else(|| ApiError::not_found("API key not found"))?;
    if source.revoked {
        return Err(ApiError::bad_request(
            "cannot rotate an already revoked API key",
        ));
    }

    let created: ApiKeyRow = sqlx::query_as(&format!(
        "INSERT INTO api_keys (key_hash, name, tier, rate_limit_per_min, scopes, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6)
         RETURNING {KEY_COLUMNS}"
    ))
    .bind(&new_hash)
    .bind(&source.name)
    .bind(&source.tier)
    .bind(source.rate_limit_per_min)
    .bind(&source.scopes)
    .bind(source.expires_at)
    .fetch_one(&mut *tx)
    .await?;

    sqlx::query(
        "UPDATE api_keys
         SET revoked = $2,
             revocation_scheduled_at = CASE
                 WHEN $2 THEN NULL
                 ELSE now() + ($3 * interval '1 second')
             END
         WHERE key_hash = $1",
    )
    .bind(&source.key_hash)
    .bind(grace_seconds == 0)
    .bind(grace_seconds)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    let metadata = ApiKeyMetadata::from(created);
    audit_action(
        &state,
        "key_rotate",
        &source.key_hash.chars().take(PREFIX_LEN).collect::<String>(),
        "/admin/keys/:prefix/rotate",
        "POST",
        StatusCode::CREATED,
    )
    .await;

    Ok((
        StatusCode::CREATED,
        Json(IssuedApiKey {
            key: plaintext,
            metadata,
        }),
    ))
}

async fn find_by_prefix(state: &AppState, prefix: &str) -> ApiResult<ApiKeyRow> {
    let prefix = validate_prefix(prefix)?;
    let rows: Vec<ApiKeyRow> = sqlx::query_as(&format!(
        "SELECT {KEY_COLUMNS}
         FROM api_keys
         WHERE key_hash LIKE $1 || '%'
         ORDER BY key_hash
         LIMIT 2"
    ))
    .bind(prefix)
    .fetch_all(&state.pool)
    .await?;
    match rows.len() {
        0 => Err(ApiError::not_found("API key not found")),
        1 => rows
            .into_iter()
            .next()
            .ok_or_else(|| ApiError::not_found("API key not found")),
        _ => Err(ApiError::bad_request(
            "key hash prefix is ambiguous; provide a longer prefix",
        )),
    }
}

fn validate_create(input: &mut CreateApiKey) -> ApiResult<()> {
    input.name = input.name.trim().to_string();
    input.tier = input.tier.trim().to_string();
    if input.name.is_empty() || input.name.len() > 255 {
        return Err(ApiError::bad_request(
            "name must contain 1 to 255 characters",
        ));
    }
    if input.tier.is_empty() || input.tier.len() > 64 {
        return Err(ApiError::bad_request(
            "tier must contain 1 to 64 characters",
        ));
    }
    if !(1..=MAX_RATE_LIMIT).contains(&input.rate_limit_per_min) {
        return Err(ApiError::bad_request(format!(
            "rate_limit_per_min must be between 1 and {MAX_RATE_LIMIT}"
        )));
    }
    if input
        .expires_at
        .is_some_and(|expires| expires <= Utc::now())
    {
        return Err(ApiError::bad_request("expires_at must be in the future"));
    }
    let scopes = input
        .scopes
        .take()
        .unwrap_or_else(|| vec!["read".to_string()]);
    if scopes.is_empty()
        || scopes
            .iter()
            .any(|scope| scope.trim().is_empty() || scope.len() > 64)
    {
        return Err(ApiError::bad_request(
            "scopes must contain non-empty values of at most 64 characters",
        ));
    }
    let mut normalized = Vec::with_capacity(scopes.len());
    for scope in scopes {
        let scope = scope.trim().to_ascii_lowercase();
        if !normalized.contains(&scope) {
            normalized.push(scope);
        }
    }
    input.scopes = Some(normalized);
    Ok(())
}

fn validate_prefix(prefix: &str) -> ApiResult<String> {
    let prefix = prefix.trim().to_ascii_lowercase();
    if !(MIN_PREFIX_LEN..=64).contains(&prefix.len())
        || !prefix.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(ApiError::bad_request(
            "key hash prefix must contain 8 to 64 hexadecimal characters",
        ));
    }
    Ok(prefix)
}

fn generate_key() -> String {
    let mut bytes = [0_u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    format!("lqk_{}", hex::encode(bytes))
}

async fn audit_action(
    state: &AppState,
    action_type: &str,
    resource_id: &str,
    route: &str,
    method: &str,
    status: StatusCode,
) {
    if let Err(error) = sqlx::query(
        "INSERT INTO audit_log
            (key_hash_prefix, route, http_method, status_code, action_type, resource_id)
         VALUES ('admin-to', $1, $2, $3, $4, $5)",
    )
    .bind(route)
    .bind(method)
    .bind(status.as_u16() as i32)
    .bind(action_type)
    .bind(resource_id)
    .execute(&state.pool)
    .await
    {
        tracing::warn!(error = %error, action_type, "failed to write admin audit event");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_key_is_prefixed_and_has_full_entropy() {
        let key = generate_key();
        assert!(key.starts_with("lqk_"));
        assert_eq!(key.len(), 68);
    }

    #[test]
    fn prefix_requires_hex_and_a_safe_minimum_length() {
        assert_eq!(validate_prefix("AbCDef12").unwrap(), "abcdef12");
        assert!(validate_prefix("abcdef1").is_err());
        assert!(validate_prefix("abcdefgh").is_err());
    }
}
