//! API-key management for operators (#431).
//!
//! Mounted under `/admin` behind [`crate::auth::admin_auth`], which requires the
//! separate `ADMIN_TOKEN` credential. Keys are created, listed, revoked and
//! rotated over HTTP, so operators no longer need database access or
//! `scripts/gen_api_key.sh`.
//!
//! Plaintext keys are returned exactly once, by create and rotate. Only the
//! SHA-256 hash is stored, as before. Every action writes an `audit_log` row in
//! the same transaction as the change, so an action is never recorded without
//! being applied, or applied without being recorded.
//!
//! Keys are addressed by the first 8 hex characters of their hash, the same
//! prefix the audit log uses. A prefix that matches more than one key is
//! rejected with 409; a longer hex prefix is then needed.

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use rand::distributions::{Alphanumeric, DistString};
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, Postgres, Transaction};
use utoipa::ToSchema;

use crate::auth::{
    hash_key, AdminCaller, SCOPE_ADMIN, SCOPE_READ, SCOPE_RPC, SCOPE_WEBHOOKS_WRITE,
};
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;

const DEFAULT_TIER: &str = "free";
const DEFAULT_RATE_LIMIT_PER_MIN: i32 = 60;
const MAX_RATE_LIMIT_PER_MIN: i32 = 100_000;
const MAX_NAME_CHARS: usize = 100;
const MAX_TIER_CHARS: usize = 32;
/// Longest grace window for a rotated key: 7 days.
const MAX_GRACE_SECS: i64 = 7 * 24 * 60 * 60;
const ALLOWED_SCOPES: [&str; 4] = [SCOPE_READ, SCOPE_RPC, SCOPE_WEBHOOKS_WRITE, SCOPE_ADMIN];

/// Route path recorded in the audit log for each action.
const ROUTE_KEYS: &str = "/admin/keys";

/// Columns selected for every key lookup, in the order [`KeyRow`] expects.
const KEY_COLUMNS: &str =
    "key_hash, name, tier, rate_limit_per_min, scopes, revoked, expires_at, last_used_at, created_at";

type KeyRow = (
    String,
    String,
    String,
    i32,
    Vec<String>,
    bool,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    DateTime<Utc>,
);

/// Key metadata. Never includes the plaintext key or its full hash.
#[derive(Serialize, ToSchema, Clone, Debug)]
pub struct KeyMeta {
    /// First 8 hex characters of the key's SHA-256 hash. Use this to address the key.
    pub key_prefix: String,
    pub name: String,
    pub tier: String,
    pub rate_limit_per_min: i32,
    pub scopes: Vec<String>,
    pub revoked: bool,
    pub expires_at: Option<DateTime<Utc>>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

impl From<KeyRow> for KeyMeta {
    fn from(row: KeyRow) -> Self {
        let (
            hash,
            name,
            tier,
            rate_limit_per_min,
            scopes,
            revoked,
            expires_at,
            last_used_at,
            created_at,
        ) = row;
        KeyMeta {
            key_prefix: hash.chars().take(8).collect(),
            name,
            tier,
            rate_limit_per_min,
            scopes,
            revoked,
            expires_at,
            last_used_at,
            created_at,
        }
    }
}

#[derive(Deserialize, ToSchema, Debug)]
pub struct CreateKeyRequest {
    /// Human-readable label, 1 to 100 characters.
    pub name: String,
    /// Pricing or quota tier label. Defaults to `free`.
    pub tier: Option<String>,
    /// Requests per minute allowed for this key. Defaults to 60.
    pub rate_limit_per_min: Option<i32>,
    /// Scopes to grant. Defaults to `["read"]`. Allowed: `read`, `rpc`, `webhooks:write`, `admin`.
    pub scopes: Option<Vec<String>>,
    /// Optional expiry in the future. Omit for a key that never expires.
    pub expires_at: Option<DateTime<Utc>>,
}

/// A newly issued key. `key` is shown only in this response.
#[derive(Serialize, ToSchema, Debug)]
pub struct CreatedKey {
    /// Plaintext API key (`lqk_` prefix). Store it now: it cannot be retrieved again.
    pub key: String,
    #[serde(flatten)]
    pub meta: KeyMeta,
}

#[derive(Deserialize, ToSchema, Debug, Default)]
pub struct RotateKeyRequest {
    /// Seconds the old key keeps working. `0` (the default) revokes it immediately.
    /// At most 604800 (7 days). After the window the old key is rejected as expired.
    pub grace_seconds: Option<i64>,
}

/// Result of a rotation: the new plaintext key and the old key's metadata.
#[derive(Serialize, ToSchema, Debug)]
pub struct RotatedKey {
    /// Plaintext replacement key. Shown only in this response.
    pub key: String,
    #[serde(flatten)]
    pub meta: KeyMeta,
    /// The key that was rotated, with its state after the grace window was applied.
    pub previous: KeyMeta,
}

/// Generate a new key in the same format as `scripts/gen_api_key.sh`.
fn generate_key() -> String {
    format!(
        "lqk_{}",
        Alphanumeric.sample_string(&mut rand::thread_rng(), 32)
    )
}

fn validate_name(raw: &str) -> ApiResult<String> {
    let name = raw.trim();
    if name.is_empty() {
        return Err(ApiError::bad_request("name must not be empty"));
    }
    if name.chars().count() > MAX_NAME_CHARS {
        return Err(ApiError::bad_request(format!(
            "name must be at most {MAX_NAME_CHARS} characters"
        )));
    }
    Ok(name.to_string())
}

fn validate_tier(raw: &str) -> ApiResult<String> {
    let valid = !raw.is_empty()
        && raw.chars().count() <= MAX_TIER_CHARS
        && raw
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_');
    if !valid {
        return Err(ApiError::bad_request(format!(
            "tier must be 1 to {MAX_TIER_CHARS} characters: a-z, 0-9, '-' or '_'"
        )));
    }
    Ok(raw.to_string())
}

fn validate_rate_limit(value: i32) -> ApiResult<i32> {
    if !(1..=MAX_RATE_LIMIT_PER_MIN).contains(&value) {
        return Err(ApiError::bad_request(format!(
            "rate_limit_per_min must be between 1 and {MAX_RATE_LIMIT_PER_MIN}"
        )));
    }
    Ok(value)
}

/// Default to `["read"]`, reject unknown or empty lists, and drop duplicates
/// while keeping the caller's order.
fn validate_scopes(scopes: Option<Vec<String>>) -> ApiResult<Vec<String>> {
    let Some(requested) = scopes else {
        return Ok(vec![SCOPE_READ.to_string()]);
    };
    if requested.is_empty() {
        return Err(ApiError::bad_request("scopes must not be empty"));
    }
    let mut out: Vec<String> = Vec::with_capacity(requested.len());
    for scope in requested {
        if !ALLOWED_SCOPES.contains(&scope.as_str()) {
            return Err(ApiError::bad_request(format!(
                "unknown scope '{scope}'; allowed: read, rpc, webhooks:write, admin"
            )));
        }
        if !out.contains(&scope) {
            out.push(scope);
        }
    }
    Ok(out)
}

fn validate_expiry(expires_at: Option<DateTime<Utc>>, now: DateTime<Utc>) -> ApiResult<()> {
    match expires_at {
        Some(at) if at <= now => Err(ApiError::bad_request("expires_at must be in the future")),
        _ => Ok(()),
    }
}

fn validate_grace(grace_seconds: i64) -> ApiResult<i64> {
    if !(0..=MAX_GRACE_SECS).contains(&grace_seconds) {
        return Err(ApiError::bad_request(format!(
            "grace_seconds must be between 0 and {MAX_GRACE_SECS}"
        )));
    }
    Ok(grace_seconds)
}

/// Accept exactly 8 to 64 lowercase hex characters. Because only hex is
/// allowed, the value can be used in a `LIKE` pattern without wildcard risk.
fn validate_prefix(raw: &str) -> ApiResult<()> {
    let valid = (8..=64).contains(&raw.len())
        && raw
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
    if !valid {
        return Err(ApiError::bad_request(
            "key prefix must be 8 to 64 lowercase hex characters",
        ));
    }
    Ok(())
}

/// Write one audit row. Takes the connection so it can share a transaction
/// with the change it records.
async fn audit(
    conn: &mut PgConnection,
    actor: &str,
    method: &str,
    status: i32,
    action: &str,
    resource_id: &str,
    metadata: serde_json::Value,
) -> ApiResult<()> {
    sqlx::query(
        "INSERT INTO audit_log
             (key_hash_prefix, route, http_method, status_code, action_type, resource_id, metadata)
         VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)",
    )
    .bind(actor)
    .bind(ROUTE_KEYS)
    .bind(method)
    .bind(status)
    .bind(action)
    .bind(resource_id)
    .bind(metadata.to_string())
    .execute(conn)
    .await?;
    Ok(())
}

/// Find the single key whose hash starts with `prefix`.
async fn resolve_prefix(conn: &mut PgConnection, prefix: &str) -> ApiResult<String> {
    validate_prefix(prefix)?;
    let matches: Vec<(String,)> =
        sqlx::query_as("SELECT key_hash FROM api_keys WHERE key_hash LIKE $1 LIMIT 2")
            .bind(format!("{prefix}%"))
            .fetch_all(conn)
            .await?;
    match matches.as_slice() {
        [] => Err(ApiError::not_found("no API key matches that prefix")),
        [(hash,)] => Ok(hash.clone()),
        _ => Err(ApiError::conflict(
            "key prefix is ambiguous; use a longer hex prefix",
        )),
    }
}

/// Insert a key row and return its metadata.
async fn insert_key(
    tx: &mut Transaction<'_, Postgres>,
    hash: &str,
    name: &str,
    tier: &str,
    rate_limit_per_min: i32,
    scopes: &[String],
    expires_at: Option<DateTime<Utc>>,
) -> ApiResult<KeyMeta> {
    let created_at: DateTime<Utc> = sqlx::query_scalar(
        "INSERT INTO api_keys (key_hash, name, tier, rate_limit_per_min, scopes, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6)
         RETURNING created_at",
    )
    .bind(hash)
    .bind(name)
    .bind(tier)
    .bind(rate_limit_per_min)
    .bind(scopes)
    .bind(expires_at)
    .fetch_one(&mut **tx)
    .await?;
    Ok(KeyMeta::from((
        hash.to_string(),
        name.to_string(),
        tier.to_string(),
        rate_limit_per_min,
        scopes.to_vec(),
        false,
        expires_at,
        None::<DateTime<Utc>>,
        created_at,
    )))
}

/// `GET /admin/keys`: metadata for every key, newest first. Never returns
/// plaintext keys or full hashes.
#[utoipa::path(
    get,
    path = "/admin/keys",
    responses(
        (status = 200, description = "All API keys (metadata only)", body = Vec<KeyMeta>),
        (status = 401, description = "Missing or invalid admin credentials"),
        (status = 404, description = "Admin API disabled (ADMIN_TOKEN unset)")
    ),
    tag = "Admin"
)]
pub async fn list_keys(
    State(state): State<AppState>,
    Extension(caller): Extension<AdminCaller>,
) -> ApiResult<Json<Vec<KeyMeta>>> {
    let mut tx = state.pool.begin().await?;
    let rows: Vec<KeyRow> = sqlx::query_as(&format!(
        "SELECT {KEY_COLUMNS} FROM api_keys ORDER BY created_at DESC, key_hash"
    ))
    .fetch_all(&mut *tx)
    .await?;
    audit(
        &mut tx,
        &caller.0,
        "GET",
        200,
        "admin_key_list",
        "",
        serde_json::json!({ "count": rows.len() }),
    )
    .await?;
    tx.commit().await?;
    Ok(Json(rows.into_iter().map(KeyMeta::from).collect()))
}

/// `POST /admin/keys`: issue a new key. The plaintext is returned once.
#[utoipa::path(
    post,
    path = "/admin/keys",
    responses(
        (status = 201, description = "Key created; plaintext shown once", body = CreatedKey),
        (status = 400, description = "Invalid name, tier, limit, scopes or expiry"),
        (status = 401, description = "Missing or invalid admin credentials"),
        (status = 404, description = "Admin API disabled (ADMIN_TOKEN unset)")
    ),
    tag = "Admin"
)]
pub async fn create_key(
    State(state): State<AppState>,
    Extension(caller): Extension<AdminCaller>,
    Json(body): Json<CreateKeyRequest>,
) -> ApiResult<(StatusCode, Json<CreatedKey>)> {
    let now = Utc::now();
    let name = validate_name(&body.name)?;
    let tier = validate_tier(body.tier.as_deref().unwrap_or(DEFAULT_TIER))?;
    let rate = validate_rate_limit(
        body.rate_limit_per_min
            .unwrap_or(DEFAULT_RATE_LIMIT_PER_MIN),
    )?;
    let scopes = validate_scopes(body.scopes)?;
    validate_expiry(body.expires_at, now)?;

    let key = generate_key();
    let hash = hash_key(&key);

    let mut tx = state.pool.begin().await?;
    let meta = insert_key(&mut tx, &hash, &name, &tier, rate, &scopes, body.expires_at).await?;
    audit(
        &mut tx,
        &caller.0,
        "POST",
        201,
        "admin_key_create",
        &meta.key_prefix,
        serde_json::json!({ "tier": tier, "scopes": scopes, "rate_limit_per_min": rate }),
    )
    .await?;
    tx.commit().await?;

    Ok((StatusCode::CREATED, Json(CreatedKey { key, meta })))
}

/// `POST /admin/keys/{prefix}/revoke`: revoke a key immediately. Idempotent.
#[utoipa::path(
    post,
    path = "/admin/keys/{prefix}/revoke",
    params(("prefix" = String, Path, description = "8+ hex characters of the key hash")),
    responses(
        (status = 200, description = "Key revoked (or already revoked)", body = KeyMeta),
        (status = 400, description = "Malformed prefix"),
        (status = 401, description = "Missing or invalid admin credentials"),
        (status = 404, description = "No key matches the prefix"),
        (status = 409, description = "Prefix matches more than one key")
    ),
    tag = "Admin"
)]
pub async fn revoke_key(
    State(state): State<AppState>,
    Extension(caller): Extension<AdminCaller>,
    Path(prefix): Path<String>,
) -> ApiResult<Json<KeyMeta>> {
    let mut tx = state.pool.begin().await?;
    let hash = resolve_prefix(&mut tx, &prefix).await?;
    let row: KeyRow = sqlx::query_as(&format!(
        "UPDATE api_keys SET revoked = TRUE WHERE key_hash = $1 RETURNING {KEY_COLUMNS}"
    ))
    .bind(&hash)
    .fetch_one(&mut *tx)
    .await?;
    let meta = KeyMeta::from(row);
    audit(
        &mut tx,
        &caller.0,
        "POST",
        200,
        "admin_key_revoke",
        &meta.key_prefix,
        serde_json::json!({}),
    )
    .await?;
    tx.commit().await?;

    // Evict from this instance's cache. Other replicas converge within the
    // API key cache TTL (`API_KEY_CACHE_TTL_SECS`).
    state.key_cache.invalidate(&hash);
    Ok(Json(meta))
}

/// `POST /admin/keys/{prefix}/rotate`: issue a replacement with the same
/// settings. The old key is revoked now, or after `grace_seconds`.
#[utoipa::path(
    post,
    path = "/admin/keys/{prefix}/rotate",
    params(("prefix" = String, Path, description = "8+ hex characters of the key hash")),
    responses(
        (status = 200, description = "Replacement key issued; plaintext shown once", body = RotatedKey),
        (status = 400, description = "Malformed prefix or grace_seconds out of range"),
        (status = 401, description = "Missing or invalid admin credentials"),
        (status = 404, description = "No key matches the prefix"),
        (status = 409, description = "Prefix is ambiguous, or the key is already revoked")
    ),
    tag = "Admin"
)]
pub async fn rotate_key(
    State(state): State<AppState>,
    Extension(caller): Extension<AdminCaller>,
    Path(prefix): Path<String>,
    body: Option<Json<RotateKeyRequest>>,
) -> ApiResult<Json<RotatedKey>> {
    let grace = validate_grace(body.and_then(|Json(b)| b.grace_seconds).unwrap_or(0))?;

    let mut tx = state.pool.begin().await?;
    let old_hash = resolve_prefix(&mut tx, &prefix).await?;
    let old: KeyRow = sqlx::query_as(&format!(
        "SELECT {KEY_COLUMNS} FROM api_keys WHERE key_hash = $1 FOR UPDATE"
    ))
    .bind(&old_hash)
    .fetch_one(&mut *tx)
    .await?;
    if old.5 {
        return Err(ApiError::conflict("key is already revoked"));
    }

    // The replacement inherits every setting of the old key.
    let new_key = generate_key();
    let new_hash = hash_key(&new_key);
    let meta = insert_key(&mut tx, &new_hash, &old.1, &old.2, old.3, &old.4, old.6).await?;

    // Grace window: cap the old key's expiry at now + grace. The existing
    // expiry check then rejects it once the window ends, with no new column.
    // Grace 0 revokes it outright.
    let old_row: KeyRow = if grace == 0 {
        sqlx::query_as(&format!(
            "UPDATE api_keys SET revoked = TRUE WHERE key_hash = $1 RETURNING {KEY_COLUMNS}"
        ))
        .bind(&old_hash)
        .fetch_one(&mut *tx)
        .await?
    } else {
        sqlx::query_as(&format!(
            "UPDATE api_keys
                SET expires_at = LEAST(COALESCE(expires_at, 'infinity'::timestamptz),
                                       now() + ($2 * interval '1 second'))
              WHERE key_hash = $1
             RETURNING {KEY_COLUMNS}"
        ))
        .bind(&old_hash)
        .bind(grace)
        .fetch_one(&mut *tx)
        .await?
    };
    let previous = KeyMeta::from(old_row);

    audit(
        &mut tx,
        &caller.0,
        "POST",
        200,
        "admin_key_rotate",
        &previous.key_prefix,
        serde_json::json!({
            "new_key_prefix": meta.key_prefix,
            "grace_seconds": grace,
        }),
    )
    .await?;
    tx.commit().await?;

    // Evict the old key so a revoke takes effect here without waiting for the cache TTL.
    state.key_cache.invalidate(&old_hash);
    Ok(Json(RotatedKey {
        key: new_key,
        meta,
        previous,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn generated_keys_have_the_script_format() {
        let key = generate_key();
        assert!(key.starts_with("lqk_"));
        assert_eq!(key.len(), 4 + 32);
        assert!(key[4..].bytes().all(|b| b.is_ascii_alphanumeric()));
        assert_ne!(key, generate_key());
    }

    #[test]
    fn scopes_default_to_read_and_are_deduplicated() {
        assert_eq!(validate_scopes(None).unwrap(), vec!["read"]);
        let got = validate_scopes(Some(vec!["rpc".into(), "read".into(), "rpc".into()])).unwrap();
        assert_eq!(got, vec!["rpc", "read"]);
    }

    #[test]
    fn unknown_or_empty_scopes_are_rejected() {
        assert!(validate_scopes(Some(vec![])).is_err());
        assert!(validate_scopes(Some(vec!["superuser".into()])).is_err());
    }

    #[test]
    fn expiry_must_be_in_the_future() {
        let now = Utc::now();
        assert!(validate_expiry(Some(now - Duration::seconds(1)), now).is_err());
        assert!(validate_expiry(Some(now + Duration::hours(1)), now).is_ok());
        assert!(validate_expiry(None, now).is_ok());
    }

    #[test]
    fn rate_limit_and_grace_are_bounded() {
        assert!(validate_rate_limit(0).is_err());
        assert!(validate_rate_limit(MAX_RATE_LIMIT_PER_MIN + 1).is_err());
        assert!(validate_rate_limit(60).is_ok());
        assert!(validate_grace(-1).is_err());
        assert!(validate_grace(MAX_GRACE_SECS + 1).is_err());
        assert_eq!(validate_grace(0).unwrap(), 0);
    }

    #[test]
    fn prefixes_are_lowercase_hex_only() {
        assert!(validate_prefix("0123abcd").is_ok());
        assert!(validate_prefix("0123ABCD").is_err());
        assert!(validate_prefix("0123abc").is_err());
        assert!(validate_prefix("0123abc%").is_err());
        assert!(validate_prefix("0123abcg").is_err());
    }

    #[test]
    fn metadata_exposes_only_an_8_char_prefix() {
        let hash = "a".repeat(64);
        let meta = KeyMeta::from((
            hash,
            "ci".into(),
            "free".into(),
            60,
            vec!["read".into()],
            false,
            None::<DateTime<Utc>>,
            None::<DateTime<Utc>>,
            Utc::now(),
        ));
        assert_eq!(meta.key_prefix, "aaaaaaaa");
    }
}
