//! Two-stage webhook pipeline, each stage a simple SQL-driven step:
//!
//!  1. **Enqueue** — match newly-indexed events (streamed by monotonic `seq`)
//!     against active subscriptions and insert `pending` delivery rows.
//!  2. **Deliver** — POST due deliveries to their URL with an HMAC-SHA256
//!     signature, retrying failures with exponential backoff.
//!
//! Two independent streams feed stage 1: contract **events**, and contract
//! **upgrades** (a new `contract_spec_versions` row — the contract's on-chain
//! interface changed). They share the delivery machinery but keep separate
//! watermarks and separate subscriptions, so an event subscriber's payload shape
//! never changes underneath them.

use anyhow::Context;
use chrono::{DateTime, Duration, Utc};
use hmac::{Hmac, Mac};
use rand::Rng;
use sha2::Sha256;
use sqlx::types::Json;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};
use url::Url;
use uuid::Uuid;

use crate::config::Config;
use futures::stream::{self, StreamExt};
use lumenqraph_core::url_validation::validate_webhook_url_at_delivery;
use lumenqraph_core::{ContractEventData, ContractUpgradeData, WebhookEnvelope};

type HmacSha256 = Hmac<Sha256>;

/// Identifies the sending binary's actual release to webhook consumers who key
/// off `User-Agent` for debugging, so delivery logs can be traced back to the
/// version that sent them.
const USER_AGENT: &str = concat!("lumenqraph-webhooks/", env!("CARGO_PKG_VERSION"));

/// Generate a stable worker identifier for lock claims (hostname:pid).
fn worker_id() -> String {
    let hostname = hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "unknown".to_string());
    format!("{}:{}", hostname, std::process::id())
}

/// Last observed count of `pending` rows in `webhook_deliveries`, refreshed once
/// per dispatcher tick by [`refresh_pending_gauge`] and read by the `/metrics`
/// endpoint (`lumenqraph_webhooks_pending_deliveries`).
///
/// Enqueue and deliver counts only describe what moved *this* tick; they go
/// quiet when the dispatcher is starved by a slow or unresponsive subscriber
/// even though the backlog is growing. This gauge makes that backlog the one
/// number an operator can alert on.
pub static PENDING_DELIVERIES: AtomicI64 = AtomicI64::new(0);

/// Refresh [`PENDING_DELIVERIES`] from the database. Called once per tick by the
/// service loop; returns the value it stored so the caller can log it.
pub async fn refresh_pending_gauge(pool: &PgPool) -> anyhow::Result<i64> {
    let pending: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM webhook_deliveries WHERE status = 'pending'")
            .fetch_one(pool)
            .await?;
    PENDING_DELIVERIES.store(pending, Ordering::Relaxed);
    Ok(pending)
}

/// Enqueue deliveries for everything new in both streams. Returns how many
/// delivery rows were created.
pub async fn enqueue(pool: &PgPool, batch: i64) -> anyhow::Result<u64> {
    let events = enqueue_events(pool, batch).await?;
    let upgrades = enqueue_upgrades(pool, batch).await?;
    Ok(events + upgrades)
}

/// Match new events to `event` subscriptions and enqueue deliveries.
async fn enqueue_events(pool: &PgPool, batch: i64) -> anyhow::Result<u64> {
    let last_seq: i64 = sqlx::query_scalar(
        "INSERT INTO webhook_state (id, last_seq) VALUES (1, 0)
         ON CONFLICT (id) DO UPDATE SET last_seq = webhook_state.last_seq
         RETURNING last_seq",
    )
    .fetch_one(pool)
    .await?;

    let global_max: i64 = sqlx::query_scalar("SELECT COALESCE(max(seq), 0) FROM events")
        .fetch_one(pool)
        .await?;

    if global_max <= last_seq {
        return Ok(0);
    }
    let upper = (last_seq + batch).min(global_max);

    let mut tx = pool.begin().await?;

    let created = sqlx::query(
        "INSERT INTO webhook_deliveries (subscription_id, event_id)
         SELECT s.id, e.event_id
         FROM events e
         JOIN webhook_subscriptions s
           ON s.active
          AND s.kind = 'event'
          AND (s.contract_id IS NULL OR s.contract_id = e.contract_id)
          AND (s.event_name  IS NULL OR s.event_name  = e.event_name)
         WHERE e.seq > GREATEST(s.starting_seq, $1) AND e.seq <= $2
         ON CONFLICT (subscription_id, event_id) DO NOTHING",
    )
    .bind(last_seq)
    .bind(upper)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    sqlx::query("UPDATE webhook_state SET last_seq = $1 WHERE id = 1")
        .bind(upper)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;

    if created > 0 {
        debug!(created, up_to_seq = upper, "enqueued webhook deliveries");
    }
    Ok(created)
}

/// Match new interface versions to `upgrade` subscriptions and enqueue
/// deliveries.
///
/// Version 1 is deliberately excluded: it's the first interface we ever saw for
/// a contract, i.e. a baseline with nothing to diff against, not an upgrade.
/// Without this, simply starting to index a contract would fire "it changed!" at
/// every subscriber watching all contracts.
async fn enqueue_upgrades(pool: &PgPool, batch: i64) -> anyhow::Result<u64> {
    let last_id: i64 = sqlx::query_scalar(
        "INSERT INTO webhook_state (id, last_upgrade_id) VALUES (1, 0)
         ON CONFLICT (id) DO UPDATE SET last_upgrade_id = webhook_state.last_upgrade_id
         RETURNING last_upgrade_id",
    )
    .fetch_one(pool)
    .await?;

    // Same watermark discipline as events: step toward the global max so a gap
    // (here, the version-1 rows we skip) can never wedge the stream.
    let global_max: i64 =
        sqlx::query_scalar("SELECT COALESCE(max(id), 0) FROM contract_spec_versions")
            .fetch_one(pool)
            .await?;
    if global_max <= last_id {
        return Ok(0);
    }
    let upper = (last_id + batch).min(global_max);

    // Atomic: deliveries and watermark advance commit together so a crash cannot
    // skip or duplicate upgrade deliveries. ON CONFLICT DO NOTHING is retained
    // as defense-in-depth.
    let mut tx = pool.begin().await?;

    let created = sqlx::query(
        "INSERT INTO webhook_deliveries (subscription_id, upgrade_id)
         SELECT s.id, v.id
         FROM contract_spec_versions v
         JOIN webhook_subscriptions s
           ON s.active
          AND s.kind = 'upgrade'
          AND (s.contract_id IS NULL OR s.contract_id = v.contract_id)
         WHERE v.id > $1 AND v.id <= $2 AND v.version > 1
         -- The dedupe index is partial, so the predicate has to be repeated here
         -- for Postgres to infer it.
         ON CONFLICT (subscription_id, upgrade_id) WHERE upgrade_id IS NOT NULL DO NOTHING",
    )
    .bind(last_id)
    .bind(upper)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    sqlx::query("UPDATE webhook_state SET last_upgrade_id = $1 WHERE id = 1")
        .bind(upper)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;

    if created > 0 {
        info!(created, up_to_id = upper, "enqueued upgrade webhooks");
    }
    Ok(created)
}

#[derive(Clone)]
struct DueDelivery {
    id: i64,
    subscription_id: uuid::Uuid,
    attempts: i32,
    url: String,
    secret: Option<String>,
    event_id: Option<String>,
    upgrade_id: Option<i64>,
    created_at: DateTime<Utc>,
}

/// Atomically claim and fetch due deliveries, handling decryption failures gracefully.
///
/// This function:
/// 1. Claims deliveries with FOR UPDATE SKIP LOCKED to prevent double-delivery (#452)
/// 2. Decrypts secrets using safe_decrypt_webhook_secret to handle failures per-row (#453)
/// 3. Handles subscription_id as UUID, not String (#454)
/// 4. Returns raw event/upgrade data for envelope construction (#455)
async fn fetch_due(pool: &PgPool, batch: i64, encryption_key: &str) -> anyhow::Result<Vec<DueDelivery>> {
    let worker = worker_id();
    let lock_duration = Duration::seconds(60);
    
    // Claim deliveries atomically with row-level locking
    let claimed: Vec<(i64, Uuid, i32, String, Option<String>, Option<String>, Option<i64>, DateTime<Utc>)> = sqlx::query_as(
        "UPDATE webhook_deliveries d
         SET locked_until = now() + $1, locked_by = $2
         WHERE d.id IN (
             SELECT id FROM webhook_deliveries
             WHERE status = 'pending' 
               AND next_attempt_at <= now()
               AND (locked_until IS NULL OR locked_until < now())
             ORDER BY next_attempt_at 
             LIMIT $3
             FOR UPDATE SKIP LOCKED
         )
         RETURNING d.id, d.subscription_id, d.attempts, 
                   (SELECT url FROM webhook_subscriptions WHERE id = d.subscription_id),
                   (SELECT safe_decrypt_webhook_secret(encrypted_secret, $4) FROM webhook_subscriptions WHERE id = d.subscription_id),
                   d.event_id, d.upgrade_id, d.created_at",
    let rows: Vec<(i64, String, i32, String, String, Json<serde_json::Value>)> = sqlx::query_as(
        "SELECT d.id, s.id, d.attempts, s.url,
                pgp_sym_decrypt(s.encrypted_secret, $1),
                CASE WHEN d.upgrade_id IS NOT NULL THEN
                    jsonb_build_object(
                        'type',               'contract.upgraded',
                        'contract_id',        v.contract_id,
                        'version',            v.version,
                        'wasm_hash',          v.wasm_hash,
                        'previous_wasm_hash', v.previous_wasm_hash,
                        'breaking',           v.breaking,
                        'diff',               v.diff,
                        'observed_at',        v.observed_at
                    )
                ELSE to_jsonb(e) - 'seq' END AS payload
         FROM webhook_deliveries d
         JOIN webhook_subscriptions s ON s.id = d.subscription_id
         LEFT JOIN events e ON e.event_id = d.event_id
         LEFT JOIN contract_spec_versions v ON v.id = d.upgrade_id
         WHERE d.status = 'pending' AND d.next_attempt_at <= now() AND s.active
         ORDER BY d.next_attempt_at
         LIMIT $2",
    )
    .bind(lock_duration)
    .bind(&worker)
    .bind(batch)
    .bind(encryption_key)
    .fetch_all(pool)
    .await?;

    let mut deliveries = Vec::new();
    let mut failed_subscriptions = Vec::new();

    for (id, sub_id, attempts, url, decrypted_secret, event_id, upgrade_id, created_at) in claimed {
        // Check if decryption succeeded
        let secret = match decrypted_secret {
            None => {
                warn!(delivery = id, subscription = %sub_id, "failed to decrypt secret (NULL or wrong key)");
                failed_subscriptions.push((sub_id, "secret unavailable: decryption failed"));
                let _ = mark_failed_undecryptable(pool, id, "secret unavailable: decryption failed").await;
                continue;
            }
            Some(s) if s.is_empty() => {
                warn!(delivery = id, subscription = %sub_id, "decrypted secret is empty");
                failed_subscriptions.push((sub_id, "secret unavailable: empty after decryption"));
                let _ = mark_failed_undecryptable(pool, id, "secret unavailable: empty").await;
                continue;
            }
            Some(s) => Some(s),
        };

        deliveries.push(DueDelivery {
            id,
            subscription_id: sub_id,
            attempts,
            url,
            secret,
            event_id,
            upgrade_id,
            created_at,
        });
    }

    // Auto-disable subscriptions with undecryptable secrets
    for (sub_id, reason) in failed_subscriptions {
        let _ = auto_disable_subscription(pool, sub_id, reason).await;
    }

    Ok(deliveries)
}

/// Mark a delivery as failed due to undecryptable secret and clear the lock.
async fn mark_failed_undecryptable(pool: &PgPool, delivery_id: i64, error_msg: &str) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE webhook_deliveries
         SET status = 'failed', last_error = $2, locked_until = NULL, locked_by = NULL
         WHERE id = $1",
    )
    .bind(delivery_id)
    .bind(error_msg)
    .execute(pool)
    .await?;
    Ok(())
}

/// Auto-disable a subscription with an explanatory reason.
async fn auto_disable_subscription(pool: &PgPool, subscription_id: Uuid, reason: &str) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE webhook_subscriptions
         SET active = false, auto_disabled_at = now(), auto_disabled_reason = $2
         WHERE id = $1",
    )
    .bind(subscription_id)
    .bind(reason)
    .execute(pool)
    .await?;
    
    warn!(subscription = %subscription_id, reason, "auto-disabled subscription due to secret error");
    Ok(())
}

/// Deliver all due rows once. Returns (delivered, failed) counts.
pub async fn deliver(
    pool: &PgPool,
    http: &reqwest::Client,
    config: &Config,
) -> anyhow::Result<(u64, u64)> {
    let deliveries = fetch_due(pool, config.batch_size, &config.encryption_key).await?;
    if deliveries.is_empty() {
        return Ok((0, 0));
    }

    let per_host_limits = build_per_host_limits(&deliveries, config.max_concurrent_per_host);
    let semaphore = Arc::new(Semaphore::new(config.max_concurrent_deliveries));
    let config = Arc::new(config.clone());

    let results: Vec<_> = stream::iter(deliveries)
        .map(|d| {
            let pool = pool.clone();
            let http = http.clone();
            let config = config.clone();
            let semaphore = semaphore.clone();
            let per_host_limits = per_host_limits.clone();

            async move {
                let host = extract_host(&d.url);
                let host_semaphore = per_host_limits
                    .get(&host)
                    .expect("host should have a semaphore");

                let _permit = semaphore.acquire().await.ok()?;
                let _host_permit = host_semaphore.acquire().await.ok()?;

                match send(&pool, &http, &d, &config).await {
                    Ok(()) => {
                        let _ = mark_delivered(&pool, d.id).await;
                        let _ = sqlx::query(
                            "UPDATE webhook_subscriptions 
                             SET consecutive_failures = 0, 
                                 last_success_at = now() 
                             WHERE id = $1"
                        )
                        .bind(d.subscription_id)
                        .execute(&pool)
                        .await;
                        Some((true, d.subscription_id))
                    }
                    Err(e) => {
                        let _ = mark_retry(&pool, &d, &e.to_string(), config.max_attempts).await;
                        warn!(delivery = d.id, url = %d.url, error = %e, "webhook delivery failed");

                        let _ = sqlx::query(
                            "UPDATE webhook_subscriptions SET consecutive_failures = consecutive_failures + 1 WHERE id = $1"
                        )
                        .bind(d.subscription_id)
                        .execute(&pool)
                        .await;

                        let _ = check_and_auto_disable(&pool, d.subscription_id, config.failure_threshold).await;
                        // Atomic increment and check for auto-disable
                        let _ = check_and_auto_disable(&pool, &d.subscription_id, config.failure_threshold).await;

                        Some((false, d.subscription_id))
                    }
                }
            }
        })
        .buffer_unordered(config.max_concurrent_deliveries)
        .collect()
        .await;

    let delivered = results.iter().filter(|r| r.map_or(false, |(s, _)| s)).count() as u64;
    let failed = results.iter().filter(|r| r.map_or(false, |(s, _)| !s)).count() as u64;

    if delivered > 0 || failed > 0 {
        info!(delivered, failed, "webhook deliveries processed");
    }
    Ok((delivered, failed))
}

fn build_per_host_limits(
    deliveries: &[DueDelivery],
    max_per_host: usize,
) -> HashMap<String, Arc<Semaphore>> {
    let mut limits: HashMap<String, Arc<Semaphore>> = HashMap::new();
    for d in deliveries {
        let host = extract_host(&d.url);
        limits
            .entry(host)
            .or_insert_with(|| Arc::new(Semaphore::new(max_per_host)));
    }
    limits
}

fn extract_host(url: &str) -> String {
    Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(|s| s.to_string()))
        .unwrap_or_default()
}

async fn send(pool: &PgPool, http: &reqwest::Client, d: &DueDelivery, config: &Config) -> anyhow::Result<()> {
    // Re-validate URL at delivery time to prevent DNS rebinding attacks.
    validate_webhook_url_at_delivery(&d.url).await
        .map_err(|e| anyhow::anyhow!("URL validation failed at delivery: {}", e))?;

    let secret = d.secret.as_ref()
        .ok_or_else(|| anyhow::anyhow!("delivery has no decrypted secret"))?;

    // Build the versioned envelope based on delivery type
    let envelope = if let Some(event_id) = &d.event_id {
        // Fetch event data to construct envelope
        let event: (String, String, i64, DateTime<Utc>, Option<String>, Json<Vec<String>>, String, Json<serde_json::Value>, Json<serde_json::Value>, Option<Json<serde_json::Value>>, String, bool) = sqlx::query_as(
            "SELECT event_id, contract_id, ledger, ledger_closed_at, event_name, 
                    topics, value, decoded_topics, decoded_value, enriched, 
                    tx_hash, in_successful_call
             FROM events WHERE event_id = $1"
        )
        .bind(event_id)
        .fetch_one(pool)
        .await?;

        let event_data = ContractEventData {
            event_id: event.0,
            contract_id: event.1,
            ledger: event.2,
            ledger_closed_at: event.3,
            event_name: event.4,
            topics: event.5.0,
            value: event.6,
            decoded_topics: event.7.0,
            decoded_value: event.8.0,
            enriched: event.9.map(|j| j.0),
            tx_hash: event.10,
            in_successful_call: event.11,
        };

        WebhookEnvelope::for_event(d.id, d.created_at, event_data)
    } else if let Some(upgrade_id) = d.upgrade_id {
        // Fetch upgrade data to construct envelope
        let upgrade: (String, i32, String, Option<String>, bool, Json<serde_json::Value>, DateTime<Utc>) = sqlx::query_as(
            "SELECT contract_id, version, wasm_hash, previous_wasm_hash, breaking, diff, observed_at
             FROM contract_spec_versions WHERE id = $1"
        )
        .bind(upgrade_id)
        .fetch_one(pool)
        .await?;

        let upgrade_data = ContractUpgradeData {
            contract_id: upgrade.0,
            version: upgrade.1,
            wasm_hash: upgrade.2,
            previous_wasm_hash: upgrade.3,
            breaking: upgrade.4,
            diff: upgrade.5.0,
            observed_at: upgrade.6,
        };

        WebhookEnvelope::for_upgrade(d.id, d.created_at, upgrade_data)
    } else {
        return Err(anyhow::anyhow!("delivery has neither event_id nor upgrade_id"));
    };

    let body = serde_json::to_vec(&envelope)?;
    let timestamp = Utc::now().to_rfc3339();

    // Compute HMAC-SHA256 signature using the webhook secret.
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).context("invalid webhook secret")?;
    mac.update(&body);
    let signature = hex::encode(mac.finalize().into_bytes());

    let req = http
async fn send(http: &reqwest::Client, d: &DueDelivery, config: &Config) -> anyhow::Result<()> {
    // Re-validate URL at delivery time and get the validated IP for connection pinning.
    // This ensures the hostname still resolves to a public address even if the
    // DNS record changed since registration, and prevents DNS rebinding attacks.
    let validated_ip = validate_webhook_url_at_delivery(&d.url).await
        .map_err(|e| anyhow::anyhow!("URL validation failed at delivery: {}", e))?;

    let body = serde_json::to_vec(&d.payload.0)?;
    let timestamp = Utc::now().timestamp();

    // New timestamped signature scheme (Stripe/Svix style):
    // signed_payload = "{timestamp}.{body}"
    // signature = HMAC-SHA256(secret, signed_payload)
    let signed_payload = format!("{}.{}", timestamp, std::str::from_utf8(&body)?);
    
    let mut mac =
        HmacSha256::new_from_slice(d.secret.as_bytes()).context("invalid webhook secret")?;
    mac.update(signed_payload.as_bytes());
    let signature_v1 = hex::encode(mac.finalize().into_bytes());
    
    // Legacy signature for backwards compatibility (will be removed in future release)
    let mut legacy_mac =
        HmacSha256::new_from_slice(d.secret.as_bytes()).context("invalid webhook secret")?;
    legacy_mac.update(&body);
    let legacy_signature = hex::encode(legacy_mac.finalize().into_bytes());

    // Determine event type from payload
    let event_type = d.payload.0.get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("contract.event");

    // Extract host for connection pinning
    let parsed_url = Url::parse(&d.url).context("failed to parse URL")?;
    let host = parsed_url.host_str().context("URL must have a host")?;
    
    // Build client with connection pinning to the validated IP
    let client = reqwest::Client::builder()
        .resolve(host, std::net::SocketAddr::new(validated_ip, parsed_url.port().unwrap_or(if parsed_url.scheme() == "https" { 443 } else { 80 })))
        .build()
        .context("failed to build pinned client")?;

    let req = client
        .post(&d.url)
        .timeout(config.total_timeout())
        .header("Content-Type", "application/json")
        .header("X-Lumenqraph-Signature", format!("t={},v1={}", timestamp, signature_v1))
        .header("X-Lumenqraph-Signature-Legacy", format!("sha256={}", legacy_signature))
        .header("X-Lumenqraph-Delivery-Id", d.id.to_string())
        .header("X-Lumenqraph-Timestamp", timestamp.to_string())
        .header("X-Lumenqraph-Attempt", d.attempts.to_string())
        .header("X-Lumenqraph-Event", &envelope.payload_type)
        .header("User-Agent", USER_AGENT)
        .body(body)
        .build()
        .context("failed to build request")?;

    let resp = client
        .execute(req)
        .await
        .context("request failed")?;

    let location = resp
        .headers()
        .get(reqwest::header::LOCATION)
        .and_then(|v| v.to_str().ok());
    delivery_outcome(resp.status(), location)
}

/// Build the HTTP client used for webhook deliveries.
///
/// Redirects are never followed: `validate_webhook_url_at_delivery` only vets
/// the subscriber's URL, so following a `3xx` would let a subscriber bounce a
/// signed request to an internal address (cloud metadata, databases, ...).
pub fn build_delivery_client(
    connect_timeout: std::time::Duration,
    total_timeout: std::time::Duration,
) -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .connect_timeout(connect_timeout)
        .timeout(total_timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
}

/// Map a delivery response to success/failure. Only `2xx` counts as delivered;
/// a `3xx` is a failure because redirects are not followed.
fn delivery_outcome(status: reqwest::StatusCode, location: Option<&str>) -> anyhow::Result<()> {
    if status.is_success() {
        Ok(())
    } else if status.is_redirection() {
        Err(anyhow::anyhow!(
            "redirects are not followed: endpoint returned {} (Location: {})",
            status,
            location.unwrap_or("<none>")
        ))
    } else {
        Err(anyhow::anyhow!("non-2xx status {}", status))
    }
}

async fn mark_delivered(pool: &PgPool, id: i64) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE webhook_deliveries
         SET status='delivered', attempts=attempts+1, delivered_at=now(), last_error=NULL,
             locked_until=NULL, locked_by=NULL
         WHERE id=$1",
    )
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn mark_retry(
    pool: &PgPool,
    d: &DueDelivery,
    err: &str,
    max_attempts: i32,
) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;

    let attempts = d.attempts + 1;
    if attempts >= max_attempts {
        sqlx::query(
            "UPDATE webhook_deliveries
             SET status='failed', attempts=$2, last_error=$3, locked_until=NULL, locked_by=NULL
             WHERE id=$1",
        )
        .bind(d.id)
        .bind(attempts)
        .bind(err)
        .execute(&mut *tx)
        .await?;
    } else {
        let max_secs = 2i64.saturating_pow(attempts as u32).min(3600);
        let mut rng = rand::thread_rng();
        let jittered_secs = rng.gen_range(0..=max_secs);
        let next: DateTime<Utc> = Utc::now() + Duration::seconds(jittered_secs);
        sqlx::query(
            "UPDATE webhook_deliveries
             SET attempts=$2, last_error=$3, next_attempt_at=$4, locked_until=NULL, locked_by=NULL
             WHERE id=$1",
        )
        .bind(d.id)
        .bind(attempts)
        .bind(err)
        .bind(next)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

async fn check_and_auto_disable(
    pool: &PgPool,
    subscription_id: Uuid,
    failure_threshold: i32,
) -> anyhow::Result<()> {
    // Atomically increment consecutive_failures and return the updated value along with timing info
    let result: Option<(i32, Option<DateTime<Utc>>, Option<DateTime<Utc>>)> = sqlx::query_as(
        "UPDATE webhook_subscriptions
         SET consecutive_failures = consecutive_failures + 1,
             first_failure_at = COALESCE(first_failure_at, now())
         WHERE id = $1
         RETURNING consecutive_failures, last_success_at, first_failure_at",
    )
    .bind(subscription_id)
    .fetch_optional(pool)
    .await?;

    if let Some((consecutive_failures, last_success_at, first_failure_at)) = result {
        // Auto-disable if:
        // 1. Consecutive failures >= threshold, AND
        // 2. No success in the last 24 hours (or never had a success and failing for 24h)
        let should_disable = consecutive_failures >= failure_threshold && {
            let now = Utc::now();
            if let Some(last_success) = last_success_at {
                (now - last_success).num_hours() >= 24
            } else if let Some(first_failure) = first_failure_at {
                (now - first_failure).num_hours() >= 24
            } else {
                false
            }
        };

        if should_disable {
            let reason = format!(
                "Auto-disabled after {} consecutive delivery failures with no success for 24+ hours",
                consecutive_failures
            );
            sqlx::query(
                "UPDATE webhook_subscriptions
                 SET active = false, auto_disabled_at = now(), auto_disabled_reason = $2
                 WHERE id = $1",
            )
            .bind(subscription_id)
            .bind(&reason)
            .execute(pool)
            .await?;

        warn!(
            subscription_id = %subscription_id,
            consecutive_failures = consecutive_failures,
            "webhook subscription auto-disabled"
        );
            warn!(
                subscription_id = subscription_id,
                consecutive_failures = consecutive_failures,
                "webhook subscription auto-disabled"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    //! Enqueue + payload tests. These need a throwaway Postgres:
    //!
    //!   TEST_DATABASE_URL=postgres://…/lumenqraph \
    //!     cargo test -p lumenqraph-webhooks -- --ignored
    //!
    //! Each test provisions its own isolated schema (no DROP CASCADE, safe for
    //! parallel runs). Pass any Postgres URL — the test schema is cleaned up
    //! automatically, so it cannot corrupt an existing database.

    use super::*;
    use sqlx::postgres::PgPoolOptions;

    /// Fresh, isolated schema per test — safe for parallel execution.
    async fn fixture() -> PgPool {
        let url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL");
        // Each test gets a unique schema so concurrent tests never interfere.
        let schema = format!("test_{}", uuid::Uuid::new_v4().simple());

        // Create the schema in the default database.
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect to TEST_DATABASE_URL");
        sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
            .execute(&admin)
            .await
            .expect("create test schema");
        admin.close().await;

        // Reconnect with search_path so migrations and all queries use it.
        let option = format!("-c search_path={schema},public");
        let sep = if url.contains('?') { "&" } else { "?" };
        let schema_url = format!("{url}{sep}options={}", percent_encode(&option));
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&schema_url)
            .await
            .expect("connect with search_path");
        sqlx::migrate!("../../migrations")
            .run(&pool)
            .await
            .expect("migrate");
        pool
    }

    fn percent_encode(s: &str) -> String {
        s.chars()
            .flat_map(|c| match c {
                'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => vec![c],
                c => format!("%{:02X}", c as u32).chars().collect(),
            })
            .collect()
    }

    /// A subscription of `kind`, optionally scoped to one contract.
    async fn subscribe(pool: &PgPool, kind: &str, contract_id: Option<&str>, encryption_key: &str) {
        sqlx::query(
            "INSERT INTO webhook_subscriptions (url, kind, contract_id, encrypted_secret)
             VALUES ('https://example.test/hook', $1, $2, $3)",
        )
        .bind(kind)
        .bind(contract_id)
        .bind(encryption_key.as_bytes())  // Store as bytes for the test
        .execute(pool)
        .await
        .expect("insert subscription");
    }

    /// One interface version for contract `C1`.
    async fn add_version(pool: &PgPool, version: i32, breaking: bool) {
        sqlx::query(
            "INSERT INTO contract_spec_versions
                (contract_id, version, wasm_hash, interface, diff, breaking)
             VALUES ('C1', $1, 'hash', '{}', $2, $3)",
        )
        .bind(version)
        .bind(serde_json::json!({
            "breaking": breaking,
            "summary": ["removed function withdraw() -> void"],
        }))
        .bind(breaking)
        .execute(pool)
        .await
        .expect("insert version");
    }

    async fn add_event(pool: &PgPool, event_id: &str) {
        sqlx::query(
            "INSERT INTO events (event_id, contract_id, ledger, ledger_closed_at, event_type,
                                 topics, event_name, value, tx_hash, in_successful_call, paging_token)
             VALUES ($1,'C1',1,now(),'contract','[]','transfer','v','tx',true,$1)",
        )
        .bind(event_id)
        .execute(pool)
        .await
        .expect("insert event");
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn an_upgrade_is_delivered_to_upgrade_subscribers_with_its_diff() {
        let pool = fixture().await;
        subscribe(&pool, "upgrade", None, "test-key").await;
        add_version(&pool, 1, false).await;
        add_version(&pool, 2, true).await;

        assert_eq!(
            enqueue(&pool, 100).await.unwrap(),
            1,
            "only the upgrade (v2) enqueues; v1 is a baseline, not a change"
        );

        let due = fetch_due(&pool, 100, "test-key").await.unwrap();
        assert_eq!(due.len(), 1);
        assert!(due[0].upgrade_id.is_some());
        assert_eq!(due[0].upgrade_id.unwrap(), 2);
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn the_two_streams_do_not_cross() {
        let pool = fixture().await;
        // An event subscriber that matches every contract must not be handed an
        // upgrade, and vice versa: the payload shapes are different.
        subscribe(&pool, "event", None, "test-key").await;
        add_version(&pool, 1, false).await;
        add_version(&pool, 2, true).await;
        assert_eq!(enqueue(&pool, 100).await.unwrap(), 0);

        let pool = fixture().await;
        subscribe(&pool, "upgrade", None, "test-key").await;
        add_event(&pool, "e1").await;
        assert_eq!(enqueue(&pool, 100).await.unwrap(), 0);
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn an_upgrade_subscription_is_scoped_to_its_contract() {
        let pool = fixture().await;
        subscribe(&pool, "upgrade", Some("C2"), "test-key").await;
        add_version(&pool, 1, false).await;
        add_version(&pool, 2, true).await; // on C1
        assert_eq!(enqueue(&pool, 100).await.unwrap(), 0);

        subscribe(&pool, "upgrade", Some("C1"), "test-key").await;
        // The watermark already passed C1's versions, so a new subscriber only
        // gets upgrades from here on — the same catch-up behaviour events have.
        add_version(&pool, 3, false).await;
        assert_eq!(enqueue(&pool, 100).await.unwrap(), 1);
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn re_enqueueing_does_not_duplicate_deliveries() {
        let pool = fixture().await;
        subscribe(&pool, "upgrade", None, "test-key").await;
        add_version(&pool, 1, false).await;
        add_version(&pool, 2, true).await;

        assert_eq!(enqueue(&pool, 100).await.unwrap(), 1);
        // The watermark has advanced, so a second pass finds nothing new.
        assert_eq!(enqueue(&pool, 100).await.unwrap(), 0);
        assert_eq!(fetch_due(&pool, 100, "test-key").await.unwrap().len(), 1);
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn webhook_retries_have_variance_in_scheduled_times() {
        let pool = fixture().await;
        subscribe(&pool, "event", None, "test-key").await;

        // Create multiple failed deliveries at the same time.
        for i in 0..5 {
            add_event(&pool, &format!("e{}", i)).await;
        }
        assert_eq!(enqueue(&pool, 100).await.unwrap(), 5);

        // Simulate failures for all 5 deliveries.
        let due = fetch_due(&pool, 100, "test-key").await.unwrap();
        assert_eq!(due.len(), 5);

        for d in due {
            mark_retry(&pool, &d, "test error", 5)
                .await
                .expect("mark retry");
        }

        // Fetch all scheduled retries and check their times are different.
        let scheduled: Vec<DateTime<Utc>> = sqlx::query_scalar(
            "SELECT next_attempt_at FROM webhook_deliveries WHERE status = 'pending' ORDER BY id",
        )
        .fetch_all(&pool)
        .await
        .expect("fetch scheduled times");

        assert_eq!(scheduled.len(), 5);

        // With full jitter, we expect variance in the scheduled times.
        // Check that at least some differ (very unlikely all 5 would be identical with jitter).
        let unique_times: std::collections::HashSet<_> = scheduled.iter().collect();
        assert!(
            unique_times.len() > 1,
            "Expected variance in retry times due to jitter, got {} unique times",
            unique_times.len()
        );
    }

    // ── Cross-language signing test vector (issue #270) ──────────────────────
    //
    // This test produces a stable, hard-coded HMAC-SHA256 signature using the
    // *exact same* algorithm as `send()`. The values are intentionally fixed so
    // the companion TypeScript test in
    //   sdk/typescript/src/webhook_roundtrip.test.ts
    // can verify that the TypeScript `verifyWebhook()` function produces the
    // same result for the same inputs — catching any encoding mismatch between
    // the two implementations.
    //
    // The body contains a non-ASCII Unicode character (€ U+20AC) to expose any
    // difference in UTF-8 encoding between the two sides.
    //
    // To regenerate the expected signature:
    //   echo -n '{"event":"transfer","amount":"100","currency":"€"}' | \
    //     openssl dgst -sha256 -hmac 'round-trip-secret-42'
    #[test]
    fn signing_produces_stable_cross_language_test_vector() {
        // Fixed inputs — do not change without updating the TypeScript fixture.
        let secret = "round-trip-secret-42";
        let body = r#"{"event":"transfer","amount":"100","currency":"€"}"#;
        let expected_hex = "7f57c5ce713d6e0af04058523af69c77096025b41bac63a883ecf8be1b2ca55e";

        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
            .expect("HMAC accepts any key size");
        mac.update(body.as_bytes());
        let computed_hex = hex::encode(mac.finalize().into_bytes());

        assert_eq!(
            computed_hex, expected_hex,
            "HMAC-SHA256 signature must match the pre-computed test vector; \
             if you changed the signing algorithm, update the TypeScript fixture too"
        );
        // Confirm the full header matches the format the TypeScript SDK expects.
        assert_eq!(
            format!("sha256={computed_hex}"),
            format!("sha256={expected_hex}")
        );
    }
}
#[cfg(test)]
mod redirect_tests {
    //! Regression tests for #447: a subscriber answering `302 Location:
    //! <internal address>` must not make the dispatcher contact that address.
    //! No database needed.

    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration as StdDuration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn redirect_to_internal_target_is_not_followed() {
        // Stand-in for an internal service (e.g. 169.254.169.254): counts hits.
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_addr = target.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_srv = hits.clone();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = target.accept().await {
                hits_srv.fetch_add(1, Ordering::SeqCst);
                let _ = sock
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                    .await;
            }
        });

        // Subscriber endpoint that always redirects to the internal target.
        let hook = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hook_addr = hook.local_addr().unwrap();
        let location = format!("http://{target_addr}/latest/meta-data/");
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = hook.accept().await {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let resp = format!(
                    "HTTP/1.1 302 Found\r\nlocation: {location}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            }
        });

        let http = build_delivery_client(StdDuration::from_secs(2), StdDuration::from_secs(5))
            .expect("client");
        let resp = http
            .post(format!("http://{hook_addr}/hook"))
            .header("Content-Type", "application/json")
            .body("{}")
            .send()
            .await
            .expect("request to hook");

        assert_eq!(resp.status(), reqwest::StatusCode::FOUND);
        let loc = resp
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let err = delivery_outcome(resp.status(), loc.as_deref()).unwrap_err();
        assert!(
            err.to_string().contains("redirects are not followed"),
            "unexpected error: {err}"
        );

        tokio::time::sleep(StdDuration::from_millis(100)).await;
        assert_eq!(hits.load(Ordering::SeqCst), 0, "redirect target was contacted");
    }

    #[test]
    fn redirect_statuses_are_failures_with_clear_error() {
        for code in [301u16, 302, 303, 307, 308] {
            let status = reqwest::StatusCode::from_u16(code).unwrap();
            let err = delivery_outcome(status, Some("http://10.0.0.5:5432/")).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("redirects are not followed"), "{code}: {msg}");
            assert!(msg.contains("http://10.0.0.5:5432/"), "{code}: {msg}");
        }
    }

    #[test]
    fn success_and_other_errors_unchanged() {
        assert!(delivery_outcome(reqwest::StatusCode::OK, None).is_ok());
        assert!(delivery_outcome(reqwest::StatusCode::NO_CONTENT, None).is_ok());
        let err = delivery_outcome(reqwest::StatusCode::INTERNAL_SERVER_ERROR, None).unwrap_err();
        assert!(err.to_string().contains("non-2xx status"));
    }
}
