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
use std::time::Duration as StdDuration;
use sqlx::types::Json;
use sqlx::PgPool;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
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

/// Advisory-lock key serialising the legacy secret backfill across replicas (#566).
const WEBHOOK_SECRET_BACKFILL_LOCK: i64 = 566_001;

/// Encrypt legacy plaintext subscription secrets into `encrypted_secret` (#566).
///
/// Runs in a transaction holding a transaction-scoped advisory lock, so
/// replicas starting together do not race. The statement is idempotent: rows
/// already encrypted no longer match. Returns the number of rows migrated.
pub async fn backfill_encrypted_secrets(pool: &PgPool, encryption_key: &str) -> anyhow::Result<u64> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(WEBHOOK_SECRET_BACKFILL_LOCK)
        .execute(&mut *tx)
        .await?;
    let migrated = sqlx::query(lumenqraph_core::crypto::BACKFILL_WEBHOOK_SECRETS_SQL)
        .bind(encryption_key)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    tx.commit().await?;
    Ok(migrated)
}

/// Enqueue deliveries for everything new in both streams, and advance any
/// per-subscription backfill cursors. Returns how many delivery rows were
/// created (live + backfill combined).
pub async fn enqueue(pool: &PgPool, batch: i64) -> anyhow::Result<u64> {
    let events = enqueue_events(pool, batch).await?;
    let upgrades = enqueue_upgrades(pool, batch).await?;
    let backfill = enqueue_backfill(pool, batch).await?;
    Ok(events + upgrades + backfill)
}

/// Advance per-subscription backfill cursors.
///
/// Subscriptions created with a `since` parameter have `backfill_seq` set to
/// a value below the global watermark at creation time. This pass delivers
/// events in the window `(backfill_seq, global_last_seq_at_creation]` in
/// bounded batches, advancing `backfill_seq` atomically so a crash cannot
/// duplicate or skip deliveries.
///
/// Once `backfill_seq` reaches or exceeds the subscription's `starting_seq`
/// (the watermark at creation) the column is cleared (set to NULL) and the
/// subscription transitions fully to the live stream.
async fn enqueue_backfill(pool: &PgPool, batch: i64) -> anyhow::Result<u64> {
    // Fetch all subscriptions that still have an outstanding backfill cursor.
    let subs: Vec<(String, i64, i64)> = sqlx::query_as(
        "SELECT id::text, backfill_seq, starting_seq
         FROM webhook_subscriptions
         WHERE active AND backfill_seq IS NOT NULL",
    )
    .fetch_all(pool)
    .await?;

    if subs.is_empty() {
        return Ok(0);
    }

    let mut total: u64 = 0;

    for (sub_id, backfill_seq, starting_seq) in subs {
        // The backfill window is (backfill_seq, starting_seq].  Once the cursor
        // reaches starting_seq the backfill is complete.
        if backfill_seq >= starting_seq {
            sqlx::query(
                "UPDATE webhook_subscriptions SET backfill_seq = NULL WHERE id = $1::uuid",
            )
            .bind(&sub_id)
            .execute(pool)
            .await?;
            continue;
        }

        let upper = (backfill_seq + batch).min(starting_seq);

        let mut tx = pool.begin().await?;

        let created = sqlx::query(
            "INSERT INTO webhook_deliveries (subscription_id, event_id)
             SELECT $1::uuid, e.event_id
             FROM events e
             JOIN webhook_subscriptions s ON s.id = $1::uuid
             WHERE s.kind = 'event'
               AND (s.contract_id IS NULL OR s.contract_id = e.contract_id OR (COALESCE(array_length(s.contract_ids, 1), 0) > 0 AND e.contract_id = ANY(s.contract_ids)))
               AND (s.event_name IS NULL OR s.event_name = e.event_name OR (COALESCE(array_length(s.event_names, 1), 0) > 0 AND e.event_name = ANY(s.event_names)))
               AND (
                    s.filter IS NULL OR s.filter = '{}'::jsonb OR (
                        (NOT (s.filter ? 'contract_id') OR s.filter->>'contract_id' = e.contract_id)
                        AND (NOT (s.filter ? 'event_name') OR s.filter->>'event_name' = e.event_name)
                        AND (NOT (s.filter ? 'topic0') OR e.decoded_topics @> jsonb_build_array((s.filter->>'topic0')::jsonb))
                        AND (NOT (s.filter ? 'topic1') OR e.decoded_topics @> jsonb_build_array('null'::jsonb, (s.filter->>'topic1')::jsonb))
                        AND (NOT (s.filter ? 'topic2') OR e.decoded_topics @> jsonb_build_array('null'::jsonb, 'null'::jsonb, (s.filter->>'topic2')::jsonb))
                        AND (NOT (s.filter ? 'topic3') OR e.decoded_topics @> jsonb_build_array('null'::jsonb, 'null'::jsonb, 'null'::jsonb, (s.filter->>'topic3')::jsonb))
                        AND (NOT (s.filter ? 'to') OR e.enriched @> jsonb_build_object('to', s.filter->'to'))
                        AND (NOT (s.filter ? 'from') OR e.enriched @> jsonb_build_object('from', s.filter->'from'))
                        AND (NOT (s.filter ? 'amount') OR e.enriched @> jsonb_build_object('amount', s.filter->'amount'))
                    )
               )
               AND e.seq > $2 AND e.seq <= $3
             ON CONFLICT (subscription_id, event_id) DO NOTHING",
        )
        .bind(&sub_id)
        .bind(backfill_seq)
        .bind(upper)
        .execute(&mut *tx)
        .await?
        .rows_affected();

        // Advance or clear the cursor.
        if upper >= starting_seq {
            sqlx::query(
                "UPDATE webhook_subscriptions SET backfill_seq = NULL WHERE id = $1::uuid",
            )
            .bind(&sub_id)
            .execute(&mut *tx)
            .await?;
        } else {
            sqlx::query(
                "UPDATE webhook_subscriptions SET backfill_seq = $2 WHERE id = $1::uuid",
            )
            .bind(&sub_id)
            .bind(upper)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        if created > 0 {
            debug!(
                sub_id = %sub_id,
                created,
                up_to_seq = upper,
                "backfill: enqueued deliveries"
            );
        }

        total += created;
    }

    Ok(total)
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
          AND (s.contract_id IS NULL OR s.contract_id = e.contract_id OR (COALESCE(array_length(s.contract_ids, 1), 0) > 0 AND e.contract_id = ANY(s.contract_ids)))
          AND (s.event_name IS NULL OR s.event_name = e.event_name OR (COALESCE(array_length(s.event_names, 1), 0) > 0 AND e.event_name = ANY(s.event_names)))
          AND (
                s.filter IS NULL OR s.filter = '{}'::jsonb OR (
                    (NOT (s.filter ? 'contract_id') OR s.filter->>'contract_id' = e.contract_id)
                    AND (NOT (s.filter ? 'event_name') OR s.filter->>'event_name' = e.event_name)
                    AND (NOT (s.filter ? 'topic0') OR e.decoded_topics @> jsonb_build_array((s.filter->>'topic0')::jsonb))
                    AND (NOT (s.filter ? 'topic1') OR e.decoded_topics @> jsonb_build_array('null'::jsonb, (s.filter->>'topic1')::jsonb))
                    AND (NOT (s.filter ? 'topic2') OR e.decoded_topics @> jsonb_build_array('null'::jsonb, 'null'::jsonb, (s.filter->>'topic2')::jsonb))
                    AND (NOT (s.filter ? 'topic3') OR e.decoded_topics @> jsonb_build_array('null'::jsonb, 'null'::jsonb, 'null'::jsonb, (s.filter->>'topic3')::jsonb))
                    AND (NOT (s.filter ? 'to') OR e.enriched @> jsonb_build_object('to', s.filter->'to'))
                    AND (NOT (s.filter ? 'from') OR e.enriched @> jsonb_build_object('from', s.filter->'from'))
                    AND (NOT (s.filter ? 'amount') OR e.enriched @> jsonb_build_object('amount', s.filter->'amount'))
                )
          )
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
          AND (s.contract_id IS NULL OR s.contract_id = v.contract_id OR (COALESCE(array_length(s.contract_ids, 1), 0) > 0 AND v.contract_id = ANY(s.contract_ids)))
         WHERE v.id > $1 AND v.id <= $2 AND v.version > 1
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
    /// Previous signing secret, present only while its rotation grace period is open.
    previous_secret: Option<String>,
    event_id: Option<String>,
    upgrade_id: Option<i64>,
    created_at: DateTime<Utc>,
    /// Pre-built envelope, joined in `fetch_due` so `send` never re-reads the
    /// event/upgrade row (#620).
    envelope: WebhookEnvelope,
}

#[derive(Clone, Debug, Default)]
pub struct DeliveryOutcome {
    pub status: Option<i32>,
    pub retry_after: Option<Duration>,
    pub body_snippet: String,
}

/// Atomically claim and fetch due deliveries, handling decryption failures gracefully.
///
/// The event/upgrade payloads are joined in the same statement so a batch of N
/// deliveries costs O(1) queries rather than N+1 (#620).
async fn fetch_due(pool: &PgPool, batch: i64, encryption_key: &str) -> anyhow::Result<Vec<DueDelivery>> {
    let worker = worker_id();
    let lock_duration = Duration::seconds(60);

    #[allow(clippy::type_complexity)]
    let claimed: Vec<(
        i64,
        Uuid,
        i32,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        DateTime<Utc>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<DateTime<Utc>>,
        Option<String>,
        Option<Json<Vec<String>>>,
        Option<String>,
        Option<Json<serde_json::Value>>,
        Option<Json<serde_json::Value>>,
        Option<Json<serde_json::Value>>,
        Option<String>,
        Option<bool>,
        Option<i32>,
        Option<String>,
        Option<String>,
        Option<bool>,
        Option<Json<serde_json::Value>>,
        Option<DateTime<Utc>>,
    )> = sqlx::query_as(
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
         RETURNING d.id,
                   d.subscription_id,
                   d.attempts,
                   (SELECT url FROM webhook_subscriptions WHERE id = d.subscription_id),
                   (SELECT safe_decrypt_webhook_secret(encrypted_secret, $4) FROM webhook_subscriptions WHERE id = d.subscription_id),
                   (SELECT CASE WHEN previous_secret_expires_at > now()
                                THEN safe_decrypt_webhook_secret(previous_encrypted_secret, $4)
                           END
                    FROM webhook_subscriptions WHERE id = d.subscription_id),
                   d.event_id,
                   d.upgrade_id,
                   d.created_at,
                   e.event_id,
                   e.contract_id,
                   e.ledger,
                   e.ledger_closed_at,
                   e.event_name,
                   e.topics,
                   e.value,
                   e.decoded_topics,
                   e.decoded_value,
                   e.enriched,
                   e.tx_hash,
                   e.in_successful_call,
                   v.contract_id,
                   v.wasm_hash,
                   v.previous_wasm_hash,
                   v.breaking,
                   v.diff,
                   v.observed_at",
    )
    .bind(lock_duration)
    .bind(&worker)
    .bind(batch)
    .bind(encryption_key)
    .fetch_all(pool)
    .await?;

    let mut deliveries = Vec::new();
    let mut failed_subscriptions = Vec::new();

    for row in claimed {
        let (
            id,
            sub_id,
            attempts,
            url,
            decrypted_secret,
            decrypted_previous,
            event_id,
            upgrade_id,
            created_at,
            ev_event_id,
            ev_contract_id,
            ev_ledger,
            ev_ledger_closed_at,
            ev_event_name,
            ev_topics,
            ev_value,
            ev_decoded_topics,
            ev_decoded_value,
            ev_enriched,
            ev_tx_hash,
            ev_in_successful_call,
            up_version,
            up_contract_id,
            up_wasm_hash,
            up_previous_wasm_hash,
            up_breaking,
            up_diff,
            up_observed_at,
        ) = row;

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
            Some(s) => s,
        };

        // Build the envelope from the joined row; no extra SELECT in `send`.
        let envelope = if let (Some(event_id), Some(contract_id), Some(ledger), Some(ledger_closed_at), Some(value), Some(decoded_topics), Some(decoded_value), Some(tx_hash), Some(in_successful_call)) = (
            ev_event_id,
            ev_contract_id,
            ev_ledger,
            ev_ledger_closed_at,
            ev_value,
            ev_decoded_topics,
            ev_decoded_value,
            ev_tx_hash,
            ev_in_successful_call,
        ) {
            let event_data = ContractEventData {
                event_id,
                contract_id,
                ledger,
                ledger_closed_at,
                event_name: ev_event_name,
                topics: ev_topics.map(|j| j.0).unwrap_or_default(),
                value,
                decoded_topics: decoded_topics.0,
                decoded_value: decoded_value.0,
                enriched: ev_enriched.map(|j| j.0),
                tx_hash,
                in_successful_call,
            };
            WebhookEnvelope::for_event(id, created_at, event_data)
        } else if let (Some(version), Some(contract_id), Some(wasm_hash), Some(breaking), Some(diff), Some(observed_at)) = (
            up_version,
            up_contract_id,
            up_wasm_hash,
            up_breaking,
            up_diff,
            up_observed_at,
        ) {
            let upgrade_data = ContractUpgradeData {
                contract_id,
                version,
                wasm_hash,
                previous_wasm_hash: up_previous_wasm_hash,
                breaking,
                diff: diff.0,
                observed_at,
            };
            WebhookEnvelope::for_upgrade(id, created_at, upgrade_data)
        } else {
            warn!(delivery = id, "delivery has neither event nor upgrade payload");
            let _ = mark_failed_undecryptable(pool, id, "delivery has neither event_id nor upgrade_id").await;
            continue;
        };

        deliveries.push(DueDelivery {
            id,
            subscription_id: sub_id,
            attempts,
            url,
            secret: Some(secret),
            previous_secret: decrypted_previous.filter(|s| !s.is_empty()),
            event_id,
            upgrade_id,
            created_at,
            envelope,
        });
    }

    for (sub_id, reason) in failed_subscriptions {
        let _ = auto_disable_subscription(pool, sub_id, reason).await;
    }

    Ok(deliveries)
}

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

pub async fn deliver(
    pool: &PgPool,
    http: &reqwest::Client,
    config: &Config,
) -> anyhow::Result<(u64, u64)> {
    deliver_with_shutdown(pool, http, config, &CancellationToken::new()).await
}

pub async fn deliver_with_shutdown(
    pool: &PgPool,
    http: &reqwest::Client,
    config: &Config,
    shutdown: &CancellationToken,
) -> anyhow::Result<(u64, u64)> {
    if shutdown.is_cancelled() {
        return Ok((0, 0));
    }

    // Continuous worker pool: claim a batch, spawn bounded sends, and claim
    // more as slots free up. A slow subscriber only occupies its own slot and
    // never blocks the pipeline for other tenants (#621).
    let semaphore = Arc::new(Semaphore::new(config.max_concurrent_deliveries));
    let per_host_limits: Arc<tokio::sync::Mutex<HashMap<String, Arc<Semaphore>>>> =
        Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    let config = Arc::new(config.clone());

    let mut delivered: u64 = 0;
    let mut failed: u64 = 0;
    let mut inflight: futures::stream::FuturesUnordered<_> = stream::FuturesUnordered::new();

    loop {
        if shutdown.is_cancelled() {
            break;
        }

        // Only claim when there is spare capacity; otherwise drain one result.
        let available = semaphore.available_permits();
        if available == 0 {
            if let Some(result) = inflight.next().await {
                if let Some((ok, _sub)) = result {
                    if ok {
                        delivered += 1;
                    } else {
                        failed += 1;
                    }
                }
            }
            continue;
        }

        let claim = available.min(config.batch_size.max(1) as usize);
        let deliveries = fetch_due(pool, claim as i64, &config.encryption_key).await?;
        if deliveries.is_empty() {
            break;
        }

        for d in deliveries {
            let pool = pool.clone();
            let http = http.clone();
            let config = config.clone();
            let semaphore = semaphore.clone();
            let per_host_limits = per_host_limits.clone();

            inflight.push(async move {
                let host = extract_host(&d.url);
                let host_semaphore = {
                    let mut limits = per_host_limits.lock().await;
                    limits
                        .entry(host)
                        .or_insert_with(|| Arc::new(Semaphore::new(config.max_concurrent_per_host)))
                        .clone()
                };

                let _permit = semaphore.acquire().await.ok()?;
                let _host_permit = host_semaphore.acquire().await.ok()?;

                match send(&pool, &http, &d, &config).await {
                    Ok(outcome) => {
                        let status = outcome.status.unwrap_or(200);
                        if (200..=299).contains(&status) {
                            let _ = mark_delivered(&pool, d.id, status, &outcome.body_snippet).await;
                            let _ = sqlx::query(
                                "UPDATE webhook_subscriptions SET consecutive_failures = 0, last_success_at = now() WHERE id = $1"
                            )
                            .bind(d.subscription_id)
                            .execute(&pool)
                            .await;
                            Some((true, d.subscription_id))
                        } else {
                            if status == 410 {
                                let _ = sqlx::query(
                                    "UPDATE webhook_subscriptions SET active = false, auto_disabled_at = now(), auto_disabled_reason = 'endpoint returned 410 Gone' WHERE id = $1"
                                )
                                .bind(d.subscription_id)
                                .execute(&pool)
                                .await;
                            }
                            let _ = mark_retry(&pool, &d, "non-2xx status", config.max_attempts, &outcome).await;
                            warn!(delivery = d.id, url = %d.url, status = status, "webhook delivery failed");
                            let _ = sqlx::query(
                                "UPDATE webhook_subscriptions SET consecutive_failures = consecutive_failures + 1 WHERE id = $1"
                            )
                            .bind(d.subscription_id)
                            .execute(&pool)
                            .await;
                            let _ = check_and_auto_disable(&pool, d.subscription_id, config.failure_threshold).await;
                            Some((false, d.subscription_id))
                        }
                    }
                    Err(e) if e.downcast_ref::<SourcePruned>().is_some() => {
                        // Permanent, non-retryable: the source row was pruned.
                        // Fail the delivery without touching attempts or the
                        // subscriber's failure counters (#602).
                        let _ = mark_failed_permanent(&pool, d.id, &e.to_string()).await;
                        warn!(delivery = d.id, url = %d.url, error = %e, "webhook delivery dropped: source pruned");
                        Some((false, d.subscription_id))
                    }
                    Err(e) => {
                        let err_str = e.to_string();
                        let _ = mark_retry(&pool, &d, &err_str, config.max_attempts, &DeliveryOutcome::default()).await;
                        warn!(delivery = d.id, url = %d.url, error = %e, "webhook delivery failed");
                        let _ = sqlx::query(
                            "UPDATE webhook_subscriptions SET consecutive_failures = consecutive_failures + 1 WHERE id = $1"
                        )
                        .bind(d.subscription_id)
                        .execute(&pool)
                        .await;
                        let _ = check_and_auto_disable(&pool, d.subscription_id, config.failure_threshold).await;
                        Some((false, d.subscription_id))
                    }
                }
            });
        }
    }

    // Drain any remaining in-flight sends.
    while let Some(result) = inflight.next().await {
        if let Some((ok, _sub)) = result {
            if ok {
                delivered += 1;
            } else {
                failed += 1;
            }
        }
    }

    if delivered > 0 || failed > 0 {
        info!(delivered, failed, "webhook deliveries processed");
    }
    Ok((delivered, failed))
}

fn extract_host(url: &str) -> String {
    Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(|s| s.to_string()))
        .unwrap_or_default()
}

/// Computes the hex-encoded HMAC-SHA256 of `signed_payload` under `secret`.
fn sign_payload(secret: &str, signed_payload: &str) -> anyhow::Result<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).context("invalid webhook secret")?;
    mac.update(signed_payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

async fn send(pool: &PgPool, http: &reqwest::Client, d: &DueDelivery, config: &Config) -> anyhow::Result<DeliveryOutcome> {
    validate_webhook_url_at_delivery(&d.url)
        .await
        .map_err(|e| anyhow::anyhow!("URL validation failed at delivery: {}", e))?;

    let secret = d.secret.as_ref().ok_or_else(|| anyhow::anyhow!("delivery has no decrypted secret"))?;

    // A delivery whose source row was pruned (retention deletes old
    // `contract_spec_versions`) can never succeed. Surface it as a distinct,
    // permanent error so the caller can fail it without retrying or counting
    // it against the subscriber (#602).
    let envelope = if let Some(event_id) = &d.event_id {
        let event: (
            String,
            String,
            i64,
            DateTime<Utc>,
            Option<String>,
            Json<Vec<String>>,
            String,
            Json<serde_json::Value>,
            Json<serde_json::Value>,
            Option<Json<serde_json::Value>>,
            String,
            bool,
        ) = sqlx::query_as(
            "SELECT event_id, contract_id, ledger, ledger_closed_at, event_name, topics, value, decoded_topics, decoded_value, enriched, tx_hash, in_successful_call FROM events WHERE event_id = $1",
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
        let upgrade: (String, i32, String, Option<String>, bool, Json<serde_json::Value>, DateTime<Utc>) = sqlx::query_as(
            "SELECT contract_id, version, wasm_hash, previous_wasm_hash, breaking, diff, observed_at FROM contract_spec_versions WHERE id = $1",
        )
        .bind(upgrade_id)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| anyhow::Error::new(SourcePruned))?;

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
        // An upgrade delivery with no `upgrade_id` means the referenced spec
        // version was pruned (FK is `ON DELETE SET NULL`), so there is nothing
        // left to deliver.
        return Err(anyhow::Error::new(SourcePruned));
    };

    let body = serde_json::to_vec(&envelope)?;
    let timestamp = Utc::now().timestamp();
    let signed_payload = format!("{}.{}", timestamp, std::str::from_utf8(&body)?);

    // During a rotation grace period the previous secret signs too, so receivers
    // that have not yet picked up the new secret keep verifying (#564).
    let mut signature_header = format!("t={},v1={}", timestamp, sign_payload(secret, &signed_payload)?);
    if let Some(previous) = &d.previous_secret {
        signature_header.push_str(&format!(",v1={}", sign_payload(previous, &signed_payload)?));
    }

    let parsed_url = Url::parse(&d.url).context("failed to parse URL")?;
    let host = parsed_url.host_str().context("URL must have a host")?;
    let validated_ip = validate_webhook_url_at_delivery(&d.url).await.map_err(|e| anyhow::anyhow!(e))?;
    // Pin the validated IP, but keep the same hardening as `build_delivery_client`:
    // never follow redirects (SSRF guard, #447/#615) and bound connect/total time.
    let client = reqwest::Client::builder()
        .resolve(
            host,
            std::net::SocketAddr::new(
                validated_ip,
                parsed_url.port().unwrap_or(if parsed_url.scheme() == "https" { 443 } else { 80 }),
            ),
        )
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(config.connect_timeout())
        .timeout(config.total_timeout())
        .build()
        .context("failed to build pinned client")?;

    let req = client
        .post(&d.url)
        .timeout(config.total_timeout())
        .header("Content-Type", "application/json")
        .header("X-Lumenqraph-Signature", signature_header)
        .header("X-Lumenqraph-Delivery-Id", d.id.to_string())
        .header("X-Lumenqraph-Timestamp", timestamp.to_string())
        .header("X-Lumenqraph-Attempt", d.attempts.to_string())
        .header("User-Agent", USER_AGENT)
        .body(body)
        .build()
        .context("failed to build request")?;

    let resp = client.execute(req).await.context("request failed")?;
    let status = resp.status();
    let location = resp
        .headers()
        .get(reqwest::header::LOCATION)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    // Redirects are never followed (SSRF guard); a 3xx is a failed delivery.
    if status.is_redirection() {
        delivery_outcome(status, location.as_deref())?;
    }
    let retry_after = resp
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .map(|secs| Duration::seconds(secs as i64));
    // Read at most `response_body_cap` bytes, streaming chunk-by-chunk so a
    // subscriber cannot force us to buffer an unbounded body (#616). Dropping
    // the response afterwards aborts the connection.
    let snippet = read_body_snippet(resp, config.response_body_cap()).await?;

    Ok(DeliveryOutcome {
        status: Some(status.as_u16() as i32),
        retry_after,
        body_snippet: snippet,
    })
}

/// Read at most `cap` bytes of a response body, streaming chunk-by-chunk so the
/// full body is never buffered in memory (#616). The response is dropped when
/// this returns, aborting the connection.
async fn read_body_snippet(mut resp: reqwest::Response, cap: usize) -> anyhow::Result<String> {
    let mut buf: Vec<u8> = Vec::with_capacity(cap.min(512));
    while buf.len() < cap {
        let chunk = resp
            .chunk()
            .await
            .context("failed to read webhook response body")?;
        match chunk {
            Some(bytes) => {
                let remaining = cap - buf.len();
                buf.extend_from_slice(&bytes[..bytes.len().min(remaining)]);
            }
            None => break,
        }
    }
    Ok(String::from_utf8_lossy(&buf).to_string())
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
        Err(anyhow::anyhow!("non-2xx status {status}"))
    }
}

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

async fn mark_delivered(
    pool: &PgPool,
    id: i64,
    status_code: i32,
    response_snippet: &str,
) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE webhook_deliveries
         SET status = 'delivered', attempts = attempts + 1, delivered_at = now(), last_error = NULL,
             locked_until = NULL, locked_by = NULL, last_status_code = $2, last_response_snippet = $3
         WHERE id = $1",
    )
    .bind(id)
    .bind(status_code)
    .bind(response_snippet)
    .execute(pool)
    .await?;
    Ok(())
}

/// Upper bound on the exponential backoff window, in seconds.
const BACKOFF_CAP_SECS: i64 = 3600;
/// Lower bound on a jittered retry delay, in seconds.
const BACKOFF_FLOOR_SECS: i64 = 1;

/// Compute the jittered retry delay for `attempts` failed attempts (#617).
///
/// Full jitter: `delay = rand(0, min(cap, base * 2^attempt))`, floored at
/// [`BACKOFF_FLOOR_SECS`]. Spreading retries across the window avoids the
/// thundering herd that lock-step exponential backoff produces during a
/// subscriber outage.
fn jittered_backoff<R: Rng + ?Sized>(attempts: i32, rng: &mut R) -> Duration {
    let window = (2i64)
        .saturating_pow(attempts.max(1) as u32)
        .min(BACKOFF_CAP_SECS)
        .max(BACKOFF_FLOOR_SECS);
    let jittered = rng.gen_range(0..=window);
    Duration::seconds(jittered.max(BACKOFF_FLOOR_SECS))
}

async fn mark_retry(
    pool: &PgPool,
    d: &DueDelivery,
    err: &str,
    max_attempts: i32,
    outcome: &DeliveryOutcome,
) -> anyhow::Result<()> {
    mark_retry_with_rng(pool, d, err, max_attempts, outcome, &mut rand::thread_rng()).await
}

/// [`mark_retry`] with an injectable RNG so backoff jitter is deterministic in
/// tests (#617).
async fn mark_retry_with_rng<R: Rng + ?Sized>(
    pool: &PgPool,
    d: &DueDelivery,
    err: &str,
    max_attempts: i32,
    outcome: &DeliveryOutcome,
    rng: &mut R,
) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;
    let attempts = d.attempts + 1;
    let final_retry = attempts >= max_attempts;
    let next_attempt_at = if let Some(retry_after) = outcome.retry_after {
        let capped = retry_after.min(Duration::hours(24));
        Utc::now() + capped
    } else {
        Utc::now() + jittered_backoff(attempts, rng)
    };

    if final_retry {
        sqlx::query(
            "UPDATE webhook_deliveries
             SET status = 'failed', attempts = $2, last_error = $3, locked_until = NULL, locked_by = NULL,
                 last_status_code = $4, last_response_snippet = $5
             WHERE id = $1",
        )
        .bind(d.id)
        .bind(attempts)
        .bind(err)
        .bind(outcome.status)
        .bind(&outcome.body_snippet)
        .execute(&mut *tx)
        .await?;
    } else {
        sqlx::query(
            "UPDATE webhook_deliveries
             SET attempts = $2, last_error = $3, next_attempt_at = $4, locked_until = NULL, locked_by = NULL,
                 last_status_code = $5, last_response_snippet = $6
             WHERE id = $1",
        )
        .bind(d.id)
        .bind(attempts)
        .bind(err)
        .bind(next_attempt_at)
        .bind(outcome.status)
        .bind(&outcome.body_snippet)
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
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
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
    pub(crate) async fn fixture() -> PgPool {
        let url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL");
        // Each test gets a unique schema so concurrent tests never interfere.
        let schema = format!("test_{}", uuid::Uuid::new_v4().simple());

        // Create the schema in the default database.
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect to TEST_DATABASE_URL");
        // pgcrypto is database-wide: install it once in `public` so every test
        // schema resolves `pgp_sym_*` through its search_path (#566).
        sqlx::query("CREATE EXTENSION IF NOT EXISTS pgcrypto SCHEMA public")
            .execute(&admin)
            .await
            .expect("create pgcrypto");
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
            "INSERT INTO webhook_subscriptions (url, kind, contract_id, secret, encrypted_secret)
             VALUES ('https://example.test/hook', $1, $2, 'test-secret',
                     pgp_sym_encrypt('test-secret', $3))",
        )
        .bind(kind)
        .bind(contract_id)
        .bind(encryption_key)
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

    /// #566 — a plaintext secret from before encryption is encrypted by the
    /// backfill, and deliveries are still signed with the original secret.
    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn legacy_plaintext_secret_is_encrypted_and_still_signs_deliveries() {
        let pool = fixture().await;
        sqlx::query(
            "INSERT INTO webhook_subscriptions (url, kind, secret)
             VALUES ('https://example.test/hook', 'upgrade', 'legacy-plain')",
        )
        .execute(&pool)
        .await
        .expect("insert legacy subscription");
        add_version(&pool, 1, false).await;
        add_version(&pool, 2, true).await;

        assert_eq!(backfill_encrypted_secrets(&pool, "test-key").await.unwrap(), 1);
        assert_eq!(
            backfill_encrypted_secrets(&pool, "test-key").await.unwrap(),
            0,
            "a second run finds nothing left to encrypt"
        );

        let unencrypted: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM webhook_subscriptions
             WHERE encrypted_secret IS NULL AND secret IS NOT NULL",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(unencrypted, 0);

        assert_eq!(enqueue(&pool, 100).await.unwrap(), 1);
        let due = fetch_due(&pool, 100, "test-key").await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].secret.as_deref(), Some("legacy-plain"));
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

    /// #602 — a pending upgrade delivery whose spec version was pruned must be
    /// failed permanently: no retry scheduled, and the subscriber's failure
    /// counters untouched (so it is never auto-disabled for it).
    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn pruned_spec_version_fails_permanently_without_penalising_subscriber() {
        let pool = fixture().await;
        subscribe(&pool, "upgrade", None, "test-key").await;
        add_version(&pool, 1, false).await;
        add_version(&pool, 2, true).await;
        assert_eq!(enqueue(&pool, 100).await.unwrap(), 1);

        // Simulate retention pruning the referenced spec version. The FK is
        // ON DELETE SET NULL, so the delivery's upgrade_id becomes NULL.
        sqlx::query("DELETE FROM contract_spec_versions WHERE id = 2")
            .execute(&pool)
            .await
            .expect("prune spec version");

        let due = fetch_due(&pool, 100, "test-key").await.unwrap();
        assert_eq!(due.len(), 1);
        assert!(due[0].upgrade_id.is_none(), "upgrade_id should be NULL after prune");

        // send() must classify this as a permanent SourcePruned error.
        let http = build_delivery_client(
            std::time::Duration::from_secs(2),
            std::time::Duration::from_secs(5),
        )
        .expect("client");
        let config = Config::for_test();
        let err = send(&pool, &http, &due[0], &config).await.unwrap_err();
        assert!(
            err.downcast_ref::<SourcePruned>().is_some(),
            "expected SourcePruned, got: {err}"
        );

        // Mirror the deliver() branch for a permanent failure.
        mark_failed_permanent(&pool, due[0].id, &err.to_string())
            .await
            .unwrap();

        let (status, attempts, last_error): (String, i32, Option<String>) = sqlx::query_as(
            "SELECT status, attempts, last_error FROM webhook_deliveries WHERE id = $1",
        )
        .bind(due[0].id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(status, "failed");
        assert_eq!(attempts, 0, "attempts must not be incremented");
        assert_eq!(last_error.as_deref(), Some("source spec version was pruned"));

        // No retry scheduled: nothing is due again.
        assert!(fetch_due(&pool, 100, "test-key").await.unwrap().is_empty());

        // Subscriber failure counters untouched.
        let (consecutive_failures, active): (i32, bool) = sqlx::query_as(
            "SELECT consecutive_failures, active FROM webhook_subscriptions LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(consecutive_failures, 0);
        assert!(active, "subscriber must not be auto-disabled");
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

        // Deterministic RNG so the variance assertion is stable (#617).
        let mut rng = rand::rngs::StdRng::seed_from_u64(0x5EED);
        for d in due {
            mark_retry_with_rng(&pool, &d, "test error", 5, &DeliveryOutcome::default(), &mut rng)
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

    #[test]
    fn jittered_backoff_spreads_retries_across_the_window() {
        use rand::SeedableRng;
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let delays: Vec<i64> = (0..32)
            .map(|_| jittered_backoff(3, &mut rng).num_seconds())
            .collect();
        // Window for attempt 3 is 8s; delays must be within [1, 8].
        assert!(delays.iter().all(|&d| (1..=8).contains(&d)), "{delays:?}");
        let unique: std::collections::HashSet<_> = delays.iter().collect();
        assert!(unique.len() > 1, "expected spread, got {delays:?}");
    }

    #[test]
    fn jittered_backoff_respects_floor_and_cap() {
        use rand::SeedableRng;
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);
        // Huge attempt count: window is capped, delay never exceeds the cap.
        for _ in 0..64 {
            let d = jittered_backoff(40, &mut rng).num_seconds();
            assert!((BACKOFF_FLOOR_SECS..=BACKOFF_CAP_SECS).contains(&d), "{d}");
        }
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

    /// Build a `DueDelivery` pointing at `url` with a fixed secret, for driving
    /// `send()` without a database.
    fn due_for(url: &str) -> DueDelivery {
        DueDelivery {
            id: 1,
            subscription_id: uuid::Uuid::nil(),
            attempts: 0,
            url: url.to_string(),
            secret: Some("test-secret".to_string()),
            previous_secret: None,
            event_id: None,
            upgrade_id: None,
            created_at: Utc::now(),
        }
    }

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

        // Drive the real delivery path. `send()` needs a pool only to build the
        // envelope, which fails before any query for a delivery with neither
        // event nor upgrade id — so we exercise the HTTP path via a lazy pool.
        let pool = sqlx::PgPool::connect_lazy("postgres://unused/unused").unwrap();
        let http = build_delivery_client(StdDuration::from_secs(2), StdDuration::from_secs(5))
            .expect("client");
        let config = Config::default();
        let d = due_for(&format!("http://{hook_addr}/hook"));
        let err = send(&pool, &http, &d, &config).await.unwrap_err();
        assert!(
            err.to_string().contains("redirects are not followed"),
            "unexpected error: {err}"
        );

        tokio::time::sleep(StdDuration::from_millis(100)).await;
        assert_eq!(hits.load(Ordering::SeqCst), 0, "redirect target was contacted");
    }

    #[tokio::test]
    async fn oversized_response_body_is_read_bounded() {
        // Server that streams ~100 MB of body without ever finishing.
        let hook = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hook_addr = hook.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = hook.accept().await {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let _ = sock
                    .write_all(
                        b"HTTP/1.1 200 OK\r\ncontent-length: 104857600\r\nconnection: close\r\n\r\n",
                    )
                    .await;
                let chunk = vec![b'A'; 64 * 1024];
                for _ in 0..1600 {
                    if sock.write_all(&chunk).await.is_err() {
                        break;
                    }
                }
            }
        });

        let http = build_delivery_client(StdDuration::from_secs(2), StdDuration::from_secs(5))
            .expect("client");
        let resp = http
            .get(format!("http://{hook_addr}/hook"))
            .send()
            .await
            .expect("request to hook");

        let started = std::time::Instant::now();
        let snippet = read_body_snippet(resp, 512).await.expect("read body");
        assert!(snippet.len() <= 512, "snippet not bounded: {}", snippet.len());
        assert!(
            started.elapsed() < StdDuration::from_secs(2),
            "reading bounded body took too long: {:?}",
            started.elapsed()
        );
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

#[cfg(test)]
mod rotation_signature_tests {
    use super::sign_payload;

    #[test]
    fn previous_secret_produces_a_distinct_signature() {
        let payload = "1700000000.{\"ok\":true}";
        let current = sign_payload("new-secret", payload).unwrap();
        let previous = sign_payload("old-secret", payload).unwrap();
        assert_ne!(current, previous);
        assert_eq!(current, sign_payload("new-secret", payload).unwrap());
    }
}
