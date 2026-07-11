//! Two-stage webhook pipeline, each stage a simple SQL-driven step:
//!
//!  1. **Enqueue** — match newly-indexed events (streamed by monotonic `seq`)
//!     against active subscriptions and insert `pending` delivery rows.
//!  2. **Deliver** — POST due deliveries to their URL with an HMAC-SHA256
//!     signature, retrying failures with exponential backoff.

use anyhow::Context;
use chrono::{DateTime, Duration, Utc};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use sqlx::types::Json;
use sqlx::PgPool;
use tracing::{debug, info, warn};

use crate::config::Config;

type HmacSha256 = Hmac<Sha256>;

/// Match new events to subscriptions and enqueue deliveries. Returns how many
/// delivery rows were created.
pub async fn enqueue(pool: &PgPool, batch: i64) -> anyhow::Result<u64> {
    let last_seq: i64 = sqlx::query_scalar(
        "INSERT INTO webhook_state (id, last_seq) VALUES (1, 0)
         ON CONFLICT (id) DO UPDATE SET last_seq = webhook_state.last_seq
         RETURNING last_seq",
    )
    .fetch_one(pool)
    .await?;

    let upper: i64 = sqlx::query_scalar(
        "SELECT COALESCE(max(seq), 0) FROM events WHERE seq > $1 AND seq <= $1 + $2",
    )
    .bind(last_seq)
    .bind(batch)
    .fetch_one(pool)
    .await?;

    if upper <= last_seq {
        return Ok(0);
    }

    let created = sqlx::query(
        "INSERT INTO webhook_deliveries (subscription_id, event_id)
         SELECT s.id, e.event_id
         FROM events e
         JOIN webhook_subscriptions s
           ON s.active
          AND (s.contract_id IS NULL OR s.contract_id = e.contract_id)
          AND (s.event_name  IS NULL OR s.event_name  = e.event_name)
         WHERE e.seq > $1 AND e.seq <= $2
         ON CONFLICT (subscription_id, event_id) DO NOTHING",
    )
    .bind(last_seq)
    .bind(upper)
    .execute(pool)
    .await?
    .rows_affected();

    sqlx::query("UPDATE webhook_state SET last_seq = $1 WHERE id = 1")
        .bind(upper)
        .execute(pool)
        .await?;

    if created > 0 {
        debug!(created, up_to_seq = upper, "enqueued webhook deliveries");
    }
    Ok(created)
}

struct DueDelivery {
    id: i64,
    attempts: i32,
    url: String,
    secret: String,
    payload: Json<serde_json::Value>,
}

/// Deliver all due rows once. Returns (delivered, failed) counts.
pub async fn deliver(
    pool: &PgPool,
    http: &reqwest::Client,
    config: &Config,
) -> anyhow::Result<(u64, u64)> {
    let rows: Vec<(i64, i32, String, String, Json<serde_json::Value>)> = sqlx::query_as(
        "SELECT d.id, d.attempts, s.url, s.secret, to_jsonb(e) - 'seq' AS payload
         FROM webhook_deliveries d
         JOIN webhook_subscriptions s ON s.id = d.subscription_id
         JOIN events e ON e.event_id = d.event_id
         WHERE d.status = 'pending' AND d.next_attempt_at <= now()
         ORDER BY d.next_attempt_at
         LIMIT $1",
    )
    .bind(config.batch_size)
    .fetch_all(pool)
    .await?;

    let mut delivered = 0u64;
    let mut failed = 0u64;
    for r in rows {
        let d = DueDelivery {
            id: r.0,
            attempts: r.1,
            url: r.2,
            secret: r.3,
            payload: r.4,
        };
        match send(http, &d).await {
            Ok(()) => {
                mark_delivered(pool, d.id).await?;
                delivered += 1;
            }
            Err(e) => {
                mark_retry(pool, &d, &e.to_string(), config.max_attempts).await?;
                failed += 1;
                warn!(delivery = d.id, url = %d.url, error = %e, "webhook delivery failed");
            }
        }
    }
    if delivered > 0 {
        info!(delivered, failed, "webhook deliveries processed");
    }
    Ok((delivered, failed))
}

async fn send(http: &reqwest::Client, d: &DueDelivery) -> anyhow::Result<()> {
    let body = serde_json::to_vec(&d.payload.0)?;

    let mut mac =
        HmacSha256::new_from_slice(d.secret.as_bytes()).context("invalid webhook secret")?;
    mac.update(&body);
    let signature = hex::encode(mac.finalize().into_bytes());

    let resp = http
        .post(&d.url)
        .header("Content-Type", "application/json")
        .header("X-Lumenqraph-Signature", format!("sha256={signature}"))
        .header("User-Agent", "lumenqraph-webhooks/0.1")
        .body(body)
        .send()
        .await
        .context("request failed")?;

    if resp.status().is_success() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("non-2xx status {}", resp.status()))
    }
}

async fn mark_delivered(pool: &PgPool, id: i64) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE webhook_deliveries
         SET status='delivered', attempts=attempts+1, delivered_at=now(), last_error=NULL
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
    let attempts = d.attempts + 1;
    if attempts >= max_attempts {
        sqlx::query(
            "UPDATE webhook_deliveries
             SET status='failed', attempts=$2, last_error=$3
             WHERE id=$1",
        )
        .bind(d.id)
        .bind(attempts)
        .bind(err)
        .execute(pool)
        .await?;
    } else {
        // Exponential backoff: 2^attempts seconds, capped at 1 hour.
        let secs = 2i64.saturating_pow(attempts as u32).min(3600);
        let next: DateTime<Utc> = Utc::now() + Duration::seconds(secs);
        sqlx::query(
            "UPDATE webhook_deliveries
             SET attempts=$2, last_error=$3, next_attempt_at=$4
             WHERE id=$1",
        )
        .bind(d.id)
        .bind(attempts)
        .bind(err)
        .bind(next)
        .execute(pool)
        .await?;
    }
    Ok(())
}
