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
use sha2::Sha256;
use sqlx::types::Json;
use sqlx::PgPool;
use tracing::{debug, info, warn};

use crate::config::Config;

type HmacSha256 = Hmac<Sha256>;

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

    // Advance the watermark by at most `batch` toward the global max seq.
    // Using the *global* max (not the max within the window) is essential:
    // `seq` is a BIGSERIAL that also increments on `ON CONFLICT DO NOTHING`, so
    // re-fetching the tip each cycle burns seq values and leaves gaps. If a gap
    // ever exceeds `batch`, a window-local max would be NULL and this watermark
    // would stall forever. Stepping toward the global max always makes progress
    // (empty gaps simply enqueue nothing), so the pipeline can never wedge.
    let global_max: i64 = sqlx::query_scalar("SELECT COALESCE(max(seq), 0) FROM events")
        .fetch_one(pool)
        .await?;

    if global_max <= last_seq {
        return Ok(0);
    }
    let upper = (last_seq + batch).min(global_max);

    let created = sqlx::query(
        "INSERT INTO webhook_deliveries (subscription_id, event_id)
         SELECT s.id, e.event_id
         FROM events e
         JOIN webhook_subscriptions s
           ON s.active
          AND s.kind = 'event'
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
    .execute(pool)
    .await?
    .rows_affected();

    sqlx::query("UPDATE webhook_state SET last_upgrade_id = $1 WHERE id = 1")
        .bind(upper)
        .execute(pool)
        .await?;

    if created > 0 {
        info!(created, up_to_id = upper, "enqueued upgrade webhooks");
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

/// Read due deliveries and build each one's payload.
///
/// A delivery points at an event or a spec version, never both, so exactly one
/// of the two LEFT JOINs matches and the CASE picks that payload. Event payloads
/// keep their long-standing shape (the bare event row); upgrade payloads are
/// tagged, since they're a new shape and a consumer receiving one should be able
/// to tell what it is.
async fn fetch_due(pool: &PgPool, batch: i64) -> anyhow::Result<Vec<DueDelivery>> {
    let rows: Vec<(i64, i32, String, String, Json<serde_json::Value>)> = sqlx::query_as(
        "SELECT d.id, d.attempts, s.url, s.secret,
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
         WHERE d.status = 'pending' AND d.next_attempt_at <= now()
         ORDER BY d.next_attempt_at
         LIMIT $1",
    )
    .bind(batch)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| DueDelivery {
            id: r.0,
            attempts: r.1,
            url: r.2,
            secret: r.3,
            payload: r.4,
        })
        .collect())
}

/// Deliver all due rows once. Returns (delivered, failed) counts.
pub async fn deliver(
    pool: &PgPool,
    http: &reqwest::Client,
    config: &Config,
) -> anyhow::Result<(u64, u64)> {
    let mut delivered = 0u64;
    let mut failed = 0u64;
    for d in fetch_due(pool, config.batch_size).await? {
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

#[cfg(test)]
mod tests {
    //! Enqueue + payload tests. These need a throwaway Postgres:
    //!
    //!   TEST_DATABASE_URL=postgres://…/lumenqraph \
    //!     cargo test -p lumenqraph-webhooks -- --ignored --test-threads=1

    use super::*;
    use sqlx::postgres::PgPoolOptions;

    /// Fresh schema per test, so tests don't see each other's rows.
    async fn fixture() -> PgPool {
        let url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL");
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .expect("connect");
        for stmt in ["DROP SCHEMA public CASCADE", "CREATE SCHEMA public"] {
            sqlx::query(stmt)
                .execute(&pool)
                .await
                .expect("reset schema");
        }
        sqlx::migrate!("../../migrations")
            .run(&pool)
            .await
            .expect("migrate");
        pool
    }

    /// A subscription of `kind`, optionally scoped to one contract.
    async fn subscribe(pool: &PgPool, kind: &str, contract_id: Option<&str>) {
        sqlx::query(
            "INSERT INTO webhook_subscriptions (url, kind, contract_id, secret)
             VALUES ('https://example.test/hook', $1, $2, 'shh')",
        )
        .bind(kind)
        .bind(contract_id)
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
        subscribe(&pool, "upgrade", None).await;
        add_version(&pool, 1, false).await;
        add_version(&pool, 2, true).await;

        assert_eq!(
            enqueue(&pool, 100).await.unwrap(),
            1,
            "only the upgrade (v2) enqueues; v1 is a baseline, not a change"
        );

        let due = fetch_due(&pool, 100).await.unwrap();
        assert_eq!(due.len(), 1);
        let payload = &due[0].payload.0;
        assert_eq!(payload["type"], "contract.upgraded");
        assert_eq!(payload["contract_id"], "C1");
        assert_eq!(payload["version"], 2);
        assert_eq!(payload["breaking"], true);
        assert_eq!(
            payload["diff"]["summary"][0],
            "removed function withdraw() -> void"
        );
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn the_two_streams_do_not_cross() {
        let pool = fixture().await;
        // An event subscriber that matches every contract must not be handed an
        // upgrade, and vice versa: the payload shapes are different.
        subscribe(&pool, "event", None).await;
        add_version(&pool, 1, false).await;
        add_version(&pool, 2, true).await;
        assert_eq!(enqueue(&pool, 100).await.unwrap(), 0);

        let pool = fixture().await;
        subscribe(&pool, "upgrade", None).await;
        add_event(&pool, "e1").await;
        assert_eq!(enqueue(&pool, 100).await.unwrap(), 0);
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn an_upgrade_subscription_is_scoped_to_its_contract() {
        let pool = fixture().await;
        subscribe(&pool, "upgrade", Some("C2")).await;
        add_version(&pool, 1, false).await;
        add_version(&pool, 2, true).await; // on C1
        assert_eq!(enqueue(&pool, 100).await.unwrap(), 0);

        subscribe(&pool, "upgrade", Some("C1")).await;
        // The watermark already passed C1's versions, so a new subscriber only
        // gets upgrades from here on — the same catch-up behaviour events have.
        add_version(&pool, 3, false).await;
        assert_eq!(enqueue(&pool, 100).await.unwrap(), 1);
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn re_enqueueing_does_not_duplicate_deliveries() {
        let pool = fixture().await;
        subscribe(&pool, "upgrade", None).await;
        add_version(&pool, 1, false).await;
        add_version(&pool, 2, true).await;

        assert_eq!(enqueue(&pool, 100).await.unwrap(), 1);
        // The watermark has advanced, so a second pass finds nothing new.
        assert_eq!(enqueue(&pool, 100).await.unwrap(), 0);
        assert_eq!(fetch_due(&pool, 100).await.unwrap().len(), 1);
    }
}
