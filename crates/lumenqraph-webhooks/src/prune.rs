//! Retention for `webhook_deliveries` (#567, #568).
//!
//! The webhooks service is the only writer of `webhook_deliveries`, so it owns
//! pruning. The loop runs on an interval, and a session-level advisory lock
//! keeps replicas from pruning at the same time.

use chrono::{Duration, Utc};
use sqlx::PgPool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration as StdDuration;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Default retention window in days. `0` disables pruning.
pub const DEFAULT_RETENTION_DAYS: i64 = 14;

/// How often the pruner runs. Slow on purpose: pruning is housekeeping.
const PRUNE_INTERVAL: StdDuration = StdDuration::from_secs(10 * 60);

/// Advisory-lock key so only one replica prunes at a time (#567).
const PRUNE_LOCK: i64 = 567_002;

/// Rows removed per DELETE, to keep each statement short.
const BATCH: i64 = 1000;

/// Rows removed by the pruner since this process started. Exported as
/// `lumenqraph_webhook_deliveries_pruned_total`.
pub static DELIVERIES_PRUNED: AtomicU64 = AtomicU64::new(0);

/// Delete delivered/failed `webhook_deliveries` rows older than `retention_days`
/// in batches. `pending` rows are never touched. Returns the number removed.
pub async fn prune_webhook_deliveries(pool: &PgPool, retention_days: i64) -> anyhow::Result<u64> {
    if retention_days <= 0 {
        return Ok(0);
    }
    let cutoff = Utc::now() - Duration::days(retention_days);
    let mut total: u64 = 0;
    loop {
        let deleted = sqlx::query(
            "DELETE FROM webhook_deliveries
             WHERE id IN (
                 SELECT id FROM webhook_deliveries
                 WHERE status IN ('delivered', 'failed')
                   AND created_at < $1
                 ORDER BY created_at
                 LIMIT $2
             )",
        )
        .bind(cutoff)
        .bind(BATCH)
        .execute(pool)
        .await?
        .rows_affected();
        total += deleted;
        if (deleted as i64) < BATCH {
            break;
        }
    }
    DELIVERIES_PRUNED.fetch_add(total, Ordering::Relaxed);
    Ok(total)
}

/// One pruning pass, skipped when another replica holds the lock.
async fn prune_once(pool: &PgPool, retention_days: i64) -> anyhow::Result<u64> {
    let mut lock_conn = pool.acquire().await?;
    let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(PRUNE_LOCK)
        .fetch_one(&mut *lock_conn)
        .await?;
    if !locked {
        return Ok(0);
    }
    let result = prune_webhook_deliveries(pool, retention_days).await;
    if let Err(e) = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(PRUNE_LOCK)
        .execute(&mut *lock_conn)
        .await
    {
        warn!(error = %e, "failed to release webhook prune lock");
    }
    result
}

/// Run the pruner until shutdown. Does nothing when retention is `0`.
pub async fn run_pruner(pool: PgPool, retention_days: i64, shutdown: CancellationToken) {
    if retention_days <= 0 {
        info!("webhook delivery pruning disabled (WEBHOOK_DELIVERY_RETENTION_DAYS=0)");
        return;
    }
    let mut ticker = tokio::time::interval(PRUNE_INTERVAL);
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown.cancelled() => return,
        }
        match prune_once(&pool, retention_days).await {
            Ok(0) => {}
            Ok(n) => info!(rows = n, "pruned old webhook deliveries"),
            Err(e) => warn!(error = %e, "failed to prune webhook deliveries"),
        }
    }
}

#[cfg(test)]
mod tests {
    //! Needs a throwaway Postgres:
    //!
    //!   TEST_DATABASE_URL=postgres://…/lumenqraph \
    //!     cargo test -p lumenqraph-webhooks -- --ignored
    use super::*;
    use crate::dispatcher::tests::fixture;
    use chrono::Duration as ChronoDuration;

    /// Insert a subscription plus one delivery row per `(status, age_days)`.
    async fn seed(pool: &PgPool, rows: &[(&str, i64)]) {
        let sub: uuid::Uuid = sqlx::query_scalar(
            "INSERT INTO webhook_subscriptions (url, kind, secret)
             VALUES ('https://example.test/hook', 'event', 'plain')
             RETURNING id",
        )
        .fetch_one(pool)
        .await
        .expect("insert subscription");
        for (i, (status, age_days)) in rows.iter().enumerate() {
            let event_id = format!("evt-{i}");
            sqlx::query(
                "INSERT INTO events (event_id, contract_id, ledger, ledger_closed_at, event_type,
                                     topics, event_name, value, tx_hash, in_successful_call, paging_token)
                 VALUES ($1,'C1',1,now(),'contract','[]','transfer','v','tx',true,$1)",
            )
                .bind(&event_id)
                .execute(pool)
                .await
                .expect("insert event");
            sqlx::query(
                "INSERT INTO webhook_deliveries (subscription_id, event_id, status, created_at)
                 VALUES ($1, $2, $3, $4)",
            )
            .bind(sub)
            .bind(&event_id)
            .bind(status)
            .bind(Utc::now() - ChronoDuration::days(*age_days))
            .execute(pool)
            .await
            .expect("insert delivery");
        }
    }

    async fn remaining(pool: &PgPool) -> Vec<String> {
        sqlx::query_scalar("SELECT status || ':' || created_at::date FROM webhook_deliveries ORDER BY id")
            .fetch_all(pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn old_delivered_and_failed_rows_are_pruned_and_pending_rows_are_kept() {
        let pool = fixture().await;
        seed(&pool, &[("delivered", 30), ("failed", 30), ("pending", 30), ("delivered", 1)]).await;

        let removed = prune_webhook_deliveries(&pool, 14).await.unwrap();
        assert_eq!(removed, 2);
        assert_eq!(remaining(&pool).await.len(), 2);
        assert!(remaining(&pool).await.iter().any(|r| r.starts_with("pending:")));
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn zero_retention_prunes_nothing() {
        let pool = fixture().await;
        seed(&pool, &[("delivered", 365)]).await;
        assert_eq!(prune_webhook_deliveries(&pool, 0).await.unwrap(), 0);
        assert_eq!(remaining(&pool).await.len(), 1);
    }
}
