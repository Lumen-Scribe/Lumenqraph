//! Retention: drop indexed history older than a rolling ledger window.
//!
//! Off by default — the indexer's normal posture is "keep everything". It exists
//! for deployments with a hard disk budget (free-tier Postgres is typically
//! ~500MB, while a hyperactive SAC emits ~500 events/ledger), where an unbounded
//! index would hit the cap in hours and stop accepting writes.
//!
//! Deletes run in bounded batches with a per-pass ceiling, so pruning a large
//! backlog costs many small transactions rather than one long lock — a shared-CPU
//! instance stays responsive to reads while it catches up.
//!
//! `token_transfers` cascades from `events` (FK `ON DELETE CASCADE`), so pruning
//! events prunes the projection with it.
//!
//! The versioned tables (`contract_state`, `contract_data`) are per-key time
//! series, so their *newest* row per key is current state, not history — it is
//! kept however old it is, and only superseded versions are pruned. Otherwise a
//! contract whose state last changed before the window would read as having no
//! state at all.

use chrono::{Duration, Utc};
use sqlx::PgPool;
use tracing::{info, warn};

/// Rows per DELETE. Small enough that each transaction is short.
const BATCH: i64 = 5_000;

/// Max batches per table per pass, bounding the work one pass can do (and so the
/// time before the poller returns to the tip). A backlog drains over many passes.
const MAX_BATCHES: usize = 10;

/// Delete indexed data older than `tip - retention_ledgers`.
///
/// Returns the total rows deleted. A no-op (and cheap — an index-only probe)
/// once the tables are inside the window, which is the steady state.
pub async fn prune(pool: &PgPool, tip: i64, retention_ledgers: i64) -> anyhow::Result<u64> {
    if retention_ledgers <= 0 {
        return Ok(0);
    }
    let cutoff = tip - retention_ledgers;
    if cutoff <= 0 {
        // Window is longer than the chain's history so far; nothing to drop.
        return Ok(0);
    }

    let events = prune_batched(
        pool,
        "DELETE FROM events WHERE event_id IN (
             SELECT event_id FROM events WHERE ledger < $1 ORDER BY ledger LIMIT $2
         )",
        cutoff,
    )
    .await?;

    // `ledger < MAX(ledger) for this key` is what spares the current version:
    // the newest row can never satisfy it.
    let state = prune_batched(
        pool,
        "DELETE FROM contract_state WHERE ctid IN (
             SELECT cs.ctid FROM contract_state cs
              WHERE cs.ledger < $1
                AND cs.ledger < (
                    SELECT MAX(x.ledger) FROM contract_state x
                     WHERE x.contract_id = cs.contract_id
                )
              LIMIT $2
         )",
        cutoff,
    )
    .await?;

    let data = prune_batched(
        pool,
        "DELETE FROM contract_data WHERE ctid IN (
             SELECT cd.ctid FROM contract_data cd
              WHERE cd.ledger < $1
                AND cd.ledger < (
                    SELECT MAX(x.ledger) FROM contract_data x
                     WHERE x.contract_id = cd.contract_id
                       AND x.key_hash = cd.key_hash
                )
              LIMIT $2
         )",
        cutoff,
    )
    .await?;

    let total = events + state + data;
    if total > 0 {
        info!(
            cutoff_ledger = cutoff,
            events, state, data, "pruned history outside the retention window"
        );
    }
    Ok(total)
}

/// Delete webhook deliveries (delivered/failed only) older than `retention_days`.
/// Pending deliveries are kept untouched. Returns the total rows deleted.
pub async fn prune_webhook_deliveries(pool: &PgPool, retention_days: i64) -> anyhow::Result<u64> {
    if retention_days <= 0 {
        return Ok(0);
    }

    let cutoff = Utc::now() - Duration::days(retention_days);

    let deleted = prune_batched_webhooks(
        pool,
        "DELETE FROM webhook_deliveries WHERE id IN (
             SELECT id FROM webhook_deliveries
              WHERE status IN ('delivered', 'failed')
                AND created_at < $1
              ORDER BY created_at LIMIT $2
         )",
        cutoff,
    )
    .await?;

    if deleted > 0 {
        info!(
            cutoff_date = %cutoff,
            retention_days, deleted, "pruned old webhook deliveries"
        );
    }
    Ok(deleted)
}

/// Run `sql` (bound: $1 = cutoff ledger, $2 = batch size) until it stops
/// deleting or hits the per-pass ceiling.
async fn prune_batched(pool: &PgPool, sql: &str, cutoff: i64) -> anyhow::Result<u64> {
    let mut deleted = 0u64;
    for _ in 0..MAX_BATCHES {
        let n = sqlx::query(sql)
            .bind(cutoff)
            .bind(BATCH)
            .execute(pool)
            .await?
            .rows_affected();
        deleted += n;
        if n < BATCH as u64 {
            return Ok(deleted);
        }
    }
    // Still more to delete than one pass allows. Expected when retention is first
    // switched on over a big index; a standing warning means the write rate is
    // outrunning the pruner and the window (or BATCH) needs revisiting.
    warn!(
        deleted,
        cutoff_ledger = cutoff,
        "retention pass hit its batch ceiling; more rows remain (will continue next pass)"
    );
    Ok(deleted)
}

/// Run `sql` (bound: $1 = cutoff datetime, $2 = batch size) for webhook deliveries until
/// it stops deleting or hits the per-pass ceiling.
async fn prune_batched_webhooks(pool: &PgPool, sql: &str, cutoff: chrono::DateTime<chrono::Utc>) -> anyhow::Result<u64> {
    let mut deleted = 0u64;
    for _ in 0..MAX_BATCHES {
        let n = sqlx::query(sql)
            .bind(cutoff)
            .bind(BATCH)
            .execute(pool)
            .await?
            .rows_affected();
        deleted += n;
        if n < BATCH as u64 {
            return Ok(deleted);
        }
    }
    warn!(
        deleted,
        cutoff_date = %cutoff,
        "webhook retention pass hit its batch ceiling; more rows remain (will continue next pass)"
    );
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    //! Needs a Postgres (schema is created by the migrations). Ignored by
    //! default; run with:
    //!   TEST_DATABASE_URL=postgres://…/lumenqraph \
    //!     cargo test -p lumenqraph-indexer -- --ignored --nocapture

    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::Row;

    /// Fresh, isolated schema per test — safe for parallel execution.
    async fn fixture() -> PgPool {
        let url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL");
        let schema = format!("test_{}", uuid::Uuid::new_v4().simple());

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

        let option = format!("-c search_path={schema},public");
        let sep = if url.contains('?') { "&" } else { "?" };
        let schema_url = format!("{url}{sep}options={}", percent_encode(&option));
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&schema_url)
            .await
            .expect("connect with search_path");
        // Separate statements: sqlx prepares, and a prepared statement can only
        // carry one command.
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

    async fn insert_event(pool: &PgPool, id: &str, ledger: i64) {
        sqlx::query(
            "INSERT INTO events (event_id, contract_id, ledger, ledger_closed_at, event_type,
                                 topics, event_name, value, tx_hash, in_successful_call, paging_token)
             VALUES ($1,'C1',$2,now(),'contract','[]','transfer','v','tx',true,$1)",
        )
        .bind(id)
        .bind(ledger)
        .execute(pool)
        .await
        .expect("insert event");
        // The projection that must disappear with its parent event.
        sqlx::query(
            "INSERT INTO token_transfers (event_id, contract_id, from_addr, to_addr, amount, ledger, ledger_closed_at)
             VALUES ($1,'C1','GA','GB','1',$2,now())",
        )
        .bind(id)
        .bind(ledger)
        .execute(pool)
        .await
        .expect("insert transfer");
    }

    async fn count(pool: &PgPool, sql: &str) -> i64 {
        sqlx::query(sql).fetch_one(pool).await.unwrap().get(0)
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn drops_events_older_than_the_window_and_cascades_transfers() {
        let pool = fixture().await;
        insert_event(&pool, "old", 100).await;
        insert_event(&pool, "new", 900).await;

        // tip 1000, keep 500 => cutoff 500.
        let deleted = prune(&pool, 1000, 500).await.expect("prune");

        assert_eq!(deleted, 1, "only the pre-cutoff event should go");
        assert_eq!(count(&pool, "SELECT count(*) FROM events").await, 1);
        assert_eq!(
            count(&pool, "SELECT count(*) FROM events WHERE event_id='new'").await,
            1,
            "in-window event must survive"
        );
        assert_eq!(
            count(&pool, "SELECT count(*) FROM token_transfers").await,
            1,
            "the pruned event's transfer should cascade away with it"
        );
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn keeps_the_newest_state_version_even_when_older_than_the_window() {
        let pool = fixture().await;
        // C1 changed twice, both before the cutoff: the newer row is still C1's
        // *current* state, so pruning it would blank the contract entirely.
        for ledger in [10i64, 20] {
            sqlx::query(
                "INSERT INTO contract_state (contract_id, ledger, storage) VALUES ('C1',$1,'{}')",
            )
            .bind(ledger)
            .execute(&pool)
            .await
            .unwrap();
        }
        // Two versions of one key, plus a different key on the same contract.
        for (key_hash, ledger) in [("k1", 10i64), ("k1", 20), ("k2", 10)] {
            sqlx::query(
                "INSERT INTO contract_data (contract_id, key_hash, key, key_xdr, durability, ledger, value)
                 VALUES ('C1',$1,'[]','xdr','persistent',$2,'{}')",
            )
            .bind(key_hash)
            .bind(ledger)
            .execute(&pool)
            .await
            .unwrap();
        }

        prune(&pool, 1000, 500).await.expect("prune");

        assert_eq!(
            count(&pool, "SELECT count(*) FROM contract_state").await,
            1,
            "superseded state versions go, the latest stays"
        );
        assert_eq!(
            count(&pool, "SELECT ledger FROM contract_state").await,
            20,
            "the surviving state row must be the newest"
        );
        // k1 keeps only its ledger-20 row; k2's sole row is current, so it stays.
        assert_eq!(count(&pool, "SELECT count(*) FROM contract_data").await, 2);
        assert_eq!(
            count(
                &pool,
                "SELECT count(*) FROM contract_data WHERE key_hash='k1' AND ledger=20"
            )
            .await,
            1
        );
        assert_eq!(
            count(
                &pool,
                "SELECT count(*) FROM contract_data WHERE key_hash='k2'"
            )
            .await,
            1,
            "a key with only one version must never be pruned to nothing"
        );
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn boundary_ledger_is_kept() {
        let pool = fixture().await;
        // cutoff = tip - retention_ledgers = 1000 - 500 = 500.
        // The DELETE condition is `ledger < 500` (strict less-than), so:
        //   ledger 499 → deleted (strictly below cutoff)
        //   ledger 500 → kept   (equal to cutoff — not strictly below)
        //   ledger 501 → kept   (above cutoff)
        insert_event(&pool, "below", 499).await;
        insert_event(&pool, "at_floor", 500).await;
        insert_event(&pool, "above", 501).await;

        let deleted = prune(&pool, 1000, 500).await.expect("prune");
        assert_eq!(deleted, 1, "only the row strictly below the cutoff is pruned");

        assert_eq!(
            count(&pool, "SELECT count(*) FROM events WHERE event_id = 'at_floor'").await,
            1,
            "row at ledger == cutoff must survive (condition is `ledger < cutoff`, not `<=`)"
        );
        assert_eq!(
            count(&pool, "SELECT count(*) FROM events WHERE event_id = 'above'").await,
            1,
            "row above the cutoff must survive"
        );
        assert_eq!(
            count(&pool, "SELECT count(*) FROM events WHERE event_id = 'below'").await,
            0,
            "row strictly below the cutoff must be removed"
        );
        // token_transfers cascade: the transfer for 'below' is gone, the two others remain.
        assert_eq!(
            count(&pool, "SELECT count(*) FROM token_transfers").await,
            2,
            "only the pruned event's transfer cascades away; in-window transfers survive"
        );
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn disabled_and_young_chain_are_no_ops() {
        let pool = fixture().await;
        insert_event(&pool, "old", 1).await;

        assert_eq!(
            prune(&pool, 1000, 0).await.unwrap(),
            0,
            "0 = keep everything"
        );
        // Window reaches back past ledger 0 — nothing is outside it yet.
        assert_eq!(prune(&pool, 100, 500).await.unwrap(), 0);
        assert_eq!(count(&pool, "SELECT count(*) FROM events").await, 1);
    }
}
