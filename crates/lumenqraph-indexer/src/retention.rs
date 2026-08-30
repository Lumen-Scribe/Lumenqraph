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

    let specs = prune_batched(
        pool,
        "DELETE FROM contract_spec_versions WHERE ctid IN (
             SELECT csv.ctid FROM contract_spec_versions csv
              WHERE csv.ledger < $1
                AND csv.version < (
                    SELECT MAX(x.version) FROM contract_spec_versions x
                     WHERE x.contract_id = csv.contract_id
                )
              LIMIT $2
         )",
        cutoff,
    )
    .await?;

    let total = events + state + data + specs;
    if total > 0 {
        info!(
            cutoff_ledger = cutoff,
            events, state, data, specs, "pruned history outside the retention window"
        );
    }
    Ok(total)
}

/// Delete old contract spec versions, respecting both the retention window and the
/// minimum number of versions to keep per contract.
///
/// Deletes versions that are:
/// 1. Outside the retention window (ledger < cutoff), AND
/// 2. NOT among the newest N versions per contract (where N = spec_version_retention)
///
/// If spec_version_retention is 0, uses only the retention window (like other tables).
pub async fn prune_spec_versions(
    pool: &PgPool,
    tip: i64,
    retention_ledgers: i64,
    spec_version_retention: i64,
) -> anyhow::Result<u64> {
    if retention_ledgers <= 0 {
        return Ok(0);
    }
    let cutoff = tip - retention_ledgers;
    if cutoff <= 0 {
        // Window is longer than the chain's history so far; nothing to drop.
        return Ok(0);
    }

    if spec_version_retention <= 0 {
        // No minimum version requirement; use only the retention window.
        // Keep the current version (newest version per contract).
        return prune_batched(
            pool,
            "DELETE FROM contract_spec_versions WHERE ctid IN (
                 SELECT csv.ctid FROM contract_spec_versions csv
                  WHERE csv.ledger < $1
                    AND csv.version < (
                        SELECT MAX(x.version) FROM contract_spec_versions x
                         WHERE x.contract_id = csv.contract_id
                    )
                  LIMIT $2
             )",
            cutoff,
        )
        .await;
    }

    // Keep the newest spec_version_retention versions per contract, even if outside window.
    let mut deleted = 0u64;
    for _ in 0..MAX_BATCHES {
        let n = sqlx::query(
            "DELETE FROM contract_spec_versions WHERE ctid IN (
                 SELECT csv.ctid FROM contract_spec_versions csv
                  WHERE csv.ledger < $1
                    AND csv.version < (
                        SELECT COALESCE(
                            (SELECT MIN(v.version) FROM (
                                SELECT version FROM contract_spec_versions
                                 WHERE contract_id = csv.contract_id
                                 ORDER BY version DESC
                                 LIMIT $3 OFFSET 1
                            ) v),
                            0
                        )
                    )
                  LIMIT $2
             )",
        )
        .bind(cutoff)
        .bind(BATCH)
        .bind(spec_version_retention)
        .execute(pool)
        .await?
        .rows_affected();
        deleted += n;
        if n < BATCH as u64 {
            break;
        }
    }

    if deleted > 0 {
        info!(
            cutoff_ledger = cutoff,
            spec_version_retention,
            deleted,
            "pruned old contract spec versions outside the retention window"
        );
    }
    Ok(deleted)
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

    // -------------------------------------------------------------------------
    // Issue #225: contract_summaries trigger DELETE handling
    // -------------------------------------------------------------------------

    async fn summary_event_count(pool: &PgPool, contract_id: &str) -> Option<i64> {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT event_count FROM contract_summaries WHERE contract_id = $1")
                .bind(contract_id)
                .fetch_optional(pool)
                .await
                .expect("query contract_summaries");
        row.map(|(c,)| c)
    }

    /// Retention pruning must decrement `contract_summaries.event_count` for every
    /// deleted event, and remove the row entirely when the count reaches zero.
    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn contract_summaries_decrements_on_event_delete() {
        let pool = fixture().await;

        // Insert three events for the same contract.
        insert_event(&pool, "e1", 100).await;
        insert_event(&pool, "e2", 200).await;
        insert_event(&pool, "e3", 300).await;

        // Sanity check: the trigger should have maintained the count during inserts.
        let before = summary_event_count(&pool, "C1")
            .await
            .expect("summary row should exist after inserts");
        assert_eq!(before, 3, "insert trigger must have counted all three events");

        // Prune events older than ledger 250 (tip=1000, retention=750 → cutoff=250).
        // Events e1 (ledger=100) and e2 (ledger=200) are below the cutoff; e3 survives.
        let deleted = prune(&pool, 1000, 750).await.expect("prune");
        assert_eq!(deleted, 2, "two events should be pruned");

        let after = summary_event_count(&pool, "C1")
            .await
            .expect("summary row should still exist because one event survives");
        assert_eq!(after, 1, "event_count must reflect the one surviving event");

        // Prune the last event (cutoff=350 > ledger=300).
        let deleted2 = prune(&pool, 1000, 650).await.expect("prune again");
        assert_eq!(deleted2, 1, "the final event should be pruned");

        let final_row = summary_event_count(&pool, "C1").await;
        assert!(
            final_row.is_none(),
            "summary row must be removed when event_count would reach zero; \
             list_contracts must never expose contracts with zero events"
        );
    }

    /// first_seen_ledger and last_seen_ledger must stay accurate after pruning.
    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn contract_summaries_ledger_bounds_after_prune() {
        let pool = fixture().await;

        // Three events spanning a wide ledger range.
        insert_event(&pool, "b1", 50).await;
        insert_event(&pool, "b2", 500).await;
        insert_event(&pool, "b3", 900).await;

        // Prune events before ledger 200 (cutoff=200 → only b1 deleted).
        prune(&pool, 1000, 800).await.expect("prune");

        let row: (i64, i64, i64) = sqlx::query_as(
            "SELECT event_count, first_seen_ledger, last_seen_ledger
               FROM contract_summaries WHERE contract_id = 'C1'",
        )
        .fetch_one(&pool)
        .await
        .expect("summary row");

        let (count_val, first, last) = row;
        assert_eq!(count_val, 2, "two events survive");
        assert_eq!(
            first, 500,
            "first_seen_ledger must advance to the next surviving event"
        );
        assert_eq!(last, 900, "last_seen_ledger must remain at the newest event");
    }

    /// A contract that was never pruned must not be affected by pruning another
    /// contract's events.
    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn contract_summaries_isolation_across_contracts() {
        let pool = fixture().await;

        // Two events for C1 and one for a second contract C2.
        insert_event(&pool, "c1e1", 100).await;
        insert_event(&pool, "c1e2", 800).await;
        // Insert a C2 event manually (insert_event always uses contract_id='C1').
        sqlx::query(
            "INSERT INTO events
                 (event_id, contract_id, ledger, ledger_closed_at, event_type,
                  topics, event_name, value, tx_hash, in_successful_call, paging_token)
             VALUES ('c2e1','C2',600,now(),'contract','[]','mint','v','tx',true,'c2e1')",
        )
        .execute(&pool)
        .await
        .expect("insert C2 event");

        // Prune events before ledger 200 (removes c1e1 only).
        prune(&pool, 1000, 800).await.expect("prune");

        let c1 = summary_event_count(&pool, "C1").await.expect("C1 row");
        let c2 = summary_event_count(&pool, "C2").await.expect("C2 row");
        assert_eq!(c1, 1, "C1 should have one surviving event");
        assert_eq!(c2, 1, "C2 should be unaffected by C1's prune");
    }
}
