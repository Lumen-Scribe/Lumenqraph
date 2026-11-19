//! The single-row indexer status (id = 1): ledger cursor plus health counters
//! that `/health` and `/metrics` read back. Uses optimistic locking (version column)
//! to detect and reject concurrent writer instances.

use std::fmt;

use sqlx::PgPool;
use tracing::warn;

/// `last_processed_ledger` value meaning "no ledger processed yet" (#708).
/// Stellar ledgers start at 1, so 0 can never be a real position.
pub const NOT_STARTED: i64 = 0;

/// The cursor as one cycle saw it: the position and the version that guards it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CursorSnapshot {
    /// Last fully-processed ledger, or `None` before the first progress write.
    pub last_processed: Option<i64>,
    /// Version observed in the same read as `last_processed` (#709).
    pub version: i64,
}

/// Raised when a progress write finds the cursor's version has moved since this
/// cycle read it, meaning another writer committed in between (#709). It is fatal
/// for the process: two writers on one cursor is a split-brain condition.
#[derive(Debug)]
pub struct CursorConflict {
    pub expected_version: i64,
}

impl fmt::Display for CursorConflict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cursor version mismatch (expected {}): another writer committed; stopping this indexer",
            self.expected_version
        )
    }
}

impl std::error::Error for CursorConflict {}

/// Read the position and version together, in one statement, so the version
/// always belongs to the position this cycle starts from (#709).
pub async fn read_snapshot(pool: &PgPool) -> anyhow::Result<CursorSnapshot> {
    let row: Option<(i64, i64)> =
        sqlx::query_as("SELECT last_processed_ledger, version FROM indexer_cursor WHERE id = 1")
            .fetch_optional(pool)
            .await?;
    Ok(match row {
        Some((last, version)) => CursorSnapshot {
            last_processed: (last > NOT_STARTED).then_some(last),
            version,
        },
        None => CursorSnapshot {
            last_processed: None,
            version: 0,
        },
    })
}

/// Last fully-processed ledger, if the index has started.
pub async fn read_last_processed(pool: &PgPool) -> anyhow::Result<Option<i64>> {
    Ok(read_snapshot(pool).await?.last_processed)
}

/// Advance the cursor and record the observed chain tip + how many events were
/// newly ingested this cycle.
///
/// The write commits only if the version is still `expected_version`, the value
/// the cycle read with [`read_snapshot`]. The position never moves backwards.
pub async fn write_progress(
    pool: &PgPool,
    expected_version: i64,
    last_processed: i64,
    chain_tip: i64,
    ingested_delta: u64,
) -> anyhow::Result<()> {
    let rows_affected = sqlx::query(
        "UPDATE indexer_cursor
         SET last_processed_ledger = GREATEST(last_processed_ledger, $1),
             chain_tip_ledger      = $2,
             events_ingested_total = events_ingested_total + $3,
             version               = version + 1,
             updated_at            = now()
         WHERE id = 1 AND version = $4",
    )
    .bind(last_processed)
    .bind(chain_tip)
    .bind(ingested_delta as i64)
    .bind(expected_version)
    .execute(pool)
    .await?
    .rows_affected();

    if rows_affected == 0 {
        warn!(expected_version, "cursor update rejected: version changed since this cycle read it");
        return Err(CursorConflict { expected_version }.into());
    }
    Ok(())
}

/// Progress write for one-shot commands (`backfill`, `recover-gaps`, ...) that
/// hold the leader lock, so no live poller can move the cursor concurrently.
/// Reads the current version, then writes against it. The poller must not use
/// this: it has to write against the version it started the cycle with.
pub async fn write_progress_locked(
    pool: &PgPool,
    last_processed: i64,
    chain_tip: i64,
    ingested_delta: u64,
) -> anyhow::Result<()> {
    let snapshot = read_snapshot(pool).await?;
    write_progress(pool, snapshot.version, last_processed, chain_tip, ingested_delta).await
}

/// Record that a poll cycle failed, for the error-rate metric.
pub async fn incr_errors(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO indexer_cursor (id, last_processed_ledger, errors_total, updated_at)
         VALUES (1, 0, 1, now())
         ON CONFLICT (id) DO UPDATE SET
            errors_total = indexer_cursor.errors_total + 1,
            updated_at   = now()",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Track enrichment metrics: events that were enriched vs not enriched.
pub async fn track_enrichment(
    pool: &PgPool,
    enriched_count: u64,
    not_enriched_count: u64,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO indexer_cursor (id, last_processed_ledger, events_enriched_total, events_not_enriched_total, updated_at)
         VALUES (1, 0, $1, $2, now())
         ON CONFLICT (id) DO UPDATE SET
            events_enriched_total     = indexer_cursor.events_enriched_total + $1,
            events_not_enriched_total = indexer_cursor.events_not_enriched_total + $2,
            updated_at                = now()",
    )
    .bind(enriched_count as i64)
    .bind(not_enriched_count as i64)
    .execute(pool)
    .await?;
    Ok(())
}

/// Track spec fetch failures.
pub async fn track_spec_fetch_failure(pool: &PgPool, count: u64) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO indexer_cursor (id, last_processed_ledger, spec_fetch_failures_total, updated_at)
         VALUES (1, 0, $1, now())
         ON CONFLICT (id) DO UPDATE SET
            spec_fetch_failures_total = indexer_cursor.spec_fetch_failures_total + $1,
            updated_at                = now()",
    )
    .bind(count as i64)
    .execute(pool)
    .await?;
    Ok(())
}

/// Track RPC call metrics: total calls, errors, and endpoint failovers.
pub async fn track_rpc_call(
    pool: &PgPool,
    call_count: u64,
    error_count: u64,
    error_32001_count: u64,
    failover_count: u64,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO indexer_cursor (id, last_processed_ledger, rpc_calls_total, rpc_errors_total, rpc_errors_32001_total, rpc_failovers_total, updated_at)
         VALUES (1, 0, $1, $2, $3, $4, now())
         ON CONFLICT (id) DO UPDATE SET
            rpc_calls_total           = indexer_cursor.rpc_calls_total + $1,
            rpc_errors_total          = indexer_cursor.rpc_errors_total + $2,
            rpc_errors_32001_total    = indexer_cursor.rpc_errors_32001_total + $3,
            rpc_failovers_total       = indexer_cursor.rpc_failovers_total + $4,
            updated_at                = now()",
    )
    .bind(call_count as i64)
    .bind(error_count as i64)
    .bind(error_32001_count as i64)
    .bind(failover_count as i64)
    .execute(pool)
    .await?;
    Ok(())
}

/// Set the current consecutive-error count for the circuit-breaker Prometheus
/// gauge (`lumenqraph_consecutive_errors`). Called on every failure increment
/// and reset to 0 on the first successful cycle after a run of failures.
pub async fn set_consecutive_errors(pool: &PgPool, count: u32) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO indexer_cursor (id, last_processed_ledger, consecutive_errors, updated_at)
         VALUES (1, 0, $1, now())
         ON CONFLICT (id) DO UPDATE SET
            consecutive_errors = $1,
            updated_at         = now()",
    )
    .bind(count as i64)
    .execute(pool)
    .await?;
    Ok(())
}


/// Increment the timestamp parse error counter (#399). Called once per event
/// whose `ledgerClosedAt` field cannot be parsed as RFC 3339 or Unix seconds.
pub async fn track_timestamp_parse_error(pool: &PgPool, count: u64) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO indexer_cursor (id, last_processed_ledger, timestamp_parse_errors_total, updated_at)
         VALUES (1, 0, $1, now())
         ON CONFLICT (id) DO UPDATE SET
            timestamp_parse_errors_total = indexer_cursor.timestamp_parse_errors_total + $1,
            updated_at                   = now()",
    )
    .bind(count as i64)
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod db_tests {
    use super::*;
    use lumenqraph_core::db_test::TestDb;

    async fn new_db() -> TestDb {
        TestDb::new_with_migrations("../../migrations").await
    }

    /// Migrations seed the row, so a fresh database has a cursor that reads as not started.
    // Multi-thread: `TestDb::drop` blocks its thread while it cleans up the schema.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore] // Requires TEST_DATABASE_URL (CI runs `--ignored`)
    async fn fresh_database_reads_as_not_started() {
        let db = new_db().await;
        let snap = read_snapshot(db.pool()).await.unwrap();
        assert_eq!(snap, CursorSnapshot { last_processed: None, version: 0 });
    }

    /// Counter write first, then progress: the progress write must succeed and
    /// the counter must not have invented a position (#708).
    // Multi-thread: `TestDb::drop` blocks its thread while it cleans up the schema.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn counter_before_progress_does_not_fabricate_a_position() {
        let db = new_db().await;
        incr_errors(db.pool()).await.unwrap();
        assert_eq!(read_last_processed(db.pool()).await.unwrap(), None);

        let snap = read_snapshot(db.pool()).await.unwrap();
        write_progress(db.pool(), snap.version, 1000, 1000, 5).await.unwrap();
        assert_eq!(read_last_processed(db.pool()).await.unwrap(), Some(1000));
    }

    /// Progress write first, then counter: the counter must leave the position alone (#708).
    // Multi-thread: `TestDb::drop` blocks its thread while it cleans up the schema.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn progress_before_counter_keeps_the_position() {
        let db = new_db().await;
        let snap = read_snapshot(db.pool()).await.unwrap();
        write_progress(db.pool(), snap.version, 1000, 1000, 0).await.unwrap();

        incr_errors(db.pool()).await.unwrap();
        track_enrichment(db.pool(), 3, 1).await.unwrap();
        assert_eq!(read_last_processed(db.pool()).await.unwrap(), Some(1000));
    }

    /// Two cycles start from the same snapshot. The second commit must be rejected (#709).
    // Multi-thread: `TestDb::drop` blocks its thread while it cleans up the schema.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn second_write_from_a_stale_snapshot_is_rejected() {
        let db = new_db().await;
        let stale = read_snapshot(db.pool()).await.unwrap();

        write_progress(db.pool(), stale.version, 1000, 1000, 0).await.unwrap();

        let err = write_progress(db.pool(), stale.version, 1100, 1100, 0)
            .await
            .expect_err("stale version must be rejected");
        assert!(err.downcast_ref::<CursorConflict>().is_some(), "got: {err}");
        assert_eq!(read_last_processed(db.pool()).await.unwrap(), Some(1000));
    }

    /// The position never decreases, even when a writer with a lower ledger commits (#709).
    // Multi-thread: `TestDb::drop` blocks its thread while it cleans up the schema.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn cursor_never_moves_backwards() {
        let db = new_db().await;
        let snap = read_snapshot(db.pool()).await.unwrap();
        write_progress(db.pool(), snap.version, 1200, 1200, 0).await.unwrap();

        let snap = read_snapshot(db.pool()).await.unwrap();
        write_progress(db.pool(), snap.version, 1150, 1150, 0).await.unwrap();

        assert_eq!(read_last_processed(db.pool()).await.unwrap(), Some(1200));
    }

    /// The locked helper for one-shot commands still advances the cursor.
    // Multi-thread: `TestDb::drop` blocks its thread while it cleans up the schema.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn locked_write_advances_the_cursor() {
        let db = new_db().await;
        write_progress_locked(db.pool(), 500, 500, 0).await.unwrap();
        write_progress_locked(db.pool(), 700, 700, 0).await.unwrap();
        assert_eq!(read_last_processed(db.pool()).await.unwrap(), Some(700));
    }
}
