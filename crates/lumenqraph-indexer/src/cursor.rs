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

/// When the cursor last advanced (`progress_at`), or `None` if it never has.
///
/// Health freshness (`/health`, `/readyz`, the stalled alert) is computed from
/// this timestamp, not from `updated_at`: every counter write bumps
/// `updated_at` without indexing anything, and must not make a stuck poller
/// look fresh (#756, #757). Only [`write_progress`] sets it.
pub async fn read_progress_at(
    pool: &PgPool,
) -> anyhow::Result<Option<chrono::DateTime<chrono::Utc>>> {
    let row: Option<(Option<chrono::DateTime<chrono::Utc>>,)> =
        sqlx::query_as("SELECT progress_at FROM indexer_cursor WHERE id = 1")
            .fetch_optional(pool)
            .await?;
    Ok(row.and_then(|(progress_at,)| progress_at))
}

/// Pin the Soroban network the database holds data for (#714).
///
/// The first run stores `live` in `indexer_cursor.network_passphrase`. Later
/// runs must present the same passphrase. A different one is a fatal error
/// that names both networks, so a mainnet database is never filled from a
/// testnet RPC (or the reverse). `allow_change` (`ALLOW_NETWORK_CHANGE=true`)
/// re-pins the database to the live network on purpose.
///
/// The row is created if it is missing. Its `last_processed_ledger` is 0,
/// which readers treat as "nothing processed yet".
pub async fn pin_network_passphrase(
    pool: &PgPool,
    live: &str,
    allow_change: bool,
) -> anyhow::Result<()> {
    // Store the live passphrase only if none is pinned yet, then read back what
    // is pinned. A concurrent first start cannot overwrite the winner.
    let stored: String = sqlx::query_scalar(
        "INSERT INTO indexer_cursor (id, last_processed_ledger, network_passphrase, updated_at)
         VALUES (1, 0, $1, now())
         ON CONFLICT (id) DO UPDATE SET
            network_passphrase = COALESCE(indexer_cursor.network_passphrase, EXCLUDED.network_passphrase)
         RETURNING network_passphrase",
    )
    .bind(live)
    .fetch_one(pool)
    .await?;

    if stored == live {
        return Ok(());
    }

    if !allow_change {
        anyhow::bail!(
            "network mismatch: this database is pinned to {stored:?} but the RPC reports {live:?}; \
             refusing to mix two networks in one database. Point RPC_URL(S) at the pinned network, \
             or set ALLOW_NETWORK_CHANGE=true to re-pin the database on purpose"
        );
    }

    warn!(
        from = %stored,
        to = %live,
        "ALLOW_NETWORK_CHANGE is set: re-pinning the database to a new network passphrase"
    );
    sqlx::query(
        "UPDATE indexer_cursor SET network_passphrase = $1, updated_at = now() WHERE id = 1",
    )
    .bind(live)
    .execute(pool)
    .await?;
    Ok(())
}

/// Advance the cursor and record the observed chain tip + how many events were
/// newly ingested this cycle.
///
/// The write commits only if the version is still `expected_version`, the value
/// the cycle read with [`read_snapshot`]. The position never moves backwards.
///
/// This is the ONLY writer of `progress_at` (besides the one-time migration
/// backfill): `progress_at` marks "the cursor actually advanced", so health
/// freshness computed from it cannot be faked by counter writes like
/// `incr_errors` that only bump `updated_at` (#756, #757).
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
             updated_at            = now(),
             progress_at           = now()
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
        warn!(
            expected_version,
            "cursor update rejected: version changed since this cycle read it"
        );
        return Err(CursorConflict { expected_version }.into());
    }
    Ok(())
}

/// Mid-cycle checkpoint: move the cursor forward to `ledger` (never backwards)
/// so a crash does not replay the whole cycle (#717).
///
/// Deliberately does NOT bump `version`: the cycle's final [`write_progress`]
/// commits against the version it read at the start, and a checkpoint must not
/// make that write look like a concurrent-writer conflict. It does mark progress,
/// since the cursor really advanced.
pub async fn checkpoint_progress(pool: &PgPool, ledger: i64) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE indexer_cursor
         SET last_processed_ledger = GREATEST(last_processed_ledger, $1),
             updated_at            = now(),
             progress_at           = now()
         WHERE id = 1",
    )
    .bind(ledger)
    .execute(pool)
    .await?;
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
    write_progress(
        pool,
        snapshot.version,
        last_processed,
        chain_tip,
        ingested_delta,
    )
    .await
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
        assert_eq!(
            snap,
            CursorSnapshot {
                last_processed: None,
                version: 0
            }
        );
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
        write_progress(db.pool(), snap.version, 1000, 1000, 5)
            .await
            .unwrap();
        assert_eq!(read_last_processed(db.pool()).await.unwrap(), Some(1000));
    }

    /// Progress write first, then counter: the counter must leave the position alone (#708).
    // Multi-thread: `TestDb::drop` blocks its thread while it cleans up the schema.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn progress_before_counter_keeps_the_position() {
        let db = new_db().await;
        let snap = read_snapshot(db.pool()).await.unwrap();
        write_progress(db.pool(), snap.version, 1000, 1000, 0)
            .await
            .unwrap();

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

        write_progress(db.pool(), stale.version, 1000, 1000, 0)
            .await
            .unwrap();

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
        write_progress(db.pool(), snap.version, 1200, 1200, 0)
            .await
            .unwrap();

        let snap = read_snapshot(db.pool()).await.unwrap();
        write_progress(db.pool(), snap.version, 1150, 1150, 0)
            .await
            .unwrap();

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

    /// Counter writes (error tracking, RPC metrics, ...) bump `updated_at` but
    /// must NOT touch `progress_at`; only a real progress write may. Otherwise
    /// a poller whose every cycle fails looks fresh forever (#756, #757).
    // Multi-thread: `TestDb::drop` blocks its thread while it cleans up the schema.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn counter_writes_do_not_bump_progress_at() {
        let db = new_db().await;

        // Fresh database: the seeded row has never progressed.
        assert_eq!(read_progress_at(db.pool()).await.unwrap(), None);

        // Failed cycles: counters move, progress does not.
        incr_errors(db.pool()).await.unwrap();
        set_consecutive_errors(db.pool(), 5).await.unwrap();
        track_rpc_call(db.pool(), 10, 3, 1, 0).await.unwrap();
        track_enrichment(db.pool(), 1, 2).await.unwrap();
        track_timestamp_parse_error(db.pool(), 1).await.unwrap();
        assert_eq!(
            read_progress_at(db.pool()).await.unwrap(),
            None,
            "counter writes must not fabricate progress (#757)"
        );

        // A real progress write sets progress_at.
        let snap = read_snapshot(db.pool()).await.unwrap();
        write_progress(db.pool(), snap.version, 1000, 1000, 7)
            .await
            .unwrap();
        let first = read_progress_at(db.pool())
            .await
            .unwrap()
            .expect("progress write must set progress_at");

        // Later counter writes leave progress_at exactly where it was.
        incr_errors(db.pool()).await.unwrap();
        set_consecutive_errors(db.pool(), 6).await.unwrap();
        let after = read_progress_at(db.pool()).await.unwrap().unwrap();
        assert_eq!(first, after, "counter writes must not touch progress_at");

        // A further progress write moves it forward (or at least not back).
        let snap = read_snapshot(db.pool()).await.unwrap();
        write_progress(db.pool(), snap.version, 1100, 1100, 0)
            .await
            .unwrap();
        let later = read_progress_at(db.pool()).await.unwrap().unwrap();
        assert!(later >= first, "progress_at must not move backwards");
    }
}

#[cfg(test)]
mod network_pin_tests {
    use super::pin_network_passphrase;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::PgPool;

    const PUBLIC: &str = "Public Global Stellar Network ; September 2015";
    const TESTNET: &str = "Test SDF Network ; September 2015";

    /// An isolated schema holding just the `indexer_cursor` columns this code
    /// reads and writes. The full migration set is not used here, so the tests
    /// do not depend on unrelated migrations.
    async fn fresh_pool() -> PgPool {
        let url = std::env::var("TEST_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .expect("TEST_DATABASE_URL or DATABASE_URL must point at a test Postgres");
        let schema = format!("test_{}", uuid::Uuid::new_v4().simple());
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap();
        sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
            .execute(&admin)
            .await
            .unwrap();
        admin.close().await;

        let options = format!("-c search_path={schema},public");
        let sep = if url.contains('?') { "&" } else { "?" };
        let schema_url = format!("{url}{sep}options={}", percent_encode(&options));
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&schema_url)
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE indexer_cursor (
                id                    INTEGER     PRIMARY KEY DEFAULT 1,
                last_processed_ledger BIGINT      NOT NULL,
                network_passphrase    TEXT,
                updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
                progress_at           TIMESTAMPTZ,
                CONSTRAINT single_row CHECK (id = 1)
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
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

    async fn stored(pool: &PgPool) -> Option<String> {
        sqlx::query_scalar("SELECT network_passphrase FROM indexer_cursor WHERE id = 1")
            .fetch_optional(pool)
            .await
            .unwrap()
            .flatten()
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn first_run_stores_passphrase_and_later_runs_must_match() {
        let pool = fresh_pool().await;
        assert_eq!(
            stored(&pool).await,
            None,
            "fresh database has nothing pinned"
        );

        pin_network_passphrase(&pool, PUBLIC, false)
            .await
            .expect("first run pins");
        assert_eq!(stored(&pool).await.as_deref(), Some(PUBLIC));

        pin_network_passphrase(&pool, PUBLIC, false)
            .await
            .expect("same network again is fine");
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn second_run_on_a_different_network_is_rejected_and_names_both() {
        let pool = fresh_pool().await;
        pin_network_passphrase(&pool, PUBLIC, false).await.unwrap();

        let err = pin_network_passphrase(&pool, TESTNET, false)
            .await
            .expect_err("a different network must be fatal");
        let msg = err.to_string();
        assert!(msg.contains(PUBLIC) && msg.contains(TESTNET), "{msg}");
        assert_eq!(
            stored(&pool).await.as_deref(),
            Some(PUBLIC),
            "a rejected run must not overwrite the pin"
        );
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn allow_network_change_repins_the_database() {
        let pool = fresh_pool().await;
        pin_network_passphrase(&pool, PUBLIC, false).await.unwrap();

        pin_network_passphrase(&pool, TESTNET, true)
            .await
            .expect("explicit override re-pins");
        assert_eq!(stored(&pool).await.as_deref(), Some(TESTNET));
    }

    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn pinning_creates_the_cursor_row_without_moving_the_cursor() {
        let pool = fresh_pool().await;
        pin_network_passphrase(&pool, PUBLIC, false).await.unwrap();

        // 0 means "nothing processed yet"; the poller and backfill ignore it.
        let last: Option<i64> = super::read_last_processed(&pool).await.unwrap();
        assert_eq!(last, Some(0));
    }
}
