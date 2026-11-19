//! The single-row indexer status (id = 1): ledger cursor plus health counters
//! that `/health` and `/metrics` read back. Uses optimistic locking (version column)
//! to detect and reject concurrent writer instances.

use sqlx::PgPool;
use tracing::warn;

/// Last fully-processed ledger, if the index has started.
pub async fn read_last_processed(pool: &PgPool) -> anyhow::Result<Option<i64>> {
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT last_processed_ledger FROM indexer_cursor WHERE id = 1")
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|r| r.0))
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
    sqlx::query("UPDATE indexer_cursor SET network_passphrase = $1, updated_at = now() WHERE id = 1")
        .bind(live)
        .execute(pool)
        .await?;
    Ok(())
}

/// Read the current version for optimistic locking.
async fn read_version(pool: &PgPool) -> anyhow::Result<i64> {
    let row: (i64,) =
        sqlx::query_as("SELECT COALESCE(version, 0) FROM indexer_cursor WHERE id = 1")
            .fetch_one(pool)
            .await?;
    Ok(row.0)
}

/// Advance the cursor and record the observed chain tip + how many events were
/// newly ingested this cycle. Uses optimistic locking to detect concurrent writers.
pub async fn write_progress(
    pool: &PgPool,
    last_processed: i64,
    chain_tip: i64,
    ingested_delta: u64,
) -> anyhow::Result<()> {
    // Read current version for optimistic locking
    let current_version = read_version(pool).await?;

    // Attempt update with version check; increment version on success
    let rows_affected = sqlx::query(
        "UPDATE indexer_cursor
         SET last_processed_ledger = $1,
             chain_tip_ledger      = $2,
             events_ingested_total = events_ingested_total + $3,
             version               = $4,
             updated_at            = now()
         WHERE id = 1 AND version = $5",
    )
    .bind(last_processed)
    .bind(chain_tip)
    .bind(ingested_delta as i64)
    .bind(current_version + 1)
    .bind(current_version)
    .execute(pool)
    .await?
    .rows_affected();

    if rows_affected == 0 {
        warn!(
            "cursor update failed: version mismatch (expected {}, cursor may have been updated by another instance)",
            current_version
        );
        return Err(anyhow::anyhow!(
            "concurrent writer detected: version mismatch on cursor update"
        ));
    }

    Ok(())
}

/// Persist a mid-cycle checkpoint: every ledger up to and including
/// `last_processed` is fully stored (#717). Only the cursor position moves.
/// Ingest counters and the observed tip are still written once per cycle by
/// [`write_progress`]. The cursor never moves backwards.
pub async fn checkpoint_progress(pool: &PgPool, last_processed: i64) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO indexer_cursor (id, last_processed_ledger, updated_at)
         VALUES (1, $1, now())
         ON CONFLICT (id) DO UPDATE SET
            last_processed_ledger = GREATEST(indexer_cursor.last_processed_ledger, EXCLUDED.last_processed_ledger),
            updated_at            = now()",
    )
    .bind(last_processed)
    .execute(pool)
    .await?;
    Ok(())
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
        let admin = PgPoolOptions::new().max_connections(1).connect(&url).await.unwrap();
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
        assert_eq!(stored(&pool).await, None, "fresh database has nothing pinned");

        pin_network_passphrase(&pool, PUBLIC, false).await.expect("first run pins");
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
