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

/// Read the current version for optimistic locking.
async fn read_version(pool: &PgPool) -> anyhow::Result<i64> {
    let row: (i64,) = sqlx::query_as(
        "SELECT COALESCE(version, 0) FROM indexer_cursor WHERE id = 1"
    )
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

/// Track RPC call metrics: total calls and errors.
pub async fn track_rpc_call(
    pool: &PgPool,
    call_count: u64,
    error_count: u64,
    error_32001_count: u64,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO indexer_cursor (id, last_processed_ledger, rpc_calls_total, rpc_errors_total, rpc_errors_32001_total, updated_at)
         VALUES (1, 0, $1, $2, $3, now())
         ON CONFLICT (id) DO UPDATE SET
            rpc_calls_total           = indexer_cursor.rpc_calls_total + $1,
            rpc_errors_total          = indexer_cursor.rpc_errors_total + $2,
            rpc_errors_32001_total    = indexer_cursor.rpc_errors_32001_total + $3,
            updated_at                = now()",
    )
    .bind(call_count as i64)
    .bind(error_count as i64)
    .bind(error_32001_count as i64)
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
