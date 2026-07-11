//! The single-row indexer status (id = 1): ledger cursor plus health counters
//! that `/health` and `/metrics` read back.

use sqlx::PgPool;

/// Last fully-processed ledger, if the index has started.
pub async fn read_last_processed(pool: &PgPool) -> anyhow::Result<Option<i64>> {
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT last_processed_ledger FROM indexer_cursor WHERE id = 1")
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|r| r.0))
}

/// Advance the cursor and record the observed chain tip + how many events were
/// newly ingested this cycle.
pub async fn write_progress(
    pool: &PgPool,
    last_processed: i64,
    chain_tip: i64,
    ingested_delta: u64,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO indexer_cursor
            (id, last_processed_ledger, chain_tip_ledger, events_ingested_total, updated_at)
         VALUES (1, $1, $2, $3, now())
         ON CONFLICT (id) DO UPDATE SET
            last_processed_ledger = EXCLUDED.last_processed_ledger,
            chain_tip_ledger      = EXCLUDED.chain_tip_ledger,
            events_ingested_total = indexer_cursor.events_ingested_total + $3,
            updated_at            = now()",
    )
    .bind(last_processed)
    .bind(chain_tip)
    .bind(ingested_delta as i64)
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
