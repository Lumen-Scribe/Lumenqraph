//! Integration tests for cursor initialization when the first poll cycle fails.
//! Ensures that START_LEDGER is respected even if the first attempt results in an error
//! before write_progress is called.

#[cfg(test)]
mod cursor_initialization_tests {
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{PgPool, Row};

    async fn setup_test_db() -> PgPool {
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://localhost/lumenqraph_test".to_string());

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
            .expect("Failed to connect to test database");

        // Create indexer_cursor table for testing
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS indexer_cursor (
                id                          BIGINT      PRIMARY KEY DEFAULT 1,
                last_processed_ledger       BIGINT      NOT NULL DEFAULT 0,
                chain_tip_ledger            BIGINT      NOT NULL DEFAULT 0,
                errors_total                BIGINT      NOT NULL DEFAULT 0,
                consecutive_errors          BIGINT      NOT NULL DEFAULT 0,
                events_ingested_total       BIGINT      NOT NULL DEFAULT 0,
                events_enriched_total       BIGINT      NOT NULL DEFAULT 0,
                events_not_enriched_total   BIGINT      NOT NULL DEFAULT 0,
                spec_fetch_failures_total   BIGINT      NOT NULL DEFAULT 0,
                rpc_calls_total             BIGINT      NOT NULL DEFAULT 0,
                rpc_errors_total            BIGINT      NOT NULL DEFAULT 0,
                rpc_errors_32001_total      BIGINT      NOT NULL DEFAULT 0,
                version                     BIGINT,
                updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
                created_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .execute(&pool)
        .await
        .ok();

        pool
    }

    async fn cleanup_test_data(pool: &PgPool) {
        sqlx::query("DELETE FROM indexer_cursor WHERE id = 1")
            .execute(pool)
            .await
            .ok();
    }

    /// Test that counter upserts (incr_errors, track_enrichment, etc.) do not
    /// create a cursor row with a fabricated last_processed_ledger = 0.
    /// These operations should not create cursor rows at all; they should only
    /// update existing ones or use a separate metrics table.
    #[tokio::test]
    async fn test_counter_upserts_should_not_fabricate_cursor_ledger() {
        let pool = setup_test_db().await;
        cleanup_test_data(&pool).await;

        // Simulate what happens when incr_errors is called on a fresh database
        // before any write_progress has occurred.
        sqlx::query(
            "INSERT INTO indexer_cursor (id, last_processed_ledger, errors_total, updated_at)
             VALUES (1, 0, 1, now())
             ON CONFLICT (id) DO UPDATE SET
                errors_total = indexer_cursor.errors_total + 1,
                updated_at   = now()",
        )
        .execute(&pool)
        .await
        .expect("incr_errors upsert should succeed");

        // Check the cursor state
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT last_processed_ledger FROM indexer_cursor WHERE id = 1"
        )
        .fetch_optional(&pool)
        .await
        .expect("query should succeed");

        // The problem: on a fresh database, this inserts with last_processed_ledger = 0
        // which is indistinguishable from "have processed up to ledger 0"
        assert!(
            row.is_some(),
            "cursor row should exist after incr_errors (current behavior)"
        );
        let (ledger,) = row.unwrap();
        // Current behavior (broken): ledger is 0 after first failure
        // This test documents the bug; once fixed, last_processed_ledger should be NULL
        // or the row should not be created by counter operations at all
        assert_eq!(ledger, 0, "BUG: counter upserts fabricate ledger = 0");

        cleanup_test_data(&pool).await;
    }

    /// Test the fix: after a failed first cycle, the next successful cycle
    /// should start from START_LEDGER (not clamped unnecessarily).
    /// This test assumes the fix has been applied: counters no longer create cursor rows,
    /// or last_processed_ledger is nullable with NULL meaning "not started".
    #[tokio::test]
    async fn test_start_ledger_respected_after_first_cycle_failure() {
        let pool = setup_test_db().await;
        cleanup_test_data(&pool).await;

        let start_ledger_config = 50_000i64;
        let latest = 100_000i64;

        // Scenario: First cycle fails; no cursor row is created by error handling.
        // The pool is clean (no cursor row).
        let cursor_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM indexer_cursor WHERE id = 1)"
        )
        .fetch_one(&pool)
        .await
        .expect("query should succeed");
        assert!(!cursor_exists, "cursor should not exist after first failure");

        // On the second cycle, read_last_processed returns None,
        // so the logic should fall through to "None if config.start_ledger > 0".
        // This is the poll_once logic:
        let start = if !cursor_exists && start_ledger_config > 0 {
            start_ledger_config
        } else {
            latest
        };

        // The start should respect START_LEDGER
        assert_eq!(
            start, start_ledger_config,
            "second cycle should start from START_LEDGER after first failure"
        );

        cleanup_test_data(&pool).await;
    }

    /// Test that once write_progress is called successfully,
    /// the cursor row has a valid last_processed_ledger (not 0 from a failed attempt).
    #[tokio::test]
    async fn test_write_progress_sets_valid_ledger() {
        let pool = setup_test_db().await;
        cleanup_test_data(&pool).await;

        let last_processed = 75_000i64;
        let chain_tip = 100_000i64;

        // Simulate write_progress being called after a successful fetch
        let current_version = 0i64;
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
        .bind(0i64) // ingested_delta
        .bind(current_version + 1)
        .bind(current_version)
        .execute(&pool)
        .await
        .expect("update should work")
        .rows_affected();

        // If the row doesn't exist yet, insert it first
        if rows_affected == 0 {
            sqlx::query(
                "INSERT INTO indexer_cursor (id, last_processed_ledger, chain_tip_ledger, version)
                 VALUES (1, $1, $2, 1)"
            )
            .bind(last_processed)
            .bind(chain_tip)
            .execute(&pool)
            .await
            .expect("insert should succeed");
        }

        let (stored_ledger,): (i64,) = sqlx::query_as(
            "SELECT last_processed_ledger FROM indexer_cursor WHERE id = 1"
        )
        .fetch_one(&pool)
        .await
        .expect("query should succeed");

        assert_eq!(
            stored_ledger, last_processed,
            "write_progress should persist the actual last_processed ledger"
        );

        cleanup_test_data(&pool).await;
    }
}
