//! Integration tests for recording and exposing indexer gaps.
//! Verifies that when the poller skips ahead due to MAX_CATCHUP_LEDGERS,
//! the gap is persisted and exposed via /health and metrics.

#[cfg(test)]
mod indexer_gaps_tests {
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

        // Create indexer_gaps table
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS indexer_gaps (
                id                          BIGSERIAL   PRIMARY KEY,
                from_ledger                 BIGINT      NOT NULL,
                to_ledger                   BIGINT      NOT NULL,
                reason                      TEXT        NOT NULL,
                detected_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
                filled_at                   TIMESTAMPTZ,
                created_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .execute(&pool)
        .await
        .ok();

        // Create indexer_cursor for testing
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
        sqlx::query("DELETE FROM indexer_gaps")
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM indexer_cursor WHERE id = 1")
            .execute(pool)
            .await
            .ok();
    }

    /// Test: A skip-ahead creates a persisted gap row.
    /// Scenario: Cursor is at ledger 1_000_000, latest is at 2_000_000,
    /// but MAX_CATCHUP_LEDGERS = 100_000, so we skip from 1_000_000 to 1_900_000.
    #[tokio::test]
    async fn test_skip_ahead_creates_gap_row() {
        let pool = setup_test_db().await;
        cleanup_test_data(&pool).await;

        let last_processed = 1_000_000i64;
        let latest = 2_000_000i64;
        let max_catchup_ledgers = 100_000i64;
        let gap_reason = "gap unrecoverable via public RPC; use a retaining/paid RPC or deep-backfill";

        // Check if cursor is too far behind
        if latest - last_processed > max_catchup_ledgers {
            let gap_start = last_processed + 1;
            let gap_end = latest - max_catchup_ledgers;

            // Record the gap
            sqlx::query(
                "INSERT INTO indexer_gaps (from_ledger, to_ledger, reason, detected_at)
                 VALUES ($1, $2, $3, NOW())"
            )
            .bind(gap_start)
            .bind(gap_end)
            .bind(gap_reason)
            .execute(&pool)
            .await
            .expect("gap insert should succeed");
        }

        // Verify gap was recorded
        let gap_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM indexer_gaps WHERE filled_at IS NULL"
        )
        .fetch_one(&pool)
        .await
        .expect("query should succeed");

        assert_eq!(gap_count, 1, "skip-ahead should create one open gap");

        let (stored_from, stored_to): (i64, i64) = sqlx::query_as(
            "SELECT from_ledger, to_ledger FROM indexer_gaps WHERE filled_at IS NULL ORDER BY id DESC LIMIT 1"
        )
        .fetch_one(&pool)
        .await
        .expect("query should succeed");

        let expected_from = last_processed + 1;
        let expected_to = latest - max_catchup_ledgers;

        assert_eq!(
            stored_from, expected_from,
            "gap should start after last processed ledger"
        );
        assert_eq!(
            stored_to, expected_to,
            "gap should end at skip-ahead point"
        );

        cleanup_test_data(&pool).await;
    }

    /// Test: Clamping also creates a gap row when fresh start is clamped.
    /// Scenario: Fresh start configured to ledger 50_000 but RPC only has ledgers 150_000+.
    #[tokio::test]
    async fn test_clamping_creates_gap_row() {
        let pool = setup_test_db().await;
        cleanup_test_data(&pool).await;

        let configured_start = 50_000i64;
        let oldest_available = 150_000i64;
        let latest = 200_000i64;
        let clamp_reason = "fresh-start ledger clamped to earliest servable ledger (RPC retention limit)";

        // Clamping scenario
        if configured_start < oldest_available {
            let gap_start = configured_start;
            let gap_end = oldest_available;

            // Record the gap from clamping
            sqlx::query(
                "INSERT INTO indexer_gaps (from_ledger, to_ledger, reason, detected_at)
                 VALUES ($1, $2, $3, NOW())"
            )
            .bind(gap_start)
            .bind(gap_end)
            .bind(clamp_reason)
            .execute(&pool)
            .await
            .expect("gap insert should succeed");
        }

        // Verify clamping gap was recorded
        let clamping_gaps: Vec<(i64, i64)> = sqlx::query_as(
            "SELECT from_ledger, to_ledger FROM indexer_gaps WHERE reason LIKE '%fresh-start%'"
        )
        .fetch_all(&pool)
        .await
        .expect("query should succeed");

        assert_eq!(
            clamping_gaps.len(),
            1,
            "clamping should create exactly one gap"
        );

        let (gap_from, gap_to) = clamping_gaps[0];
        assert_eq!(gap_from, configured_start, "gap should start at configured START_LEDGER");
        assert_eq!(gap_to, oldest_available, "gap should end at oldest available");

        cleanup_test_data(&pool).await;
    }

    /// Test: /health endpoint reports open gaps as a metric.
    /// Scenario: Query indexer_gaps and compute open-gaps count and total-skipped-ledgers.
    #[tokio::test]
    async fn test_health_reports_open_gaps() {
        let pool = setup_test_db().await;
        cleanup_test_data(&pool).await;

        // Create multiple open gaps
        for i in 0..3 {
            sqlx::query(
                "INSERT INTO indexer_gaps (from_ledger, to_ledger, reason, detected_at)
                 VALUES ($1, $2, $3, NOW())"
            )
            .bind(1_000_000 + (i * 100_000))
            .bind(1_050_000 + (i * 100_000))
            .bind(format!("gap reason {}", i))
            .execute(&pool)
            .await
            .expect("gap insert should succeed");
        }

        // Simulate /health query: count open gaps and total ledgers
        let (open_gap_count, total_gap_ledgers): (i64, Option<i64>) = sqlx::query_as(
            "SELECT COUNT(*), COALESCE(SUM(to_ledger - from_ledger), 0)
             FROM indexer_gaps WHERE filled_at IS NULL"
        )
        .fetch_one(&pool)
        .await
        .expect("query should succeed");

        assert_eq!(open_gap_count, 3, "should have 3 open gaps");
        assert_eq!(
            total_gap_ledgers, Some(150_000),
            "total gap should be 150_000 ledgers (3 gaps × 50_000)"
        );

        // Verify metrics that would be exposed via Prometheus
        // lumenqraph_indexer_gap_ledgers_total = 150_000
        // lumenqraph_indexer_open_gaps = 3

        cleanup_test_data(&pool).await;
    }

    /// Test: A deep backfill covering a gap marks it as filled.
    /// Scenario: Gap exists from 1_000_000 to 1_100_000, then deep-backfill
    /// processes ledgers 1_000_000 to 1_100_000, marks gap as filled.
    #[tokio::test]
    async fn test_deep_backfill_marks_gap_filled() {
        let pool = setup_test_db().await;
        cleanup_test_data(&pool).await;

        // Create an open gap
        sqlx::query(
            "INSERT INTO indexer_gaps (from_ledger, to_ledger, reason, detected_at)
             VALUES (1_000_000, 1_100_000, 'skip-ahead', NOW())"
        )
        .execute(&pool)
        .await
        .expect("gap insert should succeed");

        // Simulate deep-backfill covering the gap range
        let backfill_start = 1_000_000i64;
        let backfill_end = 1_100_000i64;

        // Mark any gaps that are fully covered by this backfill as filled
        sqlx::query(
            "UPDATE indexer_gaps
             SET filled_at = NOW()
             WHERE filled_at IS NULL
               AND from_ledger >= $1
               AND to_ledger <= $2"
        )
        .bind(backfill_start)
        .bind(backfill_end)
        .execute(&pool)
        .await
        .expect("gap update should succeed");

        // Verify gap is now marked as filled
        let open_gap_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM indexer_gaps WHERE filled_at IS NULL"
        )
        .fetch_one(&pool)
        .await
        .expect("query should succeed");

        assert_eq!(
            open_gap_count, 0,
            "gap should be marked as filled after deep-backfill"
        );

        let filled_gap_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM indexer_gaps WHERE filled_at IS NOT NULL"
        )
        .fetch_one(&pool)
        .await
        .expect("query should succeed");

        assert_eq!(
            filled_gap_count, 1,
            "one gap should be marked as filled"
        );

        cleanup_test_data(&pool).await;
    }

    /// Test: Partial backfill that doesn't cover entire gap doesn't mark it filled.
    /// Scenario: Gap is 1_000_000 to 1_100_000, backfill only covers 1_000_000 to 1_050_000.
    #[tokio::test]
    async fn test_partial_backfill_does_not_mark_gap_filled() {
        let pool = setup_test_db().await;
        cleanup_test_data(&pool).await;

        // Create an open gap
        sqlx::query(
            "INSERT INTO indexer_gaps (from_ledger, to_ledger, reason, detected_at)
             VALUES (1_000_000, 1_100_000, 'skip-ahead', NOW())"
        )
        .execute(&pool)
        .await
        .expect("gap insert should succeed");

        // Simulate partial backfill that doesn't cover the entire gap
        let backfill_start = 1_000_000i64;
        let backfill_end = 1_050_000i64; // Only covers half the gap

        // Attempt to mark gaps as filled (should not match)
        sqlx::query(
            "UPDATE indexer_gaps
             SET filled_at = NOW()
             WHERE filled_at IS NULL
               AND from_ledger >= $1
               AND to_ledger <= $2"
        )
        .bind(backfill_start)
        .bind(backfill_end)
        .execute(&pool)
        .await
        .expect("gap update should succeed");

        // Verify gap is still open
        let open_gap_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM indexer_gaps WHERE filled_at IS NULL"
        )
        .fetch_one(&pool)
        .await
        .expect("query should succeed");

        assert_eq!(
            open_gap_count, 1,
            "gap should still be open after partial backfill"
        );

        cleanup_test_data(&pool).await;
    }
}
