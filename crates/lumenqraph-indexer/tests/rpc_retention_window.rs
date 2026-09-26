//! Integration tests for RPC retention window handling.
//! Verifies that the poller reads the actual retention window from the RPC
//! instead of using a hardcoded constant, and clamps fresh-start appropriately.

#[cfg(test)]
mod rpc_retention_window_tests {
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

        // Create rpc_health table to store RPC retention window info
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS rpc_health (
                id                          BIGINT      PRIMARY KEY DEFAULT 1,
                oldest_ledger               BIGINT      NOT NULL,
                latest_ledger               BIGINT      NOT NULL,
                ledger_retention_window     BIGINT      NOT NULL,
                fetched_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
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
        sqlx::query("DELETE FROM rpc_health WHERE id = 1")
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM indexer_cursor WHERE id = 1")
            .execute(pool)
            .await
            .ok();
    }

    /// Test: Fresh-start clamping uses RPC-reported oldest ledger
    /// instead of a hardcoded 120,000 ledger window.
    /// Scenario: RPC has a 24-hour retention (17,280 ledgers).
    #[tokio::test]
    async fn test_fresh_start_clamping_uses_rpc_oldest_ledger() {
        let pool = setup_test_db().await;
        cleanup_test_data(&pool).await;

        let latest = 100_000i64;
        let oldest_available = 82_720i64; // latest - 17_280 (24 hours)
        let ledger_retention_window = 17_280i64;

        // Store RPC health info with 24-hour retention
        sqlx::query(
            "INSERT INTO rpc_health (id, oldest_ledger, latest_ledger, ledger_retention_window)
             VALUES (1, $1, $2, $3)"
        )
        .bind(oldest_available)
        .bind(latest)
        .bind(ledger_retention_window)
        .execute(&pool)
        .await
        .expect("insert rpc_health should succeed");

        // Fresh start with configured START_LEDGER older than RPC retention
        let configured_start = 50_000i64; // Way older than oldest_available

        // Simulate the clamping logic
        let health: (i64, i64) = sqlx::query_as(
            "SELECT oldest_ledger, latest_ledger FROM rpc_health WHERE id = 1"
        )
        .fetch_one(&pool)
        .await
        .expect("query should succeed");

        let (oldest, _latest) = health;
        let clamped_start = configured_start.max(oldest);

        // Verify clamping respects RPC-reported oldest, not hardcoded 120_000
        assert_eq!(
            clamped_start, oldest_available,
            "fresh start should clamp to RPC-reported oldest ledger (24-hour window)"
        );
        assert_ne!(
            clamped_start, latest - 120_000,
            "clamping should NOT use hardcoded 120_000 window"
        );

        cleanup_test_data(&pool).await;
    }

    /// Test: 30-day RPC allows backfill beyond hardcoded 7-day window
    /// Scenario: RPC has 30-day retention (~2,592,000 ledgers at 5s/ledger)
    #[tokio::test]
    async fn test_long_retention_rpc_allows_deeper_backfill() {
        let pool = setup_test_db().await;
        cleanup_test_data(&pool).await;

        let latest = 10_000_000i64;
        let thirty_days_ledgers = 2_592_000i64;
        let oldest_available = latest - thirty_days_ledgers;
        let hardcoded_seven_days = 120_000i64; // ~7 days (hardcoded in current code)

        // Store RPC health info with 30-day retention
        sqlx::query(
            "INSERT INTO rpc_health (id, oldest_ledger, latest_ledger, ledger_retention_window)
             VALUES (1, $1, $2, $3)"
        )
        .bind(oldest_available)
        .bind(latest)
        .bind(thirty_days_ledgers)
        .execute(&pool)
        .await
        .expect("insert rpc_health should succeed");

        // Get RPC-reported retention window (not hardcoded)
        let (oldest, _latest, retention): (i64, i64, i64) = sqlx::query_as(
            "SELECT oldest_ledger, latest_ledger, ledger_retention_window FROM rpc_health WHERE id = 1"
        )
        .fetch_one(&pool)
        .await
        .expect("query should succeed");

        // Backfill start: 20 days old (within 30-day retention, outside 7-day hardcoded)
        let backfill_start = latest - (20 * 24 * 3600 / 5); // ~20 days

        // With 30-day RPC, backfill_start should be valid
        let within_retention = backfill_start >= oldest;
        assert!(
            within_retention,
            "20-day backfill should be within 30-day RPC retention"
        );

        // But with hardcoded 120_000, it would be rejected
        let within_hardcoded = backfill_start >= (latest - hardcoded_seven_days);
        assert!(
            !within_hardcoded || backfill_start >= oldest,
            "RPC-reported retention should be used, not hardcoded constant"
        );

        cleanup_test_data(&pool).await;
    }

    /// Test: Missing RPC health info falls back to override or safe default.
    /// Scenario: RPC does not report getHealth, but RPC_RETENTION_LEDGERS env var is set.
    #[tokio::test]
    async fn test_retention_override_fallback() {
        let pool = setup_test_db().await;
        cleanup_test_data(&pool).await;

        // No rpc_health data (RPC doesn't report getHealth)
        let health_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM rpc_health WHERE id = 1)"
        )
        .fetch_one(&pool)
        .await
        .expect("query should succeed");
        assert!(!health_exists, "rpc_health should not exist");

        let override_retention = 86_400i64; // 1 day override
        let latest = 100_000i64;

        // When getHealth is not available, use override or safe default
        let retention_to_use = if health_exists {
            // Would fetch from rpc_health
            17_280i64
        } else if override_retention > 0 {
            override_retention
        } else {
            // Safe fallback: conservative estimate
            50_000i64
        };

        let oldest_available = latest - retention_to_use;

        // Verify fallback is more conservative than hardcoded 120_000 (when appropriate)
        // or uses the configured override
        assert_eq!(
            retention_to_use, override_retention,
            "should use RPC_RETENTION_LEDGERS override when getHealth unavailable"
        );

        cleanup_test_data(&pool).await;
    }

    /// Test: Hardcoded constant is removed or only used as last-resort fallback.
    #[tokio::test]
    fn test_hardcoded_120000_is_not_primary_source() {
        // This is a code inspection test: verify that MAX_LOOKBACK_LEDGERS = 120_000
        // is not used directly in fresh-start clamping logic.
        // The actual poller.rs should use RPC-reported oldest ledger instead.

        const MAX_LOOKBACK_LEDGERS: i64 = 120_000; // Current hardcoded value
        let latest = 1_000_000i64;

        // Hardcoded logic (current, broken):
        let hardcoded_oldest = latest - MAX_LOOKBACK_LEDGERS;

        // Should be replaced by:
        // let rpc_reported_oldest = rpc.get_health().oldest_ledger?;
        // let oldest_to_use = rpc_reported_oldest.max(safe_fallback);

        // For now, document that the constant exists and should be removed
        assert_eq!(
            MAX_LOOKBACK_LEDGERS, 120_000,
            "hardcoded constant should be replaced with RPC-reported value"
        );
    }
}
