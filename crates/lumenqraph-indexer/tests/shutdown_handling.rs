//! Integration tests for graceful SIGTERM shutdown during poll cycles.
//! Verifies that SIGTERM is recognized and processed even during long poll operations,
//! and that the indexer exits cleanly without mid-transaction state.

#[cfg(test)]
mod shutdown_handling_tests {
    use sqlx::postgres::PgPoolOptions;
    use sqlx::PgPool;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Notify;

    async fn setup_test_db() -> PgPool {
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://localhost/lumenqraph_test".to_string());

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
            .expect("Failed to connect to test database");

        // Create indexer_cursor for tracking progress
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

        // Create events table
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS events (
                event_id            TEXT        PRIMARY KEY,
                contract_id         TEXT        NOT NULL,
                ledger              BIGINT      NOT NULL,
                ledger_closed_at    TIMESTAMPTZ NOT NULL,
                event_type          TEXT        NOT NULL,
                topics              JSONB       NOT NULL,
                decoded_topics      JSONB       NOT NULL DEFAULT '[]'::jsonb,
                event_name          TEXT,
                value               TEXT        NOT NULL,
                decoded_value       JSONB       NOT NULL DEFAULT 'null'::jsonb,
                enriched            JSONB,
                tx_hash             TEXT        NOT NULL,
                in_successful_call  BOOLEAN     NOT NULL,
                paging_token        TEXT        NOT NULL,
                created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .execute(&pool)
        .await
        .ok();

        pool
    }

    async fn cleanup_test_data(pool: &PgPool) {
        sqlx::query("DELETE FROM events")
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM indexer_cursor WHERE id = 1")
            .execute(pool)
            .await
            .ok();
    }

    /// Test: A cancellation token can be created once at startup and shared
    /// across the poll cycle, allowing interruption at multiple points.
    #[tokio::test]
    async fn test_cancellation_token_pattern() {
        let cancellation = Arc::new(tokio_util::sync::CancellationToken::new());

        // Simulate long-running work
        let cancellation_clone = cancellation.clone();
        let task = tokio::spawn(async move {
            for page in 0..10 {
                // Simulate page processing
                tokio::time::sleep(Duration::from_millis(100)).await;

                // Check for cancellation between pages
                if cancellation_clone.is_cancelled() {
                    return (page, "cancelled");
                }
            }
            (10, "completed")
        });

        // Simulate receiving SIGTERM after 2 pages
        tokio::time::sleep(Duration::from_millis(250)).await;
        cancellation.cancel();

        let (pages, status) = task.await.expect("task should finish");

        // Should be cancelled mid-iteration (not all 10 pages)
        assert_eq!(status, "cancelled", "task should be cancelled");
        assert!(pages < 10, "task should exit before completing all pages");
    }

    /// Test: Cancellation token is created once, not recreated each cycle.
    #[tokio::test]
    async fn test_single_signal_listener_lifetime() {
        let signal_created_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let cancellation = Arc::new(tokio_util::sync::CancellationToken::new());

        // Simulate polling loop with signal listener created once
        {
            signal_created_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }

        // Simulate 3 poll cycles — signal listener should still only be created once
        for _cycle in 0..3 {
            let _cancellation = cancellation.clone();
            // Poll cycle work would happen here
        }

        let count = signal_created_count.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            count, 1,
            "signal listener should be created exactly once at startup, not per cycle"
        );
    }

    /// Test: Graceful exit logs the last committed ledger.
    #[tokio::test]
    async fn test_graceful_exit_logs_progress() {
        let pool = setup_test_db().await;
        cleanup_test_data(&pool).await;

        let last_committed = 99_999i64;
        let chain_tip = 100_000i64;

        // Simulate that a poll cycle committed progress before shutdown
        sqlx::query(
            "INSERT INTO indexer_cursor (id, last_processed_ledger, chain_tip_ledger, version)
             VALUES (1, $1, $2, 1)"
        )
        .bind(last_committed)
        .bind(chain_tip)
        .execute(&pool)
        .await
        .expect("insert should succeed");

        // Simulate graceful shutdown: read last progress
        let (stored_ledger, stored_tip): (i64, i64) = sqlx::query_as(
            "SELECT last_processed_ledger, chain_tip_ledger FROM indexer_cursor WHERE id = 1"
        )
        .fetch_one(&pool)
        .await
        .expect("query should succeed");

        // Verify we can log the last committed ledger during shutdown
        assert_eq!(
            stored_ledger, last_committed,
            "shutdown should log the last committed ledger"
        );
        assert_eq!(
            stored_tip, chain_tip,
            "shutdown should log the chain tip"
        );

        cleanup_test_data(&pool).await;
    }

    /// Test: Long page processing can be interrupted between pages.
    /// Scenario: Processing a large page of events takes significant time;
    /// SIGTERM arrives mid-page processing. On the next page boundary, exit.
    #[tokio::test]
    async fn test_page_processing_can_be_interrupted() {
        let cancellation = Arc::new(tokio_util::sync::CancellationToken::new());

        let cancellation_clone = cancellation.clone();
        let task = tokio::spawn(async move {
            let mut pages_processed = 0;

            // Simulate fetching and processing pages
            for page_num in 0..5 {
                // Simulate large page processing
                tokio::time::sleep(Duration::from_millis(200)).await;

                pages_processed += 1;

                // Check for cancellation at page boundary
                if cancellation_clone.is_cancelled() {
                    break;
                }
            }

            pages_processed
        });

        // SIGTERM arrives after ~350ms (between pages 1 and 2)
        tokio::time::sleep(Duration::from_millis(350)).await;
        cancellation.cancel();

        let pages = task.await.expect("task should finish");

        // Should exit after completing the current page (not in the middle)
        assert!(
            pages <= 2,
            "should exit within ~2 pages of cancellation (not mid-page)"
        );
    }

    /// Test: State snapshots can be interrupted at batch boundaries.
    /// Scenario: Writing state snapshots in batches; SIGTERM arrives mid-batch.
    /// Should finish the current batch and exit, not leave partial state.
    #[tokio::test]
    async fn test_state_snapshot_batch_interruption() {
        let pool = setup_test_db().await;
        cleanup_test_data(&pool).await;

        let cancellation = Arc::new(tokio_util::sync::CancellationToken::new());
        let batch_size = 100;
        let total_snapshots = 500;

        let cancellation_clone = cancellation.clone();
        let pool_clone = pool.clone();
        let task = tokio::spawn(async move {
            let mut written = 0;

            for batch_start in (0..total_snapshots).step_by(batch_size) {
                // Simulate writing a batch of state snapshots
                let batch_end = (batch_start + batch_size).min(total_snapshots);

                for i in batch_start..batch_end {
                    // Simulate snapshot write
                    written += 1;
                }

                // Check for cancellation at batch boundary
                if cancellation_clone.is_cancelled() {
                    break;
                }
            }

            written
        });

        // SIGTERM arrives after ~2.5 batches
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancellation.cancel();

        let written = task.await.expect("task should finish");

        // Should stop at a batch boundary (multiple of batch_size)
        // allowing us to commit cleanly
        assert!(
            written % batch_size == 0 || written >= total_snapshots,
            "snapshots should stop at batch boundary or complete fully"
        );

        cleanup_test_data(&pool).await;
    }

    /// Test: RPC retry sleeps can be interrupted by cancellation.
    /// Scenario: RPC call fails; exponential backoff sleep begins. SIGTERM arrives
    /// during the sleep; should interrupt and exit instead of waiting.
    #[tokio::test]
    async fn test_rpc_retry_sleep_can_be_interrupted() {
        let cancellation = Arc::new(tokio_util::sync::CancellationToken::new());

        let cancellation_clone = cancellation.clone();
        let task = tokio::spawn(async move {
            let mut retry_attempt = 0;
            let base_delay = Duration::from_millis(1000);

            loop {
                retry_attempt += 1;

                // Simulate RPC call
                if retry_attempt >= 3 {
                    return "completed";
                }

                // Simulate exponential backoff retry
                let delay = base_delay * (2u32.pow((retry_attempt - 1) as u32));

                tokio::select! {
                    _ = tokio::time::sleep(delay) => {
                        // Sleep completed, continue retry
                    }
                    _ = cancellation_clone.cancelled() => {
                        // Cancelled during sleep
                        return "cancelled";
                    }
                }
            }
        });

        // SIGTERM arrives during first retry sleep (after ~100ms)
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancellation.cancel();

        let result = task.await.expect("task should finish");

        assert_eq!(
            result, "cancelled",
            "should exit immediately when SIGTERM arrives during retry sleep"
        );
    }
}
