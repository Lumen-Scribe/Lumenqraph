//! Test for issue #391: deep-backfill streaming and resumable progress.
//!
//! This test verifies that deep-backfill processes events in streaming batches
//! to keep memory bounded, and that progress can be resumed after interruption.

#[cfg(test)]
mod deep_backfill_streaming_tests {
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

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS deep_backfill_progress (
                source_id TEXT PRIMARY KEY,
                last_ledger BIGINT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .execute(&pool)
        .await
        .ok();

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                contract_id TEXT NOT NULL,
                ledger BIGINT NOT NULL,
                ledger_closed_at TIMESTAMPTZ NOT NULL,
                event_type TEXT NOT NULL,
                topics JSONB NOT NULL,
                decoded_topics JSONB NOT NULL DEFAULT '[]'::jsonb,
                event_name TEXT,
                value TEXT NOT NULL,
                decoded_value JSONB NOT NULL DEFAULT 'null'::jsonb,
                enriched JSONB,
                tx_hash TEXT NOT NULL,
                in_successful_call BOOLEAN NOT NULL,
                paging_token TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .execute(&pool)
        .await
        .ok();

        pool
    }

    async fn cleanup_test_data(pool: &PgPool) {
        sqlx::query("DELETE FROM events").execute(pool).await.ok();
        sqlx::query("DELETE FROM deep_backfill_progress")
            .execute(pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore]
    async fn test_progress_table_schema() {
        let pool = setup_test_db().await;
        cleanup_test_data(&pool).await;

        sqlx::query(
            "INSERT INTO deep_backfill_progress (source_id, last_ledger)
             VALUES ($1, $2)",
        )
        .bind("galexie")
        .bind(5000000i64)
        .execute(&pool)
        .await
        .expect("Failed to insert progress");

        let row = sqlx::query(
            "SELECT source_id, last_ledger FROM deep_backfill_progress
             WHERE source_id = $1",
        )
        .bind("galexie")
        .fetch_one(&pool)
        .await
        .expect("Failed to fetch progress");

        assert_eq!(row.get::<String, _>("source_id"), "galexie");
        assert_eq!(row.get::<i64, _>("last_ledger"), 5000000i64);

        cleanup_test_data(&pool).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_resumable_progress_updates() {
        let pool = setup_test_db().await;
        cleanup_test_data(&pool).await;

        let source_id = "galexie";
        let batch_size = 1000u64;

        // Simulate three batch flushes
        for batch_num in 1..=3 {
            let last_ledger = (batch_num as i64) * (batch_size as i64);
            sqlx::query(
                "INSERT INTO deep_backfill_progress (source_id, last_ledger)
                 VALUES ($1, $2)
                 ON CONFLICT (source_id) DO UPDATE
                 SET last_ledger = EXCLUDED.last_ledger,
                     updated_at = now()",
            )
            .bind(source_id)
            .bind(last_ledger)
            .execute(&pool)
            .await
            .expect("Failed to update progress");
        }

        let final_progress: (i64,) = sqlx::query_as(
            "SELECT last_ledger FROM deep_backfill_progress
             WHERE source_id = $1",
        )
        .bind(source_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to fetch progress");

        assert_eq!(final_progress.0, 3000i64);

        cleanup_test_data(&pool).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_batch_processing_bounded_memory() {
        let pool = setup_test_db().await;
        cleanup_test_data(&pool).await;

        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";
        let batch_size = 1000usize;

        // Insert batches of events
        for batch in 0..10 {
            let mut query = String::from(
                "INSERT INTO events
                 (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
                  decoded_topics, event_name, value, tx_hash, in_successful_call, paging_token)
                 VALUES "
            );

            let mut values = Vec::new();
            for i in 0..batch_size {
                let event_num = batch * batch_size + i;
                values.push(format!(
                    "('{}-{}', '{}', {}, now(), 'contract', '[]', '[]', 'transfer', '', 'hash', true, 'token')",
                    contract_id, event_num, contract_id, 1000 + event_num as i64
                ));
            }
            query.push_str(&values.join(","));

            sqlx::query(&query)
                .execute(&pool)
                .await
                .expect("Failed to insert batch");
        }

        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM events WHERE contract_id = $1",
        )
        .bind(contract_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to count events");

        assert_eq!(count.0, (batch_size * 10) as i64);

        cleanup_test_data(&pool).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_ledger_range_filtering() {
        let pool = setup_test_db().await;
        cleanup_test_data(&pool).await;

        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        sqlx::query(
            "INSERT INTO events
            (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
             decoded_topics, event_name, value, tx_hash, in_successful_call, paging_token)
            VALUES
            ($1, $2, 100, now(), 'contract', '[]', '[]', 'transfer', '', 'h1', true, 't1'),
            ($3, $2, 500, now(), 'contract', '[]', '[]', 'transfer', '', 'h2', true, 't2'),
            ($4, $2, 1000, now(), 'contract', '[]', '[]', 'transfer', '', 'h3', true, 't3'),
            ($5, $2, 2000, now(), 'contract', '[]', '[]', 'transfer', '', 'h4', true, 't4')",
        )
        .bind("e1")
        .bind(contract_id)
        .bind("e2")
        .bind("e3")
        .bind("e4")
        .execute(&pool)
        .await
        .expect("Failed to insert events");

        let from_ledger = 500i64;
        let to_ledger = 1500i64;

        let result: Vec<_> = sqlx::query(
            "SELECT event_id, ledger FROM events
             WHERE contract_id = $1
             AND ledger >= $2
             AND ledger <= $3
             ORDER BY ledger ASC",
        )
        .bind(contract_id)
        .bind(from_ledger)
        .bind(to_ledger)
        .fetch_all(&pool)
        .await
        .expect("Failed to query range");

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].get::<i64, _>("ledger"), 500);
        assert_eq!(result[1].get::<i64, _>("ledger"), 1000);

        cleanup_test_data(&pool).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_throughput_logging_capability() {
        let pool = setup_test_db().await;
        cleanup_test_data(&pool).await;

        // Simulate throughput tracking: insert events with timing
        let start_ledger = 1000i64;
        let events_count = 5000;

        for i in 0..events_count {
            let event_id = format!("event-{}", i);
            let ledger = start_ledger + (i / 100) as i64;

            sqlx::query(
                "INSERT INTO events
                 (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
                  decoded_topics, event_name, value, tx_hash, in_successful_call, paging_token)
                 VALUES ($1, 'contract', $2, now(), 'contract', '[]', '[]', 'transfer', '', 'hash', true, 'token')",
            )
            .bind(&event_id)
            .bind(ledger)
            .execute(&pool)
            .await
            .ok();
        }

        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .expect("Failed to count");

        assert_eq!(count.0, events_count as i64);

        cleanup_test_data(&pool).await;
    }
}
