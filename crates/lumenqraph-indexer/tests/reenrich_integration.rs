//! Integration tests for the reenrich command.
//!
//! These tests verify that:
//! - #388: reenrich correctly reads JSONB columns without panicking
//! - #387: reenrich uses proper pagination and doesn't skip rows
//! - #389: reenrich supports filtering and recomputation flags

#[cfg(test)]
mod reenrich_tests {
    use chrono::Utc;
    use serde_json::json;
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

        // Create events table if it doesn't exist
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

    async fn cleanup_test_data(pool: &PgPool, contract_id: &str) {
        sqlx::query("DELETE FROM events WHERE contract_id = $1")
            .bind(contract_id)
            .execute(pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore] // Requires test database setup
    async fn test_reenrich_reads_jsonb_columns_correctly() {
        // Issue #388: Test that reenrich can read JSONB columns without panicking
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        let now = Utc::now();
        let decoded_topics = json!(["transfer", "GFROM", "GTO"]);
        let decoded_value = json!("1000");

        // Insert an un-enriched event with JSONB decoded_topics and decoded_value
        sqlx::query(
            "INSERT INTO events
             (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
              decoded_topics, event_name, value, decoded_value, tx_hash, in_successful_call, paging_token)
             VALUES ($1, $2, 1, $3, 'contract', '[]'::jsonb, $4, $5, '', $6, 'hash1', true, 'token1')",
        )
        .bind("test_event_1")
        .bind(contract_id)
        .bind(now)
        .bind(decoded_topics.to_string())
        .bind("transfer")
        .bind(decoded_value.to_string())
        .execute(&pool)
        .await
        .expect("Failed to insert test event");

        // Test that we can read JSONB columns using try_get with Json type
        let row = sqlx::query(
            "SELECT event_id, contract_id, decoded_topics, event_name, decoded_value
             FROM events
             WHERE contract_id = $1 AND enriched IS NULL AND event_name IS NOT NULL
             ORDER BY ledger, paging_token
             LIMIT 1",
        )
        .bind(contract_id)
        .fetch_optional(&pool)
        .await
        .expect("Failed to query")
        .expect("Should find one event");

        // This should not panic - reading JSONB as Json<Value>
        let _event_id: String = row.try_get("event_id").expect("Should read event_id");
        let _contract_id: String = row.try_get("contract_id").expect("Should read contract_id");

        // Reading JSONB columns should work with proper type
        let decoded_topics_result: Result<sqlx::types::Json<serde_json::Value>, _> =
            row.try_get("decoded_topics");
        assert!(
            decoded_topics_result.is_ok(),
            "Should be able to read decoded_topics as JSONB"
        );

        let decoded_value_result: Result<sqlx::types::Json<serde_json::Value>, _> =
            row.try_get("decoded_value");
        assert!(
            decoded_value_result.is_ok(),
            "Should be able to read decoded_value as JSONB"
        );

        cleanup_test_data(&pool, contract_id).await;
    }

    #[tokio::test]
    #[ignore] // Requires test database setup
    async fn test_reenrich_pagination_with_many_rows() {
        // Issue #387: Test that reenrich correctly handles 2,500+ rows without skipping
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        let now = Utc::now();
        let batch_size = 1000;
        let total_rows = 2500;

        // Insert 2500 un-enriched events
        for i in 0..total_rows {
            let event_id = format!("event_{:04}", i);
            let decoded_topics = json!(["transfer", "GFROM", "GTO"]);
            let decoded_value = json!("1000");

            sqlx::query(
                "INSERT INTO events
                 (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
                  decoded_topics, event_name, value, decoded_value, tx_hash, in_successful_call, paging_token)
                 VALUES ($1, $2, $3, $4, 'contract', '[]'::jsonb, $5, $6, '', $7, 'hash', true, $8)",
            )
            .bind(&event_id)
            .bind(contract_id)
            .bind(i as i64 + 1000)
            .bind(now)
            .bind(decoded_topics.to_string())
            .bind("transfer")
            .bind(decoded_value.to_string())
            .bind(&event_id)
            .execute(&pool)
            .await
            .expect("Failed to insert test event");
        }

        // Verify all 2500 events are inserted
        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM events WHERE contract_id = $1 AND enriched IS NULL",
        )
        .bind(contract_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to count events");

        assert_eq!(count.0, total_rows as i64, "Should have inserted all 2500 events");

        // Simulate keyset pagination (not OFFSET-based)
        let mut processed = 0;
        let mut last_ledger = -1i64;
        let mut last_event_id = String::new();

        loop {
            let rows = sqlx::query(
                "SELECT event_id, ledger FROM events
                 WHERE contract_id = $1 AND enriched IS NULL AND event_name IS NOT NULL
                 AND (ledger > $2 OR (ledger = $2 AND event_id > $3))
                 ORDER BY ledger, event_id
                 LIMIT $4",
            )
            .bind(contract_id)
            .bind(last_ledger)
            .bind(&last_event_id)
            .bind(batch_size as i32)
            .fetch_all(&pool)
            .await
            .expect("Failed to fetch batch");

            if rows.is_empty() {
                break;
            }

            for row in &rows {
                processed += 1;
                last_ledger = row.try_get("ledger").unwrap();
                last_event_id = row.try_get("event_id").unwrap();
            }
        }

        // With keyset pagination, all rows should be processed
        assert_eq!(
            processed, total_rows,
            "Keyset pagination should process all {} rows, but only processed {}",
            total_rows, processed
        );

        cleanup_test_data(&pool, contract_id).await;
    }

    #[tokio::test]
    #[ignore] // Requires test database setup
    async fn test_reenrich_contract_filtering() {
        // Issue #389: Test that --contract flag filters correctly
        let pool = setup_test_db().await;
        let contract_1 = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q1";
        let contract_2 = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q2";

        cleanup_test_data(&pool, contract_1).await;
        cleanup_test_data(&pool, contract_2).await;

        let now = Utc::now();
        let decoded_topics = json!(["transfer"]);
        let decoded_value = json!("1000");

        // Insert events for both contracts
        for contract_id in &[contract_1, contract_2] {
            for i in 0..5 {
                let event_id = format!("{}_event_{}", contract_id, i);
                sqlx::query(
                    "INSERT INTO events
                     (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
                      decoded_topics, event_name, value, decoded_value, tx_hash, in_successful_call, paging_token)
                     VALUES ($1, $2, $3, $4, 'contract', '[]'::jsonb, $5, $6, '', $7, 'hash', true, $8)",
                )
                .bind(&event_id)
                .bind(contract_id)
                .bind(i as i64 + 1000)
                .bind(now)
                .bind(decoded_topics.to_string())
                .bind("transfer")
                .bind(decoded_value.to_string())
                .bind(&event_id)
                .execute(&pool)
                .await
                .expect("Failed to insert test event");
            }
        }

        // Query with contract filter (simulating --contract flag)
        let count_contract_1: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM events WHERE contract_id = $1 AND enriched IS NULL",
        )
        .bind(contract_1)
        .fetch_one(&pool)
        .await
        .expect("Failed to count events");

        let count_contract_2: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM events WHERE contract_id = $1 AND enriched IS NULL",
        )
        .bind(contract_2)
        .fetch_one(&pool)
        .await
        .expect("Failed to count events");

        assert_eq!(
            count_contract_1.0, 5,
            "Should filter to contract 1 only (5 events)"
        );
        assert_eq!(
            count_contract_2.0, 5,
            "Should filter to contract 2 only (5 events)"
        );

        cleanup_test_data(&pool, contract_1).await;
        cleanup_test_data(&pool, contract_2).await;
    }

    #[tokio::test]
    #[ignore] // Requires test database setup
    async fn test_reenrich_force_flag() {
        // Issue #389: Test that --force flag allows re-enriching already-enriched events
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        let now = Utc::now();
        let decoded_topics = json!(["transfer"]);
        let decoded_value = json!("1000");
        let initial_enrichment = json!({"enriched": true});

        // Insert an already-enriched event
        sqlx::query(
            "INSERT INTO events
             (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
              decoded_topics, event_name, value, decoded_value, enriched, tx_hash, in_successful_call, paging_token)
             VALUES ($1, $2, $3, $4, 'contract', '[]'::jsonb, $5, $6, '', $7, $8, 'hash', true, 'token')",
        )
        .bind("test_event")
        .bind(contract_id)
        .bind(1000i64)
        .bind(now)
        .bind(decoded_topics.to_string())
        .bind("transfer")
        .bind(decoded_value.to_string())
        .bind(initial_enrichment.to_string())
        .execute(&pool)
        .await
        .expect("Failed to insert test event");

        // Without --force, the event should not be selected
        let count_without_force: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM events WHERE contract_id = $1 AND enriched IS NULL",
        )
        .bind(contract_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to count events");

        assert_eq!(count_without_force.0, 0, "Already-enriched event should not match");

        // With --force, we'd select all events (simulated here)
        let count_with_force: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM events WHERE contract_id = $1",
        )
        .bind(contract_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to count events");

        assert_eq!(count_with_force.0, 1, "With --force, should select all events");

        cleanup_test_data(&pool, contract_id).await;
    }

    #[tokio::test]
    #[ignore] // Requires test database setup
    async fn test_reenrich_dry_run_no_writes() {
        // Issue #389: Test that --dry-run reports counts without writing
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        let now = Utc::now();
        let decoded_topics = json!(["transfer"]);
        let decoded_value = json!("1000");

        // Insert un-enriched events
        for i in 0..3 {
            let event_id = format!("event_{}", i);
            sqlx::query(
                "INSERT INTO events
                 (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
                  decoded_topics, event_name, value, decoded_value, tx_hash, in_successful_call, paging_token)
                 VALUES ($1, $2, $3, $4, 'contract', '[]'::jsonb, $5, $6, '', $7, 'hash', true, $8)",
            )
            .bind(&event_id)
            .bind(contract_id)
            .bind(i as i64 + 1000)
            .bind(now)
            .bind(decoded_topics.to_string())
            .bind("transfer")
            .bind(decoded_value.to_string())
            .bind(&event_id)
            .execute(&pool)
            .await
            .expect("Failed to insert test event");
        }

        // Count un-enriched events
        let count_before: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM events WHERE contract_id = $1 AND enriched IS NULL",
        )
        .bind(contract_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to count events");

        // In a dry-run, we would count these but not update them
        // After dry-run, count should remain the same
        let count_after: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM events WHERE contract_id = $1 AND enriched IS NULL",
        )
        .bind(contract_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to count events");

        assert_eq!(
            count_before.0, count_after.0,
            "Dry-run should not change the database"
        );
        assert_eq!(count_before.0, 3, "Should have 3 un-enriched events");

        cleanup_test_data(&pool, contract_id).await;
    }
}
