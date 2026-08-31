//! Integration tests for JSONB topic filtering in the events table.
//!
//! These tests verify that the positional topic filtering using JSONB containment
//! works correctly across various edge cases, including null topics, empty arrays,
//! and multi-position matches.

#[cfg(test)]
mod topic_filtering_tests {
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

        // Ensure the events table exists with the schema we expect
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
        .ok(); // Ignore if table already exists

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
    async fn test_topic0_filtering() {
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        // Insert test events with different topic0 values
        sqlx::query(
            "INSERT INTO events
            (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
             decoded_topics, event_name, value, tx_hash, in_successful_call, paging_token)
            VALUES
            ($1, $2, 1, now(), 'contract', '[]', $3, 'transfer', '', 'hash1', true, 'token1'),
            ($4, $2, 2, now(), 'contract', '[]', $5, 'transfer', '', 'hash2', true, 'token2'),
            ($6, $2, 3, now(), 'contract', '[]', $7, 'mint', '', 'hash3', true, 'token3')",
        )
        .bind("event1")
        .bind(contract_id)
        .bind(r#"["transfer"]"#)
        .bind("event2")
        .bind(r#"["transfer"]"#)
        .bind("event3")
        .bind(r#"["mint"]"#)
        .execute(&pool)
        .await
        .expect("Failed to insert test events");

        // Test topic0 filtering
        let result: Vec<_> = sqlx::query(
            "SELECT event_id FROM events
             WHERE contract_id = $1
             AND decoded_topics @> jsonb_build_array($2::jsonb)
             ORDER BY ledger ASC",
        )
        .bind(contract_id)
        .bind(r#""transfer""#)
        .fetch_all(&pool)
        .await
        .expect("Failed to query");

        assert_eq!(
            result.len(),
            2,
            "Should find 2 transfer events when filtering by topic0"
        );
        assert_eq!(result[0].get::<String, _>("event_id"), "event1");
        assert_eq!(result[1].get::<String, _>("event_id"), "event2");

        cleanup_test_data(&pool, contract_id).await;
    }

    #[tokio::test]
    #[ignore] // Requires test database setup
    async fn test_topic1_filtering() {
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        // Insert test events with different topic1 values
        sqlx::query(
            "INSERT INTO events
            (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
             decoded_topics, event_name, value, tx_hash, in_successful_call, paging_token)
            VALUES
            ($1, $2, 1, now(), 'contract', '[]', $3, NULL, '', 'hash1', true, 'token1'),
            ($4, $2, 2, now(), 'contract', '[]', $5, NULL, '', 'hash2', true, 'token2'),
            ($6, $2, 3, now(), 'contract', '[]', $7, NULL, '', 'hash3', true, 'token3')",
        )
        .bind("event1")
        .bind(contract_id)
        .bind(r#"["transfer", "alice"]"#)
        .bind("event2")
        .bind(r#"["transfer", "bob"]"#)
        .bind("event3")
        .bind(r#"["transfer", "charlie"]"#)
        .execute(&pool)
        .await
        .expect("Failed to insert test events");

        // Test topic1 filtering for "bob"
        let result: Vec<_> = sqlx::query(
            "SELECT event_id FROM events
             WHERE contract_id = $1
             AND decoded_topics @> jsonb_build_array(jsonb_null::jsonb, $2::jsonb)
             ORDER BY ledger ASC",
        )
        .bind(contract_id)
        .bind(r#""bob""#)
        .fetch_all(&pool)
        .await
        .expect("Failed to query");

        assert_eq!(
            result.len(),
            1,
            "Should find 1 event when filtering by topic1='bob'"
        );
        assert_eq!(result[0].get::<String, _>("event_id"), "event2");

        cleanup_test_data(&pool, contract_id).await;
    }

    #[tokio::test]
    #[ignore] // Requires test database setup
    async fn test_topic_filtering_with_null_topics() {
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        // Insert events with varying topic counts
        sqlx::query(
            "INSERT INTO events
            (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
             decoded_topics, event_name, value, tx_hash, in_successful_call, paging_token)
            VALUES
            ($1, $2, 1, now(), 'contract', '[]', $3, NULL, '', 'hash1', true, 'token1'),
            ($4, $2, 2, now(), 'contract', '[]', $5, NULL, '', 'hash2', true, 'token2'),
            ($6, $2, 3, now(), 'contract', '[]', $7, NULL, '', 'hash3', true, 'token3')",
        )
        .bind("event1")
        .bind(contract_id)
        .bind(r#"[]"#) // Empty topics
        .bind("event2")
        .bind(r#"["transfer"]"#) // Only topic0
        .bind("event3")
        .bind(r#"["transfer", "alice", "bob"]"#) // Multiple topics
        .execute(&pool)
        .await
        .expect("Failed to insert test events");

        // Query topic1 filtering - should only match events with at least 2 topics
        let result: Vec<_> = sqlx::query(
            "SELECT event_id FROM events
             WHERE contract_id = $1
             AND decoded_topics @> jsonb_build_array(jsonb_null::jsonb, $2::jsonb)
             ORDER BY ledger ASC",
        )
        .bind(contract_id)
        .bind(r#""alice""#)
        .fetch_all(&pool)
        .await
        .expect("Failed to query");

        assert_eq!(
            result.len(),
            1,
            "Should only match events with at least 2 topics"
        );
        assert_eq!(result[0].get::<String, _>("event_id"), "event3");

        cleanup_test_data(&pool, contract_id).await;
    }

    #[tokio::test]
    #[ignore] // Requires test database setup
    async fn test_multi_topic_filtering() {
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        // Insert events
        sqlx::query(
            "INSERT INTO events
            (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
             decoded_topics, event_name, value, tx_hash, in_successful_call, paging_token)
            VALUES
            ($1, $2, 1, now(), 'contract', '[]', $3, NULL, '', 'hash1', true, 'token1'),
            ($4, $2, 2, now(), 'contract', '[]', $5, NULL, '', 'hash2', true, 'token2'),
            ($6, $2, 3, now(), 'contract', '[]', $7, NULL, '', 'hash3', true, 'token3')",
        )
        .bind("event1")
        .bind(contract_id)
        .bind(r#"["transfer", "alice", "bob"]"#)
        .bind("event2")
        .bind(r#"["transfer", "alice", "charlie"]"#)
        .bind("event3")
        .bind(r#"["transfer", "bob", "alice"]"#)
        .execute(&pool)
        .await
        .expect("Failed to insert test events");

        // Filter by topic0='transfer' AND topic1='alice'
        let result: Vec<_> = sqlx::query(
            "SELECT event_id FROM events
             WHERE contract_id = $1
             AND decoded_topics @> jsonb_build_array($2::jsonb)
             AND decoded_topics @> jsonb_build_array(jsonb_null::jsonb, $3::jsonb)
             ORDER BY ledger ASC",
        )
        .bind(contract_id)
        .bind(r#""transfer""#)
        .bind(r#""alice""#)
        .fetch_all(&pool)
        .await
        .expect("Failed to query");

        assert_eq!(
            result.len(),
            2,
            "Should find events matching both topic0 and topic1"
        );
        assert_eq!(result[0].get::<String, _>("event_id"), "event1");
        assert_eq!(result[1].get::<String, _>("event_id"), "event2");

        cleanup_test_data(&pool, contract_id).await;
    }
}
