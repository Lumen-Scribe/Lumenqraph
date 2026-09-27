//! Test for issue #388: reenrich panics on its first row due to JSONB/String type mismatch
//!
//! This test verifies the fix for reading JSONB columns (decoded_topics and decoded_value)
//! using the correct sqlx type conversion to prevent panics.

#[cfg(test)]
mod reenrich_jsonb_fix_tests {
    use chrono::Utc;
    use serde_json::json;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::Row;

    async fn setup_test_db() -> sqlx::PgPool {
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://localhost/lumenqraph_test".to_string());

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
            .expect("Failed to connect to test database");

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

    async fn cleanup_test_data(pool: &sqlx::PgPool, contract_id: &str) {
        sqlx::query("DELETE FROM events WHERE contract_id = $1")
            .bind(contract_id)
            .execute(pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore] // Requires test database setup
    async fn test_reenrich_does_not_panic_on_jsonb_read() {
        // Issue #388: Before the fix, reading JSONB columns as String caused panics
        // This test verifies that we can read JSONB without panicking
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        let now = Utc::now();
        let decoded_topics = json!(["transfer", "GFROM", "GTO"]);
        let decoded_value = json!("1000");

        // Insert event with JSONB columns (not as text)
        sqlx::query(
            "INSERT INTO events
             (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
              decoded_topics, event_name, value, decoded_value, tx_hash, in_successful_call, paging_token)
             VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, $10::jsonb, $11, $12, $13)",
        )
        .bind("test_event")
        .bind(contract_id)
        .bind(1000i64)
        .bind(now)
        .bind("contract")
        .bind("[]") // topics as jsonb
        .bind(decoded_topics.to_string()) // decoded_topics as jsonb
        .bind("transfer") // event_name
        .bind("") // value
        .bind(decoded_value.to_string()) // decoded_value as jsonb
        .bind("tx_hash")
        .bind(true)
        .bind("paging_token")
        .execute(&pool)
        .await
        .expect("Failed to insert event");

        // Query EXACTLY as reenrich.rs does
        let rows = sqlx::query(
            "SELECT event_id, contract_id, decoded_topics, event_name, decoded_value
             FROM events
             WHERE contract_id = $1 AND enriched IS NULL AND event_name IS NOT NULL
             ORDER BY ledger, paging_token
             LIMIT 10",
        )
        .bind(contract_id)
        .fetch_all(&pool)
        .await
        .expect("Failed to query events");

        assert_eq!(rows.len(), 1, "Should find one event");

        let row = &rows[0];

        // WRONG way (causes panic): reading JSONB as String
        // This would panic: row.get::<String, _>("decoded_topics")

        // CORRECT way: use try_get with proper JSON types
        let event_id: String = row.try_get("event_id").expect("Should read event_id");
        assert_eq!(event_id, "test_event");

        let contract_id_read: String = row.try_get("contract_id").expect("Should read contract_id");
        assert_eq!(contract_id_read, contract_id);

        // This is the fix: decode JSONB as sqlx::types::Json<Value>
        let decoded_topics_json: Result<sqlx::types::Json<serde_json::Value>, _> =
            row.try_get("decoded_topics");
        assert!(
            decoded_topics_json.is_ok(),
            "Should successfully read decoded_topics as JSONB without panic"
        );

        let decoded_value_json: Result<sqlx::types::Json<serde_json::Value>, _> =
            row.try_get("decoded_value");
        assert!(
            decoded_value_json.is_ok(),
            "Should successfully read decoded_value as JSONB without panic"
        );

        // Verify we got the right values
        let topics_value = decoded_topics_json.unwrap().0;
        assert_eq!(topics_value[0].as_str().unwrap(), "transfer");

        let value_value = decoded_value_json.unwrap().0;
        assert_eq!(value_value.as_str().unwrap(), "1000");

        cleanup_test_data(&pool, contract_id).await;
    }

    #[tokio::test]
    #[ignore] // Requires test database setup
    async fn test_reenrich_handles_mixed_jsonb_types() {
        // Verify that decoded_value can be different JSON types (string, object, array)
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        let now = Utc::now();
        let test_cases = vec![
            ("event_string", json!("1000")),
            ("event_object", json!({"amount": "1000"})),
            ("event_array", json!([100, 200, 300])),
            ("event_null", json!(null)),
        ];

        for (event_id, decoded_value) in test_cases {
            let decoded_topics = json!(["transfer"]);

            sqlx::query(
                "INSERT INTO events
                 (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
                  decoded_topics, event_name, value, decoded_value, tx_hash, in_successful_call, paging_token)
                 VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, $10::jsonb, $11, $12, $13)",
            )
            .bind(event_id)
            .bind(contract_id)
            .bind(1000i64 + (event_id.chars().last().unwrap() as i64 - '0' as i64))
            .bind(now)
            .bind("contract")
            .bind("[]")
            .bind(decoded_topics.to_string())
            .bind("transfer")
            .bind("")
            .bind(decoded_value.to_string())
            .bind("tx_hash")
            .bind(true)
            .bind(event_id)
            .execute(&pool)
            .await
            .expect("Failed to insert event");
        }

        // Read all events - should not panic regardless of JSON type
        let rows = sqlx::query(
            "SELECT event_id, decoded_value FROM events
             WHERE contract_id = $1 AND enriched IS NULL AND event_name IS NOT NULL
             ORDER BY ledger",
        )
        .bind(contract_id)
        .fetch_all(&pool)
        .await
        .expect("Failed to query events");

        assert_eq!(
            rows.len(),
            4,
            "Should find all 4 events with different JSON types"
        );

        for row in rows {
            let _event_id: String = row.try_get("event_id").unwrap();
            let _decoded_value: Result<sqlx::types::Json<serde_json::Value>, _> =
                row.try_get("decoded_value");

            // Should not panic on any type
            assert!(
                _decoded_value.is_ok(),
                "Should read decoded_value as JSON regardless of type"
            );
        }

        cleanup_test_data(&pool, contract_id).await;
    }

    #[tokio::test]
    #[ignore] // Requires test database setup
    async fn test_decoded_topics_as_jsonb_array() {
        // Verify decoded_topics is correctly read as a JSON array
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        let now = Utc::now();

        // Complex decoded_topics with multiple nested elements
        let decoded_topics = json!([
            "complex_event",
            "GADDRESS1",
            "GADDRESS2",
            {"nested": "object"},
            [1, 2, 3]
        ]);

        sqlx::query(
            "INSERT INTO events
             (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
              decoded_topics, event_name, value, decoded_value, tx_hash, in_successful_call, paging_token)
             VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, $10::jsonb, $11, $12, $13)",
        )
        .bind("complex_event")
        .bind(contract_id)
        .bind(1000i64)
        .bind(now)
        .bind("contract")
        .bind("[]")
        .bind(decoded_topics.to_string())
        .bind("complex_event")
        .bind("")
        .bind("null")
        .bind("tx_hash")
        .bind(true)
        .bind("paging_token")
        .execute(&pool)
        .await
        .expect("Failed to insert event");

        let row = sqlx::query(
            "SELECT decoded_topics FROM events WHERE event_id = $1",
        )
        .bind("complex_event")
        .fetch_one(&pool)
        .await
        .expect("Failed to query");

        let decoded_topics_result: Result<sqlx::types::Json<serde_json::Value>, _> =
            row.try_get("decoded_topics");

        assert!(decoded_topics_result.is_ok(), "Should read complex decoded_topics");

        let topics_value = decoded_topics_result.unwrap().0;
        assert!(topics_value.is_array(), "decoded_topics should be an array");
        assert_eq!(topics_value.as_array().unwrap().len(), 5, "Should have 5 elements");
        assert_eq!(
            topics_value[0].as_str().unwrap(),
            "complex_event",
            "First element should be event name"
        );

        cleanup_test_data(&pool, contract_id).await;
    }
}
