//! Test for issue #387: reenrich skips most rows due to OFFSET pagination
//!
//! This test verifies the fix for pagination that uses keyset-based pagination
//! instead of OFFSET-based pagination, which incorrectly skips rows when the WHERE
//! clause filters change during iteration.

#[cfg(test)]
mod reenrich_keyset_pagination_tests {
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
    async fn test_offset_pagination_skips_rows() {
        // Issue #387: Demonstrate the problem with OFFSET pagination
        // When processing rows with a predicate that gets modified, OFFSET skips rows
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        let now = Utc::now();
        let batch_size = 50;
        let total_rows = 250;

        // Insert 250 un-enriched events
        for i in 0..total_rows {
            let event_id = format!("event_{:04}", i);
            let decoded_topics = json!(["transfer"]);
            let decoded_value = json!("1000");

            sqlx::query(
                "INSERT INTO events
                 (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
                  decoded_topics, event_name, value, decoded_value, tx_hash, in_successful_call, paging_token)
                 VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, $10::jsonb, $11, $12, $13)",
            )
            .bind(&event_id)
            .bind(contract_id)
            .bind(1000i64 + i as i64)
            .bind(now)
            .bind("contract")
            .bind("[]")
            .bind(decoded_topics.to_string())
            .bind("transfer")
            .bind("")
            .bind(decoded_value.to_string())
            .bind("tx_hash")
            .bind(true)
            .bind(&event_id)
            .execute(&pool)
            .await
            .expect("Failed to insert event");
        }

        // BROKEN approach: OFFSET-based pagination (what issue #387 is about)
        // When we enrich rows, they no longer match `enriched IS NULL`
        // So the next batch's OFFSET skips over them
        let mut processed_broken = 0;
        let mut offset = 0i32;

        loop {
            // This query selects rows where enriched IS NULL
            let rows = sqlx::query(
                "SELECT event_id, ledger FROM events
                 WHERE contract_id = $1 AND enriched IS NULL AND event_name IS NOT NULL
                 ORDER BY ledger, paging_token
                 LIMIT $2 OFFSET $3",
            )
            .bind(contract_id)
            .bind(batch_size as i32)
            .bind(offset)
            .fetch_all(&pool)
            .await
            .expect("Failed to fetch batch");

            if rows.is_empty() {
                break;
            }

            // Simulate enriching these rows (setting enriched != NULL)
            for row in &rows {
                let event_id: String = row.try_get("event_id").unwrap();
                sqlx::query("UPDATE events SET enriched = $1 WHERE event_id = $2")
                    .bind(json!({"enriched": true}).to_string())
                    .bind(&event_id)
                    .execute(&pool)
                    .await
                    .ok();

                processed_broken += 1;
            }

            // OFFSET increases regardless of how many rows matched the predicate
            offset += batch_size as i32;

            if processed_broken > 300 {
                // Safety check to avoid infinite loop
                break;
            }
        }

        // With OFFSET pagination, many rows are skipped when all batch rows match the predicate
        println!(
            "OFFSET pagination processed {} out of {} rows",
            processed_broken, total_rows
        );
        assert!(
            processed_broken < total_rows,
            "OFFSET pagination should skip rows (demonstrating the bug)"
        );

        cleanup_test_data(&pool, contract_id).await;
    }

    #[tokio::test]
    #[ignore] // Requires test database setup
    async fn test_keyset_pagination_processes_all_rows() {
        // Issue #387: Demonstrate the fix with keyset-based pagination
        // Using an immutable key (ledger, event_id) ensures no rows are skipped
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        let now = Utc::now();
        let batch_size = 50;
        let total_rows = 250;

        // Insert 250 un-enriched events
        for i in 0..total_rows {
            let event_id = format!("event_{:04}", i);
            let decoded_topics = json!(["transfer"]);
            let decoded_value = json!("1000");

            sqlx::query(
                "INSERT INTO events
                 (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
                  decoded_topics, event_name, value, decoded_value, tx_hash, in_successful_call, paging_token)
                 VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, $10::jsonb, $11, $12, $13)",
            )
            .bind(&event_id)
            .bind(contract_id)
            .bind(1000i64 + i as i64)
            .bind(now)
            .bind("contract")
            .bind("[]")
            .bind(decoded_topics.to_string())
            .bind("transfer")
            .bind("")
            .bind(decoded_value.to_string())
            .bind("tx_hash")
            .bind(true)
            .bind(&event_id)
            .execute(&pool)
            .await
            .expect("Failed to insert event");
        }

        // CORRECT approach: Keyset-based pagination
        // Track the last key (ledger, event_id) and use it to fetch the next batch
        let mut processed_correct = 0;
        let mut last_ledger = -1i64;
        let mut last_event_id = String::new();

        loop {
            // This query uses keyset pagination: (ledger, event_id) > (last_ledger, last_event_id)
            // This ensures we never skip rows even if the WHERE predicate changes
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

            // Simulate enriching these rows
            for row in &rows {
                let event_id: String = row.try_get("event_id").unwrap();
                let ledger: i64 = row.try_get("ledger").unwrap();

                sqlx::query("UPDATE events SET enriched = $1 WHERE event_id = $2")
                    .bind(json!({"enriched": true}).to_string())
                    .bind(&event_id)
                    .execute(&pool)
                    .await
                    .ok();

                processed_correct += 1;

                // Update our position for the next iteration
                last_ledger = ledger;
                last_event_id = event_id;
            }
        }

        // With keyset pagination, all rows should be processed
        assert_eq!(
            processed_correct, total_rows,
            "Keyset pagination must process all {} rows, but only processed {}",
            total_rows, processed_correct
        );

        cleanup_test_data(&pool, contract_id).await;
    }

    #[tokio::test]
    #[ignore] // Requires test database setup
    async fn test_keyset_pagination_large_batch() {
        // Issue #387: Test keyset pagination with 2,500+ rows as specified
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        let now = Utc::now();
        let batch_size = 1000;
        let total_rows = 2500;

        // Insert 2,500 un-enriched events
        for i in 0..total_rows {
            let event_id = format!("event_{:05}", i);
            let decoded_topics = json!(["transfer"]);
            let decoded_value = json!("1000");

            sqlx::query(
                "INSERT INTO events
                 (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
                  decoded_topics, event_name, value, decoded_value, tx_hash, in_successful_call, paging_token)
                 VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, $10::jsonb, $11, $12, $13)",
            )
            .bind(&event_id)
            .bind(contract_id)
            .bind(1000i64 + i as i64)
            .bind(now)
            .bind("contract")
            .bind("[]")
            .bind(decoded_topics.to_string())
            .bind("transfer")
            .bind("")
            .bind(decoded_value.to_string())
            .bind("tx_hash")
            .bind(true)
            .bind(&event_id)
            .execute(&pool)
            .await
            .expect("Failed to insert event");
        }

        // Keyset pagination through all 2,500 rows
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
                let event_id: String = row.try_get("event_id").unwrap();
                let ledger: i64 = row.try_get("ledger").unwrap();

                // Simulate enrichment
                sqlx::query("UPDATE events SET enriched = $1 WHERE event_id = $2")
                    .bind(json!({"enriched": true}).to_string())
                    .bind(&event_id)
                    .execute(&pool)
                    .await
                    .ok();

                processed += 1;
                last_ledger = ledger;
                last_event_id = event_id;
            }
        }

        // Verify all 2,500 rows were processed
        assert_eq!(
            processed, total_rows,
            "Should process all {} rows with keyset pagination",
            total_rows
        );

        // Verify that after enrichment, no un-enriched rows remain
        let remaining: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM events WHERE contract_id = $1 AND enriched IS NULL",
        )
        .bind(contract_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to count");

        assert_eq!(
            remaining.0, 0,
            "After reenrich, zero events should remain with enriched IS NULL"
        );

        cleanup_test_data(&pool, contract_id).await;
    }

    #[tokio::test]
    #[ignore] // Requires test database setup
    async fn test_keyset_pagination_with_high_enrichment_rate() {
        // Issue #387: Test with high enrichment hit rate
        // When all rows in a batch get enriched, OFFSET pagination skips many rows
        // Keyset pagination should still process all
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        let now = Utc::now();
        let batch_size = 100;
        let total_rows = 500;

        // Insert 500 un-enriched events
        for i in 0..total_rows {
            let event_id = format!("event_{:04}", i);
            let decoded_topics = json!(["transfer"]);
            let decoded_value = json!("1000");

            sqlx::query(
                "INSERT INTO events
                 (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
                  decoded_topics, event_name, value, decoded_value, tx_hash, in_successful_call, paging_token)
                 VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, $10::jsonb, $11, $12, $13)",
            )
            .bind(&event_id)
            .bind(contract_id)
            .bind(1000i64 + i as i64)
            .bind(now)
            .bind("contract")
            .bind("[]")
            .bind(decoded_topics.to_string())
            .bind("transfer")
            .bind("")
            .bind(decoded_value.to_string())
            .bind("tx_hash")
            .bind(true)
            .bind(&event_id)
            .execute(&pool)
            .await
            .expect("Failed to insert event");
        }

        // Process with keyset pagination
        let mut processed = 0;
        let mut last_ledger = -1i64;
        let mut last_event_id = String::new();
        let mut batch_count = 0;

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

            batch_count += 1;

            // Process all rows in this batch
            for row in &rows {
                let event_id: String = row.try_get("event_id").unwrap();
                let ledger: i64 = row.try_get("ledger").unwrap();

                // Simulate enrichment (100% hit rate in this test)
                sqlx::query("UPDATE events SET enriched = $1 WHERE event_id = $2")
                    .bind(json!({"enriched": true}).to_string())
                    .bind(&event_id)
                    .execute(&pool)
                    .await
                    .ok();

                processed += 1;
                last_ledger = ledger;
                last_event_id = event_id;
            }
        }

        // Even with high enrichment rate, all rows should be processed
        assert_eq!(
            processed, total_rows,
            "Even with 100% enrichment rate, keyset pagination should process all {} rows",
            total_rows
        );

        // Should have taken exactly 5 batches (500 / 100)
        assert_eq!(batch_count, 5, "Should have taken exactly 5 batches");

        cleanup_test_data(&pool, contract_id).await;
    }
}
