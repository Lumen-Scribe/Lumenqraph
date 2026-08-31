//! Integration tests for retention pruner interaction with the contract_summaries trigger.
//!
//! When the retention pruner deletes events from the events table, the contract_summaries
//! trigger should fire and update (or potentially delete) the corresponding summary row.
//! This test suite verifies end-to-end interaction between pruning and summary maintenance.

#[cfg(test)]
mod retention_contract_summaries_tests {
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{PgPool, Row};
    use std::path::Path;

    /// Create an isolated test database schema with all migrations.
    async fn setup_test_db() -> PgPool {
        let database_url =
            std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
                "postgres://localhost/lumenqraph_test".to_string()
            });

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .expect("Failed to connect to test database");

        // Create isolated schema for this test
        let schema_name = format!("test_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA \"{schema_name}\""))
            .execute(&admin_pool)
            .await
            .expect("Failed to create test schema");

        admin_pool.close().await;

        // Connect to new schema and run migrations
        let options = format!("-c search_path={schema_name},public");
        let sep = if database_url.contains('?') {
            "&"
        } else {
            "?"
        };
        let schema_url = format!(
            "{url}{sep}options={opts}",
            url = database_url,
            opts = percent_encode(&options)
        );

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&schema_url)
            .await
            .expect("Failed to connect with schema");

        // Run migrations from the migrations directory
        sqlx::migrate!("../../migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        pool
    }

    fn percent_encode(s: &str) -> String {
        s.chars()
            .flat_map(|c| match c {
                'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => vec![c],
                c => format!("%{:02X}", c as u32).chars().collect(),
            })
            .collect()
    }

    /// Insert a test event for a contract.
    async fn insert_event(pool: &PgPool, event_id: &str, contract_id: &str, ledger: i64) {
        sqlx::query(
            "INSERT INTO events (event_id, contract_id, ledger, ledger_closed_at, event_type,
                                 topics, event_name, value, tx_hash, in_successful_call, paging_token)
             VALUES ($1, $2, $3, now(), 'contract', '[]', 'transfer', 'v', 'tx', true, $1)",
        )
        .bind(event_id)
        .bind(contract_id)
        .bind(ledger)
        .execute(pool)
        .await
        .expect("Failed to insert event");
    }

    /// Get the event_count for a contract from contract_summaries.
    async fn get_summary_event_count(pool: &PgPool, contract_id: &str) -> Option<i64> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT event_count FROM contract_summaries WHERE contract_id = $1",
        )
        .bind(contract_id)
        .fetch_optional(pool)
        .await
        .expect("Failed to query contract_summaries");
        row.map(|(count,)| count)
    }

    /// Get the first_seen_ledger for a contract.
    async fn get_summary_first_ledger(pool: &PgPool, contract_id: &str) -> Option<i64> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT first_seen_ledger FROM contract_summaries WHERE contract_id = $1",
        )
        .bind(contract_id)
        .fetch_optional(pool)
        .await
        .expect("Failed to query contract_summaries");
        row.map(|(ledger,)| ledger)
    }

    /// Get the last_seen_ledger for a contract.
    async fn get_summary_last_ledger(pool: &PgPool, contract_id: &str) -> Option<i64> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT last_seen_ledger FROM contract_summaries WHERE contract_id = $1",
        )
        .bind(contract_id)
        .fetch_optional(pool)
        .await
        .expect("Failed to query contract_summaries");
        row.map(|(ledger,)| ledger)
    }

    /// Count events in the events table for a contract.
    async fn count_events(pool: &PgPool, contract_id: &str) -> i64 {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM events WHERE contract_id = $1")
            .bind(contract_id)
            .fetch_one(pool)
            .await
            .expect("Failed to count events");
        row.0
    }

    /// Run the retention pruner for a given tip and retention window.
    async fn prune_events(pool: &PgPool, tip: i64, retention_ledgers: i64) -> u64 {
        // Manually run the pruning logic inline
        if retention_ledgers <= 0 {
            return 0;
        }
        let cutoff = tip - retention_ledgers;
        if cutoff <= 0 {
            return 0;
        }

        let result = sqlx::query(
            "DELETE FROM events WHERE event_id IN (
                 SELECT event_id FROM events WHERE ledger < $1 ORDER BY ledger LIMIT 5000
             )",
        )
        .bind(cutoff)
        .execute(pool)
        .await
        .expect("Failed to prune events");

        result.rows_affected()
    }

    #[tokio::test]
    #[ignore] // Requires TEST_DATABASE_URL
    async fn test_contract_summaries_decrements_on_pruning() {
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        // Insert three events for the same contract at different ledgers
        insert_event(&pool, "e1", contract_id, 100).await;
        insert_event(&pool, "e2", contract_id, 200).await;
        insert_event(&pool, "e3", contract_id, 300).await;

        // Verify insert trigger updated the summary
        let before = get_summary_event_count(&pool, contract_id)
            .await
            .expect("Summary row should exist after inserts");
        assert_eq!(before, 3, "Insert trigger should have counted all three events");

        // Prune events older than ledger 250 (tip=1000, retention=750 → cutoff=250)
        // Events e1 (ledger=100) and e2 (ledger=200) are below cutoff; e3 survives
        let deleted = prune_events(&pool, 1000, 750).await;
        assert_eq!(deleted, 2, "Two events should be pruned");

        // Verify summary was decremented
        let after = get_summary_event_count(&pool, contract_id)
            .await
            .expect("Summary row should still exist (one event survives)");
        assert_eq!(
            after, 1,
            "Event count should be decremented to reflect one surviving event"
        );

        // Prune the last event (cutoff=350 > ledger=300)
        let deleted2 = prune_events(&pool, 1000, 650).await;
        assert_eq!(deleted2, 1, "The final event should be pruned");

        // Verify summary row is deleted when count reaches zero
        let final_count = get_summary_event_count(&pool, contract_id).await;
        assert!(
            final_count.is_none(),
            "Summary row must be deleted when event_count reaches zero"
        );
    }

    #[tokio::test]
    #[ignore] // Requires TEST_DATABASE_URL
    async fn test_contract_summaries_ledger_bounds_after_pruning() {
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        // Insert events spanning a wide ledger range
        insert_event(&pool, "b1", contract_id, 50).await;
        insert_event(&pool, "b2", contract_id, 500).await;
        insert_event(&pool, "b3", contract_id, 900).await;

        // Verify initial state
        let initial_first = get_summary_first_ledger(&pool, contract_id).await;
        let initial_last = get_summary_last_ledger(&pool, contract_id).await;
        assert_eq!(initial_first, Some(50), "First ledger should be the oldest event");
        assert_eq!(initial_last, Some(900), "Last ledger should be the newest event");

        // Prune events before ledger 200 (cutoff=200 → only b1 deleted)
        prune_events(&pool, 1000, 800).await;

        // Verify ledger bounds were updated
        let new_first = get_summary_first_ledger(&pool, contract_id).await;
        let new_last = get_summary_last_ledger(&pool, contract_id).await;
        assert_eq!(
            new_first, Some(500),
            "first_seen_ledger should advance to the next surviving event"
        );
        assert_eq!(
            new_last, Some(900),
            "last_seen_ledger should remain at the newest event"
        );

        // Verify event count is correct
        let count = get_summary_event_count(&pool, contract_id).await;
        assert_eq!(count, Some(2), "Event count should be 2 after pruning");
    }

    #[tokio::test]
    #[ignore] // Requires TEST_DATABASE_URL
    async fn test_contract_summaries_edge_case_all_events_pruned() {
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        // Insert a single event
        insert_event(&pool, "only_event", contract_id, 100).await;

        // Verify summary was created
        let before = get_summary_event_count(&pool, contract_id).await;
        assert_eq!(before, Some(1), "Summary row should exist with count 1");

        // Prune the only event
        let deleted = prune_events(&pool, 1000, 500).await;
        assert_eq!(deleted, 1, "The only event should be pruned");

        // Verify summary row is deleted
        let after = get_summary_event_count(&pool, contract_id).await;
        assert!(
            after.is_none(),
            "Summary row must be deleted when all events are pruned"
        );

        // Verify the events table is also empty
        let event_count = count_events(&pool, contract_id).await;
        assert_eq!(
            event_count, 0,
            "Events table should be empty after pruning all events"
        );
    }

    #[tokio::test]
    #[ignore] // Requires TEST_DATABASE_URL
    async fn test_contract_summaries_isolation_across_contracts() {
        let pool = setup_test_db().await;

        // Insert events for two different contracts
        let c1 = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";
        let c2 = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q6";

        insert_event(&pool, "c1e1", c1, 100).await;
        insert_event(&pool, "c1e2", c1, 800).await;
        insert_event(&pool, "c2e1", c2, 600).await;

        // Verify both contracts have summaries
        let c1_before = get_summary_event_count(&pool, c1).await;
        let c2_before = get_summary_event_count(&pool, c2).await;
        assert_eq!(c1_before, Some(2), "C1 should have 2 events");
        assert_eq!(c2_before, Some(1), "C2 should have 1 event");

        // Prune only affects C1 (events before ledger 200)
        prune_events(&pool, 1000, 800).await;

        // Verify C1 was affected but C2 was not
        let c1_after = get_summary_event_count(&pool, c1).await;
        let c2_after = get_summary_event_count(&pool, c2).await;
        assert_eq!(
            c1_after, Some(1),
            "C1 should have 1 event after pruning c1e1"
        );
        assert_eq!(
            c2_after, Some(1),
            "C2 should still have 1 event (unaffected by C1's prune)"
        );
    }

    #[tokio::test]
    #[ignore] // Requires TEST_DATABASE_URL
    async fn test_contract_summaries_multiple_pruning_passes() {
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        // Insert 5 events at different ledgers
        for i in 1..=5 {
            insert_event(&pool, &format!("e{i}"), contract_id, i * 100).await;
        }

        assert_eq!(
            get_summary_event_count(&pool, contract_id).await,
            Some(5),
            "Initial count should be 5"
        );

        // First prune pass: remove first 2 events (ledger < 250)
        prune_events(&pool, 1000, 750).await;
        assert_eq!(
            get_summary_event_count(&pool, contract_id).await,
            Some(3),
            "After first prune, count should be 3"
        );

        // Second prune pass: remove next 2 events (ledger < 350)
        prune_events(&pool, 1000, 650).await;
        assert_eq!(
            get_summary_event_count(&pool, contract_id).await,
            Some(1),
            "After second prune, count should be 1"
        );

        // Final prune pass: remove last event (ledger < 450)
        prune_events(&pool, 1000, 550).await;
        assert!(
            get_summary_event_count(&pool, contract_id).await.is_none(),
            "After final prune, summary row should be deleted"
        );
    }
}
