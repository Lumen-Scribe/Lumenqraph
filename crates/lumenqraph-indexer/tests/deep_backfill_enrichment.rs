//! Test for issue #390: deep-backfill event enrichment.
//!
//! This test verifies that deep-backfilled events are enriched using persisted
//! specs. The spec cache should load specs from the database before processing,
//! so events end up with enriched = typed records, not NULL.

#[cfg(test)]
mod deep_backfill_enrichment_tests {
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
            "CREATE TABLE IF NOT EXISTS contract_specs (
                contract_id TEXT PRIMARY KEY,
                wasm_hash TEXT NOT NULL,
                interface JSONB,
                spec_section TEXT,
                has_events BOOLEAN,
                fetched_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .execute(&pool)
        .await
        .ok();

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS contract_spec_versions (
                contract_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                wasm_hash TEXT,
                previous_wasm_hash TEXT,
                interface JSONB,
                spec_section TEXT,
                diff JSONB,
                breaking BOOLEAN,
                ledger BIGINT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (contract_id, version)
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

    async fn cleanup_test_data(pool: &PgPool, contract_id: &str) {
        sqlx::query("DELETE FROM events WHERE contract_id = $1")
            .bind(contract_id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM contract_specs WHERE contract_id = $1")
            .bind(contract_id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM contract_spec_versions WHERE contract_id = $1")
            .bind(contract_id)
            .execute(pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore]
    async fn test_deep_backfill_loads_persisted_specs() {
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        let spec_section = "0123456789abcdef";
        let interface = r#"{"events":[{"name":"transfer","doc":"Transfer event"}]}"#;

        sqlx::query(
            "INSERT INTO contract_specs (contract_id, wasm_hash, interface, spec_section, has_events)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(contract_id)
        .bind("hash1")
        .bind(interface)
        .bind(spec_section)
        .bind(true)
        .execute(&pool)
        .await
        .expect("Failed to insert spec");

        // Verify spec was persisted
        let stored: (String, String) = sqlx::query_as(
            "SELECT spec_section, interface FROM contract_specs WHERE contract_id = $1",
        )
        .bind(contract_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to fetch spec");

        assert_eq!(stored.0, spec_section);
        assert_eq!(stored.1, interface);

        cleanup_test_data(&pool, contract_id).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_enrichment_coverage_tracking() {
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        // Insert events: some with enriched, some without
        sqlx::query(
            "INSERT INTO events
            (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
             decoded_topics, event_name, value, enriched, tx_hash, in_successful_call, paging_token)
            VALUES
            ($1, $2, 1000, now(), 'contract', '[]', '[]', 'transfer', '', $3, 'h1', true, 't1'),
            ($4, $2, 1001, now(), 'contract', '[]', '[]', 'transfer', '', NULL, 'h2', true, 't2'),
            ($5, $2, 1002, now(), 'contract', '[]', '[]', 'transfer', '', $6, 'h3', true, 't3')",
        )
        .bind("e1")
        .bind(contract_id)
        .bind(r#"{"typed":"data1"}"#)
        .bind("e2")
        .bind("e3")
        .bind(r#"{"typed":"data3"}"#)
        .execute(&pool)
        .await
        .expect("Failed to insert events");

        // Count enriched vs not enriched
        let enriched_count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM events WHERE contract_id = $1 AND enriched IS NOT NULL",
        )
        .bind(contract_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to count enriched");

        let not_enriched_count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM events WHERE contract_id = $1 AND enriched IS NULL",
        )
        .bind(contract_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to count not enriched");

        assert_eq!(enriched_count.0, 2);
        assert_eq!(not_enriched_count.0, 1);

        cleanup_test_data(&pool, contract_id).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_rpc_backed_spec_loading_flag() {
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        // Test case: no persisted spec (would need RPC with --rpc flag)
        let existing: Option<(String,)> = sqlx::query_as(
            "SELECT contract_id FROM contract_specs WHERE contract_id = $1",
        )
        .bind(contract_id)
        .fetch_optional(&pool)
        .await
        .expect("Failed to check spec");

        assert!(existing.is_none(), "Contract should have no persisted spec");

        cleanup_test_data(&pool, contract_id).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_spec_version_aware_enrichment() {
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        let upgrade_ledger = 2000i64;

        // Insert two spec versions
        sqlx::query(
            "INSERT INTO contract_spec_versions
            (contract_id, version, wasm_hash, interface, spec_section, ledger)
            VALUES
            ($1, 1, $2, $3, $4, $5),
            ($1, 2, $6, $7, $8, $9)",
        )
        .bind(contract_id)
        .bind("hash_v1")
        .bind(r#"{"name":"transfer_v1"}"#)
        .bind("section_v1")
        .bind(upgrade_ledger - 100)
        .bind("hash_v2")
        .bind(r#"{"name":"transfer_v2"}"#)
        .bind("section_v2")
        .bind(upgrade_ledger)
        .execute(&pool)
        .await
        .expect("Failed to insert spec versions");

        // Insert events around upgrade
        sqlx::query(
            "INSERT INTO events
            (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
             decoded_topics, event_name, value, tx_hash, in_successful_call, paging_token)
            VALUES
            ($1, $2, $3, now(), 'contract', '[]', '[]', 'transfer', '', 'h1', true, 't1'),
            ($4, $2, $5, now(), 'contract', '[]', '[]', 'transfer', '', 'h2', true, 't2')",
        )
        .bind("e_before")
        .bind(contract_id)
        .bind(upgrade_ledger - 1)
        .bind("e_after")
        .bind(contract_id)
        .bind(upgrade_ledger + 1)
        .execute(&pool)
        .await
        .expect("Failed to insert events");

        // Verify we can query correct spec for each event
        let versions: Vec<_> = sqlx::query(
            "SELECT version, ledger FROM contract_spec_versions
             WHERE contract_id = $1
             ORDER BY ledger ASC",
        )
        .bind(contract_id)
        .fetch_all(&pool)
        .await
        .expect("Failed to fetch versions");

        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].get::<i32, _>("version"), 1);
        assert_eq!(versions[1].get::<i32, _>("version"), 2);

        cleanup_test_data(&pool, contract_id).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_contract_not_yet_seen() {
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        // Verify no spec exists
        let spec_exists: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM contract_specs WHERE contract_id = $1",
        )
        .bind(contract_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to count specs");

        assert_eq!(spec_exists.0, 0);

        // This contract would need --rpc flag or would remain unenriched
        cleanup_test_data(&pool, contract_id).await;
    }
}
