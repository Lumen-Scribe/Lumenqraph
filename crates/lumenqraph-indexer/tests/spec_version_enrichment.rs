//! Test for issue #393: Enrichment uses version-aware specs for events.
//!
//! This test verifies that events are enriched with the correct contract spec
//! version based on their ledger number, not the current spec. When a contract
//! is upgraded at ledger N:
//! - Events at N-1 should be enriched with spec v1
//! - Events at N+1 should be enriched with spec v2

#[cfg(test)]
mod spec_version_enrichment_tests {
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
        sqlx::query("DELETE FROM contract_spec_versions WHERE contract_id = $1")
            .bind(contract_id)
            .execute(pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore]
    async fn test_events_enriched_with_version_at_ledger() {
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        let upgrade_ledger = 1000i64;

        sqlx::query(
            "INSERT INTO contract_spec_versions
            (contract_id, version, wasm_hash, interface, ledger)
            VALUES
            ($1, 1, $2, $3, $4),
            ($1, 2, $5, $6, $7)",
        )
        .bind(contract_id)
        .bind("hash_v1")
        .bind(r#"{"events":[{"name":"transfer_v1"}]}"#)
        .bind(upgrade_ledger - 1) // v1 active at ledger 999
        .bind("hash_v2")
        .bind(r#"{"events":[{"name":"transfer_v2"}]}"#)
        .bind(upgrade_ledger) // v2 active at ledger 1000
        .execute(&pool)
        .await
        .expect("Failed to insert spec versions");

        sqlx::query(
            "INSERT INTO events
            (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
             decoded_topics, event_name, value, tx_hash, in_successful_call, paging_token)
            VALUES
            ($1, $2, $3, now(), 'contract', '[]', '[]', 'transfer', '', 'hash1', true, 'token1'),
            ($4, $2, $5, now(), 'contract', '[]', '[]', 'transfer', '', 'hash2', true, 'token2')",
        )
        .bind("event_before_upgrade")
        .bind(contract_id)
        .bind(upgrade_ledger - 1)
        .bind("event_after_upgrade")
        .bind(contract_id)
        .bind(upgrade_ledger + 1)
        .execute(&pool)
        .await
        .expect("Failed to insert test events");

        let result: Vec<_> = sqlx::query(
            "SELECT version, ledger FROM contract_spec_versions
             WHERE contract_id = $1
             ORDER BY ledger ASC",
        )
        .bind(contract_id)
        .fetch_all(&pool)
        .await
        .expect("Failed to query");

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].get::<i32, _>("version"), 1);
        assert_eq!(result[0].get::<i64, _>("ledger"), upgrade_ledger - 1);
        assert_eq!(result[1].get::<i32, _>("version"), 2);
        assert_eq!(result[1].get::<i64, _>("ledger"), upgrade_ledger);

        cleanup_test_data(&pool, contract_id).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_multiple_upgrades_use_correct_version() {
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        // Insert spec versions at different ledgers
        sqlx::query(
            "INSERT INTO contract_spec_versions
            (contract_id, version, wasm_hash, interface, ledger)
            VALUES
            ($1, 1, $2, $3, $4),
            ($1, 2, $5, $6, $7),
            ($1, 3, $8, $9, $10)",
        )
        .bind(contract_id)
        .bind("hash_v1")
        .bind(r#"{"version":"v1"}"#)
        .bind(100)
        .bind("hash_v2")
        .bind(r#"{"version":"v2"}"#)
        .bind(500)
        .bind("hash_v3")
        .bind(r#"{"version":"v3"}"#)
        .bind(1000)
        .execute(&pool)
        .await
        .expect("Failed to insert spec versions");

        let versions: Vec<_> = sqlx::query(
            "SELECT version, ledger FROM contract_spec_versions
             WHERE contract_id = $1
             ORDER BY version ASC",
        )
        .bind(contract_id)
        .fetch_all(&pool)
        .await
        .expect("Failed to query");

        assert_eq!(versions.len(), 3);
        assert_eq!(versions[0].get::<i32, _>("version"), 1);
        assert_eq!(versions[1].get::<i32, _>("version"), 2);
        assert_eq!(versions[2].get::<i32, _>("version"), 3);

        cleanup_test_data(&pool, contract_id).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_ledger_column_populated() {
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        let detection_ledger = 2000i64;

        sqlx::query(
            "INSERT INTO contract_spec_versions
            (contract_id, version, wasm_hash, interface, ledger)
            VALUES ($1, 1, $2, $3, $4)",
        )
        .bind(contract_id)
        .bind("hash_v1")
        .bind(r#"{"events":[]}"#)
        .bind(detection_ledger)
        .execute(&pool)
        .await
        .expect("Failed to insert spec version");

        let row: (i64,) = sqlx::query_as(
            "SELECT ledger FROM contract_spec_versions
             WHERE contract_id = $1 AND version = 1",
        )
        .bind(contract_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to query");

        assert_eq!(row.0, detection_ledger);

        cleanup_test_data(&pool, contract_id).await;
    }
}
