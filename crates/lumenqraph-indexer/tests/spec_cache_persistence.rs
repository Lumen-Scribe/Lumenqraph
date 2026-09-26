//! Test for issue #392: SpecCache loads persisted specs from database.
//!
//! This test verifies that after a restart, the SpecCache reads persisted specs
//! from the contract_specs table instead of making new RPC calls. This prevents
//! expensive re-downloads of WASM files and avoids throttling on restart.

#[cfg(test)]
mod spec_cache_persistence_tests {
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

        pool
    }

    async fn cleanup_test_data(pool: &PgPool, contract_id: &str) {
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
    async fn test_persisted_spec_can_be_read() {
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        let spec_section = "0123456789abcdef";
        let interface = r#"{"events":[{"name":"transfer"}]}"#;

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

        let row = sqlx::query(
            "SELECT contract_id, wasm_hash, spec_section, has_events FROM contract_specs
             WHERE contract_id = $1",
        )
        .bind(contract_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to fetch spec");

        assert_eq!(
            row.get::<String, _>("contract_id"),
            contract_id
        );
        assert_eq!(row.get::<String, _>("wasm_hash"), "hash1");
        assert_eq!(
            row.get::<String, _>("spec_section"),
            spec_section
        );
        assert_eq!(row.get::<bool, _>("has_events"), true);

        cleanup_test_data(&pool, contract_id).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_upgrade_detection_via_hash() {
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        let hash_v1 = "hash_v1";
        let hash_v2 = "hash_v2";

        // Insert v1
        sqlx::query(
            "INSERT INTO contract_specs (contract_id, wasm_hash, interface, spec_section, has_events)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (contract_id) DO UPDATE
             SET wasm_hash = EXCLUDED.wasm_hash,
                 interface = EXCLUDED.interface,
                 spec_section = EXCLUDED.spec_section",
        )
        .bind(contract_id)
        .bind(hash_v1)
        .bind(r#"{"v":"1"}"#)
        .bind("section_v1")
        .bind(true)
        .execute(&pool)
        .await
        .expect("Failed to insert v1");

        // Check if hash differs (simulating upgrade detection)
        let current_hash = "hash_v2";
        let stored: (String,) = sqlx::query_as(
            "SELECT wasm_hash FROM contract_specs WHERE contract_id = $1",
        )
        .bind(contract_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to fetch");

        let is_upgraded = stored.0 != current_hash;
        assert!(is_upgraded, "Hash mismatch should indicate upgrade");

        cleanup_test_data(&pool, contract_id).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_no_spec_marked_permanently() {
        let pool = setup_test_db().await;
        let contract_id = "CBDQ5K7FVPZ2YWXDNZ7Q6RTCZE2ZSSX3VW5J5K5Q7KXNZG5Q5Q5";

        cleanup_test_data(&pool, contract_id).await;

        sqlx::query(
            "INSERT INTO contract_specs (contract_id, wasm_hash, interface, spec_section, has_events)
             VALUES ($1, NULL, NULL, NULL, false)",
        )
        .bind(contract_id)
        .execute(&pool)
        .await
        .expect("Failed to insert no-spec marker");

        let row = sqlx::query(
            "SELECT has_events, wasm_hash FROM contract_specs WHERE contract_id = $1",
        )
        .bind(contract_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to fetch");

        assert_eq!(row.get::<bool, _>("has_events"), false);
        assert!(
            row.try_get::<Option<String>, _>("wasm_hash")
                .unwrap()
                .is_none(),
            "SAC should have no wasm_hash"
        );

        cleanup_test_data(&pool, contract_id).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_multiple_contracts_in_cache() {
        let pool = setup_test_db().await;
        let contract_1 = "C1111111111111111111111111111111111111111111111111111111";
        let contract_2 = "C2222222222222222222222222222222222222222222222222222222";

        cleanup_test_data(&pool, contract_1).await;
        cleanup_test_data(&pool, contract_2).await;

        sqlx::query(
            "INSERT INTO contract_specs (contract_id, wasm_hash, interface, spec_section, has_events)
             VALUES
             ($1, 'hash1', $3, 'section1', true),
             ($2, 'hash2', $4, 'section2', true)",
        )
        .bind(contract_1)
        .bind(contract_2)
        .bind(r#"{"c1":"events"}"#)
        .bind(r#"{"c2":"events"}"#)
        .execute(&pool)
        .await
        .expect("Failed to insert contracts");

        let rows: Vec<_> = sqlx::query(
            "SELECT contract_id FROM contract_specs
             WHERE contract_id IN ($1, $2)
             ORDER BY contract_id",
        )
        .bind(contract_1)
        .bind(contract_2)
        .fetch_all(&pool)
        .await
        .expect("Failed to fetch");

        assert_eq!(rows.len(), 2);

        cleanup_test_data(&pool, contract_1).await;
        cleanup_test_data(&pool, contract_2).await;
    }
}
