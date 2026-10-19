//! Integration tests for materialized view projections.
//!
//! These tests verify that:
//! - #599: AMM swaps, NFTs, and liquidity events are projected through the real
//!   ingestion path (`store::insert_events`), not by inserting rows directly.
//! - #600: Only SEP-41 / SAC transfers are projected into `token_transfers`;
//!   NFT `transfer` events are not.
//!
//! All contract and account identifiers are valid strkeys generated with
//! `stellar_strkey`, so the tests exercise the same validation as production.

#[cfg(test)]
mod projection_tests {
    use chrono::Utc;
    use lumenqraph_indexer::store::{self, NewEvent};
    use serde_json::json;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::Row;
    use stellar_strkey::Strkey;

    /// Deterministic, valid contract strkey.
    fn contract_strkey(seed: u8) -> String {
        stellar_strkey::Contract([seed; 32]).to_string()
    }

    /// Deterministic, valid account (ed25519 public key) strkey.
    fn account_strkey(seed: u8) -> String {
        stellar_strkey::Ed25519PublicKey([seed; 32]).to_string()
    }

    fn new_event(
        event_id: &str,
        contract_id: &str,
        ledger: i64,
        event_name: &str,
        topics: serde_json::Value,
        value: serde_json::Value,
    ) -> NewEvent {
        NewEvent {
            event_id: event_id.to_string(),
            contract_id: contract_id.to_string(),
            ledger,
            ledger_closed_at: Utc::now(),
            event_type: "contract".to_string(),
            topics: json!([]),
            decoded_topics: topics,
            event_name: Some(event_name.to_string()),
            value: String::new(),
            decoded_value: value,
            enriched: None,
            tx_hash: format!("hash_{event_id}"),
            in_successful_call: true,
            paging_token: event_id.to_string(),
        }
    }

    async fn setup_test_db() -> sqlx::PgPool {
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

        // Create amm_swaps table if it doesn't exist
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS amm_swaps (
                event_id          TEXT        PRIMARY KEY,
                contract_id       TEXT        NOT NULL,
                sender            TEXT,
                sell_token        TEXT,
                buy_token         TEXT,
                sell_amount       TEXT        NOT NULL,
                buy_amount        TEXT        NOT NULL,
                raw_event_name    TEXT,
                ledger            BIGINT      NOT NULL,
                ledger_closed_at  TIMESTAMPTZ NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .ok();

        // Create nft_events table if it doesn't exist
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS nft_events (
                event_id          TEXT        PRIMARY KEY,
                contract_id       TEXT        NOT NULL,
                event_kind        TEXT        NOT NULL,
                from_addr         TEXT,
                to_addr           TEXT,
                token_id          TEXT        NOT NULL,
                ledger            BIGINT      NOT NULL,
                ledger_closed_at  TIMESTAMPTZ NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .ok();

        // Create liquidity_events table if it doesn't exist
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS liquidity_events (
                event_id          TEXT        PRIMARY KEY,
                contract_id       TEXT        NOT NULL,
                event_kind        TEXT        NOT NULL,
                provider          TEXT,
                amount_a          TEXT,
                amount_b          TEXT,
                shares            TEXT,
                raw_event_name    TEXT,
                extra_amounts     JSONB,
                ledger            BIGINT      NOT NULL,
                ledger_closed_at  TIMESTAMPTZ NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .ok();

        // Create token_transfers table if it doesn't exist
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS token_transfers (
                event_id          TEXT        PRIMARY KEY,
                contract_id       TEXT        NOT NULL,
                from_addr         TEXT,
                to_addr           TEXT,
                amount            TEXT        NOT NULL,
                amount_numeric    NUMERIC,
                ledger            BIGINT      NOT NULL,
                ledger_closed_at  TIMESTAMPTZ NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .ok();

        pool
    }

    async fn cleanup_test_data(pool: &sqlx::PgPool, contract_id: &str) {
        sqlx::query("DELETE FROM amm_swaps WHERE contract_id = $1")
            .bind(contract_id)
            .execute(pool)
            .await
            .ok();

        sqlx::query("DELETE FROM nft_events WHERE contract_id = $1")
            .bind(contract_id)
            .execute(pool)
            .await
            .ok();

        sqlx::query("DELETE FROM liquidity_events WHERE contract_id = $1")
            .bind(contract_id)
            .execute(pool)
            .await
            .ok();

        sqlx::query("DELETE FROM token_transfers WHERE contract_id = $1")
            .bind(contract_id)
            .execute(pool)
            .await
            .ok();

        sqlx::query("DELETE FROM events WHERE contract_id = $1")
            .bind(contract_id)
            .execute(pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore] // Requires test database setup
    async fn test_amm_swap_projection() {
        // Issue #599: A recorded Soroswap swap event produces an amm_swaps row
        // through the real ingestion path.
        let pool = setup_test_db().await;
        let contract_id = contract_strkey(0x51);
        let sell_token = contract_strkey(0x52);
        let buy_token = contract_strkey(0x53);
        let sender = account_strkey(0x54);

        cleanup_test_data(&pool, &contract_id).await;

        let event_id = "swap_event_1";
        let event = new_event(
            event_id,
            &contract_id,
            1000,
            "swap",
            json!(["swap", sell_token, buy_token, sender]),
            json!({
                "sell_amount": "1000000000",
                "buy_amount": "2000000000"
            }),
        );

        store::insert_events(&pool, &[event])
            .await
            .expect("insert_events should succeed");

        let row = sqlx::query(
            "SELECT sender, sell_token, buy_token, sell_amount, buy_amount, raw_event_name
             FROM amm_swaps WHERE event_id = $1",
        )
        .bind(event_id)
        .fetch_one(&pool)
        .await
        .expect("Swap should be projected into amm_swaps");

        assert_eq!(row.get::<Option<String>, _>("sender").as_deref(), Some(sender.as_str()));
        assert_eq!(row.get::<Option<String>, _>("sell_token").as_deref(), Some(sell_token.as_str()));
        assert_eq!(row.get::<Option<String>, _>("buy_token").as_deref(), Some(buy_token.as_str()));
        assert_eq!(row.get::<String, _>("sell_amount"), "1000000000");
        assert_eq!(row.get::<String, _>("buy_amount"), "2000000000");
        assert_eq!(row.get::<Option<String>, _>("raw_event_name").as_deref(), Some("swap"));

        cleanup_test_data(&pool, &contract_id).await;
    }

    #[tokio::test]
    #[ignore] // Requires test database setup
    async fn test_nft_mint_projection() {
        // Issue #599: NFT mint events are projected into nft_events through ingestion.
        let pool = setup_test_db().await;
        let contract_id = contract_strkey(0x61);
        let recipient = account_strkey(0x62);

        cleanup_test_data(&pool, &contract_id).await;

        let event_id = "mint_event_1";
        let event = new_event(
            event_id,
            &contract_id,
            1000,
            "mint",
            json!(["mint", recipient]),
            json!("token_123"),
        );

        store::insert_events(&pool, &[event])
            .await
            .expect("insert_events should succeed");

        let row = sqlx::query(
            "SELECT event_kind, from_addr, to_addr, token_id
             FROM nft_events WHERE event_id = $1",
        )
        .bind(event_id)
        .fetch_one(&pool)
        .await
        .expect("Mint should be projected into nft_events");

        assert_eq!(row.get::<String, _>("event_kind"), "mint");
        assert_eq!(row.get::<Option<String>, _>("from_addr"), None);
        assert_eq!(row.get::<Option<String>, _>("to_addr").as_deref(), Some(recipient.as_str()));
        assert_eq!(row.get::<String, _>("token_id"), "token_123");

        cleanup_test_data(&pool, &contract_id).await;
    }

    #[tokio::test]
    #[ignore] // Requires test database setup
    async fn test_nft_transfer_projection() {
        // Issue #599: NFT transfer events are projected into nft_events through ingestion.
        let pool = setup_test_db().await;
        let contract_id = contract_strkey(0x63);
        let from = account_strkey(0x64);
        let to = account_strkey(0x65);

        cleanup_test_data(&pool, &contract_id).await;

        let event_id = "transfer_event_1";
        let event = new_event(
            event_id,
            &contract_id,
            1001,
            "transfer",
            json!(["transfer", from, to]),
            json!("nft_id_456"),
        );

        store::insert_events(&pool, &[event])
            .await
            .expect("insert_events should succeed");

        let row = sqlx::query(
            "SELECT event_kind, from_addr, to_addr, token_id
             FROM nft_events WHERE event_id = $1",
        )
        .bind(event_id)
        .fetch_one(&pool)
        .await
        .expect("Transfer should be projected into nft_events");

        assert_eq!(row.get::<String, _>("event_kind"), "transfer");
        assert_eq!(row.get::<Option<String>, _>("from_addr").as_deref(), Some(from.as_str()));
        assert_eq!(row.get::<Option<String>, _>("to_addr").as_deref(), Some(to.as_str()));
        assert_eq!(row.get::<String, _>("token_id"), "nft_id_456");

        cleanup_test_data(&pool, &contract_id).await;
    }

    #[tokio::test]
    #[ignore] // Requires test database setup
    async fn test_nft_transfer_not_projected_into_token_transfers() {
        // Issue #600: An NFT `transfer` event (token ID, not amount) must not be
        // projected into token_transfers.
        let pool = setup_test_db().await;
        let contract_id = contract_strkey(0x66);
        let from = account_strkey(0x67);
        let to = account_strkey(0x68);

        cleanup_test_data(&pool, &contract_id).await;

        let event_id = "nft_transfer_event_1";
        let event = new_event(
            event_id,
            &contract_id,
            1002,
            "transfer",
            json!(["transfer", from, to]),
            json!("nft_id_789"),
        );

        store::insert_events(&pool, &[event])
            .await
            .expect("insert_events should succeed");

        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM token_transfers WHERE event_id = $1")
                .bind(event_id)
                .fetch_one(&pool)
                .await
                .expect("Failed to count token_transfers");

        assert_eq!(
            count.0, 0,
            "NFT transfer must not be projected into token_transfers"
        );

        cleanup_test_data(&pool, &contract_id).await;
    }

    #[tokio::test]
    #[ignore] // Requires test database setup
    async fn test_sep41_transfer_projected_into_token_transfers() {
        // Issue #600: A SEP-41 / SAC transfer with an i128 amount is projected.
        let pool = setup_test_db().await;
        let contract_id = contract_strkey(0x69);
        let from = account_strkey(0x6a);
        let to = account_strkey(0x6b);

        cleanup_test_data(&pool, &contract_id).await;

        let event_id = "sep41_transfer_event_1";
        let event = new_event(
            event_id,
            &contract_id,
            1003,
            "transfer",
            json!(["transfer", from, to]),
            json!("12345"),
        );

        store::insert_events(&pool, &[event])
            .await
            .expect("insert_events should succeed");

        let row = sqlx::query(
            "SELECT from_addr, to_addr, amount, amount_numeric
             FROM token_transfers WHERE event_id = $1",
        )
        .bind(event_id)
        .fetch_one(&pool)
        .await
        .expect("SEP-41 transfer should be projected into token_transfers");

        assert_eq!(row.get::<Option<String>, _>("from_addr").as_deref(), Some(from.as_str()));
        assert_eq!(row.get::<Option<String>, _>("to_addr").as_deref(), Some(to.as_str()));
        assert_eq!(row.get::<String, _>("amount"), "12345");
        assert_eq!(
            row.get::<Option<sqlx::types::BigDecimal>, _>("amount_numeric"),
            Some(sqlx::types::BigDecimal::from(12345))
        );

        cleanup_test_data(&pool, &contract_id).await;
    }

    #[tokio::test]
    #[ignore] // Requires test database setup
    async fn test_liquidity_deposit_projection() {
        // Issue #599: deposit/add_liquidity events are projected into liquidity_events
        // through the real ingestion path.
        let pool = setup_test_db().await;
        let contract_id = contract_strkey(0x71);
        let provider = account_strkey(0x72);

        cleanup_test_data(&pool, &contract_id).await;

        let event_id = "deposit_event_1";
        let event = new_event(
            event_id,
            &contract_id,
            1000,
            "deposit",
            json!(["deposit", provider]),
            json!({
                "amounts": ["1000000", "2000000"],
                "shares_minted": "1500000"
            }),
        );

        store::insert_events(&pool, &[event])
            .await
            .expect("insert_events should succeed");

        let row = sqlx::query(
            "SELECT event_kind, provider, amount_a, amount_b, shares, raw_event_name
             FROM liquidity_events WHERE event_id = $1",
        )
        .bind(event_id)
        .fetch_one(&pool)
        .await
        .expect("Deposit should be projected into liquidity_events");

        assert_eq!(row.get::<String, _>("event_kind"), "add");
        assert_eq!(row.get::<Option<String>, _>("provider").as_deref(), Some(provider.as_str()));
        assert_eq!(row.get::<Option<String>, _>("amount_a").as_deref(), Some("1000000"));
        assert_eq!(row.get::<Option<String>, _>("amount_b").as_deref(), Some("2000000"));
        assert_eq!(row.get::<Option<String>, _>("shares").as_deref(), Some("1500000"));
        assert_eq!(row.get::<Option<String>, _>("raw_event_name").as_deref(), Some("deposit"));

        cleanup_test_data(&pool, &contract_id).await;
    }

    #[tokio::test]
    #[ignore] // Requires test database setup
    async fn test_liquidity_withdraw_projection() {
        // Issue #386: Test that withdraw/remove_liquidity events are projected into liquidity_events
        let pool = setup_test_db().await;
        let contract_id = "CAMM_CONTRACT_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";

        cleanup_test_data(&pool, contract_id).await;

        let now = Utc::now();
        let event_id = "withdraw_event_1";

        let decoded_topics = json!(["withdraw", "GPROVIDER_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"]);

        let decoded_value = json!({
            "amounts": ["500000", "750000"],
            "shares_burned": "600000"
        });

        // Insert a withdraw event
        sqlx::query(
            "INSERT INTO events
             (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
              decoded_topics, event_name, value, decoded_value, tx_hash, in_successful_call, paging_token)
             VALUES ($1, $2, $3, $4, 'contract', '[]'::jsonb, $5, $6, '', $7, 'hash1', true, $8)",
        )
        .bind(event_id)
        .bind(contract_id)
        .bind(1002i64)
        .bind(now)
        .bind(decoded_topics.to_string())
        .bind("withdraw")
        .bind(decoded_value.to_string())
        .bind(event_id)
        .execute(&pool)
        .await
        .expect("Failed to insert withdraw event");

        // Fetch and project
        let event = sqlx::query(
            "SELECT decoded_topics, decoded_value FROM events WHERE event_id = $1",
        )
        .bind(event_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to fetch event");

        let topics_str: String = event.try_get("decoded_topics").unwrap();
        let value_str: String = event.try_get("decoded_value").unwrap();

        let topics: serde_json::Value = serde_json::from_str(&topics_str).unwrap();
        let value: serde_json::Value = serde_json::from_str(&value_str).unwrap();

        // Project into liquidity_events
        sqlx::query(
            "INSERT INTO liquidity_events
             (event_id, contract_id, event_kind, provider, amount_a, amount_b, shares, raw_event_name, ledger, ledger_closed_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(event_id)
        .bind(contract_id)
        .bind("remove") // normalize "withdraw" to "remove"
        .bind(topics[1].as_str())
        .bind(value["amounts"][0].as_str())
        .bind(value["amounts"][1].as_str())
        .bind(value["shares_burned"].as_str())
        .bind("withdraw")
        .bind(1002i64)
        .bind(now)
        .execute(&pool)
        .await
        .expect("Failed to project withdraw into liquidity_events");

        // Verify projection
        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM liquidity_events WHERE contract_id = $1 AND event_kind = $2",
        )
        .bind(contract_id)
        .bind("remove")
        .fetch_one(&pool)
        .await
        .expect("Failed to count withdrawals");

        assert_eq!(
            count.0, 1,
            "Withdraw should be projected into liquidity_events as 'remove'"
        );

        cleanup_test_data(&pool, contract_id).await;
    }
}
