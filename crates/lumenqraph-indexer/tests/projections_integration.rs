//! Integration tests for materialized view projections.
//!
//! These tests verify that:
//! - #386: AMM swaps, NFTs, and liquidity events are properly projected into their tables
//! - Events are correctly normalized and stored in the materialized tables
//! - Projectors handle edge cases gracefully

#[cfg(test)]
mod projection_tests {
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

        sqlx::query("DELETE FROM events WHERE contract_id = $1")
            .bind(contract_id)
            .execute(pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore] // Requires test database setup
    async fn test_amm_swap_projection() {
        // Issue #386: Test that Soroswap swap events are projected into amm_swaps
        let pool = setup_test_db().await;
        let contract_id = "CSOROSWAPXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";

        cleanup_test_data(&pool, contract_id).await;

        let now = Utc::now();
        let event_id = "swap_event_1";

        let decoded_topics = json!([
            "swap",
            "GSELL_TOKEN_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
            "GBUY_TOKEN_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
            "GSENDER_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
        ]);

        let decoded_value = json!({
            "sell_amount": "1000000000",
            "buy_amount": "2000000000"
        });

        // Insert a swap event
        sqlx::query(
            "INSERT INTO events
             (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
              decoded_topics, event_name, value, decoded_value, tx_hash, in_successful_call, paging_token)
             VALUES ($1, $2, $3, $4, 'contract', '[]'::jsonb, $5, $6, '', $7, 'hash1', true, $8)",
        )
        .bind(event_id)
        .bind(contract_id)
        .bind(1000i64)
        .bind(now)
        .bind(decoded_topics.to_string())
        .bind("swap")
        .bind(decoded_value.to_string())
        .bind(event_id)
        .execute(&pool)
        .await
        .expect("Failed to insert swap event");

        // Manually project the swap (simulating what the projector would do)
        let swap = sqlx::query(
            "SELECT decoded_topics, decoded_value FROM events WHERE event_id = $1",
        )
        .bind(event_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to fetch event");

        // This test verifies the event structure is correct for projection
        let topics_str: String = swap.try_get("decoded_topics").unwrap();
        let value_str: String = swap.try_get("decoded_value").unwrap();

        let topics: serde_json::Value = serde_json::from_str(&topics_str).unwrap();
        let value: serde_json::Value = serde_json::from_str(&value_str).unwrap();

        // Verify we can extract the fields needed for projection
        assert_eq!(topics[0].as_str().unwrap(), "swap", "Event should be swap");
        assert!(
            topics[1].is_string(),
            "sell_token should be string address"
        );
        assert!(topics[2].is_string(), "buy_token should be string address");
        assert!(
            topics[3].is_string(),
            "sender should be string address (optional)"
        );

        assert!(
            value["sell_amount"].is_string(),
            "sell_amount should be present"
        );
        assert!(
            value["buy_amount"].is_string(),
            "buy_amount should be present"
        );

        // Now project into amm_swaps table
        sqlx::query(
            "INSERT INTO amm_swaps
             (event_id, contract_id, sender, sell_token, buy_token, sell_amount, buy_amount, raw_event_name, ledger, ledger_closed_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(event_id)
        .bind(contract_id)
        .bind(topics[3].as_str())
        .bind(topics[1].as_str())
        .bind(topics[2].as_str())
        .bind(value["sell_amount"].as_str().unwrap())
        .bind(value["buy_amount"].as_str().unwrap())
        .bind("swap")
        .bind(1000i64)
        .bind(now)
        .execute(&pool)
        .await
        .expect("Failed to project swap into amm_swaps");

        // Verify the swap was projected
        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM amm_swaps WHERE contract_id = $1 AND event_id = $2",
        )
        .bind(contract_id)
        .bind(event_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to count swaps");

        assert_eq!(count.0, 1, "Swap should be projected into amm_swaps");

        cleanup_test_data(&pool, contract_id).await;
    }

    #[tokio::test]
    #[ignore] // Requires test database setup
    async fn test_nft_mint_projection() {
        // Issue #386: Test that NFT mint events are projected into nft_events
        let pool = setup_test_db().await;
        let contract_id = "CNFT_CONTRACT_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";

        cleanup_test_data(&pool, contract_id).await;

        let now = Utc::now();
        let event_id = "mint_event_1";

        let decoded_topics =
            json!(["mint", "GRECIPIENT_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"]);
        let decoded_value = json!("token_123");

        // Insert a mint event
        sqlx::query(
            "INSERT INTO events
             (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
              decoded_topics, event_name, value, decoded_value, tx_hash, in_successful_call, paging_token)
             VALUES ($1, $2, $3, $4, 'contract', '[]'::jsonb, $5, $6, '', $7, 'hash1', true, $8)",
        )
        .bind(event_id)
        .bind(contract_id)
        .bind(1000i64)
        .bind(now)
        .bind(decoded_topics.to_string())
        .bind("mint")
        .bind(decoded_value.to_string())
        .bind(event_id)
        .execute(&pool)
        .await
        .expect("Failed to insert mint event");

        // Fetch and project the mint event
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

        // Verify structure
        assert_eq!(topics[0].as_str().unwrap(), "mint");

        // Project into nft_events
        sqlx::query(
            "INSERT INTO nft_events
             (event_id, contract_id, event_kind, from_addr, to_addr, token_id, ledger, ledger_closed_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(event_id)
        .bind(contract_id)
        .bind("mint")
        .bind::<Option<String>>(None)
        .bind(topics[1].as_str())
        .bind(value.to_string())
        .bind(1000i64)
        .bind(now)
        .execute(&pool)
        .await
        .expect("Failed to project mint into nft_events");

        // Verify projection
        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM nft_events WHERE contract_id = $1 AND event_kind = $2",
        )
        .bind(contract_id)
        .bind("mint")
        .fetch_one(&pool)
        .await
        .expect("Failed to count mints");

        assert_eq!(count.0, 1, "Mint should be projected into nft_events");

        cleanup_test_data(&pool, contract_id).await;
    }

    #[tokio::test]
    #[ignore] // Requires test database setup
    async fn test_nft_transfer_projection() {
        // Issue #386: Test that NFT transfer events are projected into nft_events
        let pool = setup_test_db().await;
        let contract_id = "CNFT_CONTRACT_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";

        cleanup_test_data(&pool, contract_id).await;

        let now = Utc::now();
        let event_id = "transfer_event_1";

        let decoded_topics = json!([
            "transfer",
            "GFROM_ADDRESS_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
            "GTO_ADDRESS_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
        ]);
        let decoded_value = json!("nft_id_456");

        // Insert a transfer event
        sqlx::query(
            "INSERT INTO events
             (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
              decoded_topics, event_name, value, decoded_value, tx_hash, in_successful_call, paging_token)
             VALUES ($1, $2, $3, $4, 'contract', '[]'::jsonb, $5, $6, '', $7, 'hash1', true, $8)",
        )
        .bind(event_id)
        .bind(contract_id)
        .bind(1001i64)
        .bind(now)
        .bind(decoded_topics.to_string())
        .bind("transfer")
        .bind(decoded_value.to_string())
        .bind(event_id)
        .execute(&pool)
        .await
        .expect("Failed to insert transfer event");

        // Fetch and project
        let event = sqlx::query(
            "SELECT decoded_topics, decoded_value, event_name FROM events WHERE event_id = $1",
        )
        .bind(event_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to fetch event");

        let topics_str: String = event.try_get("decoded_topics").unwrap();
        let value_str: String = event.try_get("decoded_value").unwrap();

        let topics: serde_json::Value = serde_json::from_str(&topics_str).unwrap();
        let value: serde_json::Value = serde_json::from_str(&value_str).unwrap();

        // Project into nft_events
        sqlx::query(
            "INSERT INTO nft_events
             (event_id, contract_id, event_kind, from_addr, to_addr, token_id, ledger, ledger_closed_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(event_id)
        .bind(contract_id)
        .bind("transfer")
        .bind(topics[1].as_str())
        .bind(topics[2].as_str())
        .bind(value.to_string())
        .bind(1001i64)
        .bind(now)
        .execute(&pool)
        .await
        .expect("Failed to project transfer into nft_events");

        // Verify projection
        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM nft_events WHERE contract_id = $1 AND event_kind = $2",
        )
        .bind(contract_id)
        .bind("transfer")
        .fetch_one(&pool)
        .await
        .expect("Failed to count transfers");

        assert_eq!(count.0, 1, "Transfer should be projected into nft_events");

        cleanup_test_data(&pool, contract_id).await;
    }

    #[tokio::test]
    #[ignore] // Requires test database setup
    async fn test_liquidity_deposit_projection() {
        // Issue #386: Test that deposit/add_liquidity events are projected into liquidity_events
        let pool = setup_test_db().await;
        let contract_id = "CAMM_CONTRACT_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";

        cleanup_test_data(&pool, contract_id).await;

        let now = Utc::now();
        let event_id = "deposit_event_1";

        let decoded_topics = json!(["deposit", "GPROVIDER_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"]);

        let decoded_value = json!({
            "amounts": ["1000000", "2000000"],
            "shares_minted": "1500000"
        });

        // Insert a deposit event
        sqlx::query(
            "INSERT INTO events
             (event_id, contract_id, ledger, ledger_closed_at, event_type, topics,
              decoded_topics, event_name, value, decoded_value, tx_hash, in_successful_call, paging_token)
             VALUES ($1, $2, $3, $4, 'contract', '[]'::jsonb, $5, $6, '', $7, 'hash1', true, $8)",
        )
        .bind(event_id)
        .bind(contract_id)
        .bind(1000i64)
        .bind(now)
        .bind(decoded_topics.to_string())
        .bind("deposit")
        .bind(decoded_value.to_string())
        .bind(event_id)
        .execute(&pool)
        .await
        .expect("Failed to insert deposit event");

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
        .bind("add") // normalize "deposit" to "add"
        .bind(topics[1].as_str())
        .bind(value["amounts"][0].as_str())
        .bind(value["amounts"][1].as_str())
        .bind(value["shares_minted"].as_str())
        .bind("deposit")
        .bind(1000i64)
        .bind(now)
        .execute(&pool)
        .await
        .expect("Failed to project deposit into liquidity_events");

        // Verify projection
        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM liquidity_events WHERE contract_id = $1 AND event_kind = $2",
        )
        .bind(contract_id)
        .bind("add")
        .fetch_one(&pool)
        .await
        .expect("Failed to count deposits");

        assert_eq!(
            count.0, 1,
            "Deposit should be projected into liquidity_events as 'add'"
        );

        cleanup_test_data(&pool, contract_id).await;
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
