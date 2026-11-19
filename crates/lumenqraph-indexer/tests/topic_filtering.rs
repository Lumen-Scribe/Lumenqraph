//! Integration tests for JSONB topic filtering in the events table.
//!
//! These tests verify that the positional topic filtering using JSONB containment
//! works correctly across various edge cases, including null topics, empty arrays,
//! and multi-position matches.

#[cfg(test)]
mod topic_filtering_tests {
    use lumenqraph_indexer::rpc_client::{
        build_event_filters, encode_topic_filter, EventFilter, TopicFilterError,
        MAX_EVENT_FILTERS, MAX_TOPICS_PER_FILTER,
    };
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

    // ------------------------------------------------------------------
    // EVENT_TOPIC_FILTERS -> getEvents `topics` pushdown
    // ------------------------------------------------------------------

    /// Minimal mock of the Soroban RPC `getEvents` JSON-RPC endpoint.
    ///
    /// It records the raw request body and replies with an empty event page so
    /// the client stops paging immediately.
    mod mock_rpc {
        use std::sync::{Arc, Mutex};

        use axum::{extract::State, routing::post, Json, Router};
        use serde_json::{json, Value};

        #[derive(Clone, Default)]
        pub struct MockRpc {
            pub requests: Arc<Mutex<Vec<Value>>>,
        }

        impl MockRpc {
            pub fn requests(&self) -> Vec<Value> {
                self.requests.lock().unwrap().clone()
            }

            /// Returns the `topics` field of the first `getEvents` filter, if any.
            pub fn first_filter_topics(&self) -> Option<Value> {
                self.requests()
                    .into_iter()
                    .find(|req| req.get("method").and_then(Value::as_str) == Some("getEvents"))
                    .and_then(|req| req.get("params").cloned())
                    .and_then(|params| params.get("filters").cloned())
                    .and_then(|filters| filters.get(0).cloned())
                    .and_then(|filter| filter.get("topics").cloned())
            }
        }

        async fn handle(
            State(state): State<MockRpc>,
            Json(body): Json<Value>,
        ) -> Json<Value> {
            state.requests.lock().unwrap().push(body.clone());
            let id = body.get("id").cloned().unwrap_or(Value::Null);
            Json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "events": [],
                    "latestLedger": 1,
                    "cursor": "0"
                }
            }))
        }

        /// Spawns the mock RPC on an ephemeral port and returns its URL.
        pub async fn spawn() -> (MockRpc, String) {
            let state = MockRpc::default();
            let app = Router::new()
                .route("/", post(handle))
                .with_state(state.clone());

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind mock rpc");
            let addr = listener.local_addr().expect("mock rpc addr");
            tokio::spawn(async move {
                axum::serve(listener, app).await.expect("mock rpc serve");
            });

            (state, format!("http://{}", addr))
        }
    }

    #[test]
    fn test_encode_topic_filter_symbols_and_wildcards() {
        // Symbols are encoded as base64 ScVal symbols; `*` passes through.
        let encoded = encode_topic_filter(&["transfer".to_string(), "*".to_string()]);
        assert_eq!(encoded.len(), 2);
        assert_eq!(encoded[1], "*");
        // base64 of the ScVal symbol "transfer" (scvSymbol = 0x0e)
        assert_eq!(encoded[0], "AAAADwAAAAh0cmFuc2Zlcg==");
    }

    #[test]
    fn test_build_event_filters_topics_only() {
        let patterns = vec![
            vec!["transfer".to_string(), "*".to_string(), "*".to_string()],
            vec!["swap".to_string()],
        ];
        let filters = build_event_filters(&[], &patterns).expect("filters");
        assert_eq!(filters.len(), 2);
        assert!(filters.iter().all(|f| f.contract_ids.is_empty()));
        assert_eq!(filters[0].topics.as_ref().unwrap().len(), 3);
        assert_eq!(filters[0].topics.as_ref().unwrap()[1], "*");
        assert_eq!(filters[1].topics.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn test_build_event_filters_cross_product_with_contracts() {
        let contracts = vec!["CAAA".to_string(), "CBBB".to_string()];
        let patterns = vec![
            vec!["transfer".to_string()],
            vec!["swap".to_string()],
        ];
        let filters = build_event_filters(&contracts, &patterns).expect("filters");
        // 2 contracts x 2 topic patterns
        assert_eq!(filters.len(), 4);
        for filter in &filters {
            assert_eq!(filter.contract_ids.len(), 1);
            assert!(filter.topics.is_some());
        }
    }

    #[test]
    fn test_build_event_filters_rejects_too_many_filters() {
        let contracts: Vec<String> = (0..MAX_EVENT_FILTERS + 1)
            .map(|i| format!("C{:0>55}", i))
            .collect();
        let err = build_event_filters(&contracts, &[]).unwrap_err();
        assert!(matches!(err, TopicFilterError::TooManyFilters { .. }));
    }

    #[test]
    fn test_build_event_filters_rejects_too_many_topics() {
        let pattern: Vec<String> = (0..MAX_TOPICS_PER_FILTER + 1)
            .map(|i| format!("topic{}", i))
            .collect();
        let err = build_event_filters(&[], &[pattern]).unwrap_err();
        assert!(matches!(err, TopicFilterError::TooManyTopics { .. }));
    }

    #[test]
    fn test_build_event_filters_rejects_cross_product_overflow() {
        let contracts: Vec<String> = (0..3).map(|i| format!("C{}", i)).collect();
        let patterns: Vec<Vec<String>> = (0..3).map(|i| vec![format!("t{}", i)]).collect();
        // 3 x 3 = 9 > MAX_EVENT_FILTERS (5)
        let err = build_event_filters(&contracts, &patterns).unwrap_err();
        assert!(matches!(err, TopicFilterError::TooManyFilters { .. }));
    }

    #[test]
    fn test_event_filter_serializes_topics_field() {
        let filter = EventFilter {
            event_type: "contract".to_string(),
            contract_ids: vec![],
            topics: Some(vec![
                encode_topic_filter(&["transfer".to_string()])[0].clone(),
                "*".to_string(),
            ]),
        };
        let json = serde_json::to_value(&filter).expect("serialize");
        assert_eq!(json["type"], "contract");
        assert_eq!(json["topics"][1], "*");
        // `topics` must be omitted entirely when unset.
        let bare = EventFilter {
            event_type: "contract".to_string(),
            contract_ids: vec![],
            topics: None,
        };
        let bare_json = serde_json::to_value(&bare).expect("serialize");
        assert!(bare_json.get("topics").is_none());
    }

    #[tokio::test]
    async fn test_topics_reach_get_events_request() {
        let (mock, url) = mock_rpc::spawn().await;

        let patterns = vec![
            vec!["transfer".to_string(), "*".to_string(), "*".to_string()],
            vec!["swap".to_string()],
        ];
        let filters = build_event_filters(&[], &patterns).expect("filters");

        let client = lumenqraph_indexer::rpc_client::RpcClient::new(&url);
        // Empty page from the mock means this returns immediately.
        let _ = client.get_events(1, &filters, None, 100).await;

        let topics = mock
            .first_filter_topics()
            .expect("getEvents request must carry a topics field");
        let topics = topics.as_array().expect("topics must be an array");
        assert_eq!(topics.len(), 3);
        assert_eq!(topics[1], "*");
        assert_eq!(topics[2], "*");
        assert_eq!(topics[0], "AAAADwAAAAh0cmFuc2Zlcg==");
    }
}
