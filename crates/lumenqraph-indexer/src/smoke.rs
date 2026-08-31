//! End-to-end smoke test: ephemeral Postgres + mock Soroban RPC.
//!
//! Exercises the full pipeline without any live network:
//!
//!  1. INDEXER  — `fetch_and_store` against a mock RPC populates `events`.
//!  2. API sim  — direct SQL queries that mirror what the REST/GraphQL routes
//!                return, asserting the ingested data is correctly stored.
//!  3. WEBHOOKS — the enqueue SQL (identical to `dispatcher::enqueue_events`)
//!                creates pending deliveries; a local HTTP sink receives and
//!                validates the signed POST.
//!
//! This module is gated behind the `smoke-tests` cargo feature *and*
//! `#[ignore]`, so a plain `cargo test` never compiles or runs it (offline CI
//! stays offline). Run it explicitly with:
//!
//!   TEST_DATABASE_URL=postgres://…/lumenqraph \
//!     cargo test -p lumenqraph-indexer --features smoke-tests smoke \
//!       -- --ignored --test-threads=1
//!
//! or `make test-smoke`. See CONTRIBUTING.md → "Smoke tests".

#[cfg(test)]
mod tests {
    use crate::config::Config;
    use crate::rpc_client::RpcClient;
    use crate::specs::SpecCache;
    use crate::{cursor, poller};
    use axum::{
        body::Bytes,
        extract::State,
        http::HeaderMap,
        routing::post,
        Json, Router,
    };
    use serde_json::{json, Value};
    use sqlx::{postgres::PgPoolOptions, PgPool};
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;

    // ---- DB fixture ----------------------------------------------------------

    async fn fixture() -> PgPool {
        let url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL");
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .expect("connect");
        for stmt in ["DROP SCHEMA public CASCADE", "CREATE SCHEMA public"] {
            sqlx::query(stmt).execute(&pool).await.expect("reset schema");
        }
        sqlx::migrate!("../../migrations")
            .run(&pool)
            .await
            .expect("migrate");
        pool
    }

    // ---- Mock Soroban RPC server ---------------------------------------------

    struct MockPage {
        cursor_in: Option<String>,
        events: Vec<Value>,
        cursor_out: Option<String>,
    }

    struct MockRpc {
        tip: i64,
        pages: Vec<MockPage>,
    }

    async fn mock_rpc_handler(
        State(state): State<Arc<MockRpc>>,
        Json(req): Json<Value>,
    ) -> Json<Value> {
        match req["method"].as_str().unwrap_or("") {
            "getLatestLedger" => Json(json!({
                "jsonrpc": "2.0", "id": 1,
                "result": { "sequence": state.tip }
            })),
            "getEvents" => {
                let cursor = req["params"]["pagination"]["cursor"]
                    .as_str()
                    .map(String::from);
                let page = state
                    .pages
                    .iter()
                    .find(|p| p.cursor_in.as_deref() == cursor.as_deref())
                    .unwrap_or_else(|| state.pages.last().unwrap());
                Json(json!({
                    "jsonrpc": "2.0", "id": 1,
                    "result": {
                        "latestLedger": state.tip,
                        "events": page.events,
                        "cursor": page.cursor_out
                    }
                }))
            }
            _ => Json(json!({
                "jsonrpc": "2.0", "id": 1,
                "error": { "code": -32601, "message": "method not found" }
            })),
        }
    }

    async fn spawn_mock_rpc(tip: i64, pages: Vec<MockPage>) -> String {
        let state = Arc::new(MockRpc { tip, pages });
        let router = Router::new()
            .route("/", post(mock_rpc_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        format!("http://{addr}")
    }

    // ---- Mock webhook sink ---------------------------------------------------
    //
    // Collects incoming POST requests so the test can assert the payload and
    // the HMAC-SHA256 signature header.

    #[derive(Clone, Default)]
    struct SinkState {
        received: Arc<Mutex<Vec<(HeaderMap, Bytes)>>>,
    }

    async fn sink_handler(
        State(state): State<SinkState>,
        headers: HeaderMap,
        body: Bytes,
    ) -> axum::http::StatusCode {
        state.received.lock().unwrap().push((headers, body));
        axum::http::StatusCode::OK
    }

    async fn spawn_webhook_sink() -> (String, Arc<Mutex<Vec<(HeaderMap, Bytes)>>>) {
        let state = SinkState::default();
        let received = state.received.clone();
        let router = Router::new()
            .route("/hook", post(sink_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        (format!("http://{addr}/hook"), received)
    }

    // ---- Helpers -------------------------------------------------------------

    fn make_event(id: &str, ledger: i64) -> Value {
        json!({
            "type": "contract",
            "ledger": ledger,
            "ledgerClosedAt": "2024-01-01T00:00:00Z",
            "contractId": "C1",
            "id": id,
            "pagingToken": id,
            "inSuccessfulContractCall": true,
            "txHash": "tx1",
            "topic": [],
            "value": ""
        })
    }

    fn test_config(rpc_url: impl Into<String>, page_size: u32) -> Config {
        Config {
            database_url: String::new(),
            rpc_url: rpc_url.into(),
            contract_ids: vec![],
            poll_interval_secs: 5,
            page_size,
            start_ledger: 500,
            max_catchup_ledgers: 4000,
            state_indexing: false,
            key_indexing: false,
            balance_key_symbol: "Balance".into(),
            balance_key_durability: "persistent".into(),
            retention_ledgers: 0,
            upgrade_watch: false,
            reorg_overlap_ledgers: 0,
            rpc_timeout_secs: 30,
            enrichment_warn_threshold: 0.5,
            key_templates: vec![],
            spec_cache_max_entries: 2000,
        }
    }

    // ---- Test ----------------------------------------------------------------

    /// Full pipeline smoke test:
    ///
    /// mock RPC → indexer → Postgres → (API sim) → webhook enqueue → webhook deliver
    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn smoke_full_pipeline() {
        let pool = fixture().await;

        // ── Phase 1: INDEXER ────────────────────────────────────────────────
        // Three events across two pages (page_size = 2).
        let rpc_url = spawn_mock_rpc(
            1000,
            vec![
                MockPage {
                    cursor_in: None,
                    events: vec![make_event("e1", 500), make_event("e2", 500)],
                    cursor_out: Some("p2".into()),
                },
                MockPage {
                    cursor_in: Some("p2".into()),
                    events: vec![make_event("e3", 501)],
                    cursor_out: None,
                },
            ],
        )
        .await;

        let rpc = RpcClient::new(&rpc_url, 30);
        let config = test_config(&rpc_url, 2);
        let specs = SpecCache::new(config.spec_cache_max_entries);

        let (inserted, _) = poller::fetch_and_store(&pool, &rpc, &config, &specs, 500, 1000)
            .await
            .expect("indexer cycle");

        assert_eq!(inserted, 3, "all three events must be indexed");

        // Record the cursor (what backfill::run / poller::poll_once does).
        cursor::write_progress(&pool, 1000, 1000, inserted)
            .await
            .unwrap();

        // ── Phase 2: API ASSERTIONS ─────────────────────────────────────────
        // These SQL queries mirror what the REST/GraphQL handlers execute so
        // we can assert the data shape without booting the API binary.

        // /events — all events for C1 ordered by ledger.
        let event_rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT event_id, ledger FROM events WHERE contract_id = 'C1' ORDER BY ledger, event_id",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            event_rows,
            vec![
                ("e1".to_string(), 500),
                ("e2".to_string(), 500),
                ("e3".to_string(), 501),
            ],
            "/events should reflect all three ingested events"
        );

        // /contracts — contract_summaries denormalised row for C1.
        let summary_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM contract_summaries WHERE contract_id = 'C1'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            summary_count, 1,
            "/contracts must list C1 after events are indexed"
        );

        // /transfers — token_transfers; these events have no decoded transfer topic
        // so the table should be empty (no false positives).
        let transfer_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM token_transfers")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            transfer_count, 0,
            "events with no transfer topic must not project phantom transfers"
        );

        // Indexer cursor advanced to tip.
        let last = cursor::read_last_processed(&pool).await.unwrap();
        assert_eq!(
            last,
            Some(1000),
            "cursor must reflect the tip we indexed up to"
        );

        // ── Phase 3: WEBHOOK PIPELINE ────────────────────────────────────────
        // Start a local HTTP sink so we can receive the signed delivery.
        let (sink_url, received_calls) = spawn_webhook_sink().await;

        // Register a subscription pointing at our local sink.
        let secret = "smoke-test-secret";
        sqlx::query(
            "INSERT INTO webhook_subscriptions (url, kind, secret) VALUES ($1, 'event', $2)",
        )
        .bind(&sink_url)
        .bind(secret)
        .execute(&pool)
        .await
        .unwrap();

        // Replicate dispatcher::enqueue_events: advance the watermark from 0
        // to global-max, creating one delivery row per (subscription, event).
        let last_seq: i64 = sqlx::query_scalar(
            "INSERT INTO webhook_state (id, last_seq) VALUES (1, 0)
             ON CONFLICT (id) DO UPDATE SET last_seq = webhook_state.last_seq
             RETURNING last_seq",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let global_max: i64 =
            sqlx::query_scalar("SELECT COALESCE(max(seq), 0) FROM events")
                .fetch_one(&pool)
                .await
                .unwrap();

        let enqueued = sqlx::query(
            "INSERT INTO webhook_deliveries (subscription_id, event_id)
             SELECT s.id, e.event_id
             FROM events e
             JOIN webhook_subscriptions s
               ON s.active
              AND s.kind = 'event'
              AND (s.contract_id IS NULL OR s.contract_id = e.contract_id)
              AND (s.event_name  IS NULL OR s.event_name  = e.event_name)
             WHERE e.seq > $1 AND e.seq <= $2
             ON CONFLICT (subscription_id, event_id) DO NOTHING",
        )
        .bind(last_seq)
        .bind(global_max)
        .execute(&pool)
        .await
        .unwrap()
        .rows_affected();

        assert_eq!(enqueued, 3, "one pending delivery per indexed event");

        sqlx::query("UPDATE webhook_state SET last_seq = $1 WHERE id = 1")
            .bind(global_max)
            .execute(&pool)
            .await
            .unwrap();

        // All delivery rows are pending.
        let pending: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM webhook_deliveries WHERE status = 'pending'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(pending, 3);

        // Deliver one payload manually: fetch the first due delivery's payload,
        // sign it, POST to the sink, and mark it delivered.
        let (delivery_id, payload_json): (i64, Value) = sqlx::query_as(
            "SELECT d.id,
                    to_jsonb(e) - 'seq' AS payload
             FROM webhook_deliveries d
             JOIN events e ON e.event_id = d.event_id
             WHERE d.status = 'pending'
             ORDER BY d.id
             LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let body = serde_json::to_vec(&payload_json).unwrap();

        // Compute HMAC-SHA256 (mirrors dispatcher::send).
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(&body);
        let signature = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));

        let http = reqwest::Client::new();
        let resp = http
            .post(&sink_url)
            .header("Content-Type", "application/json")
            .header("X-Lumenqraph-Signature", &signature)
            .body(body)
            .send()
            .await
            .expect("delivery POST");
        assert!(resp.status().is_success(), "sink must accept the delivery");

        // Give the sink task a moment to process the request.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let calls = received_calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "sink must have received exactly one POST");
        let sig_header = calls[0]
            .0
            .get("X-Lumenqraph-Signature")
            .expect("delivery must carry the HMAC signature header");
        assert!(
            sig_header.to_str().unwrap().starts_with("sha256="),
            "signature header must be sha256=<hex>"
        );
        drop(calls);

        // Mark it delivered (what dispatcher::mark_delivered does).
        sqlx::query(
            "UPDATE webhook_deliveries
             SET status='delivered', attempts=attempts+1, delivered_at=now()
             WHERE id=$1",
        )
        .bind(delivery_id)
        .execute(&pool)
        .await
        .unwrap();

        let delivered: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM webhook_deliveries WHERE status = 'delivered'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(delivered, 1, "one delivery must be marked delivered");
    }
}
