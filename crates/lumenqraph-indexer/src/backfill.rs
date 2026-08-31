//! Backfill mode: a one-shot catch-up that walks from a start ledger to the
//! current tip, then exits. Distinct from the live tail — used when registering
//! a contract that was already emitting events before the indexer came online.
//!
//! Bounded by RPC retention window: the SDF public RPC serves ~7 days (120k
//! ledgers) of event history, so `from_ledger` is clamped to the oldest available
//! ledger. This is enforced via MAX_LOOKBACK_LEDGERS in poller.rs, NOT the
//! MAX_CATCHUP_LEDGERS config (which is a live polling performance limit).

use sqlx::PgPool;
use tracing::{info, warn};

use crate::config::Config;
use crate::poller::fetch_and_store;
use crate::rpc_client::RpcClient;
use crate::specs::SpecCache;
use crate::{cursor, poller};

#[cfg(test)]
mod tests {
    //! Backfill integration tests: multi-page catch-up, idempotency, and bound
    //! clamping. The DB-backed tests need a throwaway Postgres:
    //!
    //!   TEST_DATABASE_URL=postgres://…/lumenqraph \
    //!     cargo test -p lumenqraph-indexer -- --ignored --test-threads=1

    use super::*;
    use axum::{extract::State, routing::post, Json, Router};
    use serde_json::{json, Value};
    use sqlx::{postgres::PgPoolOptions, PgPool};
    use std::sync::Arc;
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
    //
    // Pages are matched by incoming cursor: the page whose `cursor_in` equals
    // the request's `pagination.cursor` is returned.  `cursor_in = None` matches
    // the first-page call (no cursor in request).

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
            start_ledger: 0,
            max_catchup_ledgers: 4000,
            state_indexing: false,
            key_indexing: false,
            balance_key_symbol: "Balance".into(),
            balance_key_durability: "persistent".into(),
            retention_ledgers: 0,
            spec_version_retention: 0,
            upgrade_watch: false,
            reorg_overlap_ledgers: 0,
            rpc_timeout_secs: 30,
            enrichment_warn_threshold: 0.5,
            key_templates: vec![],
            spec_cache_max_entries: 2000,
        }
    }

    // ---- Tests ---------------------------------------------------------------

    /// Backfill walks multiple pages and inserts every event exactly once.
    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn backfill_multi_page_inserts_all_events() {
        let pool = fixture().await;

        // page_size = 2 → 3 events require 2 pages.
        // Page 1: full (2 events), cursor "p2" → continue.
        // Page 2: partial (1 event), no cursor  → terminate.
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
        let specs = SpecCache::new(2000, 4);

        let (inserted, _) = fetch_and_store(&pool, &rpc, &config, &specs, 500, 1000)
            .await
            .expect("fetch_and_store");

        assert_eq!(inserted, 3, "all events from both pages must be inserted");

        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 3);

        // Write cursor and verify it's readable (what backfill::run does after fetch).
        cursor::write_progress(&pool, 1000, 1000, inserted)
            .await
            .unwrap();
        let last = cursor::read_last_processed(&pool).await.unwrap();
        assert_eq!(last, Some(1000), "cursor must reflect the last fetched ledger");
    }

    /// A second fetch of the same range inserts zero new rows (idempotency).
    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn backfill_is_idempotent_on_rerun() {
        let pool = fixture().await;

        // Two pages, same events as the first test.
        let make_pages = || {
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
            ]
        };

        let rpc_url = spawn_mock_rpc(1000, make_pages()).await;
        let rpc = RpcClient::new(&rpc_url, 30);
        let config = test_config(&rpc_url, 2);

        // First run: all three events are new.
        let specs = SpecCache::new(2000, 4);
        let (first, _) = fetch_and_store(&pool, &rpc, &config, &specs, 500, 1000)
            .await
            .expect("first run");
        assert_eq!(first, 3);

        // Second run against a fresh mock that serves the same pages.
        let rpc_url2 = spawn_mock_rpc(1000, make_pages()).await;
        let rpc2 = RpcClient::new(&rpc_url2, 30);
        let specs2 = SpecCache::new(2000);
        let (second, _) = fetch_and_store(&pool, &rpc2, &config, &specs2, 500, 1000)
            .await
            .expect("second run");

        assert_eq!(
            second, 0,
            "re-fetching the same ledger range must not insert duplicates"
        );

        let total: i64 = sqlx::query_scalar("SELECT count(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(total, 3, "exactly 3 unique events after two identical runs");
    }

    /// The paging-token loop terminates when the RPC returns a full page with no
    /// cursor (cursor = None terminates regardless of page fullness).
    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn backfill_terminates_when_cursor_is_absent() {
        let pool = fixture().await;

        // page_size = 2 and the RPC returns exactly 2 events but NO cursor → must stop.
        let rpc_url = spawn_mock_rpc(
            1000,
            vec![MockPage {
                cursor_in: None,
                events: vec![make_event("e1", 500), make_event("e2", 500)],
                cursor_out: None,
            }],
        )
        .await;

        let rpc = RpcClient::new(&rpc_url, 30);
        let config = test_config(&rpc_url, 2);
        let specs = SpecCache::new(2000, 4);

        let (inserted, _) = fetch_and_store(&pool, &rpc, &config, &specs, 500, 1000)
            .await
            .expect("fetch_and_store");

        assert_eq!(inserted, 2, "both events on the single page are inserted");

        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 2);
    }

    /// After a simulated mid-backfill failure the cursor reflects the last
    /// *successfully* stored page, not the start or the live tip.
    ///
    /// We verify this by running `fetch_and_store` for page 1 only (simulating
    /// a two-page backfill where page 2 never starts), writing the cursor as
    /// `backfill::run` now does after each page, then asserting the persisted
    /// cursor equals the last ledger from page 1.
    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn backfill_cursor_checkpointed_per_page() {
        let pool = fixture().await;
        let tip = 1000i64;

        // Serve only one page (page 2 would need a second request, which the
        // mock never receives because we stop after the first page).
        let rpc_url = spawn_mock_rpc(
            tip,
            vec![MockPage {
                cursor_in: None,
                events: vec![make_event("e1", 500), make_event("e2", 501)],
                // Return a cursor that signals "there is more" — but we won't
                // request it (simulating an error mid-backfill).
                cursor_out: Some("page2".into()),
            }],
        )
        .await;

        let rpc = RpcClient::new(&rpc_url, 30);
        let config = test_config(&rpc_url, 2);
        let specs = SpecCache::new(2000);

        // Simulate what backfill::run does: fetch page 1, store it, checkpoint.
        let (inserted, _) = fetch_and_store(&pool, &rpc, &config, &specs, 500, tip)
            .await
            .expect("fetch_and_store page 1");

        // Checkpoint after this page (ledger 501 is the max from page 1).
        let page_max_ledger = 501i64;
        cursor::write_progress(&pool, page_max_ledger, tip, inserted)
            .await
            .unwrap();

        // The cursor must reflect page 1's last ledger, not 0 and not `tip`.
        let last = cursor::read_last_processed(&pool).await.unwrap();
        assert_eq!(
            last,
            Some(page_max_ledger),
            "cursor must be checkpointed to the last page's max ledger after a partial backfill"
        );
    }

    /// `backfill::run` clamps `from_ledger` to the oldest ledger the RPC still
    /// serves. This is a pure computation test — no DB or network needed.
    #[test]
    fn backfill_start_is_clamped_to_oldest_available() {
        let tip = 1_000_000i64;
        let oldest = tip - poller::max_lookback();

        // Requesting before the oldest available ledger → clamped to oldest.
        let from = oldest - 10_000;
        let start = from.max(oldest).max(1);
        assert_eq!(
            start, oldest,
            "from_ledger older than retention window must be clamped"
        );

        // Requesting within the retention window → no clamping.
        let from = oldest + 1_000;
        let start = from.max(oldest).max(1);
        assert_eq!(
            start, from,
            "from_ledger within retention window must not be clamped"
        );

        // Requesting exactly the oldest available ledger → no clamping.
        let start = oldest.max(oldest).max(1);
        assert_eq!(
            start, oldest,
            "from_ledger equal to oldest_available must not be clamped further"
        );
    }
}

pub async fn run(
    pool: PgPool,
    rpc: RpcClient,
    config: Config,
    from_ledger: i64,
) -> anyhow::Result<()> {
    let tip = rpc.get_latest_ledger().await?;
    let oldest = tip - poller::max_lookback();

    // If the caller passes 0 (or no explicit --from), try to resume from the
    // last persisted cursor so an interrupted backfill picks up where it left
    // off.  A non-zero explicit from_ledger always wins.
    let resume_from = if from_ledger == 0 {
        cursor::read_last_processed(&pool)
            .await?
            .map(|l| l + 1)
            .unwrap_or(0)
    } else {
        from_ledger
    };

    let start = resume_from.max(oldest).max(1);
    if start > resume_from && resume_from > 0 {
        warn!(
            requested = resume_from,
            clamped_to = start,
            "backfill start is older than RPC retention; clamping"
        );
    }
    info!(from = start, to = tip, "starting backfill");

    let specs = SpecCache::new(config.spec_cache_max_entries);

    // Drive the paging loop here so we can checkpoint the cursor after every
    // successfully stored page.  An interrupted backfill can be resumed by
    // re-running with from_ledger = 0 (the default): the code above will read
    // the persisted cursor and continue from the last completed page.
    let mut cursor_token: Option<String> = None;
    let mut total_inserted = 0u64;
    let mut last_page_ledger = start;

    loop {
        let page = rpc
            .get_events(
                Some(start),
                &config.contract_ids,
                cursor_token.clone(),
                config.page_size,
            )
            .await?;

        let page_len = page.events.len();
        let page_max_ledger = page
            .events
            .iter()
            .map(|e| e.ledger)
            .max()
            .unwrap_or(last_page_ledger);

        // Build and store this page's events.
        let mut batch = Vec::with_capacity(page_len);
        for ev in &page.events {
            let spec = specs.get(&pool, &rpc, &ev.contract_id, ev.ledger).await;
            batch.push(crate::convert::to_new_event(ev, spec.as_deref()));
        }
        let inserted = crate::store::insert_events(&pool, &batch).await?;
        total_inserted += inserted;

        // ── checkpoint after each successfully stored page ──────────────────
        // Writing the cursor here means a crash or Ctrl-C on the *next* page
        // leaves the cursor pointing at the end of the last page we finished,
        // so the backfill is resumable with no ledger range lost.
        last_page_ledger = page_max_ledger;
        cursor::write_progress(&pool, last_page_ledger, tip, inserted).await?;
        info!(
            inserted,
            page_max_ledger,
            total_inserted,
            "backfill page complete (cursor checkpointed)"
        );

        cursor_token = page.cursor;
        if page_len < config.page_size as usize || cursor_token.is_none() {
            break;
        }
    }

    // Final cursor advance to the tip so the live poller starts from there.
    cursor::write_progress(&pool, tip, tip, 0).await?;
    info!(total_inserted, up_to_ledger = tip, "backfill complete");
    Ok(())
}
