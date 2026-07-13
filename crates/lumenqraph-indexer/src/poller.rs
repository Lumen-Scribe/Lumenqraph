//! The live polling loop: fetch new events from the tip, page through them,
//! store, advance the cursor, sleep, repeat. Failures are logged, counted, and
//! retried with exponential backoff so a flaky RPC never kills the process.
//! Responds to Ctrl-C / SIGTERM for a clean shutdown.

use std::time::Duration;

use lumenqraph_core::NewEvent;
use sqlx::PgPool;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::convert::to_new_event;
use crate::rpc_client::RpcClient;
use crate::specs::SpecCache;
use crate::{cursor, store};

/// getEvents only serves recent history. If our cursor falls further than this
/// behind the tip, we clamp forward and log the (unrecoverable) gap.
const MAX_LOOKBACK_LEDGERS: i64 = 120_000; // ~7 days at ~5s/ledger

/// The retention-window bound shared with backfill.
pub fn max_lookback() -> i64 {
    MAX_LOOKBACK_LEDGERS
}

pub async fn run(pool: PgPool, rpc: RpcClient, config: Config) -> anyhow::Result<()> {
    let base_interval = Duration::from_secs(config.poll_interval_secs.max(1));
    let mut backoff = base_interval;
    // One spec cache for the process lifetime: each contract's interface is
    // fetched and parsed once, then reused to enrich every event.
    let specs = SpecCache::new();

    loop {
        let sleep_for = match poll_once(&pool, &rpc, &config, &specs).await {
            Ok(processed_to) => {
                backoff = base_interval;
                if let Some(ledger) = processed_to {
                    debug!(ledger, "cycle complete");
                }
                base_interval
            }
            Err(e) => {
                warn!(error = %e, backoff_secs = backoff.as_secs(), "poll cycle failed; backing off");
                let _ = cursor::incr_errors(&pool).await;
                let this = backoff;
                backoff = (backoff * 2).min(Duration::from_secs(60));
                this
            }
        };

        tokio::select! {
            _ = tokio::time::sleep(sleep_for) => {}
            _ = shutdown_signal() => {
                info!("shutdown signal received; stopping poller");
                return Ok(());
            }
        }
    }
}

/// One catch-up to the current tip. Returns the ledger we advanced to.
async fn poll_once(
    pool: &PgPool,
    rpc: &RpcClient,
    config: &Config,
    specs: &SpecCache,
) -> anyhow::Result<Option<i64>> {
    let latest = rpc.get_latest_ledger().await?;

    let mut start = match cursor::read_last_processed(pool).await? {
        Some(c) => c + 1,
        None if config.start_ledger > 0 => config.start_ledger,
        None => latest,
    };

    if start > latest {
        // Nothing new closed; still record the tip so lag reflects reality.
        cursor::write_progress(pool, start - 1, latest, 0).await?;
        return Ok(None);
    }
    if latest - start > MAX_LOOKBACK_LEDGERS {
        let clamped = latest - MAX_LOOKBACK_LEDGERS;
        warn!(
            from = start,
            to = clamped,
            "cursor beyond RPC retention; skipping ahead (gap unrecoverable via RPC)"
        );
        start = clamped;
    }

    let inserted = fetch_and_store(pool, rpc, config, specs, start, latest).await?;
    cursor::write_progress(pool, latest, latest, inserted).await?;
    if inserted > 0 {
        info!(inserted, up_to_ledger = latest, "indexed events");
    }
    Ok(Some(latest))
}

/// Page through events from `start` to the tip, storing each page. Shared by the
/// live poller and the backfill command.
pub async fn fetch_and_store(
    pool: &PgPool,
    rpc: &RpcClient,
    config: &Config,
    specs: &SpecCache,
    start: i64,
    _tip: i64,
) -> anyhow::Result<u64> {
    let mut cursor_token: Option<String> = None;
    let mut total_inserted = 0u64;
    loop {
        let page = rpc
            .get_events(
                Some(start),
                &config.contract_ids,
                cursor_token.clone(),
                config.page_size,
            )
            .await?;
        let mut batch: Vec<NewEvent> = Vec::with_capacity(page.events.len());
        for ev in &page.events {
            // Interface lookups are cached, so this is one fetch per contract.
            let spec = specs.get(pool, rpc, &ev.contract_id).await;
            batch.push(to_new_event(ev, spec.as_deref()));
        }
        let n = batch.len();
        total_inserted += store::insert_events(pool, &batch).await?;

        cursor_token = page.cursor;
        if n < config.page_size as usize || cursor_token.is_none() {
            break;
        }
    }
    Ok(total_inserted)
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut sig) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            sig.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
