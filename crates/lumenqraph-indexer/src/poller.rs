//! The live polling loop: fetch new events from the tip, page through them,
//! store, advance the cursor, sleep, repeat. Failures are logged, counted, and
//! retried with exponential backoff so a flaky RPC never kills the process.
//! Responds to Ctrl-C / SIGTERM for a clean shutdown.

use std::time::{Duration, Instant};

use lumenqraph_core::NewEvent;
use sqlx::PgPool;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::convert::to_new_event;
use crate::rpc_client::RpcClient;
use crate::specs::{self, SpecCache};
use crate::{cursor, keys, retention, state, store};

/// How often to prune outside the retention window. Decoupled from the poll
/// interval: retention is a slow-moving disk concern, and re-checking it every
/// few seconds would spend more on probes than the deletes are worth.
const PRUNE_INTERVAL: Duration = Duration::from_secs(60);

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
    // None => prune on the first cycle that reaches the tip, so a deployment
    // that switches retention on starts reclaiming immediately.
    let mut last_prune: Option<Instant> = None;

    loop {
        let sleep_for = match poll_once(&pool, &rpc, &config, &specs).await {
            Ok(processed_to) => {
                backoff = base_interval;
                if let Some(ledger) = processed_to {
                    debug!(ledger, "cycle complete");
                    if config.retention_ledgers > 0
                        && last_prune.is_none_or(|t| t.elapsed() >= PRUNE_INTERVAL)
                    {
                        // Never fatal: falling behind on disk reclamation is bad,
                        // but stopping the tail over it is worse.
                        if let Err(e) =
                            retention::prune(&pool, ledger, config.retention_ledgers).await
                        {
                            warn!(error = %e, "retention prune failed");
                        }
                        last_prune = Some(Instant::now());
                    }
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
    if latest - start > config.max_catchup_ledgers {
        let clamped = latest - config.max_catchup_ledgers;
        warn!(
            from = start,
            to = clamped,
            gap_ledgers = clamped - start,
            "cursor too far behind tip; skipping ahead to the catch-up window \
             (gap unrecoverable via public RPC — use a retaining/paid RPC or a \
             datalake backfill for gapless history, or raise MAX_CATCHUP_LEDGERS)"
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
    // Contracts seen this cycle, used to bound per-contract instance reads when
    // no explicit CONTRACT_IDS list does it for us.
    let mut active_contracts: std::collections::HashSet<String> = std::collections::HashSet::new();
    let tracks_active_contracts =
        (config.state_indexing || config.upgrade_watch) && config.contract_ids.is_empty();
    // contract -> holder addresses seen this cycle (for per-key balance snapshots).
    let mut holders_by_contract: std::collections::HashMap<
        String,
        std::collections::HashSet<String>,
    > = std::collections::HashMap::new();
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
            // Only needed for index-all instance reads (see below).
            if tracks_active_contracts {
                active_contracts.insert(ev.contract_id.clone());
            }
            let new_event = to_new_event(ev, spec.as_deref());
            // Discover holder addresses to snapshot per-key balances for.
            if config.key_indexing {
                for holder in keys::holders_in_event(&new_event) {
                    holders_by_contract
                        .entry(new_event.contract_id.clone())
                        .or_default()
                        .insert(holder);
                }
            }
            batch.push(new_event);
        }
        let n = batch.len();
        total_inserted += store::insert_events(pool, &batch).await?;

        cursor_token = page.cursor;
        if n < config.page_size as usize || cursor_token.is_none() {
            break;
        }
    }

    // Read each tracked contract's instance entry. With an explicit CONTRACT_IDS
    // list we track those contracts every cycle; in index-all mode we restrict
    // to contracts active this cycle to bound the extra RPC calls.
    //
    // State indexing and the upgrade watch both want this entry — for the
    // storage map and the executable hash respectively — so whenever state
    // indexing is on it covers both, and the upgrade watch adds no RPC calls.
    if config.state_indexing || config.upgrade_watch {
        let targets: Vec<&String> = if config.contract_ids.is_empty() {
            active_contracts.iter().collect()
        } else {
            config.contract_ids.iter().collect()
        };
        for contract_id in targets {
            if config.state_indexing {
                // Change-detected, so unchanged instances are no-op writes.
                // Also notes the executable hash, detecting upgrades for free.
                state::snapshot(pool, rpc, specs, contract_id).await;
            } else {
                specs::check_for_upgrade(pool, rpc, specs, contract_id).await;
            }
        }
    }

    // Snapshot per-holder balances discovered from this cycle's token events.
    // When CONTRACT_IDS is set we only track balances for those contracts.
    if config.key_indexing {
        let durability = keys::parse_durability(&config.balance_key_durability);
        for (contract_id, holders) in &holders_by_contract {
            if !config.contract_ids.is_empty() && !config.contract_ids.contains(contract_id) {
                continue;
            }
            for holder in holders {
                match keys::balance_key(&config.balance_key_symbol, holder) {
                    Ok(key) => {
                        state::snapshot_data(
                            pool,
                            rpc,
                            contract_id,
                            &key,
                            durability,
                            Some("balance"),
                        )
                        .await
                    }
                    Err(e) => debug!(holder, error = %e, "skipping unbuildable balance key"),
                }
            }
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
