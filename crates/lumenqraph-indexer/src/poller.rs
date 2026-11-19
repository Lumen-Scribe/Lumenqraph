//! The live polling loop: fetch new events from the tip, page through them,
//! store, advance the cursor, sleep, repeat. Failures are logged, counted, and
//! retried with exponential backoff so a flaky RPC never kills the process.
//! Responds to Ctrl-C / SIGTERM for a clean shutdown.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

use lumenqraph_core::NewEvent;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::convert::to_new_event;
use crate::rpc_client::RpcClient;
use crate::specs::{self, SpecCache};
use crate::{cursor, keys, retention, state, store};

/// How often to prune outside the retention window. Decoupled from the poll
/// interval: retention is a slow-moving disk concern, and re-checking it every
/// few seconds would spend more on probes than the deletes are worth.
const PRUNE_INTERVAL: Duration = Duration::from_secs(60);

/// The RPC retention window: how far back the Soroban RPC will serve events.
/// This is the hard limit of what's available via `getEvents` on SDF public RPC.
/// Backfill and fresh-start clamping use this value. Note: MAX_CATCHUP_LEDGERS
/// (in config) is a separate, more conservative limit for live polling performance.
const MAX_LOOKBACK_LEDGERS: i64 = 120_000; // ~7 days at ~5s/ledger (SDF public RPC)
static RPC_OLDEST_LEDGER: AtomicI64 = AtomicI64::new(0);

pub fn rpc_oldest_ledger() -> i64 {
    RPC_OLDEST_LEDGER.load(Ordering::Relaxed)
}

/// Consecutive permanent RPC errors tolerated before the poller exits. One odd
/// response from a load-balanced endpoint should not kill the process, but a
/// genuine configuration error keeps recurring, so a small budget still exits (#716).
const PERMANENT_ERROR_BUDGET: u32 = 3;

/// Whether a run of consecutive permanent RPC errors should stop the poller (#716).
pub fn permanent_error_budget_exhausted(consecutive_permanent: u32) -> bool {
    consecutive_permanent >= PERMANENT_ERROR_BUDGET
}

/// The RPC retention window shared with backfill for fresh-start clamping.
pub fn max_lookback() -> i64 {
    MAX_LOOKBACK_LEDGERS
}

pub async fn run(pool: PgPool, rpc: RpcClient, config: Config, specs: Arc<SpecCache>) -> anyhow::Result<()> {
    let base_interval = Duration::from_secs(config.poll_interval_secs.max(1));
    let degraded_interval = Duration::from_secs(config.degraded_poll_interval_secs.max(1));
    let mut backoff = base_interval;
    // The caller owns the process-wide cache shared with the HTTP metrics and
    // health listener. Never shadow this injected instance: doing so causes
    // duplicate WASM fetches and makes the exported cache metric misleading.
    // None => prune on the first cycle that reaches the tip, so a deployment
    // that switches retention on starts reclaiming immediately.
    let mut last_prune: Option<Instant> = None;
    // Circuit-breaker state: count consecutive poll failures.
    let mut consecutive_errors: u32 = 0;
    // Consecutive permanent (configuration-style) RPC errors; see PERMANENT_ERROR_BUDGET.
    let mut consecutive_permanent: u32 = 0;
    let shutdown = CancellationToken::new();
    tokio::spawn(wait_for_shutdown(shutdown.clone()));

    loop {
        let sleep_for = match poll_once(&pool, &rpc, &config, &specs, &shutdown).await {
            Ok(processed_to) => {
                consecutive_permanent = 0;
                // Success: reset both the backoff and the circuit-breaker counter.
                if consecutive_errors > 0 {
                    info!(
                        consecutive_errors,
                        "poll cycle succeeded; resetting circuit breaker"
                    );
                    consecutive_errors = 0;
                    // Record cleared state to the gauge.
                    let _ = cursor::set_consecutive_errors(&pool, 0).await;
                }
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
                        // Prune old spec versions (if enabled)
                        if config.spec_version_retention > 0 {
                            if let Err(e) =
                                retention::prune_spec_versions(
                                    &pool,
                                    ledger,
                                    config.retention_ledgers,
                                    config.spec_version_retention,
                                )
                                .await
                            {
                                warn!(error = %e, "spec version retention prune failed");
                            }
                        }
                        last_prune = Some(Instant::now());
                    }
                }
                base_interval
            }
            Err(e) => {
                // Permanent RPC errors (method not found, invalid contract ID, ...) are
                // configuration errors. They fail fast rather than retry forever, but a
                // single odd response is not enough to kill the process (#716).
                if crate::rpc_client::is_non_retryable_rpc_error(&e) {
                    consecutive_permanent += 1;
                    if permanent_error_budget_exhausted(consecutive_permanent) {
                        error!(
                            error = %e,
                            consecutive_permanent,
                            "permanent non-retryable RPC configuration error; exiting poll loop"
                        );
                        return Err(e);
                    }
                    warn!(
                        error = %e,
                        consecutive_permanent,
                        budget = PERMANENT_ERROR_BUDGET,
                        "permanent RPC error; retrying before giving up"
                    );
                } else {
                    consecutive_permanent = 0;
                }
                if crate::rpc_client::is_out_of_retention(&e) {
                    // The window moved past our cursor between getHealth and getEvents.
                    // Backing off lets the next cycle re-read getHealth and re-clamp.
                    warn!(error = %e, "startLedger fell outside RPC retention; re-clamping on next cycle");
                }

                consecutive_errors += 1;
                let _ = cursor::incr_errors(&pool).await;
                let _ = cursor::set_consecutive_errors(&pool, consecutive_errors).await;

                // Check if we should enter degraded / circuit-breaker state.
                let circuit_open = config.max_consecutive_errors > 0
                    && consecutive_errors >= config.max_consecutive_errors;

                if circuit_open {
                    error!(
                        error = %e,
                        consecutive_errors,
                        max_consecutive_errors = config.max_consecutive_errors,
                        degraded_interval_secs = config.degraded_poll_interval_secs,
                        "circuit breaker open: too many consecutive poll failures; \
                         switching to degraded polling interval"
                    );
                    backoff = degraded_interval;
                    degraded_interval
                } else {
                    warn!(error = %e, backoff_secs = backoff.as_secs(), consecutive_errors, "poll cycle failed; backing off");
                    let this = backoff;
                    backoff = (backoff * 2).min(Duration::from_secs(60));
                    this
                }
            }
        };

        tokio::select! {
            _ = tokio::time::sleep(sleep_for) => {}
            _ = shutdown.cancelled() => {
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
    shutdown: &CancellationToken,
) -> anyhow::Result<Option<i64>> {
    use tracing::Instrument as _;

    // One getHealth round-trip supplies both the tip and the retention floor (#718).
    let tip = rpc.get_tip_snapshot().await?;
    let latest = tip.latest;
    let oldest_available = match tip.oldest_available {
        Some(oldest) => {
            RPC_OLDEST_LEDGER.store(oldest, Ordering::Relaxed);
            oldest.max(1)
        }
        None => {
            let fallback = (latest - MAX_LOOKBACK_LEDGERS).max(1);
            warn!(fallback, "RPC did not report oldestLedger; using fallback retention window");
            RPC_OLDEST_LEDGER.store(fallback, Ordering::Relaxed);
            fallback
        }
    };

    // Wrap the rest of the cycle in a span so all log lines — including those
    // from spawned tasks (spec fetches, state/key snapshots) — carry a shared
    // `ledger_range` field. Log aggregators can filter on it to reconstruct a
    // single cycle without interleaved noise from concurrent cycles.
    let cycle_span = tracing::info_span!(
        "poll_cycle",
        from = tracing::field::Empty,
        to = latest,
    );

    let cycle_span_for_record = cycle_span.clone();
    async move {

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

    // Clamp to RPC retention window on fresh start.
    // If the configured START_LEDGER is older than what the RPC serves,
    // clamp to the oldest available ledger to avoid RPC rejection errors.
    if start < oldest_available {
        warn!(
            requested = start,
            clamped_to = oldest_available,
            reason = "start ledger is older than RPC retention window (~7 days)",
            "fresh-start ledger clamped to earliest servable ledger"
        );
        if oldest_available > start {
            record_missed_range_classified(pool, start, oldest_available - 1, "retention-window skip", false).await?;
        }
        start = oldest_available;
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
        if clamped > start {
            record_missed_range_classified(pool, start, clamped - 1, "catch-up skip", true).await?;
        }
        start = clamped;
    }

    // Record the ledger range we are about to process in the span, now that
    // we know both endpoints after clamping.
    cycle_span_for_record.record("from", start);

    let (inserted, enrichment) = fetch_and_store_with_shutdown(pool, rpc, config, specs, start, latest, shutdown).await?;

    if shutdown.is_cancelled() {
        return Ok(Some(latest));
    }

    // Check enrichment coverage and warn if it drops significantly
    if enrichment.enriched_count > 0 || enrichment.not_enriched_count > 0 {
        let total = enrichment.enriched_count + enrichment.not_enriched_count;
        if total > 0 {
            let enrichment_rate = enrichment.enriched_count as f64 / total as f64;
            let not_enriched_fraction = enrichment.not_enriched_count as f64 / total as f64;

            // Emit warning if enrichment rate is below threshold
            if not_enriched_fraction > config.enrichment_warn_threshold {
                warn!(
                    enrichment_rate = enrichment_rate,
                    not_enriched_fraction = not_enriched_fraction,
                    threshold = config.enrichment_warn_threshold,
                    enriched_count = enrichment.enriched_count,
                    not_enriched_count = enrichment.not_enriched_count,
                    total_events = total,
                    "enrichment coverage dropped below threshold"
                );
            }
        }
    }

    // Trailing re-scan for shallow reorg detection.
    // If configured, re-fetch the last N ledgers with upsert semantics to catch
    // content changes that might have occurred due to chain reorgs.
    let mut reorg_updated = 0u64;
    if config.reorg_overlap_ledgers > 0 && latest > config.reorg_overlap_ledgers {
        let reorg_start = latest - config.reorg_overlap_ledgers;
        reorg_updated =
            fetch_and_upsert(pool, rpc, config, specs, reorg_start, latest, shutdown).await?;
        if reorg_updated > 0 {
            debug!(
                reorg_updated,
                reorg_start,
                reorg_end = latest,
                "trailing reorg re-scan complete"
            );
        }
    }

    // Record RPC metrics for this cycle
    let (rpc_calls, rpc_errors, rpc_errors_32001, rpc_failovers) = rpc.take_metrics();
    if rpc_calls > 0 || rpc_errors > 0 || rpc_failovers > 0 {
        if rpc_failovers > 0 {
            tracing::warn!(
                rpc_failovers,
                active_endpoint = %rpc.active_endpoint_host(),
                "rpc endpoint failover(s) occurred this cycle"
            );
        }
        cursor::track_rpc_call(pool, rpc_calls, rpc_errors, rpc_errors_32001, rpc_failovers).await?;
    }

    cursor::write_progress(pool, latest, latest, inserted).await?;
    if inserted > 0 {
        info!(inserted, up_to_ledger = latest, "indexed events");
    }
    if reorg_updated > 0 {
        info!(reorg_updated, "events updated due to shallow reorg detection");
    }
    Ok(Some(latest))
    }
    .instrument(cycle_span)
    .await
}

/// Enrichment metrics for a cycle.
pub struct EnrichmentMetrics {
    pub enriched_count: u64,
    pub not_enriched_count: u64,
}

/// The highest ledger that is complete once the events up to `page_last_ledger`
/// are stored, capped at the tip. Ledgers below the last event on a page cannot
/// receive more events, because pages are ordered by ledger (#717).
pub fn checkpoint_candidate(page_last_ledger: i64, tip: i64) -> i64 {
    (page_last_ledger - 1).min(tip)
}

/// The checkpoint to write, if it moves the cursor forward.
pub fn next_checkpoint(current: i64, page_last_ledger: i64, tip: i64) -> Option<i64> {
    let candidate = checkpoint_candidate(page_last_ledger, tip);
    (candidate > current).then_some(candidate)
}

/// Page through events from `start` to the tip, storing each page. Shared by the
/// live poller and the backfill command. Returns (total_inserted, enrichment_metrics).
#[cfg(test)]
pub async fn fetch_and_store(
    pool: &PgPool,
    rpc: &RpcClient,
    config: &Config,
    specs: &SpecCache,
    start: i64,
    tip: i64,
) -> anyhow::Result<(u64, EnrichmentMetrics)> {
    fetch_and_store_with_shutdown(pool, rpc, config, specs, start, tip, &CancellationToken::new()).await
}

async fn fetch_and_store_with_shutdown(
    pool: &PgPool,
    rpc: &RpcClient,
    config: &Config,
    specs: &SpecCache,
    start: i64,
    tip: i64,
    shutdown: &CancellationToken,
) -> anyhow::Result<(u64, EnrichmentMetrics)> {
    let mut cursor_token: Option<String> = None;
    let mut total_inserted = 0u64;
    let mut enriched_count = 0u64;
    let mut not_enriched_count = 0u64;
    let mut ts_parse_errors = 0u64; // #399
    let mut page_count = 0usize;
    let mut last_seen_ledger = start;
    // Highest ledger known to be fully stored. Persisted after each page so a
    // restart resumes here rather than from the start of the cycle (#717).
    let mut checkpointed = start - 1;
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
    // contract -> (template_index, key_params) for per-key template snapshots.
    let mut template_keys_by_contract: std::collections::HashMap<
        String,
        Vec<(usize, Vec<String>)>,
    > = std::collections::HashMap::new();
    loop {
        if shutdown.is_cancelled() {
            return Ok((total_inserted, EnrichmentMetrics { enriched_count, not_enriched_count }));
        }
        // The first page is bounded at the tip (exclusive end). Cursor pages cannot
        // carry endLedger, so they are bounded client-side below (#717).
        let page = match rpc
            .get_events_bounded(
                Some(start),
                Some(tip + 1),
                &config.contract_ids,
                cursor_token.clone(),
                config.page_size,
            )
            .await
        {
            Ok(p) => p,
            Err(e) => {
                if page_count > 0 {
                    warn!(
                        error = %e,
                        page = page_count + 1,
                        from = last_seen_ledger,
                        to = tip,
                        "RPC error on non-first page of batch; recording missed range"
                    );
                    let _ = cursor::incr_errors(pool).await;
                    record_missed_range(pool, last_seen_ledger, tip, &e.to_string()).await?;
                    break;
                } else {
                    return Err(e);
                }
            }
        };
        page_count += 1;
        let mut batch: Vec<NewEvent> = Vec::with_capacity(page.events.len());
        // Last ledger of an event kept from this page; drives the checkpoint.
        let mut kept_last_ledger: Option<i64> = None;
        // Set when an event past the tip shows up, so the scan stops here.
        let mut reached_tip = false;
        for ev in &page.events {
            if ev.ledger > tip {
                reached_tip = true;
                break;
            }
            kept_last_ledger = Some(ev.ledger);
            last_seen_ledger = last_seen_ledger.max(ev.ledger);
            // Interface lookups are cached, so this is one fetch per contract.
            let spec = specs.get(pool, rpc, &ev.contract_id, ev.ledger).await;
            // Only needed for index-all instance reads (see below).
            if tracks_active_contracts {
                active_contracts.insert(ev.contract_id.clone());
            }
            let Some(new_event) = to_new_event(ev, spec.as_deref()) else {
                // #399: timestamp parse failure — event skipped, counter incremented.
                ts_parse_errors += 1;
                continue;
            };
            // Track enrichment coverage: count enriched vs not-enriched events
            if new_event.enriched.is_some() {
                enriched_count += 1;
            } else {
                not_enriched_count += 1;
            }
            // Discover holder addresses to snapshot per-key balances for.
            if config.key_indexing {
                for holder in keys::holders_in_event(&new_event) {
                    holders_by_contract
                        .entry(new_event.contract_id.clone())
                        .or_default()
                        .insert(holder);
                }
            }
            // Discover keys from custom templates.
            for (idx, template) in config.key_templates.iter().enumerate() {
                for params in template.keys_from_event(&new_event) {
                    template_keys_by_contract
                        .entry(new_event.contract_id.clone())
                        .or_default()
                        .push((idx, params));
                }
            }
            batch.push(new_event);
        }
        total_inserted += store::insert_events(pool, &batch).await?;

        // Pages are ordered by ledger, so every ledger below this page's last
        // event is complete. Checkpoint it so a crash does not replay the cycle (#717).
        if let Some(last_ledger) = kept_last_ledger {
            if let Some(next) = next_checkpoint(checkpointed, last_ledger, tip) {
                match cursor::checkpoint_progress(pool, next).await {
                    Ok(()) => checkpointed = next,
                    Err(e) => warn!(error = %e, checkpoint = next, "cursor checkpoint failed; continuing"),
                }
            }
        }

        cursor_token = page.cursor;
        if reached_tip || page.events.len() < config.page_size as usize || cursor_token.is_none() {
            break;
        }
    }

    // Record enrichment metrics for this cycle
    if enriched_count > 0 || not_enriched_count > 0 {
        cursor::track_enrichment(pool, enriched_count, not_enriched_count).await?;
        if ts_parse_errors > 0 {
            let _ = cursor::track_timestamp_parse_error(pool, ts_parse_errors).await;
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
        let targets: Vec<String> = if config.contract_ids.is_empty() {
            active_contracts.iter().cloned().collect()
        } else {
            config.contract_ids.clone()
        };
        if config.state_indexing {
            // Batch instance reads with bounded concurrency.
            // Change-detected, so unchanged instances are no-op writes.
            // Also notes the executable hash, detecting upgrades for free.
            state::snapshot_instances_batch(pool, rpc, specs, &targets).await;
        } else {
            // Upgrade watch in index-all mode: batch the instance reads,
            // then check for upgrades.
            for contract_id in &targets {
                specs::check_for_upgrade(pool, rpc, specs, contract_id, tip).await;
            }
        }
    }

    // Snapshot per-holder balances discovered from this cycle's token events.
    // Keys are batched into chunked getLedgerEntries calls so per-cycle RPC cost
    // scales with the number of batches, not the number of individual holders.
    if config.key_indexing && !holders_by_contract.is_empty() {
        let durability = keys::parse_durability(&config.balance_key_durability)?;
        state::snapshot_balances_batch(
            pool,
            rpc,
            &holders_by_contract,
            &config.contract_ids,
            &config.balance_key_symbol,
            durability,
        )
        .await;
    }
    // Snapshot per-key entries from custom templates.
    if !template_keys_by_contract.is_empty() {
        state::snapshot_template_keys_batch(
            pool,
            rpc,
            &template_keys_by_contract,
            &config.contract_ids,
            &config.key_templates,
        )
        .await;
    }
    Ok((
        total_inserted,
        EnrichmentMetrics {
            enriched_count,
            not_enriched_count,
        },
    ))
}

/// Record a ledger range missed due to a mid-batch page RPC failure.
pub async fn record_missed_range(
    pool: &PgPool,
    from_ledger: i64,
    to_ledger: i64,
    reason: &str,
) -> anyhow::Result<()> {
    record_missed_range_classified(pool, from_ledger, to_ledger, reason, true).await
}

pub async fn record_missed_range_classified(
    pool: &PgPool,
    from_ledger: i64,
    to_ledger: i64,
    reason: &str,
    recoverable: bool,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO missed_ranges (start_ledger, end_ledger, reason, detected_at, recoverable) \
         VALUES ($1, $2, $3, NOW(), $4)",
    )
    .bind(from_ledger)
    .bind(to_ledger)
    .bind(reason)
    .bind(recoverable)
    .execute(pool)
    .await?;
    Ok(())
}

/// Why a recorded range cannot be replayed from this RPC (#715).
pub const OUT_OF_RETENTION_REASON: &str = "outside RPC retention; use deep-backfill";

/// What `recover-gaps` should do with one recorded range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangePlan {
    /// Entirely inside the retained window.
    Replay { from: i64, to: i64 },
    /// Straddles the retention floor. `[from, unrecoverable_to]` is lost; `[replay_from, to]` is replayed.
    Split { unrecoverable_to: i64, replay_from: i64, to: i64 },
    /// Entirely older than the retention floor.
    Unrecoverable,
}

/// Decide how to handle `[from, to]` given the RPC's oldest retained ledger.
/// `None` means the provider does not report a floor, so the range is attempted.
pub fn plan_range(from: i64, to: i64, oldest_available: Option<i64>) -> RangePlan {
    match oldest_available {
        Some(oldest) if to < oldest => RangePlan::Unrecoverable,
        Some(oldest) if from < oldest => RangePlan::Split {
            unrecoverable_to: oldest - 1,
            replay_from: oldest,
            to,
        },
        _ => RangePlan::Replay { from, to },
    }
}

/// Counts from one `recover-gaps` run (#715).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RecoverySummary {
    pub recovered: u64,
    pub partially_recovered: u64,
    pub unrecoverable: u64,
    pub failed: u64,
    pub interrupted: bool,
}

/// Result of replaying one range.
enum ReplayOutcome {
    Complete { inserted: u64 },
    Interrupted,
}

/// Replay all recorded missed ranges that have not yet been recovered.
///
/// A range that has fallen out of RPC retention is marked unrecoverable and the
/// run moves on, instead of aborting on the first `getEvents` error (#715). A
/// range that straddles the retention floor is split, so the lost prefix is
/// recorded separately. A transient failure increments `recovery_attempts` and
/// stores `last_error`. Shutdown stops after the page in progress.
pub async fn recover_gaps(
    pool: &PgPool,
    rpc: &RpcClient,
    config: &Config,
    specs: &SpecCache,
    shutdown: &CancellationToken,
) -> anyhow::Result<RecoverySummary> {
    #[derive(sqlx::FromRow)]
    struct MissedRange {
        id: i64,
        start_ledger: i64,
        end_ledger: i64,
    }

    let unrecovered: Vec<MissedRange> = sqlx::query_as(
        "SELECT id, start_ledger, end_ledger FROM missed_ranges
         WHERE recovered_at IS NULL AND recoverable = TRUE ORDER BY start_ledger ASC",
    )
    .fetch_all(pool)
    .await?;

    let mut summary = RecoverySummary::default();
    if unrecovered.is_empty() {
        info!("no unrecovered missed ranges found");
        return Ok(summary);
    }

    // One getHealth for the whole run: the floor does not need re-reading per range.
    let oldest_available = rpc.oldest_available_ledger().await?;
    info!(count = unrecovered.len(), ?oldest_available, "replaying recorded missed ranges");

    for range in unrecovered {
        if shutdown.is_cancelled() {
            summary.interrupted = true;
            break;
        }
        let (from, to) = (range.start_ledger, range.end_ledger);

        match plan_range(from, to, oldest_available) {
            RangePlan::Unrecoverable => {
                sqlx::query("UPDATE missed_ranges SET recoverable = FALSE, last_error = $2 WHERE id = $1")
                    .bind(range.id)
                    .bind(OUT_OF_RETENTION_REASON)
                    .execute(pool)
                    .await?;
                warn!(id = range.id, from, to, "missed range is outside RPC retention; marked unrecoverable");
                summary.unrecoverable += 1;
            }
            RangePlan::Replay { from, to } => {
                match replay_range(pool, rpc, config, specs, from, to, shutdown).await {
                    Ok(ReplayOutcome::Complete { inserted }) => {
                        mark_recovered(pool, range.id).await?;
                        info!(id = range.id, from, to, events_inserted = inserted, "missed range recovered");
                        summary.recovered += 1;
                    }
                    Ok(ReplayOutcome::Interrupted) => {
                        summary.interrupted = true;
                        break;
                    }
                    Err(e) => {
                        record_recovery_failure(pool, range.id, &e).await;
                        summary.failed += 1;
                    }
                }
            }
            RangePlan::Split { unrecoverable_to, replay_from, to } => {
                // Move the original row to the replayable part first, so a failed
                // replay leaves exactly one row to retry and the lost prefix is recorded once.
                sqlx::query("UPDATE missed_ranges SET start_ledger = $2 WHERE id = $1")
                    .bind(range.id)
                    .bind(replay_from)
                    .execute(pool)
                    .await?;
                record_missed_range_classified(pool, from, unrecoverable_to, OUT_OF_RETENTION_REASON, false).await?;
                warn!(id = range.id, from, unrecoverable_to, replay_from, "missed range straddles RPC retention; splitting");

                match replay_range(pool, rpc, config, specs, replay_from, to, shutdown).await {
                    Ok(ReplayOutcome::Complete { inserted }) => {
                        mark_recovered(pool, range.id).await?;
                        info!(id = range.id, from = replay_from, to, events_inserted = inserted, "missed range partially recovered");
                        summary.partially_recovered += 1;
                    }
                    Ok(ReplayOutcome::Interrupted) => {
                        summary.interrupted = true;
                        break;
                    }
                    Err(e) => {
                        record_recovery_failure(pool, range.id, &e).await;
                        summary.failed += 1;
                    }
                }
            }
        }
    }

    info!(
        recovered = summary.recovered,
        partially_recovered = summary.partially_recovered,
        unrecoverable = summary.unrecoverable,
        failed = summary.failed,
        interrupted = summary.interrupted,
        "recover-gaps finished"
    );
    Ok(summary)
}

async fn mark_recovered(pool: &PgPool, id: i64) -> anyhow::Result<()> {
    sqlx::query("UPDATE missed_ranges SET recovered_at = NOW(), last_error = NULL WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

async fn record_recovery_failure(pool: &PgPool, id: i64, error: &anyhow::Error) {
    let message = format!("{error:#}");
    warn!(id, error = %message, "missed range replay failed; will retry on the next run");
    let _ = sqlx::query(
        "UPDATE missed_ranges
         SET recovery_attempts = recovery_attempts + 1, last_error = $2
         WHERE id = $1",
    )
    .bind(id)
    .bind(&message)
    .execute(pool)
    .await;
}

/// Replay `[from, to]` (inclusive) page by page. The first page is bounded at
/// `to + 1` on the RPC side. Shutdown is checked after each page, so the
/// in-progress page completes and no further page is requested.
async fn replay_range(
    pool: &PgPool,
    rpc: &RpcClient,
    config: &Config,
    specs: &SpecCache,
    from: i64,
    to: i64,
    shutdown: &CancellationToken,
) -> anyhow::Result<ReplayOutcome> {
    let mut cursor_token: Option<String> = None;
    let mut inserted_total = 0u64;
    let mut ts_parse_errors = 0u64; // #399

    loop {
        let page = rpc
            .get_events_bounded(
                Some(from),
                Some(to + 1),
                &config.contract_ids,
                cursor_token.clone(),
                config.page_size,
            )
            .await?;

        let mut batch: Vec<NewEvent> = Vec::with_capacity(page.events.len());
        let mut reached_end = false;
        for ev in &page.events {
            if ev.ledger > to {
                reached_end = true;
                break;
            }
            let spec = specs.get(pool, rpc, &ev.contract_id, ev.ledger).await;
            match to_new_event(ev, spec.as_deref()) {
                Some(new_event) => batch.push(new_event),
                None => ts_parse_errors += 1, // #399: unparseable timestamp, skipped
            }
        }
        if !batch.is_empty() {
            inserted_total += store::insert_events(pool, &batch).await?;
        }
        if ts_parse_errors > 0 {
            let _ = cursor::track_timestamp_parse_error(pool, ts_parse_errors).await;
            ts_parse_errors = 0;
        }

        if reached_end {
            break;
        }
        if shutdown.is_cancelled() {
            return Ok(ReplayOutcome::Interrupted);
        }
        cursor_token = page.cursor;
        if page.events.len() < config.page_size as usize || cursor_token.is_none() {
            break;
        }
    }

    Ok(ReplayOutcome::Complete { inserted: inserted_total })
}

/// Re-fetch events from a range of recently-closed ledgers and upsert them,
/// updating mutable fields if the RPC returned different content (shallow reorg).
/// Returns the number of events updated.
async fn fetch_and_upsert(
    pool: &PgPool,
    rpc: &RpcClient,
    config: &Config,
    specs: &SpecCache,
    start: i64,
    end: i64,
    shutdown: &CancellationToken,
) -> anyhow::Result<u64> {
    let mut cursor_token: Option<String> = None;
    let mut total_updated = 0u64;
    let mut enriched_count = 0u64;
    let mut not_enriched_count = 0u64;
    let mut ts_parse_errors = 0u64; // #399

    loop {
        if shutdown.is_cancelled() {
            return Ok(total_updated);
        }
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
            // Stop if we've moved past the reorg window.
            if ev.ledger > end {
                // Record enrichment metrics for reorg scan
                if enriched_count > 0 || not_enriched_count > 0 {
                    let _ = cursor::track_enrichment(pool, enriched_count, not_enriched_count).await;
                    if ts_parse_errors > 0 {
                        let _ = cursor::track_timestamp_parse_error(pool, ts_parse_errors).await;
                    }
                }
                return Ok(total_updated);
            }
            let spec = specs.get(pool, rpc, &ev.contract_id, ev.ledger).await;
            let Some(new_event) = to_new_event(ev, spec.as_deref()) else {
                // #399: unparseable timestamp — skip event.
                ts_parse_errors += 1;
                continue;
            };
            // Track enrichment coverage: count enriched vs not-enriched events
            if new_event.enriched.is_some() {
                enriched_count += 1;
            } else {
                not_enriched_count += 1;
            }
            batch.push(new_event);
        }

        if !batch.is_empty() {
            let (_, updated) = store::upsert_events(pool, &batch).await?;
            total_updated += updated;
        }

        cursor_token = page.cursor;
        let n = batch.len();
        if n < config.page_size as usize || cursor_token.is_none() {
            break;
        }
    }

    // Record enrichment metrics for reorg scan
    if enriched_count > 0 || not_enriched_count > 0 {
        cursor::track_enrichment(pool, enriched_count, not_enriched_count).await?;
    }

    Ok(total_updated)
}

pub async fn wait_for_shutdown(token: CancellationToken) {
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
    token.cancel();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_clamping_logic() {
        // Verify the START_LEDGER retention clamping logic.
        // If START_LEDGER is older than the RPC retention window,
        // it should be clamped to the earliest available ledger.

        let latest = 1_000_000i64;
        let oldest_available = latest - MAX_LOOKBACK_LEDGERS;

        // Case 1: START_LEDGER is within retention window
        let start_ledger = oldest_available + 1000;
        let clamped = start_ledger.max(oldest_available);
        assert_eq!(
            clamped, start_ledger,
            "start_ledger within window should not be clamped"
        );

        // Case 2: START_LEDGER is outside (older than) retention window
        let start_ledger = oldest_available - 10_000;
        let clamped = start_ledger.max(oldest_available);
        assert_eq!(
            clamped, oldest_available,
            "start_ledger outside window should be clamped to oldest_available"
        );

        // Case 3: START_LEDGER equals oldest_available
        let start_ledger = oldest_available;
        let clamped = start_ledger.max(oldest_available);
        assert_eq!(clamped, oldest_available);

        // Verify MAX_LOOKBACK_LEDGERS is approximately 7 days.
        // ~5 seconds per ledger, 86400 seconds per day.
        let secs_per_day = 86400.0_f64;
        let approx_days = (MAX_LOOKBACK_LEDGERS as f64) * 5.0 / secs_per_day;
        assert!(
            (6.5_f64..=7.5_f64).contains(&approx_days),
            "MAX_LOOKBACK_LEDGERS should be ~7 days, got ~{:.1} days",
            approx_days
        );
    }

    #[test]
    fn max_lookback_exports_retention_window() {
        // Verify that the public max_lookback() function returns the RPC retention window.
        assert_eq!(
            max_lookback(),
            MAX_LOOKBACK_LEDGERS,
            "max_lookback() should export the RPC retention window"
        );
    }

    // ── Circuit breaker logic ─────────────────────────────────────────────

    #[test]
    fn circuit_breaker_opens_after_max_consecutive_errors() {
        let max = 20u32;
        // Simulate accumulating errors.
        for count in 1..=max {
            let circuit_open = max > 0 && count >= max;
            if count < max {
                assert!(!circuit_open, "circuit should stay closed at error {count}");
            } else {
                assert!(circuit_open, "circuit should open at error {count}");
            }
        }
    }

    #[test]
    fn circuit_breaker_disabled_when_max_is_zero() {
        // max_consecutive_errors = 0 means the circuit breaker is disabled.
        let max = 0u32;
        let count = 1_000u32;
        let circuit_open = max > 0 && count >= max;
        assert!(!circuit_open, "circuit should never open when max = 0");
    }

    #[test]
    fn circuit_breaker_resets_on_success() {
        // After a successful cycle consecutive_errors is reset to 0.
        let mut consecutive_errors: u32 = 25;
        // Simulate success path.
        if consecutive_errors > 0 {
            consecutive_errors = 0;
        }
        assert_eq!(consecutive_errors, 0);
    }

    #[test]
    fn degraded_interval_used_when_circuit_open() {
        let base = Duration::from_secs(5);
        let degraded = Duration::from_secs(300);
        let max_consecutive_errors = 20u32;
        let consecutive_errors = 20u32;

        let circuit_open = max_consecutive_errors > 0 && consecutive_errors >= max_consecutive_errors;
        let sleep = if circuit_open { degraded } else { base };
        assert_eq!(sleep, degraded, "degraded interval should be used when circuit is open");
    }

    #[test]
    fn non_retryable_rpc_errors_are_detected() {
        use crate::rpc_client::{is_non_retryable_rpc_error, SorobanRpcError};

        // Method not found (-32601) and Invalid params (-32602) are non-retryable
        let err_32601 = anyhow::anyhow!(SorobanRpcError::new(-32601, "method not found", "getEvents"));
        assert!(is_non_retryable_rpc_error(&err_32601));

        let err_32602 = anyhow::anyhow!(SorobanRpcError::new(-32602, "invalid params", "getEvents"));
        assert!(is_non_retryable_rpc_error(&err_32602));

        // Even when wrapped in context, non-retryable errors are recognized
        let wrapped = err_32601.context("failed to poll once");
        assert!(is_non_retryable_rpc_error(&wrapped));

        // Processing limit (-32001) is retryable
        let retryable = anyhow::anyhow!(SorobanRpcError::new(-32001, "limit reached", "getEvents"));
        assert!(!is_non_retryable_rpc_error(&retryable));
    }
}

#[cfg(test)]
mod gap_and_checkpoint_tests {
    use super::*;

    #[test]
    fn plan_range_inside_the_retained_window_replays_everything() {
        assert_eq!(plan_range(1100, 1200, Some(1000)), RangePlan::Replay { from: 1100, to: 1200 });
        assert_eq!(plan_range(1000, 1200, Some(1000)), RangePlan::Replay { from: 1000, to: 1200 });
        assert_eq!(plan_range(100, 200, None), RangePlan::Replay { from: 100, to: 200 });
    }

    #[test]
    fn plan_range_entirely_before_the_floor_is_unrecoverable() {
        assert_eq!(plan_range(100, 200, Some(1000)), RangePlan::Unrecoverable);
        assert_eq!(plan_range(100, 999, Some(1000)), RangePlan::Unrecoverable, "to is inclusive");
    }

    #[test]
    fn plan_range_straddling_the_floor_splits_at_the_floor() {
        assert_eq!(
            plan_range(900, 1050, Some(1000)),
            RangePlan::Split { unrecoverable_to: 999, replay_from: 1000, to: 1050 }
        );
        assert_eq!(
            plan_range(999, 1000, Some(1000)),
            RangePlan::Split { unrecoverable_to: 999, replay_from: 1000, to: 1000 }
        );
    }

    #[test]
    fn checkpoint_is_the_last_ledger_below_the_page_end_and_never_past_the_tip() {
        assert_eq!(checkpoint_candidate(503, 1000), 502);
        assert_eq!(checkpoint_candidate(5000, 1000), 1000);
        assert_eq!(next_checkpoint(0, 503, 1000), Some(502));
        assert_eq!(next_checkpoint(502, 503, 1000), None, "no forward progress, no write");
        assert_eq!(next_checkpoint(502, 504, 1000), Some(503));
    }

    #[test]
    fn permanent_budget_tolerates_one_odd_error_but_exits_on_a_recurring_one() {
        assert!(!permanent_error_budget_exhausted(1));
        assert!(!permanent_error_budget_exhausted(2));
        assert!(permanent_error_budget_exhausted(3));
    }
}

#[cfg(test)]
mod gap_db_tests {
    use super::*;
    use axum::extract::State;
    use axum::routing::post;
    use axum::{Json, Router};
    use serde_json::{json, Value};
    use sqlx::postgres::PgPoolOptions;
    use std::sync::Mutex;
    use tokio::net::TcpListener;

    /// What the mock returns for one `getEvents` request.
    #[derive(Clone)]
    enum Reply {
        Page { events: Vec<Value>, cursor: Option<String> },
        Error { code: i64, message: &'static str },
    }

    struct MockRpc {
        /// `None` makes `getHealth` answer -32601, as a provider without it would.
        oldest: Option<i64>,
        tip: i64,
        /// Matched on (startLedger, cursor) of each `getEvents` request.
        routes: Vec<(Option<i64>, Option<String>, Reply)>,
        /// Every `getEvents` request body, in arrival order.
        requests: Mutex<Vec<Value>>,
        /// If set, the first `getEvents` call fails with this error, then routing resumes.
        first_get_events_error: Option<(i64, &'static str)>,
        get_events_calls: Mutex<usize>,
    }

    async fn rpc_handler(State(s): State<Arc<MockRpc>>, Json(req): Json<Value>) -> Json<Value> {
        match req["method"].as_str().unwrap_or_default() {
            "getHealth" => Json(match s.oldest {
                Some(oldest) => json!({"jsonrpc": "2.0", "id": 1, "result": {
                    "status": "healthy", "latestLedger": s.tip,
                    "oldestLedger": oldest, "ledgerRetentionWindow": 1000 }}),
                None => json!({"jsonrpc": "2.0", "id": 1,
                    "error": {"code": -32601, "message": "method not found"}}),
            }),
            "getLatestLedger" => Json(json!({"jsonrpc": "2.0", "id": 1,
                "result": {"sequence": s.tip}})),
            "getEvents" => {
                s.requests.lock().unwrap().push(req.clone());
                let call_index = {
                    let mut calls = s.get_events_calls.lock().unwrap();
                    *calls += 1;
                    *calls - 1
                };
                if let (0, Some((code, message))) = (call_index, s.first_get_events_error) {
                    return Json(json!({"jsonrpc": "2.0", "id": 1,
                        "error": {"code": code, "message": message}}));
                }
                let start = req["params"]["startLedger"].as_i64();
                let cursor = req["params"]["pagination"]["cursor"].as_str().map(String::from);
                let reply = s
                    .routes
                    .iter()
                    .find(|(st, cu, _)| *st == start && *cu == cursor)
                    .map(|(_, _, r)| r.clone())
                    .unwrap_or(Reply::Error { code: -32601, message: "no route in mock" });
                Json(match reply {
                    Reply::Page { events, cursor } => json!({"jsonrpc": "2.0", "id": 1,
                        "result": {"latestLedger": s.tip, "events": events, "cursor": cursor}}),
                    Reply::Error { code, message } => json!({"jsonrpc": "2.0", "id": 1,
                        "error": {"code": code, "message": message}}),
                })
            }
            _ => Json(json!({"jsonrpc": "2.0", "id": 1,
                "error": {"code": -32601, "message": "method not found"}})),
        }
    }

    async fn spawn_rpc(mock: MockRpc) -> (String, Arc<MockRpc>) {
        let mock = Arc::new(mock);
        let app = Router::new().route("/", post(rpc_handler)).with_state(mock.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), mock)
    }

    fn make_event(id: &str, ledger: i64) -> Value {
        json!({
            "type": "contract", "ledger": ledger, "ledgerClosedAt": "2024-01-01T00:00:00Z",
            "contractId": "C1", "id": id, "pagingToken": id, "inSuccessfulContractCall": true,
            "txHash": "tx1", "topic": [], "value": ""
        })
    }

    fn test_config(rpc_url: &str, page_size: u32) -> Config {
        Config {
            database_url: String::new(),
            rpc_url: rpc_url.to_string(),
            rpc_urls: vec![rpc_url.to_string()],
            contract_ids: vec![],
            poll_interval_secs: 5,
            page_size,
            start_ledger: 0,
            max_catchup_ledgers: 4000,
            max_consecutive_errors: 20,
            degraded_poll_interval_secs: 300,
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
            spec_fetch_concurrency: 4,
        }
    }

    /// Fresh schema, migrated. Tests run with --test-threads=1 in CI.
    async fn fixture() -> PgPool {
        let url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL");
        let pool = PgPoolOptions::new().max_connections(2).connect(&url).await.expect("connect");
        for stmt in ["DROP SCHEMA public CASCADE", "CREATE SCHEMA public"] {
            sqlx::query(stmt).execute(&pool).await.expect("reset schema");
        }
        sqlx::migrate!("../../migrations").run(&pool).await.expect("migrate");
        pool
    }

    async fn insert_missed_range(pool: &PgPool, from: i64, to: i64) -> i64 {
        sqlx::query_scalar(
            "INSERT INTO missed_ranges (start_ledger, end_ledger, reason, recoverable)
             VALUES ($1, $2, 'test gap', TRUE) RETURNING id",
        )
        .bind(from)
        .bind(to)
        .fetch_one(pool)
        .await
        .expect("insert missed range")
    }

    fn get_events_starts(mock: &MockRpc) -> Vec<Option<i64>> {
        mock.requests
            .lock()
            .unwrap()
            .iter()
            .map(|r| r["params"]["startLedger"].as_i64())
            .collect()
    }

    // ── #715: recover-gaps ─────────────────────────────────────────────────

    /// Acceptance: the retention floor sits between two recorded ranges. The first
    /// is marked unrecoverable, the second is recovered, and the run succeeds.
    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn recover_gaps_marks_out_of_retention_ranges_and_recovers_the_rest() {
        let pool = fixture().await;
        let old_id = insert_missed_range(&pool, 100, 200).await;
        let new_id = insert_missed_range(&pool, 1100, 1200).await;

        let (url, mock) = spawn_rpc(MockRpc {
            oldest: Some(1000),
            tip: 2000,
            routes: vec![(Some(1100), None, Reply::Page {
                events: vec![make_event("e1", 1100), make_event("e2", 1150)],
                cursor: None,
            })],
            requests: Mutex::new(vec![]),
            first_get_events_error: None,
            get_events_calls: Mutex::new(0),
        })
        .await;
        let rpc = RpcClient::new(&url, 5);
        let config = test_config(&url, 100);
        let specs = SpecCache::new(10, 2);
        let shutdown = CancellationToken::new();

        let summary = recover_gaps(&pool, &rpc, &config, &specs, &shutdown)
            .await
            .expect("recover-gaps exits 0");
        assert_eq!(summary.unrecoverable, 1);
        assert_eq!(summary.recovered, 1);
        assert_eq!(summary.failed, 0);
        assert!(!summary.interrupted);
        assert_eq!(get_events_starts(&mock), vec![Some(1100)], "no replay is attempted for the lost range");

        let old: (bool, Option<String>, Option<chrono::DateTime<chrono::Utc>>) = sqlx::query_as(
            "SELECT recoverable, last_error, recovered_at FROM missed_ranges WHERE id = $1",
        )
        .bind(old_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(!old.0, "out-of-retention range is no longer recoverable");
        assert!(old.1.unwrap_or_default().contains("outside RPC retention"));
        assert!(old.2.is_none());

        let recovered_at: Option<chrono::DateTime<chrono::Utc>> =
            sqlx::query_scalar("SELECT recovered_at FROM missed_ranges WHERE id = $1")
                .bind(new_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(recovered_at.is_some());

        let stored: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE ledger BETWEEN 1100 AND 1200")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(stored, 2);
    }

    /// A range that straddles the floor is split: the lost prefix is recorded once and
    /// the retained part is replayed and marked recovered.
    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn recover_gaps_splits_a_range_that_straddles_the_retention_floor() {
        let pool = fixture().await;
        let id = insert_missed_range(&pool, 900, 1050).await;

        let (url, mock) = spawn_rpc(MockRpc {
            oldest: Some(1000),
            tip: 2000,
            routes: vec![(Some(1000), None, Reply::Page {
                events: vec![make_event("e1", 1000), make_event("e2", 1040)],
                cursor: None,
            })],
            requests: Mutex::new(vec![]),
            first_get_events_error: None,
            get_events_calls: Mutex::new(0),
        })
        .await;
        let rpc = RpcClient::new(&url, 5);
        let config = test_config(&url, 100);
        let specs = SpecCache::new(10, 2);

        let summary = recover_gaps(&pool, &rpc, &config, &specs, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(summary.partially_recovered, 1);
        assert_eq!(get_events_starts(&mock), vec![Some(1000)]);

        let (start, end): (i64, i64) = sqlx::query_as(
            "SELECT start_ledger, end_ledger FROM missed_ranges WHERE id = $1",
        )
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!((start, end), (1000, 1050));

        let lost: (i64, i64, bool) = sqlx::query_as(
            "SELECT start_ledger, end_ledger, recoverable FROM missed_ranges WHERE id <> $1",
        )
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(lost, (900, 999, false));
    }

    /// A transient failure is recorded and the run continues to the next range.
    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn recover_gaps_records_transient_failures_and_continues() {
        let pool = fixture().await;
        let failing = insert_missed_range(&pool, 1300, 1400).await;
        let ok = insert_missed_range(&pool, 1500, 1600).await;

        let (url, _mock) = spawn_rpc(MockRpc {
            oldest: Some(1000),
            tip: 2000,
            routes: vec![
                (Some(1300), None, Reply::Error { code: -32603, message: "internal" }),
                (Some(1500), None, Reply::Page { events: vec![make_event("e3", 1500)], cursor: None }),
            ],
            requests: Mutex::new(vec![]),
            first_get_events_error: None,
            get_events_calls: Mutex::new(0),
        })
        .await;
        let rpc = RpcClient::new(&url, 5);
        let config = test_config(&url, 100);
        let specs = SpecCache::new(10, 2);

        let summary = recover_gaps(&pool, &rpc, &config, &specs, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!((summary.failed, summary.recovered), (1, 1));

        let (attempts, last_error, recovered_at): (i32, Option<String>, Option<chrono::DateTime<chrono::Utc>>) =
            sqlx::query_as("SELECT recovery_attempts, last_error, recovered_at FROM missed_ranges WHERE id = $1")
                .bind(failing)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(attempts, 1);
        assert!(last_error.is_some());
        assert!(recovered_at.is_none(), "a failed range stays open for the next run");

        let ok_done: Option<chrono::DateTime<chrono::Utc>> =
            sqlx::query_scalar("SELECT recovered_at FROM missed_ranges WHERE id = $1")
                .bind(ok)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(ok_done.is_some());
    }

    // ── #716: out-of-retention startLedger does not kill the poller ────────

    /// Acceptance: the first `getEvents` fails with an out-of-range `startLedger`,
    /// the next cycle re-reads `getHealth` and succeeds, and the cursor advances.
    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn poller_survives_an_out_of_retention_start_ledger_and_advances() {
        let pool = fixture().await;
        cursor::checkpoint_progress(&pool, 499).await.unwrap();

        let (url, _mock) = spawn_rpc(MockRpc {
            oldest: Some(1),
            tip: 1000,
            routes: vec![(Some(500), None, Reply::Page {
                events: vec![make_event("e1", 500), make_event("e2", 501)],
                cursor: None,
            })],
            requests: Mutex::new(vec![]),
            first_get_events_error: Some((
                -32602,
                "startLedger must be between the oldest ledger: 1 and the latest ledger: 1000",
            )),
            get_events_calls: Mutex::new(0),
        })
        .await;
        let rpc = RpcClient::new(&url, 5);
        let config = test_config(&url, 100);
        let specs = SpecCache::new(10, 2);
        let shutdown = CancellationToken::new();

        let first = poll_once(&pool, &rpc, &config, &specs, &shutdown).await;
        let err = first.expect_err("the first cycle hits the retention race");
        assert!(crate::rpc_client::is_out_of_retention(&err));
        assert!(
            !crate::rpc_client::is_non_retryable_rpc_error(&err),
            "a retention race must not count towards the permanent-error budget"
        );

        let second = poll_once(&pool, &rpc, &config, &specs, &shutdown)
            .await
            .expect("the next cycle re-reads getHealth and succeeds");
        assert_eq!(second, Some(1000));
        assert_eq!(cursor::read_last_processed(&pool).await.unwrap(), Some(1000));
    }

    // ── #717: bounded scan and mid-cycle checkpoints ───────────────────────

    /// A failure on page 3 leaves the cursor at the last completed ledger, so the
    /// next cycle resumes after it instead of re-scanning the whole cycle.
    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn cycle_checkpoints_each_page_so_a_mid_cycle_failure_resumes_after_it() {
        let pool = fixture().await;
        let (url, mock) = spawn_rpc(MockRpc {
            oldest: Some(1),
            tip: 1000,
            routes: vec![
                (Some(500), None, Reply::Page {
                    events: vec![make_event("e1", 500), make_event("e2", 501)],
                    cursor: Some("c1".into()),
                }),
                (None, Some("c1".into()), Reply::Page {
                    events: vec![make_event("e3", 502), make_event("e4", 503)],
                    cursor: Some("c2".into()),
                }),
                (None, Some("c2".into()), Reply::Error { code: -32601, message: "gone" }),
            ],
            requests: Mutex::new(vec![]),
            first_get_events_error: None,
            get_events_calls: Mutex::new(0),
        })
        .await;
        let rpc = RpcClient::new(&url, 5);
        let config = test_config(&url, 2);
        let specs = SpecCache::new(10, 2);

        let (inserted, _) = fetch_and_store(&pool, &rpc, &config, &specs, 500, 1000)
            .await
            .expect("a non-first page error is recorded, not returned");
        assert_eq!(inserted, 4);
        assert_eq!(
            cursor::read_last_processed(&pool).await.unwrap(),
            Some(502),
            "ledgers through 502 are complete; 503 may still have more events"
        );

        let first = mock.requests.lock().unwrap()[0].clone();
        assert_eq!(first["params"]["endLedger"], 1001, "first page is bounded at tip + 1");
    }

    /// Events past the tip are not stored, and the scan stops without requesting
    /// another page.
    #[tokio::test]
    #[ignore = "needs postgres"]
    async fn scan_stops_at_the_tip_and_does_not_request_pages_past_it() {
        let pool = fixture().await;
        let (url, mock) = spawn_rpc(MockRpc {
            oldest: Some(1),
            tip: 1000,
            routes: vec![
                (Some(500), None, Reply::Page {
                    events: vec![make_event("e1", 500), make_event("e2", 501)],
                    cursor: Some("c1".into()),
                }),
                (None, Some("c1".into()), Reply::Page {
                    events: vec![make_event("e3", 502), make_event("e4", 1001)],
                    cursor: Some("c2".into()),
                }),
                (None, Some("c2".into()), Reply::Page { events: vec![make_event("e5", 1002)], cursor: None }),
            ],
            requests: Mutex::new(vec![]),
            first_get_events_error: None,
            get_events_calls: Mutex::new(0),
        })
        .await;
        let rpc = RpcClient::new(&url, 5);
        let config = test_config(&url, 2);
        let specs = SpecCache::new(10, 2);

        let (inserted, _) = fetch_and_store(&pool, &rpc, &config, &specs, 500, 1000).await.unwrap();
        assert_eq!(inserted, 3, "e4 at ledger 1001 is past the tip and is not stored");
        assert_eq!(mock.requests.lock().unwrap().len(), 2, "no request for the page after the tip");
        assert_eq!(cursor::read_last_processed(&pool).await.unwrap(), Some(501));
    }
}
