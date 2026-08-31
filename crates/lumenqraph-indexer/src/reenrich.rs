//! Re-enrich historical events that were indexed before their contract's spec
//! was available. For events where enriched IS NULL AND event_name IS NOT NULL,
//! look up the (now-cached) spec and backfill enriched.
//!
//! This is a one-shot backfill pass that can be run manually or automatically
//! when a spec is first successfully fetched for a contract with stored events.

use std::io::{self, IsTerminal};
use std::time::Instant;

use lumenqraph_core::NewEvent;
use sqlx::{PgPool, Row};
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::convert::to_new_event;
use crate::rpc_client::RpcClient;
use crate::specs::SpecCache;

/// Run a complete re-enrichment pass: find all events where enriched IS NULL
/// AND event_name IS NOT NULL, re-enrich them against the (now-cached) spec,
/// and update the database.
pub async fn run_reenrich(pool: PgPool, rpc: RpcClient, config: Config) -> anyhow::Result<()> {
    let specs = SpecCache::new(config.spec_cache_max_entries, config.spec_fetch_concurrency);
    let mut processed = 0u64;
    let mut updated = 0u64;

    // Fetch events in batches to avoid loading the entire table into memory.
    const BATCH_SIZE: i32 = 1000;
    const PROGRESS_INTERVAL: u64 = 10_000;
    let mut offset = 0i32;

    let is_tty = io::stderr().is_terminal();
    let start_time = Instant::now();
    let mut last_progress_time = start_time;
    let mut last_progress_count = 0u64;

    loop {
        let rows = sqlx::query(
            "SELECT event_id, contract_id, decoded_topics, event_name, decoded_value
             FROM events
             WHERE enriched IS NULL AND event_name IS NOT NULL
             ORDER BY ledger, paging_token
             LIMIT $1 OFFSET $2",
        )
        .bind(BATCH_SIZE)
        .bind(offset)
        .fetch_all(&pool)
        .await?;

        if rows.is_empty() {
            break;
        }

        for row in rows {
            let event_id: String = row.get("event_id");
            let contract_id: String = row.get("contract_id");
            let decoded_topics_json: String = row.get("decoded_topics");
            let event_name: String = row.get("event_name");
            let decoded_value_json: String = row.get("decoded_value");

            processed += 1;

            // Report progress every PROGRESS_INTERVAL events
            if processed % PROGRESS_INTERVAL == 0 {
                let elapsed = start_time.elapsed();
                let events_since_last = processed - last_progress_count;
                let time_since_last = last_progress_time.elapsed();

                if time_since_last.as_secs_f64() > 0.0 {
                    let throughput = events_since_last as f64 / time_since_last.as_secs_f64();
                    let remaining = total_events.saturating_sub(processed);
                    let remaining_estimate = if throughput > 0.0 {
                        std::time::Duration::from_secs_f64(remaining as f64 / throughput)
                    } else {
                        std::time::Duration::ZERO
                    };

                    let elapsed_secs = elapsed.as_secs();
                    let remaining_secs = remaining_estimate.as_secs();
                    let remaining_fmt = if remaining_secs < 3600 {
                        format!("{:.0}m", remaining_secs as f64 / 60.0)
                    } else {
                        format!("{:.1}h", remaining_secs as f64 / 3600.0)
                    };

                    if is_tty {
                        eprintln!(
                            "Re-enriching… {processed:>10}/{total_events:<10} | {elapsed_secs:>5}s | {throughput:.0} evt/s | ~{remaining_fmt} remaining"
                        );
                    }

                    info!(
                        processed,
                        total_events,
                        updated,
                        elapsed_secs,
                        throughput = throughput as u64,
                        remaining_secs,
                        "re-enrichment progress"
                    );

                    last_progress_time = Instant::now();
                    last_progress_count = processed;
                }
            }

            // Re-parse the decoded data for enrichment.
            let decoded_topics: Vec<serde_json::Value> =
                match serde_json::from_str(&decoded_topics_json) {
                    Ok(dt) => dt,
                    Err(e) => {
                        warn!(event_id, error = %e, "failed to parse decoded_topics");
                        continue;
                    }
                };

            let decoded_value: serde_json::Value = match serde_json::from_str(&decoded_value_json) {
                Ok(dv) => dv,
                Err(e) => {
                    warn!(event_id, error = %e, "failed to parse decoded_value");
                    continue;
                }
            };

            // Fetch the spec for this contract (cached after first fetch).
            let spec = specs.get(&pool, &rpc, &contract_id).await;

            // Try to enrich against the spec.
            let enriched = if let Some(spec_ref) = spec.as_deref() {
                spec_ref.enrich_event(&event_name, &decoded_topics, &decoded_value)
            } else {
                None
            };

            // Update the database if we got an enrichment.
            if let Some(enriched_json) = enriched {
                let enriched_str = serde_json::to_string(&enriched_json)
                    .unwrap_or_else(|_| "null".to_string());
                if let Err(e) = sqlx::query("UPDATE events SET enriched = $1 WHERE event_id = $2")
                    .bind(&enriched_str)
                    .bind(&event_id)
                    .execute(&pool)
                    .await
                {
                    warn!(event_id, error = %e, "failed to update enriched field");
                } else {
                    updated += 1;
                }
            } else {
                debug!(event_id, contract_id, event_name, "no enrichment available");
            }
        }

        offset += BATCH_SIZE;
    }

    info!(processed, updated, "re-enrichment pass complete");
    Ok(())
}
