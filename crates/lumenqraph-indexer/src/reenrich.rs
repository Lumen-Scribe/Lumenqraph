//! Re-enrich historical events that were indexed before their contract's spec
//! was available, or whose enrichment should be refreshed after a decoder change.

use std::io::{self, IsTerminal};
use std::time::Instant;

use sqlx::types::Json;
use sqlx::{PgPool, Row};
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::rpc_client::RpcClient;
use crate::specs::SpecCache;

const BATCH_SIZE: i64 = 1000;
const PROGRESS_EVERY_PAGES: u64 = 10;
const BATCH_UPDATE_SQL: &str = "UPDATE events AS e SET enriched = batch.enriched
     FROM UNNEST($1::text[], $2::jsonb[]) AS batch(event_id, enriched)
     WHERE e.event_id = batch.event_id";

#[derive(Debug, Clone, Default)]
pub struct ReenrichOptions {
    pub contract: Option<String>,
    pub force: bool,
}

/// Run a complete keyset-paginated re-enrichment pass. The cursor is based on
/// immutable `(ledger, event_id)` values, so updating `enriched` cannot cause
/// rows to be skipped while the predicate changes.
pub async fn run_reenrich(
    pool: PgPool,
    rpc: RpcClient,
    config: Config,
    options: ReenrichOptions,
) -> anyhow::Result<()> {
    let specs = SpecCache::new(config.spec_cache_max_entries, config.spec_fetch_concurrency);
    let filter = if options.force {
        "event_name IS NOT NULL"
    } else {
        "enriched IS NULL AND event_name IS NOT NULL"
    };

    let total_events: i64 = if let Some(contract) = options.contract.as_deref() {
        sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM events WHERE {filter} AND contract_id = $1"
        ))
        .bind(contract)
        .fetch_one(&pool)
        .await?
    } else {
        sqlx::query_scalar(&format!("SELECT COUNT(*) FROM events WHERE {filter}"))
            .fetch_one(&pool)
            .await?
    };

    let is_tty = io::stderr().is_terminal();
    let start_time = Instant::now();
    let mut last_progress_time = start_time;
    let mut last_progress_count = 0u64;
    let mut processed = 0u64;
    let mut updated = 0u64;
    let mut cleared = 0u64;
    let mut pages = 0u64;
    let mut cursor: Option<(i64, String)> = None;

    loop {
        let rows = if let Some(contract) = options.contract.as_deref() {
            let query = if cursor.is_some() {
                format!(
                    "SELECT ledger, event_id, contract_id, decoded_topics, event_name, decoded_value,
                            enriched IS NOT NULL AS had_enrichment
                     FROM events WHERE {filter} AND contract_id = $1 AND (ledger, event_id) > ($2, $3)
                     ORDER BY ledger, event_id LIMIT $4"
                )
            } else {
                format!(
                    "SELECT ledger, event_id, contract_id, decoded_topics, event_name, decoded_value,
                            enriched IS NOT NULL AS had_enrichment
                     FROM events WHERE {filter} AND contract_id = $1
                     ORDER BY ledger, event_id LIMIT $2"
                )
            };
            let mut q = sqlx::query(&query).bind(contract);
            if let Some((ledger, event_id)) = cursor.as_ref() {
                q = q.bind(*ledger).bind(event_id);
            }
            q.bind(BATCH_SIZE).fetch_all(&pool).await?
        } else {
            let query = if cursor.is_some() {
                format!(
                    "SELECT ledger, event_id, contract_id, decoded_topics, event_name, decoded_value,
                            enriched IS NOT NULL AS had_enrichment
                     FROM events WHERE {filter} AND (ledger, event_id) > ($1, $2)
                     ORDER BY ledger, event_id LIMIT $3"
                )
            } else {
                format!(
                    "SELECT ledger, event_id, contract_id, decoded_topics, event_name, decoded_value,
                            enriched IS NOT NULL AS had_enrichment
                     FROM events WHERE {filter}
                     ORDER BY ledger, event_id LIMIT $1"
                )
            };
            let mut q = sqlx::query(&query);
            if let Some((ledger, event_id)) = cursor.as_ref() {
                q = q.bind(*ledger).bind(event_id);
            }
            q.bind(BATCH_SIZE).fetch_all(&pool).await?
        };

        if rows.is_empty() {
            break;
        }
        pages += 1;
        let mut batch_ids: Vec<String> = Vec::with_capacity(rows.len());
        let mut batch_enriched: Vec<Option<Json<serde_json::Value>>> =
            Vec::with_capacity(rows.len());
        let mut batch_updated = 0u64;
        let mut batch_cleared = 0u64;

        for row in rows {
            let ledger: i64 = row.try_get("ledger")?;
            let event_id: String = row.try_get("event_id")?;
            cursor = Some((ledger, event_id.clone()));
            processed += 1;

            let contract_id: String = match row.try_get("contract_id") {
                Ok(value) => value,
                Err(error) => {
                    warn!(event_id, %error, "failed to decode contract_id");
                    continue;
                }
            };
            let event_name: String = match row.try_get("event_name") {
                Ok(value) => value,
                Err(error) => {
                    warn!(event_id, %error, "failed to decode event_name");
                    continue;
                }
            };
            let Json(decoded_topics): Json<Vec<serde_json::Value>> =
                match row.try_get("decoded_topics") {
                    Ok(value) => value,
                    Err(error) => {
                        warn!(event_id, %error, "failed to decode decoded_topics");
                        continue;
                    }
                };
            let Json(decoded_value): Json<serde_json::Value> = match row.try_get("decoded_value") {
                Ok(value) => value,
                Err(error) => {
                    warn!(event_id, %error, "failed to decode decoded_value");
                    continue;
                }
            };

            // Pass the event's own ledger so the spec version in force at that
            // ledger is used, not the contract's current spec. This keeps
            // `reenrich --force` from overwriting correct historical enrichment
            // with a newer version's field names after an upgrade.
            let spec = specs.get(&pool, &rpc, &contract_id, ledger).await;
            let enriched = spec
                .as_deref()
                .and_then(|spec| spec.enrich_event(&event_name, &decoded_topics, &decoded_value));
            if let Some(enriched_json) = enriched {
                if let Err(error) =
                    sqlx::query("UPDATE events SET enriched = $1 WHERE event_id = $2")
                        .bind(Json(enriched_json))
                        .bind(&event_id)
                        .execute(&pool)
                        .await
                {
                    warn!(event_id, %error, "failed to update enriched field");
                } else {
                    updated += 1;
                }
            } else {
                debug!(event_id, contract_id, event_name, "no enrichment available");
            }
        }

        if !batch_ids.is_empty() {
            if let Err(error) = sqlx::query(BATCH_UPDATE_SQL)
                .bind(&batch_ids)
                .bind(&batch_enriched)
                .execute(&pool)
                .await
            {
                warn!(rows = batch_ids.len(), %error, "failed to update enrichment batch");
            } else {
                updated += batch_updated;
                cleared += batch_cleared;
            }
        }

        #[allow(clippy::manual_is_multiple_of)] // is_multiple_of needs Rust 1.87; MSRV is 1.85
        let progress_due = pages % PROGRESS_EVERY_PAGES == 0;
        if progress_due {
            let elapsed = start_time.elapsed();
            let since_last = processed - last_progress_count;
            let since_last_time = last_progress_time.elapsed().as_secs_f64();
            let throughput = if since_last_time > 0.0 {
                since_last as f64 / since_last_time
            } else {
                0.0
            };
            let remaining = (total_events as u64).saturating_sub(processed);
            let eta_secs = if throughput > 0.0 {
                remaining as f64 / throughput
            } else {
                0.0
            };
            if is_tty {
                eprintln!("Re-enriching… {processed:>10}/{total_events:<10} | {updated} updated | {cleared} cleared | {throughput:.0} evt/s | ETA {eta_secs:.0}s");
            }
            info!(
                processed,
                updated,
                cleared,
                total_events,
                elapsed_secs = elapsed.as_secs(),
                throughput = throughput as u64,
                eta_secs,
                "re-enrichment progress"
            );
            last_progress_count = processed;
            last_progress_time = Instant::now();
        }
    }

    info!(processed, updated, cleared, total_events, force = options.force, contract = ?options.contract, "re-enrichment pass complete");
    Ok(())
}
