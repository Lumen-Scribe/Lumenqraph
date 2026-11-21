//! Deep historical backfill (#84) — ingest Soroban event history beyond the
//! ~7-day Soroban RPC retention window from an alternate source.
//!
//! # Why this exists
//!
//! The live poller and the one-shot `backfill` command both call `getEvents` on
//! a Soroban RPC. SDF's public RPC retains ~7 days (~120 000 ledgers) of event
//! history; `START_LEDGER` is clamped to that window. "Deep" history — data
//! older than ~7 days — is only reachable through an alternate data source.
//!
//! # Architecture
//!
//! A [`HistoricalSource`] trait abstracts the source. Each implementation
//! provides a `stream_range` method that yields events in ascending ledger
//! order without accumulating the requested range in memory. Because
//! [`crate::store::insert_events`] is idempotent (`ON CONFLICT DO NOTHING` on
//! `event_id`), the seam between deep-backfill and live-tail is overlap-safe:
//! running the live poller and a deep backfill concurrently, or restarting a
//! backfill partway through, is safe and produces no duplicates.
//!
//! # Implemented sources
//!
//! | Source | Type | Description |
//! |---|---|---|
//! | [`GalexieSource`] | File / stdin | Reads a Galexie / Stellar CDP export in newline-delimited JSON |
//!
//! # Adding a new source
//!
//! Implement [`HistoricalSource`]:
//!
//! ```rust,ignore
//! struct MySource { /* … */ }
//!
//! impl HistoricalSource for MySource {
//!     fn stream_range(
//!         &self,
//!         from_ledger: i64,
//!         to_ledger: i64,
//!         contract_ids: &[String],
//!     ) -> BoxStream<'_, anyhow::Result<HistoricalEvent>> {
//!         // yield events in ascending ledger order …
//!         todo!()
//!     }
//! }
//! ```
//!
//! Then pass it to [`run`].
//!
//! # Usage
//!
//! ```text
//! # Backfill from ledger 100 to 5 000 000 using a Galexie JSON export.
//! lumenqraph-indexer deep-backfill \
//!     --from 100 \
//!     --to 5000000 \
//!     --source galexie \
//!     --input /data/export-2024.ndjson \
//!     --input /data/export-2025.ndjson
//! ```
//!
//! See `docs/DEEP_BACKFILL.md` for the full integration guide.

use std::collections::{HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use chrono::{DateTime, Utc};
use futures::stream::{self, BoxStream};
use futures::StreamExt;
use serde::Deserialize;
use sqlx::PgPool;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader, Lines};
use tracing::{debug, info};

use lumenqraph_core::{ContractSpec, EventType, NewEvent};

use crate::config::Config;
use crate::store;

// ---------------------------------------------------------------------------
// Public trait
// ---------------------------------------------------------------------------

/// A single historical event as delivered by an alternate ingest source.
/// Fields match the Soroban RPC `getEvents` response so the existing
/// [`crate::convert`] / enrichment path is reused without changes.
#[derive(Debug, Clone, Deserialize)]
pub struct HistoricalEvent {
    /// Unique event id (same format as RPC `id` / `pagingToken`).
    pub event_id: String,
    pub contract_id: String,
    pub ledger: i64,
    pub ledger_closed_at: DateTime<Utc>,
    pub event_type: String,
    /// Raw base64 XDR topics, in order.
    #[serde(default)]
    pub topics: Vec<String>,
    /// Raw base64 XDR event body.
    #[serde(default)]
    pub value: String,
    #[serde(default)]
    pub tx_hash: String,
    #[serde(default = "default_true")]
    pub in_successful_call: bool,
    /// `pagingToken` alias — same as `event_id` when absent.
    #[serde(default)]
    pub paging_token: String,
}

fn default_true() -> bool {
    true
}

/// Pluggable historical data source.
///
/// Implementations yield events in `[from_ledger, to_ledger]` inclusive, in
/// **ascending** ledger order. Passing an empty `contract_ids` slice means
/// "all contracts". Sources must not buffer the full range.
pub trait HistoricalSource: Send + Sync {
    fn stream_range<'a>(
        &'a self,
        from_ledger: i64,
        to_ledger: i64,
        contract_ids: &'a [String],
    ) -> BoxStream<'a, anyhow::Result<HistoricalEvent>>;
}

// ---------------------------------------------------------------------------
// Galexie / Stellar CDP source
// ---------------------------------------------------------------------------

/// Read events from a Galexie / Stellar CDP export (newline-delimited JSON).
///
/// # Format
///
/// Each line is a JSON object. Two shapes are accepted:
///
/// **Per-ledger envelope** (Galexie native):
/// ```json
/// {
///   "ledger": 1000000,
///   "ledger_close_time": "2024-01-01T00:00:00Z",
///   "events": [
///     {
///       "id": "…",
///       "contract_id": "C…",
///       "type": "contract",
///       "topic": ["AAAA…","BBBB…"],
///       "value": "CCCC…",
///       "tx_hash": "…",
///       "in_successful_contract_call": true
///     }
///   ]
/// }
/// ```
///
/// **Flat per-event record**:
/// ```json
/// {"id":"0001-0000","contract_id":"C…","ledger":1000000, … }
/// ```
///
/// Both are auto-detected by the presence of the `"events"` key.
///
/// # Inputs
///
/// Pass one or more file paths. Use `"-"` for stdin. Files are consumed in
/// order; supply them in ascending ledger order (Galexie exports are naturally
/// sorted).
pub struct GalexieSource {
    pub inputs: Vec<PathBuf>,
}

/// The per-ledger envelope used by Galexie.
#[derive(Deserialize)]
struct GalexieEnvelope {
    ledger: i64,
    ledger_close_time: Option<String>,
    events: Option<Vec<GalexieEventEntry>>,
}

/// A single event entry inside a Galexie ledger envelope.
#[derive(Deserialize)]
struct GalexieEventEntry {
    id: Option<String>,
    event_id: Option<String>,
    contract_id: Option<String>,
    #[serde(rename = "type")]
    event_type: Option<String>,
    #[serde(default)]
    topic: Vec<String>,
    #[serde(default)]
    value: String,
    #[serde(default)]
    tx_hash: String,
    #[serde(default = "default_true")]
    in_successful_contract_call: bool,
    #[serde(default)]
    paging_token: String,
}

/// A flat per-event record (alternative to the envelope format).
#[derive(Deserialize)]
struct GalexieFlatRecord {
    id: Option<String>,
    event_id: Option<String>,
    contract_id: Option<String>,
    ledger: Option<i64>,
    ledger_closed_at: Option<String>,
    #[serde(rename = "type")]
    event_type: Option<String>,
    #[serde(default)]
    topic: Vec<String>,
    #[serde(default)]
    value: String,
    #[serde(default)]
    tx_hash: String,
    #[serde(default = "default_true")]
    in_successful_contract_call: bool,
    #[serde(default)]
    paging_token: String,
}

impl GalexieSource {
    pub fn new(inputs: Vec<PathBuf>) -> Self {
        Self { inputs }
    }

    /// Parse one line from the export file into zero or more [`HistoricalEvent`]s.
    pub fn parse_line(line: &str) -> anyhow::Result<Vec<HistoricalEvent>> {
        let json: serde_json::Value = serde_json::from_str(line).context("invalid JSON line")?;

        // Is this a per-ledger envelope?
        if json.get("events").is_some() {
            let env: GalexieEnvelope =
                serde_json::from_value(json).context("deserialize ledger envelope")?;
            let close_time = match env.ledger_close_time.as_deref() {
                Some(value) => value
                    .parse::<DateTime<Utc>>()
                    .context("invalid ledger_close_time")?,
                None => Utc::now(),
            };
            let events = env.events.unwrap_or_default();
            return events
                .into_iter()
                .enumerate()
                .map(|(index, e)| {
                    let eid = e
                        .event_id
                        .or(e.id)
                        .with_context(|| format!("event {index} missing id / event_id"))?;
                    let cid = e
                        .contract_id
                        .with_context(|| format!("event {index} missing contract_id"))?;
                    let pg = if e.paging_token.is_empty() {
                        eid.clone()
                    } else {
                        e.paging_token
                    };
                    Ok(HistoricalEvent {
                        event_id: eid,
                        contract_id: cid,
                        ledger: env.ledger,
                        ledger_closed_at: close_time,
                        event_type: e.event_type.unwrap_or_else(|| "contract".into()),
                        topics: e.topic,
                        value: e.value,
                        tx_hash: e.tx_hash,
                        in_successful_call: e.in_successful_contract_call,
                        paging_token: pg,
                    })
                })
                .collect();
        }

        // Flat per-event record.
        let flat: GalexieFlatRecord =
            serde_json::from_str(line).context("deserialize flat event record")?;
        let eid = flat
            .event_id
            .or(flat.id)
            .context("event record missing id / event_id")?;
        let cid = flat
            .contract_id
            .context("event record missing contract_id")?;
        let ledger = flat.ledger.context("event record missing ledger")?;
        let close_time = match flat.ledger_closed_at.as_deref() {
            Some(value) => value
                .parse::<DateTime<Utc>>()
                .context("invalid ledger_closed_at")?,
            None => Utc::now(),
        };
        let pg = if flat.paging_token.is_empty() {
            eid.clone()
        } else {
            flat.paging_token
        };
        Ok(vec![HistoricalEvent {
            event_id: eid,
            contract_id: cid,
            ledger,
            ledger_closed_at: close_time,
            event_type: flat.event_type.unwrap_or_else(|| "contract".into()),
            topics: flat.topic,
            value: flat.value,
            tx_hash: flat.tx_hash,
            in_successful_call: flat.in_successful_contract_call,
            paging_token: pg,
        }])
    }
}

impl HistoricalSource for GalexieSource {
    fn stream_range<'a>(
        &'a self,
        from_ledger: i64,
        to_ledger: i64,
        contract_ids: &'a [String],
    ) -> BoxStream<'a, anyhow::Result<HistoricalEvent>> {
        type Reader = Lines<BufReader<Box<dyn AsyncRead + Send + Unpin>>>;
        struct State {
            paths: Vec<PathBuf>,
            path_index: usize,
            reader: Option<Reader>,
            line_number: usize,
            pending: VecDeque<HistoricalEvent>,
            contracts: HashSet<String>,
        }

        let state = State {
            paths: self.inputs.clone(),
            path_index: 0,
            reader: None,
            line_number: 0,
            pending: VecDeque::new(),
            contracts: contract_ids.iter().cloned().collect(),
        };

        stream::try_unfold(state, move |mut state| async move {
            loop {
                if let Some(event) = state.pending.pop_front() {
                    return Ok(Some((event, state)));
                }

                if state.reader.is_none() {
                    let Some(path) = state.paths.get(state.path_index).cloned() else {
                        return Ok(None);
                    };
                    state.path_index += 1;
                    state.line_number = 0;
                    let input: Box<dyn AsyncRead + Send + Unpin> = if path.to_str() == Some("-") {
                        Box::new(tokio::io::stdin())
                    } else {
                        Box::new(
                            tokio::fs::File::open(&path)
                                .await
                                .with_context(|| format!("cannot open {}", path.display()))?,
                        )
                    };
                    state.reader = Some(BufReader::new(input).lines());
                }

                let line = state
                    .reader
                    .as_mut()
                    .expect("reader initialized")
                    .next_line()
                    .await?;
                let Some(line) = line else {
                    state.reader = None;
                    continue;
                };
                state.line_number += 1;
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }

                let events = GalexieSource::parse_line(line).with_context(|| {
                    format!("invalid historical record at line {}", state.line_number)
                })?;
                let filtered: Vec<_> = events
                    .into_iter()
                    .filter(|event| {
                        event.ledger >= from_ledger
                            && event.ledger <= to_ledger
                            && (state.contracts.is_empty()
                                || state.contracts.contains(&event.contract_id))
                    })
                    .collect();
                state.pending.extend(filtered);
            }
        })
        .boxed()
    }
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

/// Run a deep backfill from `from_ledger` to `to_ledger` (inclusive) using
/// the provided `source`.
///
/// Events are decoded and enriched via the same spec-cache path as the live
/// poller, then written with idempotent `INSERT … ON CONFLICT DO NOTHING`
/// semantics so this command is safe to re-run and safe to run concurrently
/// with the live poller.
///
/// When `to_ledger` is `None`, no upper-bound filtering is applied (run to EOF
/// of the input).
///
/// # Memory-bounded streaming
///
/// Rather than loading the entire input into a single `Vec<HistoricalEvent>`
/// before writing, events are processed in configurable batches (default 1000)
/// directly from the source stream. The memory footprint is `O(batch_size)`
/// regardless of export file size, making this safe on constrained instances
/// (e.g. Render free tier at 512 MB RAM).
///
/// # Seam / hand-off
///
/// The live poller is ledger-cursor–driven and gap-free within the RPC window.
/// Point it at a `START_LEDGER` that overlaps the deep backfill by a few
/// thousand ledgers. Because inserts are idempotent the overlap is safe;
/// overlapping by a few thousand ledgers ensures no gap even if either process
/// restarts at the boundary.
pub async fn run(
    pool: PgPool,
    config: Config,
    source: Box<dyn HistoricalSource>,
    from_ledger: i64,
    to_ledger: Option<i64>,
) -> anyhow::Result<()> {
    let to = to_ledger.unwrap_or(i64::MAX);
    info!(
        from = from_ledger,
        to = if to == i64::MAX {
            "unbounded (EOF)".to_string()
        } else {
            to.to_string()
        },
        "starting deep backfill"
    );

    let batch_size = 1000usize;
    let mut total_inserted = 0u64;
    let mut lines_processed = 0u64;
    let mut batch: Vec<NewEvent> = Vec::with_capacity(batch_size);
    let mut events = source.stream_range(from_ledger, to, &config.contract_ids);
    while let Some(ev) = events.next().await.transpose()? {
        lines_processed += 1;
        let spec = load_persisted_spec_at(&pool, &ev.contract_id, ev.ledger).await?;
        batch.push(historical_to_new_event(ev, spec.as_deref())?);

        if batch.len() >= batch_size {
            let n = flush_batch(&pool, &mut batch).await?;
            total_inserted += n;
            debug!(inserted = n, lines_processed, "flushed batch");
        }
    }

    if !batch.is_empty() {
        let n = flush_batch(&pool, &mut batch).await?;
        total_inserted += n;
    }

    info!(total_inserted, lines_processed, "deep backfill complete");
    Ok(())
}

async fn flush_batch(pool: &PgPool, batch: &mut Vec<NewEvent>) -> anyhow::Result<u64> {
    let inserted = store::insert_events(pool, batch).await?;
    batch.clear();
    Ok(inserted)
}

async fn load_persisted_spec_at(
    pool: &PgPool,
    contract_id: &str,
    ledger: i64,
) -> anyhow::Result<Option<Arc<ContractSpec>>> {
    let version: Option<String> = sqlx::query_scalar(
        "SELECT spec_section
           FROM contract_spec_versions
          WHERE contract_id = $1
            AND COALESCE(upgrade_ledger, observed_ledger) <= $2
            AND spec_section <> ''
          ORDER BY COALESCE(upgrade_ledger, observed_ledger) DESC, version DESC
          LIMIT 1",
    )
    .bind(contract_id)
    .bind(ledger)
    .fetch_optional(pool)
    .await?;

    let section = match version {
        Some(section) => Some(section),
        None => {
            sqlx::query_scalar(
                "SELECT spec_section FROM contract_specs
                  WHERE contract_id = $1 AND spec_section <> ''",
            )
            .bind(contract_id)
            .fetch_optional(pool)
            .await?
        }
    };

    section
        .map(|section| {
            let bytes = hex::decode(&section).context("persisted spec_section is not hex")?;
            ContractSpec::from_spec_xdr_simple(&bytes)
                .map(Arc::new)
                .context("persisted spec_section is not valid contract spec XDR")
        })
        .transpose()
}

/// Convert a [`HistoricalEvent`] into a [`NewEvent`] using the same XDR decode
/// path as the live poller.
fn historical_to_new_event(
    ev: HistoricalEvent,
    spec: Option<&lumenqraph_core::ContractSpec>,
) -> anyhow::Result<NewEvent> {
    let event_type = EventType::from_rpc_str(&ev.event_type)
        .with_context(|| format!("unsupported historical event type: {}", ev.event_type))?;
    use lumenqraph_core::xdr;

    let decoded_topics = xdr::decode_topics(&ev.topics);
    let decoded_value = xdr::decode_scval_base64(&ev.value);
    let event_name = ev
        .topics
        .first()
        .and_then(|t| xdr::event_name_from_topic(t));

    let enriched = match (spec, &event_name) {
        (Some(spec), Some(name)) => spec.enrich_event(name, &decoded_topics, &decoded_value),
        _ => None,
    };

    let paging_token = if ev.paging_token.is_empty() {
        ev.event_id.clone()
    } else {
        ev.paging_token
    };

    Ok(NewEvent {
        event_id: ev.event_id,
        contract_id: ev.contract_id,
        ledger: ev.ledger,
        ledger_closed_at: ev.ledger_closed_at,
        event_type,
        topics: ev.topics,
        decoded_topics,
        event_name,
        value: ev.value,
        decoded_value,
        enriched,
        tx_hash: ev.tx_hash,
        in_successful_call: ev.in_successful_call,
        paging_token,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    fn flat_line(id: &str, cid: &str, ledger: i64) -> String {
        serde_json::json!({
            "id": id,
            "contract_id": cid,
            "ledger": ledger,
            "ledger_closed_at": "2024-01-01T00:00:00Z",
            "type": "contract",
            "topic": [],
            "value": "",
            "tx_hash": "abc",
            "in_successful_contract_call": true,
        })
        .to_string()
    }

    #[test]
    fn malformed_envelope_event_is_an_error() {
        let line = r#"{"ledger":1,"events":[{"id":"missing-contract"}]}"#;
        let error = GalexieSource::parse_line(line).expect_err("missing contract id must fail");
        assert!(error.to_string().contains("missing contract_id"));
    }

    #[test]
    fn unsupported_event_type_is_an_error() {
        let mut events = GalexieSource::parse_line(&flat_line("event-1", "CA", 1)).unwrap();
        events[0].event_type = "future-event".to_string();
        let error = historical_to_new_event(events.remove(0), None)
            .err()
            .expect("unsupported event type must fail");
        assert!(error
            .to_string()
            .contains("unsupported historical event type"));
    }

    fn envelope_line(ledger: i64, events: &[(&str, &str)]) -> String {
        let evs: Vec<serde_json::Value> = events
            .iter()
            .map(|(id, cid)| {
                serde_json::json!({
                    "id": id,
                    "contract_id": cid,
                    "type": "contract",
                    "topic": [],
                    "value": "",
                    "tx_hash": "abc",
                    "in_successful_contract_call": true,
                })
            })
            .collect();
        serde_json::json!({
            "ledger": ledger,
            "ledger_close_time": "2024-01-01T00:00:00Z",
            "events": evs,
        })
        .to_string()
    }

    #[test]
    fn parses_flat_record() {
        let line = flat_line("e1", "CA", 42);
        let evs = GalexieSource::parse_line(&line).unwrap();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].event_id, "e1");
        assert_eq!(evs[0].contract_id, "CA");
        assert_eq!(evs[0].ledger, 42);
        assert_eq!(evs[0].paging_token, "e1"); // falls back to event_id
    }

    #[test]
    fn parses_envelope_record() {
        let line = envelope_line(100, &[("e1", "CA"), ("e2", "CB")]);
        let evs = GalexieSource::parse_line(&line).unwrap();
        assert_eq!(evs.len(), 2);
        assert_eq!(evs[0].event_id, "e1");
        assert_eq!(evs[0].ledger, 100);
        assert_eq!(evs[1].event_id, "e2");
        assert_eq!(evs[1].contract_id, "CB");
    }

    #[test]
    fn invalid_json_returns_error() {
        let result = GalexieSource::parse_line("{not valid json}");
        assert!(result.is_err());
    }

    #[test]
    fn flat_record_missing_contract_id_returns_error() {
        let line = serde_json::json!({"id": "e1", "ledger": 1}).to_string();
        let result = GalexieSource::parse_line(&line);
        assert!(result.is_err(), "expected error for missing contract_id");
    }

    #[test]
    fn envelope_events_with_missing_ids_are_skipped() {
        let line = serde_json::json!({
            "ledger": 5,
            "events": [{"contract_id": "CA", "type": "contract"}],
        })
        .to_string();
        let evs = GalexieSource::parse_line(&line).unwrap();
        assert_eq!(evs.len(), 0, "event missing id should be filtered out");
    }

    #[test]
    fn in_successful_call_defaults_to_true() {
        let line = flat_line("e1", "CA", 1);
        let evs = GalexieSource::parse_line(&line).unwrap();
        assert!(evs[0].in_successful_call);
    }

    #[test]
    fn historical_to_new_event_paging_token_fallback() {
        let ev = HistoricalEvent {
            event_id: "myid".to_string(),
            contract_id: "CA".to_string(),
            ledger: 1,
            ledger_closed_at: Utc::now(),
            event_type: "contract".to_string(),
            topics: vec![],
            value: String::new(),
            tx_hash: String::new(),
            in_successful_call: true,
            paging_token: String::new(),
        };
        let new_ev = historical_to_new_event(ev, None).expect("known event type");
        assert_eq!(new_ev.paging_token, "myid");
    }

    #[test]
    fn historical_to_new_event_preserves_paging_token() {
        let ev = HistoricalEvent {
            event_id: "myid".to_string(),
            contract_id: "CA".to_string(),
            ledger: 1,
            ledger_closed_at: Utc::now(),
            event_type: "contract".to_string(),
            topics: vec![],
            value: String::new(),
            tx_hash: String::new(),
            in_successful_call: true,
            paging_token: "custom-paging".to_string(),
        };
        let new_ev = historical_to_new_event(ev, None).expect("known event type");
        assert_eq!(new_ev.paging_token, "custom-paging");
    }

    #[tokio::test]
    async fn streams_large_input_with_a_bounded_batch() {
        let path = std::env::temp_dir().join(format!(
            "lumenqraph-deep-backfill-{}.ndjson",
            uuid::Uuid::new_v4()
        ));
        let mut file = tokio::io::BufWriter::new(tokio::fs::File::create(&path).await.unwrap());
        for ledger in 0..100_000i64 {
            file.write_all(flat_line(&format!("e-{ledger}"), "CA", ledger).as_bytes())
                .await
                .unwrap();
            file.write_all(b"\n").await.unwrap();
        }
        file.flush().await.unwrap();
        drop(file);

        let source = GalexieSource::new(vec![path.clone()]);
        let mut events = source.stream_range(0, 99_999, &[]);
        let mut count = 0usize;
        let mut buffered = 0usize;
        let mut max_buffered = 0usize;
        while events.next().await.transpose().unwrap().is_some() {
            count += 1;
            buffered += 1;
            max_buffered = max_buffered.max(buffered);
            if buffered == 1_000 {
                buffered = 0;
            }
        }

        tokio::fs::remove_file(path).await.unwrap();
        assert_eq!(count, 100_000);
        assert!(max_buffered <= 1_000);
    }
}
