//! Indexer pipeline micro-benchmarks.
//!
//! Three isolated phases are benchmarked independently so that each measured
//! number reflects one component — never network latency, which would swamp
//! every other signal:
//!
//! | Phase        | What is measured                               | I/O  |
//! |--------------|------------------------------------------------|------|
//! | `xdr_decode` | Base64 XDR → decoded JSON per event           | none |
//! | `enrichment` | Spec-driven named/typed enrichment per event   | none |
//! | `db_insert`  | UNNEST batch INSERT into Postgres             | DB   |
//!
//! The XDR decode and enrichment phases are pure-CPU: they use hard-coded
//! sample data and never touch a network or database.  The db_insert phase
//! requires a Postgres database pointed to by `TEST_DATABASE_URL`; if that
//! variable is absent the benchmark is skipped with a warning.
//!
//! Run all phases:
//!   cargo bench --bench bench_indexer
//!
//! Run a single phase:
//!   cargo bench --bench bench_indexer -- xdr_decode
//!
//! Run the DB phase (requires a test database):
//!   TEST_DATABASE_URL=postgres://… cargo bench --bench bench_indexer -- db_insert

use chrono::Utc;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use lumenqraph_core::{xdr, ContractSpec, NewEvent};
use serde_json::json;
use stellar_xdr::curr::{Limits, ScSpecEntry, ScSpecTypeDef, ScSymbol, WriteXdr};

// ── Synthetic test data ───────────────────────────────────────────────────────

/// A realistic Base64-encoded `ScVal::Symbol("transfer")` as it arrives from
/// the Soroban RPC `getEvents` topics array.
///
/// Encoded via: ScVal::Symbol(ScSymbol("transfer".try_into().unwrap()))
const TRANSFER_TOPIC_B64: &str = "AAAADwAAAAh0cmFuc2Zlcg==";

/// A realistic Base64-encoded `ScVal::I128(1_000_000)` as the event value.
const TRANSFER_VALUE_B64: &str = "AAAACgAAAAAAAAAAAAAAAA8nEA==";

/// Build a batch of synthetic `EventInfo`-equivalent raw events ready for the
/// decode phase. These are the structures the RPC client hands to `convert::to_new_event`.
/// We represent them here as plain tuples `(topic_vec, value_str)` to avoid
/// depending on the private `EventInfo` type.
fn synthetic_raw_events(n: usize) -> Vec<(Vec<String>, String)> {
    (0..n)
        .map(|_| {
            (
                vec![
                    TRANSFER_TOPIC_B64.to_string(),
                    // Encode a fake address as a second topic (just reuse the
                    // transfer symbol for shape — decode handles unknown gracefully).
                    TRANSFER_TOPIC_B64.to_string(),
                ],
                TRANSFER_VALUE_B64.to_string(),
            )
        })
        .collect()
}

/// Build a batch of pre-decoded `NewEvent`s ready for the enrichment or DB phase.
fn synthetic_new_events(n: usize) -> Vec<NewEvent> {
    (0..n)
        .map(|i| NewEvent {
            event_id: format!("bench-event-{i:08}-0000000000"),
            contract_id: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM".into(),
            ledger: 1_000_000 + i as i64,
            ledger_closed_at: Utc::now(),
            event_type: "contract".into(),
            topics: vec![TRANSFER_TOPIC_B64.into()],
            decoded_topics: vec![json!("transfer")],
            event_name: Some("transfer".into()),
            value: TRANSFER_VALUE_B64.into(),
            decoded_value: json!("1000000"),
            enriched: None,
            tx_hash: format!("deadbeef{i:056x}"),
            in_successful_call: true,
            paging_token: format!("bench-event-{i:08}-0000000000"),
        })
        .collect()
}

/// Build a minimal `ContractSpec` with one event entry for `transfer`.
fn minimal_contract_spec() -> ContractSpec {
    use stellar_xdr::curr::{
        ScSpecEventDataFormat, ScSpecEventParamLocationV0, ScSpecEventParamV0, ScSpecEventV0,
        StringM, VecM,
    };

    let params: VecM<ScSpecEventParamV0, 50> = vec![ScSpecEventParamV0 {
        doc: "".try_into().unwrap(),
        name: StringM::try_from("amount").unwrap(),
        type_: ScSpecTypeDef::I128,
        location: ScSpecEventParamLocationV0::Data,
    }]
    .try_into()
    .unwrap();

    let entry = ScSpecEntry::EventV0(ScSpecEventV0 {
        doc: "".try_into().unwrap(),
        lib: "".try_into().unwrap(),
        name: ScSymbol("transfer".try_into().unwrap()),
        prefix_topics: vec![ScSymbol("transfer".try_into().unwrap())]
            .try_into()
            .unwrap(),
        params,
        data_format: ScSpecEventDataFormat::SingleValue,
    });
    let bytes = entry.to_xdr(Limits::none()).unwrap();
    ContractSpec::from_spec_xdr(&bytes).unwrap_or_default()
}

// ── Phase 1: XDR decode ───────────────────────────────────────────────────────

fn bench_xdr_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("xdr_decode");

    for &batch in &[1usize, 100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch),
            &batch,
            |b, &n| {
                let raw = synthetic_raw_events(n);
                b.iter(|| {
                    // Replicate what `convert::to_new_event` does for XDR.
                    let mut results = Vec::with_capacity(n);
                    for (topics, value) in &raw {
                        let decoded_topics = xdr::decode_topics(topics);
                        let decoded_value = xdr::decode_scval_base64(value);
                        let event_name = topics
                            .first()
                            .and_then(|t| xdr::event_name_from_topic(t));
                        results.push((decoded_topics, decoded_value, event_name));
                    }
                    results
                });
            },
        );
    }

    group.finish();
}

// ── Phase 2: Enrichment ───────────────────────────────────────────────────────

fn bench_enrichment(c: &mut Criterion) {
    let mut group = c.benchmark_group("enrichment");
    let spec = minimal_contract_spec();

    for &batch in &[1usize, 100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch),
            &batch,
            |b, &n| {
                let events = synthetic_new_events(n);
                b.iter(|| {
                    // Replicate what `convert::to_new_event` does for enrichment.
                    let mut enriched_count = 0usize;
                    for ev in &events {
                        if let Some(name) = &ev.event_name {
                            if spec
                                .enrich_event(name, &ev.decoded_topics, &ev.decoded_value)
                                .is_some()
                            {
                                enriched_count += 1;
                            }
                        }
                    }
                    enriched_count
                });
            },
        );
    }

    group.finish();
}

// ── Phase 3: Database insert ──────────────────────────────────────────────────
//
// This phase requires a real Postgres database.  Set TEST_DATABASE_URL to run it;
// otherwise the benchmark group is empty and criterion prints a warning.

fn bench_db_insert(c: &mut Criterion) {
    let db_url = match std::env::var("TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!(
                "\n[bench_indexer] db_insert phase SKIPPED — set TEST_DATABASE_URL to enable it.\n"
            );
            return;
        }
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    // Set up a fresh schema for this benchmark run so inserts don't accumulate.
    let pool = rt.block_on(async {
        use sqlx::postgres::PgPoolOptions;
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&db_url)
            .await
            .expect("connect to TEST_DATABASE_URL");
        sqlx::migrate!("../../migrations")
            .run(&pool)
            .await
            .expect("run migrations");
        pool
    });

    let mut group = c.benchmark_group("db_insert");

    for &batch in &[10usize, 100, 500, 1_000] {
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch),
            &batch,
            |b, &n| {
                b.to_async(&rt).iter_batched(
                    // Setup: generate a fresh set of events with unique IDs.
                    || synthetic_new_events(n),
                    // Routine: insert the batch and clean up.
                    |events| {
                        let pool = pool.clone();
                        async move {
                            // Inline the core of `store::insert_events` to avoid
                            // depending on the private indexer module.
                            let event_ids: Vec<_> =
                                events.iter().map(|e| e.event_id.clone()).collect();
                            let contract_ids: Vec<_> =
                                events.iter().map(|e| e.contract_id.clone()).collect();
                            let ledgers: Vec<i64> =
                                events.iter().map(|e| e.ledger).collect();
                            let closed_ats: Vec<_> =
                                events.iter().map(|e| e.ledger_closed_at).collect();
                            let event_types: Vec<_> =
                                events.iter().map(|e| e.event_type.clone()).collect();
                            let topics_json: Vec<String> = events
                                .iter()
                                .map(|e| serde_json::to_string(&e.topics).unwrap())
                                .collect();
                            let decoded_topics_json: Vec<String> = events
                                .iter()
                                .map(|e| serde_json::to_string(&e.decoded_topics).unwrap())
                                .collect();
                            let event_names: Vec<Option<String>> =
                                events.iter().map(|e| e.event_name.clone()).collect();
                            let values: Vec<_> =
                                events.iter().map(|e| e.value.clone()).collect();
                            let decoded_values_json: Vec<String> = events
                                .iter()
                                .map(|e| serde_json::to_string(&e.decoded_value).unwrap())
                                .collect();
                            let enriched_json: Vec<Option<String>> = events
                                .iter()
                                .map(|e| {
                                    e.enriched
                                        .as_ref()
                                        .map(|v| serde_json::to_string(v).unwrap())
                                })
                                .collect();
                            let tx_hashes: Vec<_> =
                                events.iter().map(|e| e.tx_hash.clone()).collect();
                            let in_successful_calls: Vec<bool> =
                                events.iter().map(|e| e.in_successful_call).collect();
                            let paging_tokens: Vec<_> =
                                events.iter().map(|e| e.paging_token.clone()).collect();

                            sqlx::query(
                                "INSERT INTO events (
                                    event_id, contract_id, ledger, ledger_closed_at, event_type,
                                    topics, decoded_topics, event_name, value, decoded_value,
                                    enriched, tx_hash, in_successful_call, paging_token
                                 )
                                 SELECT
                                    event_id, contract_id, ledger, ledger_closed_at, event_type,
                                    topics::jsonb, decoded_topics::jsonb, event_name, value,
                                    decoded_value::jsonb, enriched::jsonb, tx_hash,
                                    in_successful_call, paging_token
                                 FROM UNNEST(
                                    $1::text[], $2::text[], $3::bigint[], $4::timestamptz[],
                                    $5::text[], $6::text[], $7::text[], $8::text[], $9::text[],
                                    $10::text[], $11::text[], $12::text[], $13::bool[], $14::text[]
                                 ) AS t(
                                    event_id, contract_id, ledger, ledger_closed_at, event_type,
                                    topics, decoded_topics, event_name, value, decoded_value,
                                    enriched, tx_hash, in_successful_call, paging_token
                                 )
                                 ON CONFLICT (event_id) DO NOTHING",
                            )
                            .bind(&event_ids)
                            .bind(&contract_ids)
                            .bind(&ledgers)
                            .bind(&closed_ats)
                            .bind(&event_types)
                            .bind(&topics_json)
                            .bind(&decoded_topics_json)
                            .bind(&event_names)
                            .bind(&values)
                            .bind(&decoded_values_json)
                            .bind(&enriched_json)
                            .bind(&tx_hashes)
                            .bind(&in_successful_calls)
                            .bind(&paging_tokens)
                            .execute(&pool)
                            .await
                            .expect("insert_events");

                            // Clean up so subsequent iterations start fresh.
                            sqlx::query("DELETE FROM events")
                                .execute(&pool)
                                .await
                                .expect("cleanup");
                        }
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
    rt.block_on(pool.close());
}

criterion_group!(benches, bench_xdr_decode, bench_enrichment, bench_db_insert);
criterion_main!(benches);
