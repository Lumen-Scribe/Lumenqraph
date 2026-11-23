//! Metrics middleware recording benchmark (#801).
//!
//! | Bench                              | What is measured                                  |
//! |------------------------------------|---------------------------------------------------|
//! | `metrics_record/8_threads_10m`     | 10 million `record` calls spread over 8 threads   |
//!
//! Recording takes only shared locks and atomic adds once a series exists, so
//! threads should not serialise on the metrics path.
//!
//!   cargo bench -p lumenqraph-api --bench metrics_record

// The API crate is a binary, so pull the module in directly.
#[allow(dead_code)]
#[path = "../src/metrics_middleware.rs"]
mod metrics_middleware;

use axum::http::{Method, StatusCode};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use metrics_middleware::MetricsCollector;

const THREADS: u64 = 8;
const OBSERVATIONS: u64 = 10_000_000;

fn bench_metrics_record(c: &mut Criterion) {
    let mut group = c.benchmark_group("metrics_record");

    let collector = MetricsCollector::new();
    let routes = ["/contracts/:contract_id/events", "/events", "/health"];
    group.throughput(Throughput::Elements(OBSERVATIONS));
    group.sample_size(10);
    group.bench_function("8_threads_10m", |b| {
        b.iter(|| {
            std::thread::scope(|scope| {
                for t in 0..THREADS {
                    let collector = &collector;
                    let routes = &routes;
                    scope.spawn(move || {
                        for i in 0..OBSERVATIONS / THREADS {
                            collector.record(
                                routes[((i + t) % 3) as usize],
                                &Method::GET,
                                StatusCode::OK,
                                0.000_4,
                            );
                        }
                    });
                }
            });
        })
    });

    group.finish();
}

criterion_group!(benches, bench_metrics_record);
criterion_main!(benches);
