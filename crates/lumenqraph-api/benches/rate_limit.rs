//! Rate-limiter request-path benchmarks (#446).
//!
//! | Bench                         | What is measured                                        |
//! |-------------------------------|---------------------------------------------------------|
//! | `rate_limit/hot_identity`     | Repeated checks for one identity (steady state)         |
//! | `rate_limit/unique_flood_200k`| Checks for 200k distinct identities against a full map  |
//!
//! The flood case models an `X-Forwarded-For` spoofing attack: every request
//! carries a new identity and the 100k-entry map is already full, so each
//! check must evict. Per-check cost should match `hot_identity` within a
//! small constant factor (O(1) amortised), not grow with the map size.
//!
//!   cargo bench -p lumenqraph-api --bench rate_limit

// The API crate is a binary, so pull the module in directly.
#[allow(dead_code)]
#[path = "../src/rate_limit.rs"]
mod rate_limit;

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rate_limit::{MemoryBackend, RateLimitBackend};

const FLOOD_IDENTITIES: usize = 200_000;

fn bench_rate_limit(c: &mut Criterion) {
    let mut group = c.benchmark_group("rate_limit");

    let backend = MemoryBackend::new();
    group.throughput(Throughput::Elements(1));
    group.bench_function("hot_identity", |b| {
        b.iter(|| black_box(backend.check(black_box("hot-key"), 1_000_000)))
    });

    // Pre-build the identities so string formatting is not measured.
    let identities: Vec<String> = (0..FLOOD_IDENTITIES)
        .map(|i| format!("198.51.{}.{}", i / 256, i % 256))
        .collect();
    let backend = MemoryBackend::new();
    // Saturate the map first so every measured check runs at capacity.
    for id in &identities[..FLOOD_IDENTITIES / 2] {
        backend.check(id, 60);
    }
    group.throughput(Throughput::Elements(FLOOD_IDENTITIES as u64));
    group.sample_size(10);
    group.bench_function("unique_flood_200k", |b| {
        b.iter(|| {
            for id in &identities {
                black_box(backend.check(id, 60));
            }
        })
    });

    group.finish();
}

criterion_group!(benches, bench_rate_limit);
criterion_main!(benches);
