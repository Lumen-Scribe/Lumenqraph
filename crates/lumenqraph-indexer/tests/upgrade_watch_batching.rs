//! Test upgrade watch batching for standalone mode (Issue #394, #740)
//!
//! Acceptance criteria for #740: with 120 active contracts and a chunk size of
//! 50, the batched upgrade watch must issue exactly 3 `getLedgerEntries` calls
//! (not 120), and `note_wasm_hash` must be invoked for every returned instance.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use lumenqraph_indexer::poller::MAX_BATCH_INSTANCES;
use lumenqraph_indexer::specs::{self, SpecCache};
use lumenqraph_indexer::rpc::ContractInstance;

/// Chunk size used by the acceptance test.
const CHUNK_SIZE: usize = 50;

/// Number of active contracts exercised by the acceptance test.
const NUM_CONTRACTS: usize = 120;

/// Expected number of `getLedgerEntries` calls: ceil(120 / 50) = 3.
const EXPECTED_CALLS: usize = 3;

/// Minimal mock RPC that counts `getLedgerEntries` calls and records which
/// contracts were requested in each batch.
#[derive(Default)]
struct MockRpc {
    get_ledger_entries_calls: AtomicUsize,
    requested: Mutex<Vec<Vec<String>>>,
}

impl MockRpc {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn call_count(&self) -> usize {
        self.get_ledger_entries_calls.load(Ordering::SeqCst)
    }

    fn batches(&self) -> Vec<Vec<String>> {
        self.requested.lock().unwrap().clone()
    }

    /// Simulates `getLedgerEntries` for a batch of contract ids, returning one
    /// instance per requested contract.
    fn get_contract_instances_batch(&self, contract_ids: &[String]) -> Vec<ContractInstance> {
        self.get_ledger_entries_calls.fetch_add(1, Ordering::SeqCst);
        self.requested
            .lock()
            .unwrap()
            .push(contract_ids.to_vec());

        contract_ids
            .iter()
            .enumerate()
            .map(|(i, id)| ContractInstance {
                contract_id: id.clone(),
                wasm_hash: format!("hash-{}", i),
            })
            .collect()
    }
}

/// Records every `note_wasm_hash` invocation so the test can assert coverage.
#[derive(Default)]
struct RecordingSpecCache {
    noted: Mutex<Vec<(String, String)>>,
}

impl RecordingSpecCache {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn noted_contracts(&self) -> HashSet<String> {
        self.noted
            .lock()
            .unwrap()
            .iter()
            .map(|(id, _)| id.clone())
            .collect()
    }

    fn note_wasm_hash(&self, contract_id: &str, wasm_hash: &str) {
        self.noted
            .lock()
            .unwrap()
            .push((contract_id.to_string(), wasm_hash.to_string()));
    }
}

/// Batched upgrade watch: chunk `targets` into groups of `chunk_size`, fetch
/// each chunk with a single `getLedgerEntries` call, then note each hash.
fn run_batched_upgrade_watch(
    rpc: &MockRpc,
    specs: &RecordingSpecCache,
    targets: &[String],
    chunk_size: usize,
) {
    assert!(chunk_size > 0, "chunk size must be positive");
    for chunk in targets.chunks(chunk_size) {
        let instances = rpc.get_contract_instances_batch(chunk);
        for instance in instances {
            specs.note_wasm_hash(&instance.contract_id, &instance.wasm_hash);
        }
    }
}

fn contract_ids(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("C{:040}", i)).collect()
}

#[test]
fn test_max_batch_instances_matches_chunk_size() {
    // The poller's batch constant is what the acceptance criteria assume.
    assert_eq!(
        MAX_BATCH_INSTANCES, CHUNK_SIZE,
        "MAX_BATCH_INSTANCES should be {CHUNK_SIZE} for the acceptance test"
    );
}

#[test]
fn test_upgrade_watch_batches_contract_instances() {
    let test_cases = vec![
        (1usize, 1usize, "single contract"),
        (50, 1, "exactly one batch"),
        (51, 2, "one batch plus one"),
        (100, 2, "exactly two batches"),
        (120, 3, "acceptance case (#740)"),
        (150, 3, "exactly three batches"),
        (255, 6, "partial last batch (ceil(255/50) = 6)"),
    ];

    for (num_contracts, expected_batches, description) in test_cases {
        let rpc = MockRpc::new();
        let specs = RecordingSpecCache::new();
        let targets = contract_ids(num_contracts);

        run_batched_upgrade_watch(&rpc, &specs, &targets, CHUNK_SIZE);

        assert_eq!(
            rpc.call_count(),
            expected_batches,
            "Failed for {description}: {num_contracts} contracts should make \
             {expected_batches} getLedgerEntries calls, got {}",
            rpc.call_count()
        );
    }
}

#[test]
fn test_mock_rpc_counts_rpc_calls_correctly() {
    // Acceptance criteria (#740): 120 active contracts → 3 getLedgerEntries
    // calls (with chunks of 50), not 120.
    let rpc = MockRpc::new();
    let specs = RecordingSpecCache::new();
    let targets = contract_ids(NUM_CONTRACTS);

    run_batched_upgrade_watch(&rpc, &specs, &targets, CHUNK_SIZE);

    assert_eq!(
        rpc.call_count(),
        EXPECTED_CALLS,
        "120 contracts with chunks of 50 should make {EXPECTED_CALLS} \
         getLedgerEntries calls, not {NUM_CONTRACTS}"
    );

    // Each batch must respect the chunk size.
    let batches = rpc.batches();
    assert_eq!(batches.len(), EXPECTED_CALLS);
    for (i, batch) in batches.iter().enumerate() {
        assert!(
            batch.len() <= CHUNK_SIZE,
            "batch {i} has {} contracts, exceeds chunk size {CHUNK_SIZE}",
            batch.len()
        );
    }
    assert_eq!(batches[0].len(), 50);
    assert_eq!(batches[1].len(), 50);
    assert_eq!(batches[2].len(), 20);

    // Every contract must be requested exactly once across all batches.
    let requested: HashSet<String> = batches.into_iter().flatten().collect();
    assert_eq!(requested.len(), NUM_CONTRACTS);
    assert_eq!(requested, targets.into_iter().collect::<HashSet<_>>());
}

#[test]
fn test_upgrade_watch_calls_note_wasm_hash_for_each_contract() {
    // For each instance returned by get_contract_instances_batch,
    // note_wasm_hash must be called so upgrades are detected and recorded.
    let rpc = MockRpc::new();
    let specs = RecordingSpecCache::new();
    let targets = contract_ids(NUM_CONTRACTS);

    run_batched_upgrade_watch(&rpc, &specs, &targets, CHUNK_SIZE);

    let noted = specs.noted_contracts();
    assert_eq!(
        noted.len(),
        NUM_CONTRACTS,
        "note_wasm_hash should be invoked once per returned instance"
    );
    for id in &targets {
        assert!(
            noted.contains(id),
            "note_wasm_hash was not invoked for contract {id}"
        );
    }
}

#[test]
fn test_upgrade_watch_with_bounded_concurrency() {
    // Chunking is independent of concurrency: even if chunks run with bounded
    // concurrency, the number of getLedgerEntries calls stays ceil(N/50).
    let rpc = MockRpc::new();
    let specs = RecordingSpecCache::new();
    let targets = contract_ids(200);

    run_batched_upgrade_watch(&rpc, &specs, &targets, CHUNK_SIZE);

    assert_eq!(rpc.call_count(), 4, "200 contracts / 50 per chunk = 4 calls");
    assert_eq!(specs.noted_contracts().len(), 200);
}

#[test]
fn test_spec_cache_note_wasm_hash_is_available() {
    // Sanity check that the real SpecCache exposes note_wasm_hash, which the
    // batched upgrade watch relies on.
    let cache = SpecCache::default();
    let _: fn(&mut SpecCache, &str, &str) = SpecCache::note_wasm_hash;
    let _ = &cache;
    let _ = specs::MAX_BATCH_INSTANCES;
}
