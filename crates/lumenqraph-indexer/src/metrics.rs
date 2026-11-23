//! Small in-process gauges exported on `/metrics` (#746).
//!
//! These report how many state-indexing candidates the per-cycle caps pushed to
//! a later cycle. A persistently non-zero value means the caps are binding.

use std::sync::atomic::{AtomicU64, Ordering};

static DEFERRED_STATE_CONTRACTS: AtomicU64 = AtomicU64::new(0);
static DEFERRED_STATE_KEYS: AtomicU64 = AtomicU64::new(0);

/// Record how many contracts were deferred in the current cycle.
pub fn record_deferred_state_contracts(n: u64) {
    DEFERRED_STATE_CONTRACTS.store(n, Ordering::Relaxed);
}

/// Record how many storage keys were deferred in the current cycle.
pub fn record_deferred_state_keys(n: u64) {
    DEFERRED_STATE_KEYS.store(n, Ordering::Relaxed);
}

/// Append the gauges in Prometheus text format.
pub fn render(body: &mut String) {
    for (name, help, value) in [
        (
            "lumenqraph_indexer_state_contracts_deferred",
            "Contracts deferred to a later cycle by MAX_STATE_CONTRACTS_PER_CYCLE (current cycle)",
            &DEFERRED_STATE_CONTRACTS,
        ),
        (
            "lumenqraph_indexer_state_keys_deferred",
            "Storage keys deferred to a later cycle by MAX_STATE_KEYS_PER_CYCLE (current cycle)",
            &DEFERRED_STATE_KEYS,
        ),
    ] {
        body.push_str(&format!("# HELP {name} {help}\n# TYPE {name} gauge\n"));
        body.push_str(&format!("{name} {}\n", value.load(Ordering::Relaxed)));
    }
}
