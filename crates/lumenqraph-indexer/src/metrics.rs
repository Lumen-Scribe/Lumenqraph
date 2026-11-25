//! Small in-process gauges exported on `/metrics` (#746).
//!
//! These report how many state-indexing candidates the per-cycle caps pushed to
//! a later cycle. A persistently non-zero value means the caps are binding.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static DEFERRED_STATE_CONTRACTS: AtomicU64 = AtomicU64::new(0);
static DEFERRED_STATE_KEYS: AtomicU64 = AtomicU64::new(0);
static HEARTBEAT_TIMESTAMP_SECONDS: AtomicU64 = AtomicU64::new(0);

/// Record the current Unix timestamp (in seconds) as the poll-loop heartbeat.
pub fn record_heartbeat() {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    record_heartbeat_timestamp(secs);
}

/// Record an explicit Unix timestamp (in seconds) as the poll-loop heartbeat.
pub fn record_heartbeat_timestamp(secs: u64) {
    HEARTBEAT_TIMESTAMP_SECONDS.store(secs, Ordering::Relaxed);
}

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
        (
            "lumenqraph_indexer_heartbeat_timestamp_seconds",
            "Unix timestamp in seconds of the last poll loop iteration",
            &HEARTBEAT_TIMESTAMP_SECONDS,
        ),
    ] {
        body.push_str(&format!("# HELP {name} {help}\n# TYPE {name} gauge\n"));
        body.push_str(&format!("{name} {}\n", value.load(Ordering::Relaxed)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_timestamp_updates_and_renders() {
        record_heartbeat_timestamp(1_700_000_000);
        let mut body = String::new();
        render(&mut body);
        assert!(body.contains("# TYPE lumenqraph_indexer_heartbeat_timestamp_seconds gauge"));
        assert!(body.contains("lumenqraph_indexer_heartbeat_timestamp_seconds 1700000000\n"));

        record_heartbeat();
        let mut body_now = String::new();
        render(&mut body_now);
        let line = body_now
            .lines()
            .find(|l| l.starts_with("lumenqraph_indexer_heartbeat_timestamp_seconds "))
            .expect("heartbeat metric line present");
        let ts: u64 = line
            .split_whitespace()
            .nth(1)
            .expect("timestamp value")
            .parse()
            .expect("valid u64");
        assert!(ts > 1_700_000_000);
    }
}
