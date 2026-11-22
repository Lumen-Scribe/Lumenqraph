//! In-process duration histograms exported on `/metrics` (#758).
//!
//! Bucket boundaries are fixed and label values come from closed sets (cycle
//! phase, RPC method, configured endpoint host, table name), so cardinality
//! stays bounded.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

/// Upper bounds in seconds, shared by every histogram.
const BUCKETS: [f64; 12] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

#[derive(Default, Clone)]
struct Series {
    counts: [u64; BUCKETS.len()],
    count: u64,
    sum: f64,
}

/// A histogram keyed by its rendered label set, e.g. `phase="events"`.
pub struct Histogram {
    name: &'static str,
    help: &'static str,
    series: Mutex<BTreeMap<String, Series>>,
}

impl Histogram {
    const fn new(name: &'static str, help: &'static str) -> Self {
        Self {
            name,
            help,
            series: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn observe(&self, labels: &[(&str, &str)], elapsed: Duration) {
        let key = labels
            .iter()
            .map(|(k, v)| format!("{k}=\"{}\"", v.replace('\\', "\\\\").replace('"', "\\\"")))
            .collect::<Vec<_>>()
            .join(",");
        let secs = elapsed.as_secs_f64();
        let mut map = self.series.lock().unwrap_or_else(|e| e.into_inner());
        let s = map.entry(key).or_default();
        for (i, le) in BUCKETS.iter().enumerate() {
            if secs <= *le {
                s.counts[i] += 1;
            }
        }
        s.count += 1;
        s.sum += secs;
    }

    pub fn render(&self, body: &mut String) {
        let map = self.series.lock().unwrap_or_else(|e| e.into_inner());
        body.push_str(&format!("# HELP {} {}\n", self.name, self.help));
        body.push_str(&format!("# TYPE {} histogram\n", self.name));
        for (labels, s) in map.iter() {
            let sep = if labels.is_empty() { "" } else { "," };
            for (i, le) in BUCKETS.iter().enumerate() {
                body.push_str(&format!(
                    "{}_bucket{{{labels}{sep}le=\"{le}\"}} {}\n",
                    self.name, s.counts[i]
                ));
            }
            body.push_str(&format!(
                "{}_bucket{{{labels}{sep}le=\"+Inf\"}} {}\n",
                self.name, s.count
            ));
            body.push_str(&format!("{}_sum{{{labels}}} {}\n", self.name, s.sum));
            body.push_str(&format!("{}_count{{{labels}}} {}\n", self.name, s.count));
        }
    }
}

/// Poll-cycle duration by `phase` (`events`, `reorg`, `total`).
pub static CYCLE_DURATION: Histogram = Histogram::new(
    "lumenqraph_indexer_cycle_duration_seconds",
    "Poll cycle duration by phase",
);

/// RPC request duration by `method` and `endpoint_host`, including retries.
pub static RPC_REQUEST_DURATION: Histogram = Histogram::new(
    "lumenqraph_indexer_rpc_request_duration_seconds",
    "RPC request duration by method and endpoint host",
);

/// Database write duration by `table`.
pub static DB_WRITE_DURATION: Histogram = Histogram::new(
    "lumenqraph_indexer_db_write_duration_seconds",
    "Database write duration by table",
);

/// Appends all duration histograms to a `/metrics` body.
pub fn render(body: &mut String) {
    CYCLE_DURATION.render(body);
    RPC_REQUEST_DURATION.render(body);
    DB_WRITE_DURATION.render(body);
}
