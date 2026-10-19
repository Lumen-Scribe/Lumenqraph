//! HTTP request metrics middleware. Records per-route latency histograms and
//! status-code counters, keyed by matched route template and method to keep
//! cardinality bounded.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use axum::extract::Request;
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use parking_lot::RwLock;

/// Upper bounds, in milliseconds, of the latency histogram buckets. Samples
/// above the last bound are counted only in `+Inf`.
pub const LATENCY_BUCKETS_MS: [u64; 9] = [1, 5, 10, 50, 100, 500, 1_000, 5_000, 10_000];

/// Fixed-size latency histogram. Its memory does not depend on how many
/// samples it has seen, so it is safe to keep for the life of the process.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LatencyHistogram {
    /// Non-cumulative count per bucket; `buckets[i]` holds samples in
    /// `(LATENCY_BUCKETS_MS[i-1], LATENCY_BUCKETS_MS[i]]`.
    pub buckets: [u64; LATENCY_BUCKETS_MS.len()],
    /// Total number of samples, including those above the last bound.
    pub count: u64,
    /// Sum of all samples in milliseconds.
    pub sum_ms: u64,
}

impl LatencyHistogram {
    pub fn observe(&mut self, elapsed_ms: u64) {
        self.count += 1;
        self.sum_ms = self.sum_ms.saturating_add(elapsed_ms);
        if let Some(i) = LATENCY_BUCKETS_MS.iter().position(|&bound| elapsed_ms <= bound) {
            self.buckets[i] += 1;
        }
    }

    /// Cumulative count for each bucket bound, as Prometheus `le` buckets expect.
    pub fn cumulative(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        LATENCY_BUCKETS_MS
            .iter()
            .zip(self.buckets.iter())
            .scan(0u64, |running, (&bound, &n)| {
                *running += n;
                Some((bound, *running))
            })
    }
}

/// Label set that identifies one histogram series.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HistogramKey {
    pub route: String,
    pub method: String,
}

pub struct MetricsCollector {
    pub histogram_buckets: Arc<RwLock<HashMap<HistogramKey, LatencyHistogram>>>,
    pub status_counters: Arc<RwLock<HashMap<String, u64>>>,
}

#[derive(Clone)]
pub struct RouteLabel(pub String);

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            histogram_buckets: Arc::new(RwLock::new(HashMap::new())),
            status_counters: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn middleware(
        self,
        req: Request,
        next: Next,
    ) -> Response {
        let method = req.method().clone();
        let uri = req.uri().path().to_string();
        let start = Instant::now();

        let response = next.run(req).await;
        let elapsed_ms = start.elapsed().as_millis() as u64;
        let status = response.status();

        let route_label = extract_route_template(&uri, &method);
        self.record(&route_label, &method, status, elapsed_ms);

        response
    }

    /// Record one finished request. Memory grows only with the number of
    /// distinct label sets, never with the number of requests.
    pub fn record(&self, route: &str, method: &Method, status: StatusCode, elapsed_ms: u64) {
        let counter_key = format!(
            "http_requests{{route=\"{}\",method=\"{}\",status=\"{}\"}}",
            route,
            method,
            status.as_u16()
        );
        {
            let mut counters = self.status_counters.write();
            *counters.entry(counter_key).or_insert(0) += 1;
        }

        let key = HistogramKey {
            route: route.to_string(),
            method: method.to_string(),
        };
        self.histogram_buckets
            .write()
            .entry(key)
            .or_default()
            .observe(elapsed_ms);
    }
}

fn extract_route_template(path: &str, _method: &Method) -> String {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    let mut template = String::new();
    for segment in segments {
        template.push('/');
        if is_uuid_like(segment) || is_numeric(segment) {
            template.push_str("{id}");
        } else {
            template.push_str(segment);
        }
    }

    if template.is_empty() {
        "/".to_string()
    } else {
        template
    }
}

fn is_uuid_like(s: &str) -> bool {
    s.len() == 36 && s.matches('-').count() == 4
}

fn is_numeric(s: &str) -> bool {
    s.parse::<i64>().is_ok()
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collector_memory_is_constant_over_one_million_requests() {
        let collector = MetricsCollector::new();
        let routes = ["/contracts", "/events", "/transfers"];
        for i in 0..1_000_000u64 {
            collector.record(routes[(i % 3) as usize], &Method::GET, StatusCode::OK, i % 1_500);
        }

        let histograms = collector.histogram_buckets.read();
        assert_eq!(histograms.len(), routes.len());
        let total: u64 = histograms.values().map(|h| h.count).sum();
        assert_eq!(total, 1_000_000);
        // Each series is a fixed-size struct: no per-sample storage exists.
        assert_eq!(
            std::mem::size_of::<LatencyHistogram>(),
            (LATENCY_BUCKETS_MS.len() + 2) * std::mem::size_of::<u64>()
        );
    }

    #[test]
    fn histogram_buckets_are_cumulative_and_include_overflow() {
        let mut h = LatencyHistogram::default();
        for ms in [0, 3, 7, 20_000] {
            h.observe(ms);
        }
        let cumulative: Vec<(u64, u64)> = h.cumulative().collect();
        assert_eq!(cumulative[0], (1, 1));
        assert_eq!(cumulative[1], (5, 2));
        assert_eq!(cumulative[2], (10, 3));
        assert_eq!(*cumulative.last().unwrap(), (10_000, 3));
        assert_eq!(h.count, 4);
        assert_eq!(h.sum_ms, 20_010);
    }

    #[test]
    fn test_route_template_extraction() {
        let cases = vec![
            ("/", "/"),
            ("/contracts", "/contracts"),
            ("/contracts/C1", "/contracts/{id}"),
            (
                "/contracts/01234567-89ab-cdef-0123-456789abcdef/events",
                "/contracts/{id}/events",
            ),
            ("/events/123", "/events/{id}"),
            ("/webhooks/456/something", "/webhooks/{id}/something"),
        ];

        for (path, expected) in cases {
            let result = extract_route_template(path, &Method::GET);
            assert_eq!(result, expected, "path: {}", path);
        }
    }
}
