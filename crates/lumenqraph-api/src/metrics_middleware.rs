//! HTTP request metrics middleware. Records per-route latency histograms and
//! status-code counters, keyed by matched route template and method to keep
//! cardinality bounded.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use axum::extract::{MatchedPath, Request};
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

/// Route label for requests that matched no registered route. All of them
/// share this one value, so probes of random paths cannot create series.
pub const UNMATCHED_ROUTE: &str = "unmatched";

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

    /// Time the inner handler and record the result under the matched route
    /// template (for example `/contracts/:contract_id/events`).
    pub async fn middleware(self: Arc<Self>, req: Request, next: Next) -> Response {
        let method = req.method().clone();
        let route = route_label(&req);
        let start = Instant::now();

        let response = next.run(req).await;
        let elapsed_ms = start.elapsed().as_millis() as u64;

        self.record(&route, &method, response.status(), elapsed_ms);
        response
    }

    /// Record one finished request. Memory grows only with the number of
    /// distinct label sets, never with the number of requests.
    pub fn record(&self, route: &str, method: &Method, status: StatusCode, elapsed_ms: u64) {
        let counter_key = format!(
            "http_requests{{route=\"{}\",method=\"{}\",status=\"{}\"}}",
            escape_label_value(route),
            escape_label_value(method.as_str()),
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

/// The matched route template for a request, or [`UNMATCHED_ROUTE`].
///
/// axum records `MatchedPath` on the request before the route's layers run,
/// so this is the registered template rather than the raw path.
fn route_label(req: &Request) -> String {
    req.extensions()
        .get::<MatchedPath>()
        .map(|matched| matched.as_str().to_string())
        .unwrap_or_else(|| UNMATCHED_ROUTE.to_string())
}

/// Escape a label value for the Prometheus text exposition format: backslash,
/// double quote and line feed must be written as `\\`, `\"` and `\n`.
pub fn escape_label_value(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for c in raw.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use axum::middleware;
    use axum::routing::get;
    use axum::Router;
    use tower::ServiceExt;

    fn app(collector: Arc<MetricsCollector>) -> Router {
        Router::new()
            .route("/contracts/:contract_id/events", get(|| async { "ok" }))
            .route("/health", get(|| async { "ok" }))
            .layer(middleware::from_fn(move |req: Request, next: Next| {
                collector.clone().middleware(req, next)
            }))
    }

    async fn hit(router: &Router, path: &str) {
        router
            .clone()
            .oneshot(HttpRequest::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
    }

    fn routes_seen(collector: &MetricsCollector) -> Vec<String> {
        let mut routes: Vec<String> = collector
            .histogram_buckets
            .read()
            .keys()
            .map(|k| k.route.clone())
            .collect();
        routes.sort();
        routes.dedup();
        routes
    }

    #[tokio::test]
    async fn route_label_is_the_registered_template() {
        let collector = Arc::new(MetricsCollector::new());
        let router = app(collector.clone());

        hit(&router, "/contracts/C1234/events").await;
        hit(&router, "/contracts/C9999/events").await;

        assert_eq!(routes_seen(&collector), vec!["/contracts/:contract_id/events"]);
    }

    #[tokio::test]
    async fn unmatched_paths_share_one_label() {
        let collector = Arc::new(MetricsCollector::new());
        let router = app(collector.clone());

        for i in 0..200 {
            hit(&router, &format!("/probe/{i}/wp-admin.php")).await;
        }

        assert_eq!(routes_seen(&collector), vec![UNMATCHED_ROUTE]);
    }

    #[tokio::test]
    async fn distinct_route_labels_are_bounded_by_registered_routes_plus_one() {
        let collector = Arc::new(MetricsCollector::new());
        let router = app(collector.clone());

        for i in 0..300 {
            hit(&router, &format!("/contracts/C{i}/events")).await;
            hit(&router, &format!("/health?x={i}")).await;
            hit(&router, &format!("/random/{i}")).await;
        }

        // Two registered routes plus the unmatched label.
        assert!(routes_seen(&collector).len() <= 3);
    }

    #[test]
    fn escape_label_value_escapes_exposition_metacharacters() {
        assert_eq!(escape_label_value("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
        assert_eq!(escape_label_value("/contracts/:id"), "/contracts/:id");
    }

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
}
