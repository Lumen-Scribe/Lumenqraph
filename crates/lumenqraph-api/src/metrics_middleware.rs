//! HTTP request metrics middleware. Records per-route latency histograms and
//! status-code counters, keyed by matched route template and method to keep
//! cardinality bounded.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use axum::extract::{MatchedPath, Request};
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use parking_lot::RwLock;

/// Upper bounds, in seconds, of the latency histogram buckets. Samples above
/// the last bound are counted only in `+Inf`.
pub const LATENCY_BUCKETS_SECONDS: [f64; 14] = [
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Point-in-time copy of an [`AtomicLatencyHistogram`], used for rendering.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct LatencyHistogram {
    /// Non-cumulative count per bucket; `buckets[i]` holds samples in
    /// `(LATENCY_BUCKETS_SECONDS[i-1], LATENCY_BUCKETS_SECONDS[i]]`.
    pub buckets: [u64; LATENCY_BUCKETS_SECONDS.len()],
    /// Total number of samples, including those above the last bound.
    pub count: u64,
    /// Sum of all samples in seconds.
    pub sum_seconds: f64,
}

impl LatencyHistogram {
    /// Cumulative count for each bucket bound, as Prometheus `le` buckets expect.
    pub fn cumulative(&self) -> impl Iterator<Item = (f64, u64)> + '_ {
        LATENCY_BUCKETS_SECONDS
            .iter()
            .zip(self.buckets.iter())
            .scan(0u64, |running, (&bound, &n)| {
                *running += n;
                Some((bound, *running))
            })
    }
}

/// Fixed-size latency histogram updated with atomic adds, so recording never
/// takes a lock. Its memory does not depend on how many samples it has seen,
/// so it is safe to keep for the life of the process.
#[derive(Debug, Default)]
pub struct AtomicLatencyHistogram {
    buckets: [AtomicU64; LATENCY_BUCKETS_SECONDS.len()],
    count: AtomicU64,
    /// Sum of all samples in nanoseconds.
    sum_nanos: AtomicU64,
}

impl AtomicLatencyHistogram {
    pub fn observe(&self, elapsed_seconds: f64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_nanos
            .fetch_add((elapsed_seconds * 1e9) as u64, Ordering::Relaxed);
        if let Some(i) = LATENCY_BUCKETS_SECONDS
            .iter()
            .position(|&bound| elapsed_seconds <= bound)
        {
            self.buckets[i].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn snapshot(&self) -> LatencyHistogram {
        LatencyHistogram {
            buckets: std::array::from_fn(|i| self.buckets[i].load(Ordering::Relaxed)),
            count: self.count.load(Ordering::Relaxed),
            sum_seconds: self.sum_nanos.load(Ordering::Relaxed) as f64 / 1e9,
        }
    }
}

/// Route label for requests that matched no registered route. All of them
/// share this one value, so probes of random paths cannot create series.
pub const UNMATCHED_ROUTE: &str = "unmatched";

/// Method label for any method outside the standard set. All extension
/// methods share this one value, so arbitrary method tokens cannot create
/// series.
pub const OTHER_METHOD: &str = "OTHER";

/// Latency histogram and status-code counters for one `(route, method)` label
/// set.
#[derive(Debug, Default)]
pub struct SeriesMetrics {
    pub latency: AtomicLatencyHistogram,
    pub status_counts: RwLock<HashMap<u16, AtomicU64>>,
}

impl SeriesMetrics {
    fn record(&self, status: StatusCode, elapsed_seconds: f64) {
        self.latency.observe(elapsed_seconds);
        let code = status.as_u16();
        if let Some(counter) = self.status_counts.read().get(&code) {
            counter.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.status_counts
            .write()
            .entry(code)
            .or_default()
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Series keyed by route, then method, so the request path can look one up
/// with borrowed labels instead of building a key.
pub type SeriesMap = HashMap<String, HashMap<String, SeriesMetrics>>;

pub struct MetricsCollector {
    pub series: Arc<RwLock<SeriesMap>>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            series: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Time the inner handler and record the result under the matched route
    /// template (for example `/contracts/:contract_id/events`).
    pub async fn middleware(self: Arc<Self>, req: Request, next: Next) -> Response {
        let method = req.method().clone();
        let matched = matched_path(&req);
        let start = Instant::now();

        let response = next.run(req).await;
        let elapsed_seconds = start.elapsed().as_secs_f64();

        let route = matched
            .as_ref()
            .map_or(UNMATCHED_ROUTE, MatchedPath::as_str);
        self.record(route, &method, response.status(), elapsed_seconds);
        response
    }

    /// Record one finished request. Memory grows only with the number of
    /// distinct label sets, never with the number of requests.
    ///
    /// Once a series exists this takes only shared locks and atomic adds, so
    /// requests and `/metrics` rendering do not block each other.
    pub fn record(&self, route: &str, method: &Method, status: StatusCode, elapsed_seconds: f64) {
        let method = method_label(method);
        if let Some(series) = self
            .series
            .read()
            .get(route)
            .and_then(|by_method| by_method.get(method))
        {
            series.record(status, elapsed_seconds);
            return;
        }
        self.series
            .write()
            .entry(route.to_string())
            .or_default()
            .entry(method.to_string())
            .or_default()
            .record(status, elapsed_seconds);
    }
}

/// The matched route template for a request, if any.
///
/// axum records `MatchedPath` on the request before the route's layers run,
/// so this is the registered template rather than the raw path. Cloning it
/// only bumps a reference count.
fn matched_path(req: &Request) -> Option<MatchedPath> {
    req.extensions().get::<MatchedPath>().cloned()
}

/// The method label for a request: the method name for the standard methods,
/// or [`OTHER_METHOD`].
fn method_label(method: &Method) -> &str {
    match method.as_str() {
        m @ ("GET" | "HEAD" | "POST" | "PUT" | "PATCH" | "DELETE" | "OPTIONS") => m,
        _ => OTHER_METHOD,
    }
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
            .oneshot(
                HttpRequest::builder()
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
    }

    fn routes_seen(collector: &MetricsCollector) -> Vec<String> {
        let mut routes: Vec<String> = collector.series.read().keys().cloned().collect();
        routes.sort();
        routes.dedup();
        routes
    }

    fn methods_seen(collector: &MetricsCollector) -> Vec<String> {
        let mut methods: Vec<String> = collector
            .series
            .read()
            .values()
            .flat_map(|by_method| by_method.keys().cloned())
            .collect();
        methods.sort();
        methods.dedup();
        methods
    }

    #[tokio::test]
    async fn route_label_is_the_registered_template() {
        let collector = Arc::new(MetricsCollector::new());
        let router = app(collector.clone());

        hit(&router, "/contracts/C1234/events").await;
        hit(&router, "/contracts/C9999/events").await;

        assert_eq!(
            routes_seen(&collector),
            vec!["/contracts/:contract_id/events"]
        );
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

    #[tokio::test]
    async fn extension_methods_share_one_label() {
        let collector = Arc::new(MetricsCollector::new());
        let router = app(collector.clone());

        for i in 0..1_000 {
            router
                .clone()
                .oneshot(
                    HttpRequest::builder()
                        .method(format!("FOOBAR{i}").as_str())
                        .uri("/health")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        assert_eq!(methods_seen(&collector), vec![OTHER_METHOD]);
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
            collector.record(
                routes[(i % 3) as usize],
                &Method::GET,
                StatusCode::OK,
                (i % 1_500) as f64 / 1_000.0,
            );
        }

        let series = collector.series.read();
        let histograms: Vec<LatencyHistogram> = series
            .values()
            .flat_map(|by_method| by_method.values())
            .map(|s| s.latency.snapshot())
            .collect();
        assert_eq!(histograms.len(), routes.len());
        let total: u64 = histograms.iter().map(|h| h.count).sum();
        assert_eq!(total, 1_000_000);
        // Each series is a fixed-size struct: no per-sample storage exists.
        assert_eq!(
            std::mem::size_of::<AtomicLatencyHistogram>(),
            (LATENCY_BUCKETS_SECONDS.len() + 2) * std::mem::size_of::<u64>()
        );
    }

    #[test]
    fn histogram_buckets_are_cumulative_and_include_overflow() {
        let h = AtomicLatencyHistogram::default();
        for seconds in [0.0, 0.003, 0.007, 20.0] {
            h.observe(seconds);
        }
        let h = h.snapshot();
        let cumulative: Vec<(f64, u64)> = h.cumulative().collect();
        assert_eq!(cumulative[0], (0.0005, 1));
        assert_eq!(cumulative[3], (0.005, 2));
        assert_eq!(cumulative[4], (0.01, 3));
        assert_eq!(*cumulative.last().unwrap(), (10.0, 3));
        assert_eq!(h.count, 4);
        assert!((h.sum_seconds - 20.01).abs() < 1e-9);
    }

    #[test]
    fn sub_millisecond_request_lands_below_one_millisecond() {
        let h = AtomicLatencyHistogram::default();
        h.observe(0.000_4);
        let cumulative: Vec<(f64, u64)> = h.snapshot().cumulative().collect();
        assert_eq!(cumulative[0], (0.0005, 1));
    }
}
