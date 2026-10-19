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

pub struct MetricsCollector {
    pub histogram_buckets: Arc<RwLock<HashMap<String, Vec<u64>>>>,
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

    pub async fn middleware(&self, req: Request, next: Next) -> Response {
        let method = req.method().clone();
        let uri = req.uri().path().to_string();
        let start = Instant::now();

        let response = next.run(req).await;
        let elapsed_ms = start.elapsed().as_millis() as u64;
        let status = response.status();

        let route_label = extract_route_template(&uri, &method);
        let counter_key = format!(
            "http_requests{{route=\"{}\",method=\"{}\",status=\"{}\"}}",
            route_label,
            method,
            status.as_u16()
        );
        let histogram_key = format!(
            "http_request_duration_ms{{route=\"{}\",method=\"{}\"}}",
            route_label, method
        );

        {
            let mut counters = self.status_counters.write();
            *counters.entry(counter_key).or_insert(0) += 1;
        }

        {
            let mut histograms = self.histogram_buckets.write();
            histograms
                .entry(histogram_key)
                .or_insert_with(Vec::new)
                .push(elapsed_ms);
        }

        response
    }
}

fn extract_route_template(path: &str, _method: &Method) -> String {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    let mut template = String::new();
    for (index, segment) in segments.iter().enumerate() {
        template.push('/');
        // Contract StrKeys are neither numeric nor UUID-shaped. Treat the
        // identifier position in known dynamic routes as a placeholder so a
        // caller-provided address cannot create unbounded metric labels.
        let follows_resource = index > 0
            && matches!(
                segments[index - 1],
                "contracts" | "events" | "webhooks" | "transactions"
            );
        if follows_resource || is_uuid_like(segment) || is_numeric(segment) {
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
