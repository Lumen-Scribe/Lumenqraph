//! Request ID / correlation ID middleware.
//! Generates or accepts X-Request-Id headers and attaches them to tracing spans.

use axum::extract::Request;
use axum::http::HeaderValue;
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::Router;
use std::sync::Arc;
use tower_http::trace::TraceLayer;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct RequestId(pub Arc<String>);

/// Build the HTTP request tracing span with `request_id` populated from the
/// outer `request_id_middleware` extension.
pub fn make_request_span<B>(req: &axum::http::Request<B>) -> tracing::Span {
    let request_id = req
        .extensions()
        .get::<RequestId>()
        .map(|id| id.0.as_str())
        .or_else(|| {
            req.headers()
                .get("x-request-id")
                .and_then(|h| h.to_str().ok())
                .filter(|id| !id.is_empty())
        })
        .unwrap_or("-");

    tracing::info_span!(
        "request",
        method = %req.method(),
        uri = %req.uri(),
        version = ?req.version(),
        request_id = %request_id,
    )
}

/// Apply `request_id_middleware` outside `TraceLayer` so the generated or
/// incoming `x-request-id` is present on the span before `TraceLayer` logs
/// the start of the request and for every log line emitted while handling it.
pub fn apply_request_tracing(router: Router) -> Router {
    router
        .layer(TraceLayer::new_for_http().make_span_with(make_request_span))
        .layer(middleware::from_fn(request_id_middleware))
}

/// Generate or extract request ID, store it in request extensions, and echo in response.
pub async fn request_id_middleware(mut req: Request, next: Next) -> Response {
    let request_id = req
        .headers()
        .get("x-request-id")
        .and_then(|h| h.to_str().ok())
        .filter(|id| !id.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    let request_id = RequestId(Arc::new(request_id));
    req.extensions_mut().insert(request_id.clone());

    let mut response = next.run(req).await;

    if let Ok(header_value) = HeaderValue::from_str(&request_id.0) {
        response.headers_mut().insert("x-request-id", header_value);
    }

    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::StatusCode;
    use axum::routing::get;
    use std::io;
    use std::sync::{Arc, Mutex};
    use tower::ServiceExt;
    use tracing_subscriber::fmt;

    #[derive(Clone, Default)]
    struct SharedBuf(Arc<Mutex<Vec<u8>>>);

    impl io::Write for SharedBuf {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> fmt::MakeWriter<'a> for SharedBuf {
        type Writer = SharedBuf;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    fn log_has_request_id(entry: &serde_json::Value, expected_id: &str) -> bool {
        if entry["span"]["request_id"] == expected_id {
            return true;
        }
        entry["spans"]
            .as_array()
            .is_some_and(|spans| spans.iter().any(|s| s["request_id"] == expected_id))
    }

    #[tokio::test]
    async fn json_logs_carry_request_id_on_every_request_log_line() {
        let buf = SharedBuf::default();
        let subscriber = fmt()
            .json()
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(buf.clone())
            .finish();
        let dispatch = tracing::Dispatch::new(subscriber);
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let app = apply_request_tracing(Router::new().route(
            "/ping",
            get(|| async {
                tracing::info!("handling ping request");
                "pong"
            }),
        ));

        let req = axum::http::Request::builder()
            .uri("/ping")
            .header("x-request-id", "req-trace-test-123")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("x-request-id").unwrap(),
            "req-trace-test-123"
        );

        let raw = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        let entries: Vec<serde_json::Value> = raw
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).expect("valid JSON log line"))
            .collect();

        assert!(
            !entries.is_empty(),
            "expected at least one JSON log line, got none"
        );
        assert!(
            entries
                .iter()
                .any(|e| e["fields"]["message"] == "handling ping request"),
            "missing handler log line in:\n{raw}"
        );
        for entry in &entries {
            assert!(
                log_has_request_id(entry, "req-trace-test-123"),
                "log line missing request_id in span: {entry}"
            );
        }
    }
}
