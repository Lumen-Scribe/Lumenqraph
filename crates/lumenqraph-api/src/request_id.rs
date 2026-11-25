//! Request ID / correlation ID middleware.
//! Generates or accepts X-Request-Id headers and attaches them to tracing spans.

use axum::extract::Request;
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::Response;
use std::sync::Arc;
use uuid::Uuid;

const MAX_REQUEST_ID_LENGTH: usize = 128;

#[derive(Clone, Debug)]
pub struct RequestId(pub Arc<String>);

fn is_valid_request_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= MAX_REQUEST_ID_LENGTH
        && id.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

/// Generate or extract request ID, attach to tracing span, and echo in response.
pub async fn request_id_middleware(mut req: Request, next: Next) -> Response {
    let request_id = req
        .headers()
        .get("x-request-id")
        .and_then(|h| h.to_str().ok())
        .filter(|id| is_valid_request_id(id))
        .map(|s| s.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    tracing::Span::current().record("request_id", &request_id);

    let request_id = RequestId(Arc::new(request_id.clone()));
    req.extensions_mut().insert(request_id.clone());

    let mut response = next.run(req).await;

    if let Ok(header_value) = HeaderValue::from_str(&request_id.0) {
        response.headers_mut().insert("x-request-id", header_value);
    }

    response
}
