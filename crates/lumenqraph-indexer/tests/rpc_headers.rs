//! Test custom HTTP headers support for authenticated RPC providers (Issue #397)

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use axum::extract::State;
use axum::routing::post;
use axum::Router;
use tokio::net::TcpListener;

#[tokio::test]
async fn test_custom_headers_are_passed_in_requests() {
    // Issue #397: Requests to the RPC should carry configured headers.

    // Spawn a mock server that captures request headers
    let headers_received = Arc::new(std::sync::Mutex::new(Vec::new()));
    let headers_received_clone = headers_received.clone();

    #[derive(Clone)]
    struct State {
        headers: Arc<std::sync::Mutex<Vec<String>>>,
    }

    async fn handler(
        State(state): State<State>,
        axum::http::HeaderMap: axum::http::HeaderMap,
    ) -> (u16, &'static str) {
        // Capture Authorization header if present
        if let Some(auth) = axum::http::HeaderMap.get("authorization") {
            if let Ok(val) = auth.to_str() {
                state.headers.lock().unwrap().push(val.to_string());
            }
        }
        (200, r#"{"jsonrpc":"2.0","id":1,"result":{"sequence":1000}}"#)
    }

    let state = State { headers: headers_received_clone };
    let app = Router::new().route("/", post(handler)).with_state(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{addr}");

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap()
    });

    // This would require RpcClient to support custom headers in its constructor.
    // The test structure demonstrates what the test should verify:
    // 1. Create an RpcClient with custom headers
    // 2. Make a request
    // 3. Verify the headers were sent

    // For now, this test serves as a skeleton for implementation.
    println!("Test URL: {}", url);
    println!("Headers received: {:?}", headers_received.lock().unwrap());
}

#[test]
fn test_header_values_are_marked_sensitive() {
    // Issue #397: Header values should never appear in logs/debug output
    // This requires HeaderValue::set_sensitive(true) to be called

    // Test structure:
    // 1. Create a reqwest::header::HeaderValue with a sensitive token
    // 2. Call set_sensitive(true)
    // 3. Verify Debug output does not contain the value

    let sensitive_value = "Bearer secret-token-12345";
    let mut header = reqwest::header::HeaderValue::from_str(sensitive_value)
        .expect("valid header value");
    header.set_sensitive(true);

    // Verify that debug output doesn't contain the sensitive value
    let debug_output = format!("{:?}", header);
    assert!(
        !debug_output.contains(sensitive_value),
        "sensitive header value should not appear in debug output: {}",
        debug_output
    );
}
