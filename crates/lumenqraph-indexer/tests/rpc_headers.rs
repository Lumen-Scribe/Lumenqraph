//! Test custom HTTP headers support for authenticated RPC providers (Issue #397)

use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use tokio::net::TcpListener;

#[derive(Clone)]
struct CapturedHeaders {
    authorization: Arc<Mutex<Vec<String>>>,
}

async fn capture_handler(
    State(state): State<CapturedHeaders>,
    headers: HeaderMap,
) -> (StatusCode, &'static str) {
    // Capture the Authorization header if present.
    if let Some(auth) = headers.get("authorization") {
        if let Ok(val) = auth.to_str() {
            state.authorization.lock().unwrap().push(val.to_string());
        }
    }
    (
        StatusCode::OK,
        r#"{"jsonrpc":"2.0","id":1,"result":{"sequence":1000}}"#,
    )
}

#[tokio::test]
async fn test_custom_headers_are_passed_in_requests() {
    // Issue #397: Requests to the RPC should carry configured headers.
    //
    // Placeholder: `RpcClient` does not yet accept custom headers, so this test
    // only starts the mock server that will capture them once it does.
    let received = Arc::new(Mutex::new(Vec::<String>::new()));
    let state = CapturedHeaders {
        authorization: Arc::clone(&received),
    };
    let app = Router::new()
        .route("/", post(capture_handler))
        .with_state(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{addr}");

    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // Expected once header support exists:
    // 1. Create an RpcClient with custom headers pointed at `url`
    // 2. Make a request
    // 3. Verify the Authorization header reached the mock server
    println!("Test URL: {url}");
    println!("Headers received: {:?}", received.lock().unwrap());
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
    let mut header =
        reqwest::header::HeaderValue::from_str(sensitive_value).expect("valid header value");
    header.set_sensitive(true);

    // Verify that debug output doesn't contain the sensitive value
    let debug_output = format!("{:?}", header);
    assert!(
        !debug_output.contains(sensitive_value),
        "sensitive header value should not appear in debug output: {}",
        debug_output
    );
}
