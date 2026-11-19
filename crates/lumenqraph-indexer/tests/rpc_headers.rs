//! Test custom HTTP headers support for authenticated RPC providers (Issue #397)

// NOTE: a skeleton `test_custom_headers_are_passed_in_requests` used to live
// here. It asserted nothing and did not compile, because RpcClient cannot yet
// take custom headers. Re-add a real test together with that feature (#397).

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
