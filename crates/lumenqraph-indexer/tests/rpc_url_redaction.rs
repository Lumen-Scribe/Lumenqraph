use lumenqraph_core::redact_url;

#[test]
fn keyed_url_redaction_preserves_only_scheme_host_and_port() {
    let input = "https://user:password@rpc.example.com:443/v1/provider-key?apikey=secret";
    let output = redact_url(input);
    assert_eq!(output, "https://rpc.example.com:443");
    assert!(!output.contains("password"));
    assert!(!output.contains("provider-key"));
    assert!(!output.contains("apikey"));
    assert!(!output.contains("secret"));
}

#[test]
fn redaction_handles_short_urls_without_secrets() {
    assert_eq!(redact_url("http://localhost:8000/rpc"), "http://localhost:8000");
    assert_eq!(redact_url("https://rpc.example.com:8545?apikey=secret"), "https://rpc.example.com:8545");
}
