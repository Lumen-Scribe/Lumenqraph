use tracing_subscriber::{fmt, prelude::*, EnvFilter};

/// Redact RPC/database URL userinfo, path, and query while retaining only the
/// scheme, host, and port useful for diagnostics.
pub fn redact_url(raw: &str) -> String {
    let Ok(mut url) = url::Url::parse(raw) else { return "[INVALID_URL]".to_string(); };
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_path("");
    url.set_query(None);
    url.set_fragment(None);
    url.to_string().trim_end_matches('/').to_string()
}

/// Initialize logging consistently for all binaries. RUST_LOG wins; otherwise
/// services emit info-level logs. LOG_FORMAT=json selects structured output.
pub fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let registry = tracing_subscriber::registry().with(filter);
    if std::env::var("LOG_FORMAT").map(|v| v.eq_ignore_ascii_case("json")).unwrap_or(false) {
        registry.with(fmt::layer().json()).init();
    } else {
        registry.with(fmt::layer()).init();
    }
}

/// Initialize logging to stderr for transports, such as MCP stdio, whose
/// stdout is reserved for a machine-readable protocol.
pub fn init_tracing_stderr() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let registry = tracing_subscriber::registry().with(filter);
    if std::env::var("LOG_FORMAT").map(|v| v.eq_ignore_ascii_case("json")).unwrap_or(false) {
        registry.with(fmt::layer().json().with_writer(std::io::stderr)).init();
    } else {
        registry.with(fmt::layer().with_writer(std::io::stderr)).init();
    }
}

#[cfg(test)]
mod tests {
    use super::redact_url;
    #[test]
    fn keyed_url_redaction_removes_secret_path_and_query() {
        let input = "https://user:password@rpc.example.com:443/v1/provider-secret?apikey=key-secret&x=y";
        let output = redact_url(input);
        assert_eq!(output, "https://rpc.example.com:443");
        assert!(!output.contains("password"));
        assert!(!output.contains("provider-secret"));
        assert!(!output.contains("apikey"));
        assert!(!output.contains("key-secret"));
    }
}
