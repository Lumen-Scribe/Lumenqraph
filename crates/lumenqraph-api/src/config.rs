//! API-service configuration (#441).
//!
//! Every setting is read from the environment exactly once, at startup, and
//! stored in `AppState`. Invalid values are a hard startup error naming the
//! variable — `ANON_RATE_LIMIT_PER_MIN=6O` must not silently become `60`. No
//! handler or middleware calls `std::env::var` at request time.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Context;

/// Postgres pool settings.
#[derive(Debug, Clone)]
pub struct DbConfig {
    pub max_connections: u32,
    pub min_connections: u32,
    pub acquire_timeout_secs: u64,
    pub idle_timeout_secs: u64,
    pub connect_retries: u32,
}

/// GraphQL schema limits.
#[derive(Debug, Clone)]
pub struct GraphqlConfig {
    pub max_depth: usize,
    pub max_complexity: usize,
    /// Enables schema introspection and the GraphiQL IDE at `GET /graphql`.
    pub introspection_enabled: bool,
}

/// Sibling-instance proxy settings (see `routes::proxy`, #442).
#[derive(Debug, Clone)]
pub struct ProxyConfig {
    /// TCP connect timeout to a mounted upstream.
    pub connect_timeout_secs: u64,
    /// Total per-request timeout (connect + headers + body). A hung upstream
    /// yields `504` once this elapses.
    pub timeout_secs: u64,
    /// Max idle pooled connections kept per upstream host.
    pub pool_max_idle_per_host: usize,
    /// Max upstream response body size; larger responses yield `502`.
    pub max_response_bytes: usize,
    /// Per-client-IP requests/min across all mounted routes. `0` disables.
    pub rate_limit_per_min: i32,
}

impl ProxyConfig {
    pub fn connect_timeout(&self) -> Duration {
        Duration::from_secs(self.connect_timeout_secs)
    }

    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_secs)
    }
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            connect_timeout_secs: 5,
            timeout_secs: 30,
            pool_max_idle_per_host: 32,
            max_response_bytes: 10 * 1024 * 1024,
            rate_limit_per_min: 600,
        }
    }
}

#[derive(Clone)]
pub struct ApiConfig {
    pub database_url: String,
    pub db: DbConfig,
    pub bind_addr: String,
    pub rpc_url: String,
    pub rpc_timeout_secs: u64,
    pub request_timeout_secs: u64,
    pub shutdown_timeout_secs: u64,
    pub max_body_bytes: u64,
    pub cors_allowed_origins: String,
    pub explorer_dir: String,

    pub require_auth: bool,
    pub anon_rate_limit: i32,
    pub rpc_require_auth: bool,
    pub rpc_anon_rate_limit: i32,
    pub metrics_require_auth: bool,
    /// Trust `X-Forwarded-For` / `Forwarded` for the client IP. Only the
    /// right-most (last-hop) entry is used — see `auth::extract_client_ip`.
    pub trust_xff: bool,
    pub max_concurrent_per_ip: usize,

    pub call_cache_max_entries: usize,
    pub call_cache_ttl_secs: u64,
    pub read_max_request_size: usize,
    pub read_max_args_size: usize,

    pub readyz_lag_threshold: i64,
    pub readyz_max_age_secs: i64,
    pub health_max_lag_ledgers: i64,
    pub health_max_stale_secs: i64,

    pub graphql: GraphqlConfig,

    /// `pgp_sym_encrypt` key for webhook secrets. `None` falls back to the
    /// legacy insecure test key (a warning is logged at startup).
    pub webhook_encryption_key: Option<String>,
    /// Retention for delivered/failed webhook deliveries. `0` disables pruning.
    pub webhook_delivery_retention_days: i64,

    /// Sibling instances mounted under a path prefix (`INSTANCE_MOUNTS`).
    pub mounts: Vec<(String, String)>,
    pub proxy: ProxyConfig,
}

impl ApiConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        // MAX_REQUEST_BODY_BYTES is the canonical name (#212); API_MAX_BODY_BYTES
        // is kept as a fallback alias for backward compatibility.
        let max_body_bytes = if is_set("MAX_REQUEST_BODY_BYTES") {
            env_parse("MAX_REQUEST_BODY_BYTES", 65536u64)?
        } else {
            env_parse("API_MAX_BODY_BYTES", 65536u64)?
        };

        let webhook_encryption_key = std::env::var("WEBHOOK_ENCRYPTION_KEY")
            .ok()
            .filter(|k| !k.trim().is_empty());

        let proxy_defaults = ProxyConfig::default();

        Ok(Self {
            database_url: std::env::var("DATABASE_URL").context("missing DATABASE_URL")?,
            db: DbConfig {
                max_connections: env_parse("DATABASE_MAX_CONNECTIONS", 10)?,
                min_connections: env_parse("DATABASE_MIN_CONNECTIONS", 1)?,
                acquire_timeout_secs: env_parse("DATABASE_ACQUIRE_TIMEOUT_SECS", 30)?,
                idle_timeout_secs: env_parse("DATABASE_IDLE_TIMEOUT_SECS", 600)?,
                connect_retries: env_parse("DATABASE_CONNECT_RETRIES", 30)?,
            },
            bind_addr: env_string("API_BIND_ADDR", "0.0.0.0:8080"),
            rpc_url: env_string("RPC_URL", "https://soroban-testnet.stellar.org"),
            rpc_timeout_secs: env_parse("RPC_TIMEOUT_SECS", 30)?,
            request_timeout_secs: env_parse("API_REQUEST_TIMEOUT_SECS", 60)?,
            shutdown_timeout_secs: env_parse("API_SHUTDOWN_TIMEOUT_SECS", 30)?,
            max_body_bytes,
            cors_allowed_origins: env_string("CORS_ALLOWED_ORIGINS", ""),
            explorer_dir: env_string("EXPLORER_DIR", "explorer"),

            require_auth: env_bool("REQUIRE_API_KEY", false)?,
            anon_rate_limit: env_parse("ANON_RATE_LIMIT_PER_MIN", 60)?,
            rpc_require_auth: env_bool("RPC_REQUIRE_API_KEY", false)?,
            rpc_anon_rate_limit: env_parse("RPC_ROUTE_RATE_LIMIT_PER_MIN", 10)?,
            metrics_require_auth: env_bool("METRICS_REQUIRE_API_KEY", false)?,
            trust_xff: env_bool("RATE_LIMIT_TRUST_XFF", false)?,
            max_concurrent_per_ip: env_parse("MAX_CONCURRENT_PER_IP", 100)?,

            call_cache_max_entries: env_parse("CALL_CACHE_MAX_ENTRIES", 1000)?,
            call_cache_ttl_secs: env_parse("CALL_CACHE_TTL_SECS", 5)?,
            read_max_request_size: env_parse("READ_MAX_REQUEST_SIZE", 256 * 1024)?,
            read_max_args_size: env_parse("READ_MAX_ARGS_SIZE", 128 * 1024)?,

            readyz_lag_threshold: env_parse("READYZ_LAG_THRESHOLD", 100)?,
            readyz_max_age_secs: env_parse("READYZ_MAX_AGE_SECS", 120)?,
            health_max_lag_ledgers: env_parse("HEALTH_MAX_LAG_LEDGERS", 100)?,
            health_max_stale_secs: env_parse("HEALTH_MAX_STALE_SECS", 120)?,

            graphql: GraphqlConfig {
                max_depth: env_parse("GRAPHQL_MAX_DEPTH", 12)?,
                max_complexity: env_parse("GRAPHQL_MAX_COMPLEXITY", 1000)?,
                introspection_enabled: env_bool("GRAPHQL_INTROSPECTION_ENABLED", false)?,
            },

            webhook_encryption_key,
            webhook_delivery_retention_days: env_parse(
                "WEBHOOK_DELIVERY_RETENTION_DAYS",
                crate::routes::webhooks::DEFAULT_WEBHOOK_DELIVERY_RETENTION_DAYS,
            )?,

            mounts: crate::routes::proxy::parse_mounts(&env_string("INSTANCE_MOUNTS", "")),
            proxy: ProxyConfig {
                connect_timeout_secs: env_parse(
                    "PROXY_CONNECT_TIMEOUT_SECS",
                    proxy_defaults.connect_timeout_secs,
                )?,
                timeout_secs: env_parse("PROXY_TIMEOUT_SECS", proxy_defaults.timeout_secs)?,
                pool_max_idle_per_host: env_parse(
                    "PROXY_POOL_MAX_IDLE_PER_HOST",
                    proxy_defaults.pool_max_idle_per_host,
                )?,
                max_response_bytes: env_parse(
                    "PROXY_MAX_RESPONSE_BYTES",
                    proxy_defaults.max_response_bytes,
                )?,
                rate_limit_per_min: env_parse(
                    "PROXY_RATE_LIMIT_PER_MIN",
                    proxy_defaults.rate_limit_per_min,
                )?,
            },
        })
    }
}

#[cfg(test)]
impl ApiConfig {
    /// Built-in defaults with no environment lookups, for tests that need an
    /// `AppState`.
    pub fn test_default() -> Self {
        Self {
            database_url: String::new(),
            db: DbConfig {
                max_connections: 2,
                min_connections: 1,
                acquire_timeout_secs: 30,
                idle_timeout_secs: 600,
                connect_retries: 0,
            },
            bind_addr: "127.0.0.1:0".into(),
            rpc_url: "http://127.0.0.1:26657".into(),
            rpc_timeout_secs: 30,
            request_timeout_secs: 60,
            shutdown_timeout_secs: 30,
            max_body_bytes: 65536,
            cors_allowed_origins: String::new(),
            explorer_dir: "explorer".into(),
            require_auth: false,
            anon_rate_limit: 60,
            rpc_require_auth: false,
            rpc_anon_rate_limit: 10,
            metrics_require_auth: false,
            trust_xff: false,
            max_concurrent_per_ip: 100,
            call_cache_max_entries: 100,
            call_cache_ttl_secs: 5,
            read_max_request_size: 256 * 1024,
            read_max_args_size: 128 * 1024,
            readyz_lag_threshold: 100,
            readyz_max_age_secs: 120,
            health_max_lag_ledgers: 100,
            health_max_stale_secs: 120,
            graphql: GraphqlConfig {
                max_depth: 12,
                max_complexity: 1000,
                introspection_enabled: false,
            },
            webhook_encryption_key: None,
            webhook_delivery_retention_days: 14,
            mounts: Vec::new(),
            proxy: ProxyConfig::default(),
        }
    }
}

/// Redacted view for the startup "effective configuration" log line: the
/// database password, any credentials embedded in the RPC URL, and the webhook
/// encryption key never reach the logs.
impl fmt::Debug for ApiConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApiConfig")
            .field("database_url", &redact_url(&self.database_url))
            .field("db", &self.db)
            .field("bind_addr", &self.bind_addr)
            .field("rpc_url", &redact_url(&self.rpc_url))
            .field("rpc_timeout_secs", &self.rpc_timeout_secs)
            .field("request_timeout_secs", &self.request_timeout_secs)
            .field("shutdown_timeout_secs", &self.shutdown_timeout_secs)
            .field("max_body_bytes", &self.max_body_bytes)
            .field("cors_allowed_origins", &self.cors_allowed_origins)
            .field("explorer_dir", &self.explorer_dir)
            .field("require_auth", &self.require_auth)
            .field("anon_rate_limit", &self.anon_rate_limit)
            .field("rpc_require_auth", &self.rpc_require_auth)
            .field("rpc_anon_rate_limit", &self.rpc_anon_rate_limit)
            .field("metrics_require_auth", &self.metrics_require_auth)
            .field("trust_xff", &self.trust_xff)
            .field("max_concurrent_per_ip", &self.max_concurrent_per_ip)
            .field("call_cache_max_entries", &self.call_cache_max_entries)
            .field("call_cache_ttl_secs", &self.call_cache_ttl_secs)
            .field("read_max_request_size", &self.read_max_request_size)
            .field("read_max_args_size", &self.read_max_args_size)
            .field("readyz_lag_threshold", &self.readyz_lag_threshold)
            .field("readyz_max_age_secs", &self.readyz_max_age_secs)
            .field("health_max_lag_ledgers", &self.health_max_lag_ledgers)
            .field("health_max_stale_secs", &self.health_max_stale_secs)
            .field("graphql", &self.graphql)
            .field(
                "webhook_encryption_key",
                &self.webhook_encryption_key.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "webhook_delivery_retention_days",
                &self.webhook_delivery_retention_days,
            )
            .field("mounts", &self.mounts)
            .field("proxy", &self.proxy)
            .finish()
    }
}

/// Mask the password in `scheme://user:pass@host/...`.
fn redact_url(url: &str) -> String {
    if let Some(scheme_end) = url.find("://") {
        let after_scheme = &url[scheme_end + 3..];
        if let Some(at_idx) = after_scheme.find('@') {
            let user_pass = &after_scheme[..at_idx];
            let rest = &after_scheme[at_idx..];
            let user = user_pass.split(':').next().unwrap_or("");
            return format!("{}://{}:[REDACTED]{}", &url[..scheme_end], user, rest);
        }
    }
    url.to_string()
}

fn is_set(key: &str) -> bool {
    std::env::var(key).map(|v| !v.trim().is_empty()).unwrap_or(false)
}

fn env_string(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

/// Parse `key`, or return `default` when unset/empty. A present but
/// unparseable value is an error naming the variable.
fn env_parse<T: FromStr>(key: &str, default: T) -> anyhow::Result<T>
where
    T::Err: fmt::Display,
{
    match std::env::var(key) {
        Ok(v) if !v.trim().is_empty() => v
            .trim()
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid {key}={v:?}: {e}")),
        _ => Ok(default),
    }
}

/// Parse a boolean `key`, or return `default` when unset/empty. Anything other
/// than `1/true/yes/on` or `0/false/no/off` (case-insensitive) is an error.
fn env_bool(key: &str, default: bool) -> anyhow::Result<bool> {
    match std::env::var(key) {
        Ok(v) if !v.trim().is_empty() => parse_bool(&v)
            .ok_or_else(|| anyhow::anyhow!("invalid {key}={v:?}: expected true/false")),
        _ => Ok(default),
    }
}

fn parse_bool(v: &str) -> Option<bool> {
    match v.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bool_accepts_common_spellings() {
        for v in ["1", "true", "TRUE", "yes", "on", " True "] {
            assert_eq!(parse_bool(v), Some(true), "{v:?}");
        }
        for v in ["0", "false", "no", "OFF"] {
            assert_eq!(parse_bool(v), Some(false), "{v:?}");
        }
        for v in ["ture", "2", "enabled"] {
            assert_eq!(parse_bool(v), None, "{v:?}");
        }
    }

    #[test]
    fn env_parse_rejects_garbage_and_names_the_variable() {
        let key = "LUMENQRAPH_TEST_CONFIG_PARSE";
        std::env::set_var(key, "6O");
        let err = env_parse::<i32>(key, 60).unwrap_err().to_string();
        std::env::remove_var(key);
        assert!(err.contains(key), "error should name the variable: {err}");
    }

    #[test]
    fn env_parse_uses_default_when_unset() {
        let key = "LUMENQRAPH_TEST_CONFIG_UNSET";
        std::env::remove_var(key);
        assert_eq!(env_parse::<u64>(key, 42).unwrap(), 42);
    }

    #[test]
    fn debug_output_redacts_secrets() {
        assert_eq!(
            redact_url("postgres://user:hunter2@db:5432/x"),
            "postgres://user:[REDACTED]@db:5432/x"
        );
        assert_eq!(redact_url("https://rpc.example"), "https://rpc.example");
    }
}
