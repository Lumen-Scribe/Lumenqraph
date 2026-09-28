//! Webhook-service configuration.

use anyhow::Context;
use std::time::Duration;

pub fn redact_database_url(url: &str) -> String {
    if let Some(scheme_end) = url.find("://") {
        let after_scheme = &url[scheme_end + 3..];
        if let Some(at_idx) = after_scheme.find('@') {
            let user_pass = &after_scheme[..at_idx];
            if let Some(colon_idx) = user_pass.find(':') {
                let user = &user_pass[..colon_idx];
                let rest = &after_scheme[at_idx..];
                return format!("{}://{}:[REDACTED]{}", &url[..scheme_end], user, rest);
            }
        }
    }
    url.to_string()
}

#[derive(Clone)]
pub struct Config {
    pub database_url: String,
    pub tick_secs: u64,
    pub batch_size: i64,
    pub max_attempts: i32,
    pub connect_timeout_secs: u64,
    pub total_timeout_secs: u64,
    pub max_concurrent_per_host: usize,
    pub max_concurrent_deliveries: usize,
    pub failure_threshold: i32,
    pub metrics_bind_addr: String,
    pub db_max_connections: u32,
    pub db_min_connections: u32,
    pub db_acquire_timeout_secs: u64,
    pub db_idle_timeout_secs: u64,
    pub db_connect_retries: u32,
    /// The `pgp_sym_encrypt` / `pgp_sym_decrypt` key used for the webhook
    /// shared secrets. Read once at startup and never falls back to a default,
    /// so a missing key is a hard startup failure rather than a silent
    /// security regression.
    pub encryption_key: String,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("database_url", &redact_database_url(&self.database_url))
            .field("tick_secs", &self.tick_secs)
            .field("batch_size", &self.batch_size)
            .field("max_attempts", &self.max_attempts)
            .field("connect_timeout_secs", &self.connect_timeout_secs)
            .field("total_timeout_secs", &self.total_timeout_secs)
            .field("max_concurrent_per_host", &self.max_concurrent_per_host)
            .field("max_concurrent_deliveries", &self.max_concurrent_deliveries)
            .field("failure_threshold", &self.failure_threshold)
            .field("metrics_bind_addr", &self.metrics_bind_addr)
            .field("db_max_connections", &self.db_max_connections)
            .field("db_min_connections", &self.db_min_connections)
            .field("db_acquire_timeout_secs", &self.db_acquire_timeout_secs)
            .field("db_idle_timeout_secs", &self.db_idle_timeout_secs)
            .field("db_connect_retries", &self.db_connect_retries)
            .field("encryption_key", &"[REDACTED]")
            .finish()
    }
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let encryption_key = std::env::var("WEBHOOK_ENCRYPTION_KEY")
            .map_err(|_| anyhow::anyhow!(
                "WEBHOOK_ENCRYPTION_KEY must be set \
                 (generate with: openssl rand -hex 32). \
                 The default test key provides no security and must not be \
                 used in production."
            ))?;
        if encryption_key.trim().is_empty() {
            anyhow::bail!(
                "WEBHOOK_ENCRYPTION_KEY is set but empty; \
                 generate a key with: openssl rand -hex 32"
            );
        }

        Ok(Self {
            database_url: std::env::var("DATABASE_URL").context("missing DATABASE_URL")?,
            tick_secs: parse("WEBHOOK_TICK_SECS", 3)?,
            batch_size: parse("WEBHOOK_BATCH_SIZE", 100)?,
            max_attempts: parse("WEBHOOK_MAX_ATTEMPTS", 6)?,
            connect_timeout_secs: parse("WEBHOOK_CONNECT_TIMEOUT_SECS", 5)?,
            total_timeout_secs: parse("WEBHOOK_TOTAL_TIMEOUT_SECS", 10)?,
            max_concurrent_per_host: parse("WEBHOOK_MAX_CONCURRENT_PER_HOST", 5)?,
            max_concurrent_deliveries: parse("WEBHOOK_MAX_CONCURRENT_DELIVERIES", 100)?,
            failure_threshold: parse("WEBHOOK_FAILURE_THRESHOLD", 10)?,
            metrics_bind_addr: std::env::var("WEBHOOKS_METRICS_BIND_ADDR")
                .ok()
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(|| "127.0.0.1:9091".to_string()),
            db_max_connections: parse("DATABASE_MAX_CONNECTIONS", 5)?,
            db_min_connections: parse("DATABASE_MIN_CONNECTIONS", 1)?,
            db_acquire_timeout_secs: parse("DATABASE_ACQUIRE_TIMEOUT_SECS", 30)?,
            db_idle_timeout_secs: parse("DATABASE_IDLE_TIMEOUT_SECS", 600)?,
            db_connect_retries: parse("DATABASE_CONNECT_RETRIES", 30)?,
            encryption_key,
        })
    }

    pub fn connect_timeout(&self) -> Duration {
        Duration::from_secs(self.connect_timeout_secs)
    }

    pub fn total_timeout(&self) -> Duration {
        Duration::from_secs(self.total_timeout_secs)
    }
}

/// Parse `key`, or return `default` when unset/empty. A present but
/// unparseable value is a startup error naming the variable (#441) — a typo
/// like `WEBHOOK_MAX_ATTEMPTS=6x` must not silently become the default.
fn parse<T: std::str::FromStr>(key: &str, default: T) -> anyhow::Result<T>
where
    T::Err: std::fmt::Display,
{
    match std::env::var(key) {
        Ok(v) if !v.trim().is_empty() => v
            .trim()
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid {key}={v:?}: {e}")),
        _ => Ok(default),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_env_fails_fast_without_encryption_key() {
        // Make sure DATABASE_URL is present (required) but WEBHOOK_ENCRYPTION_KEY
        // is absent — Config::from_env() must return an error.
        // We use a controlled sub-environment by temporarily unsetting the var.
        // This is a best-effort unit test; the real guard is the integration.
        let result = {
            // Temporarily remove the key from this process's env if it's set.
            let original = std::env::var("WEBHOOK_ENCRYPTION_KEY").ok();
            unsafe { std::env::remove_var("WEBHOOK_ENCRYPTION_KEY"); }
            let r = Config::from_env();
            // Restore.
            if let Some(val) = original {
                unsafe { std::env::set_var("WEBHOOK_ENCRYPTION_KEY", val); }
            }
            r
        };
        // The error must mention WEBHOOK_ENCRYPTION_KEY.
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("WEBHOOK_ENCRYPTION_KEY"),
            "error should mention the missing var: {err}"
        );
    }

    #[test]
    fn parse_rejects_garbage_and_names_the_variable() {
        let key = "LUMENQRAPH_WEBHOOKS_TEST_PARSE";
        std::env::set_var(key, "6x");
        let err = parse::<i32>(key, 6).unwrap_err().to_string();
        std::env::remove_var(key);
        assert!(err.contains(key), "error should name the variable: {err}");
    }

    #[test]
    fn parse_uses_default_when_unset() {
        let key = "LUMENQRAPH_WEBHOOKS_TEST_UNSET";
        std::env::remove_var(key);
        assert_eq!(parse::<u64>(key, 3).unwrap(), 3);
    }
}
