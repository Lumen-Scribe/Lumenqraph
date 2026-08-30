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
            .finish()
    }
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            database_url: std::env::var("DATABASE_URL").context("missing DATABASE_URL")?,
            tick_secs: parse("WEBHOOK_TICK_SECS", 3),
            batch_size: parse("WEBHOOK_BATCH_SIZE", 100),
            max_attempts: parse("WEBHOOK_MAX_ATTEMPTS", 6),
            connect_timeout_secs: parse("WEBHOOK_CONNECT_TIMEOUT_SECS", 5),
            total_timeout_secs: parse("WEBHOOK_TOTAL_TIMEOUT_SECS", 10),
            max_concurrent_per_host: parse("WEBHOOK_MAX_CONCURRENT_PER_HOST", 5),
            max_concurrent_deliveries: parse("WEBHOOK_MAX_CONCURRENT_DELIVERIES", 100),
            failure_threshold: parse("WEBHOOK_FAILURE_THRESHOLD", 10),
        })
    }

    pub fn connect_timeout(&self) -> Duration {
        Duration::from_secs(self.connect_timeout_secs)
    }

    pub fn total_timeout(&self) -> Duration {
        Duration::from_secs(self.total_timeout_secs)
    }
}

fn parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
