//! Webhook-service configuration.

use anyhow::Context;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub tick_secs: u64,
    pub batch_size: i64,
    pub max_attempts: i32,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            database_url: std::env::var("DATABASE_URL").context("missing DATABASE_URL")?,
            tick_secs: parse("WEBHOOK_TICK_SECS", 3),
            batch_size: parse("WEBHOOK_BATCH_SIZE", 100),
            max_attempts: parse("WEBHOOK_MAX_ATTEMPTS", 6),
        })
    }
}

fn parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
