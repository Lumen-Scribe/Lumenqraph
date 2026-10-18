use std::time::Duration;

use anyhow::anyhow;
use sqlx::postgres::PgPoolOptions;
use tracing::{info, warn};

/// Common Postgres pool settings shared by every Lumenqraph service.
#[derive(Debug, Clone)]
pub struct DbConfig {
    pub max_connections: u32,
    pub min_connections: u32,
    pub acquire_timeout_secs: u64,
    pub idle_timeout_secs: u64,
    pub connect_retries: u32,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self { max_connections: 10, min_connections: 0, acquire_timeout_secs: 30, idle_timeout_secs: 600, connect_retries: 30 }
    }
}

/// Connect with bounded exponential backoff so startup tolerates a database
/// becoming ready after the service process (common in compose deployments).
pub async fn connect_with_retry(database_url: &str, config: &DbConfig) -> anyhow::Result<sqlx::PgPool> {
    let mut attempt = 0;
    let mut retry_delay = Duration::from_secs(1);
    let max_delay = Duration::from_secs(30);
    loop {
        match PgPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .acquire_timeout(Duration::from_secs(config.acquire_timeout_secs))
            .idle_timeout(Duration::from_secs(config.idle_timeout_secs))
            .connect(database_url)
            .await
        {
            Ok(pool) => {
                if attempt > 0 { info!(attempt, "successfully connected to Postgres after retries"); }
                return Ok(pool);
            }
            Err(error) if attempt < config.connect_retries => {
                attempt += 1;
                warn!(%error, attempt, max_retries = config.connect_retries, retry_delay_secs = retry_delay.as_secs(), "failed to connect to Postgres, retrying");
                tokio::time::sleep(retry_delay).await;
                retry_delay = (retry_delay * 2).min(max_delay);
            }
            Err(error) => return Err(anyhow!("failed to connect to Postgres after {} retries: {error}", config.connect_retries)),
        }
    }
}
