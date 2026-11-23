use std::time::Duration;

use anyhow::anyhow;
use sqlx::postgres::PgPoolOptions;
use sqlx::Executor;
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
        Self {
            max_connections: 10,
            min_connections: 0,
            acquire_timeout_secs: 30,
            idle_timeout_secs: 600,
            connect_retries: 30,
        }
    }
}

/// Connect with bounded exponential backoff so startup tolerates a database
/// becoming ready after the service process (common in compose deployments).
pub async fn connect_with_retry(
    database_url: &str,
    config: &DbConfig,
) -> anyhow::Result<sqlx::PgPool> {
    let mut attempt = 0;
    let mut retry_delay = Duration::from_secs(1);
    let max_delay = Duration::from_secs(30);
    loop {
        match PgPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .acquire_timeout(Duration::from_secs(config.acquire_timeout_secs))
            .idle_timeout(Duration::from_secs(config.idle_timeout_secs))
            // #648: pin every session to UTC so `timestamp`/`timestamptz`
            // comparisons never depend on the server's default TimeZone.
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    conn.execute("SET TIME ZONE 'UTC'").await?;
                    Ok(())
                })
            })
            .connect(database_url)
            .await
        {
            Ok(pool) => {
                if attempt > 0 {
                    info!(attempt, "successfully connected to Postgres after retries");
                }
                return Ok(pool);
            }
            Err(error) if attempt < config.connect_retries => {
                attempt += 1;
                warn!(%error, attempt, max_retries = config.connect_retries, retry_delay_secs = retry_delay.as_secs(), "failed to connect to Postgres, retrying");
                tokio::time::sleep(retry_delay).await;
                retry_delay = (retry_delay * 2).min(max_delay);
            }
            Err(error) => {
                return Err(anyhow!(
                    "failed to connect to Postgres after {} retries: {error}",
                    config.connect_retries
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    /// These need a throwaway Postgres (run with `-- --ignored`):
    ///
    ///   TEST_DATABASE_URL=postgres://…/lumenqraph cargo test -p lumenqraph-core -- --ignored

    fn test_url() -> String {
        std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL")
    }

    /// #648: every pooled connection runs in UTC, whatever the server default.
    #[tokio::test]
    #[ignore]
    async fn pooled_sessions_run_in_utc() {
        let pool = connect_with_retry(&test_url(), &DbConfig::default())
            .await
            .unwrap();
        let tz: String = sqlx::query_scalar("SHOW TIME ZONE")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(tz, "UTC");
    }

    /// #648: a `timestamptz` bound filter (the form `list_events` and `/stats`
    /// now use) selects the same rows regardless of the session time zone.
    /// The old `::timestamp` cast would drop the zone and shift the window.
    #[tokio::test]
    #[ignore]
    async fn timestamptz_filter_is_session_timezone_independent() {
        let pool = connect_with_retry(&test_url(), &DbConfig::default())
            .await
            .unwrap();
        let mut conn = pool.acquire().await.unwrap();
        sqlx::query("CREATE TEMP TABLE tz_probe (ts TIMESTAMPTZ NOT NULL)")
            .execute(&mut *conn)
            .await
            .unwrap();
        for ts in [
            "2026-03-01 00:00:00+00",
            "2026-03-01 12:00:00+00",
            "2026-03-02 00:00:00+00",
        ] {
            sqlx::query("INSERT INTO tz_probe (ts) VALUES ($1::timestamptz)")
                .bind(ts)
                .execute(&mut *conn)
                .await
                .unwrap();
        }
        let since = Utc.with_ymd_and_hms(2026, 3, 1, 10, 0, 0).unwrap();

        // A TEMP table lives on one connection, so every zone is queried on
        // that same connection. `SET TIME ZONE` is session-local, so each
        // iteration deliberately moves away from the UTC pooled default.
        for tz in ["UTC", "America/New_York", "Asia/Tokyo"] {
            sqlx::query(&format!("SET TIME ZONE '{tz}'"))
                .execute(&mut *conn)
                .await
                .unwrap();
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM tz_probe WHERE ($1::timestamptz IS NULL OR ts >= $1)",
            )
            .bind(since)
            .fetch_one(&mut *conn)
            .await
            .unwrap();
            assert_eq!(count, 2, "time zone {tz} changed the rows selected");
        }
    }
}
