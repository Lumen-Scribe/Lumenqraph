use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use sqlx::postgres::PgPoolOptions;
use sqlx::Executor;
use tracing::{info, warn};

const ACQUIRE_BUCKETS: [f64; 10] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0,
];

static DB_POOL_ACQUIRE_TIMEOUTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static DB_POOL_ACQUIRE_DURATION: Mutex<AcquireHistogram> = Mutex::new(AcquireHistogram::new());

struct AcquireHistogram {
    counts: [u64; ACQUIRE_BUCKETS.len()],
    count: u64,
    sum: f64,
}

impl AcquireHistogram {
    const fn new() -> Self {
        Self {
            counts: [0; ACQUIRE_BUCKETS.len()],
            count: 0,
            sum: 0.0,
        }
    }

    fn observe(&mut self, elapsed: Duration) {
        let secs = elapsed.as_secs_f64();
        for (i, le) in ACQUIRE_BUCKETS.iter().enumerate() {
            if secs <= *le {
                self.counts[i] += 1;
            }
        }
        self.count += 1;
        self.sum += secs;
    }
}

/// Record a completed `pool.acquire()` duration.
pub fn observe_acquire_duration(elapsed: Duration) {
    let mut hist = DB_POOL_ACQUIRE_DURATION
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    hist.observe(elapsed);
}

/// Increment the pool acquire-timeout counter.
pub fn record_acquire_timeout() {
    DB_POOL_ACQUIRE_TIMEOUTS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Acquire a connection from `pool`, recording acquire latency and timeouts.
pub async fn acquire_conn(
    pool: &sqlx::PgPool,
) -> Result<sqlx::pool::PoolConnection<sqlx::Postgres>, sqlx::Error> {
    let start = Instant::now();
    let res = pool.acquire().await;
    observe_acquire_duration(start.elapsed());
    if matches!(&res, Err(sqlx::Error::PoolTimedOut)) {
        record_acquire_timeout();
    }
    res
}

/// Render `lumenqraph_db_pool_*` metrics in Prometheus text exposition format.
pub fn render_pool_metrics(body: &mut String, pool: &sqlx::PgPool) {
    let size = pool.size() as usize;
    let idle = pool.num_idle();
    let in_use = size.saturating_sub(idle);
    let max = pool.options().get_max_connections() as usize;

    body.push_str("# HELP lumenqraph_db_pool_connections Postgres connection pool connections by state\n");
    body.push_str("# TYPE lumenqraph_db_pool_connections gauge\n");
    body.push_str(&format!(
        "lumenqraph_db_pool_connections{{state=\"idle\"}} {idle}\n"
    ));
    body.push_str(&format!(
        "lumenqraph_db_pool_connections{{state=\"in_use\"}} {in_use}\n"
    ));
    body.push_str(&format!(
        "lumenqraph_db_pool_connections{{state=\"max\"}} {max}\n"
    ));

    body.push_str("# HELP lumenqraph_db_pool_acquire_timeouts_total Total Postgres pool acquire timeouts\n");
    body.push_str("# TYPE lumenqraph_db_pool_acquire_timeouts_total counter\n");
    body.push_str(&format!(
        "lumenqraph_db_pool_acquire_timeouts_total {}\n",
        DB_POOL_ACQUIRE_TIMEOUTS_TOTAL.load(Ordering::Relaxed)
    ));

    let (counts, count, sum) = {
        let hist = DB_POOL_ACQUIRE_DURATION
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        (hist.counts, hist.count, hist.sum)
    };
    body.push_str("# HELP lumenqraph_db_pool_acquire_duration_seconds Postgres pool acquire latency\n");
    body.push_str("# TYPE lumenqraph_db_pool_acquire_duration_seconds histogram\n");
    for (i, le) in ACQUIRE_BUCKETS.iter().enumerate() {
        body.push_str(&format!(
            "lumenqraph_db_pool_acquire_duration_seconds_bucket{{le=\"{le}\"}} {}\n",
            counts[i]
        ));
    }
    body.push_str(&format!(
        "lumenqraph_db_pool_acquire_duration_seconds_bucket{{le=\"+Inf\"}} {count}\n"
    ));
    body.push_str(&format!(
        "lumenqraph_db_pool_acquire_duration_seconds_sum {sum}\n"
    ));
    body.push_str(&format!(
        "lumenqraph_db_pool_acquire_duration_seconds_count {count}\n"
    ));
}

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

    #[tokio::test]
    async fn render_pool_metrics_exports_gauges_timeouts_and_histogram() {
        let pool = PgPoolOptions::new()
            .max_connections(7)
            .connect_lazy("postgres://localhost:5432/lumenqraph")
            .unwrap();
        observe_acquire_duration(Duration::from_millis(12));
        record_acquire_timeout();

        let mut body = String::new();
        render_pool_metrics(&mut body, &pool);

        assert!(body.contains("# TYPE lumenqraph_db_pool_connections gauge"));
        assert!(body.contains("lumenqraph_db_pool_connections{state=\"idle\"} 0\n"));
        assert!(body.contains("lumenqraph_db_pool_connections{state=\"in_use\"} 0\n"));
        assert!(body.contains("lumenqraph_db_pool_connections{state=\"max\"} 7\n"));
        assert!(body.contains("# TYPE lumenqraph_db_pool_acquire_timeouts_total counter"));
        assert!(body.contains("lumenqraph_db_pool_acquire_timeouts_total "));
        assert!(body.contains("# TYPE lumenqraph_db_pool_acquire_duration_seconds histogram"));
        assert!(body.contains("lumenqraph_db_pool_acquire_duration_seconds_bucket{le=\"0.025\"} "));
        assert!(body.contains("lumenqraph_db_pool_acquire_duration_seconds_bucket{le=\"+Inf\"} "));
        assert!(body.contains("lumenqraph_db_pool_acquire_duration_seconds_sum "));
        assert!(body.contains("lumenqraph_db_pool_acquire_duration_seconds_count "));
    }
}
