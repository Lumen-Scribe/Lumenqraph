//! Token-bucket rate limiter to prevent burst attacks at window boundaries.
//! Keyed by identity (API key or "anon").
//!
//! Token-bucket algorithm:
//! - Tokens refill at a constant rate (limit/60 tokens per second).
//! - Each request costs 1 token.
//! - Maximum tokens = limit (prevents accumulation).
//! - Tracks (tokens, last_refill_time) per identity.
//! - Prevents the 2x burst that fixed-window allows at boundaries.
//!
//! Supports two backends:
//! - In-memory (default): per-instance limits, fine for single-replica deploys
//! - Redis: global limits enforced across all replicas

use std::hash::BuildHasher;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use lru::LruCache;

/// Upper bound on identities tracked by the in-memory backend. When full, the
/// least-recently-seen identity is evicted in O(1). An evicted identity just
/// starts again with a full bucket, which is the state it would have refilled
/// to anyway once it had been idle for a minute.
const MAX_TRACKED_IDENTITIES: usize = 100_000;

/// Number of independently locked shards. Requests for different identities
/// contend only when they hash to the same shard.
const SHARDS: usize = 64;

#[derive(Debug, Clone)]
pub struct RateLimitStatus {
    pub allowed: bool,
    pub tokens_remaining: i32,
    pub retry_after_secs: Option<u64>,
}

#[derive(Debug, Clone)]
struct TokenBucketState {
    tokens: f64,
    /// Monotonic timestamp of the last refill. `Instant` never goes backwards,
    /// so NTP steps or VM clock jumps cannot drain or instantly refill a bucket.
    last_refill: Instant,
}

/// Trait abstracting rate limit storage backends
pub trait RateLimitBackend: Send + Sync {
    fn check(&self, identity: &str, limit_per_min: i32) -> RateLimitStatus;
}

/// In-memory backend: per-instance limits.
///
/// Buckets live in [`SHARDS`] mutex-protected LRU maps selected by a randomly
/// seeded hash of the identity, so there is no global lock and a flood of
/// unique identities costs O(1) per request (one LRU eviction) instead of a
/// full sweep of the map.
pub struct MemoryBackend {
    shards: Box<[Mutex<LruCache<String, TokenBucketState>>]>,
    hasher: std::collections::hash_map::RandomState,
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::with_capacity(MAX_TRACKED_IDENTITIES)
    }
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }

    /// Backend tracking at most roughly `capacity` identities in total.
    pub fn with_capacity(capacity: usize) -> Self {
        let per_shard = NonZeroUsize::new(capacity.div_ceil(SHARDS).max(1)).expect("non-zero");
        let shards = (0..SHARDS)
            .map(|_| Mutex::new(LruCache::new(per_shard)))
            .collect();
        Self {
            shards,
            hasher: Default::default(),
        }
    }

    fn shard(&self, identity: &str) -> MutexGuard<'_, LruCache<String, TokenBucketState>> {
        let idx = (self.hasher.hash_one(identity) as usize) % self.shards.len();
        // A panic while holding the lock cannot leave a bucket half-updated in a
        // way that matters, so recover from poisoning instead of failing forever.
        self.shards[idx]
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Number of identities currently tracked (all shards).
    #[cfg(test)]
    fn tracked(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.lock().unwrap_or_else(|p| p.into_inner()).len())
            .sum()
    }
}

impl RateLimitBackend for MemoryBackend {
    fn check(&self, identity: &str, limit_per_min: i32) -> RateLimitStatus {
        if limit_per_min <= 0 {
            return RateLimitStatus {
                allowed: true,
                tokens_remaining: limit_per_min,
                retry_after_secs: None,
            };
        }

        let now = Instant::now();
        let tokens_per_sec = limit_per_min as f64 / 60.0;
        let mut shard = self.shard(identity);

        // `get_mut` promotes the entry to most-recently-used; `push` of a new
        // identity into a full shard evicts the least-recently-used one (O(1)).
        if !shard.contains(identity) {
            shard.push(
                identity.to_string(),
                TokenBucketState {
                    tokens: limit_per_min as f64,
                    last_refill: now,
                },
            );
        }
        let bucket = shard.get_mut(identity).expect("present: just checked or inserted");

        // Refill tokens based on elapsed monotonic time (never negative).
        let elapsed = now.saturating_duration_since(bucket.last_refill).as_secs_f64();
        let refilled = elapsed * tokens_per_sec;
        bucket.tokens = (bucket.tokens + refilled).min(limit_per_min as f64);
        bucket.last_refill = now;

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            let tokens_remaining = bucket.tokens.floor() as i32;
            RateLimitStatus {
                allowed: true,
                tokens_remaining,
                retry_after_secs: None,
            }
        } else {
            let tokens_needed = 1.0 - bucket.tokens;
            let secs_until_token = tokens_needed / tokens_per_sec;
            let retry_after = (secs_until_token.ceil()) as u64;
            RateLimitStatus {
                allowed: false,
                tokens_remaining: 0,
                retry_after_secs: Some(retry_after.max(1)),
            }
        }
    }
}

/// Which storage backend the limiter uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKind {
    Memory,
    Redis,
}

/// What a Redis failure means for a request. `Open` allows the request
/// (availability over strictness); `Closed` rejects it (protection over
/// availability).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailMode {
    Open,
    Closed,
}

/// Validated rate-limit configuration, parsed once at startup by `ApiConfig`.
/// Deliberately not `Debug`: the Redis URL may carry credentials.
#[derive(Clone)]
pub struct RateLimitSettings {
    pub backend: BackendKind,
    pub redis_url: Option<String>,
    pub fail_mode: FailMode,
}

impl RateLimitSettings {
    /// Validate the raw `RATE_LIMIT_BACKEND`, `REDIS_URL` and
    /// `RATE_LIMIT_FAIL_MODE` values. An unknown backend or a missing URL is a
    /// startup error, so a typo such as `reddis` cannot silently select the
    /// in-memory backend.
    pub fn parse(backend: &str, redis_url: Option<&str>, fail_mode: &str) -> anyhow::Result<Self> {
        let backend = match backend.trim().to_ascii_lowercase().as_str() {
            "memory" => BackendKind::Memory,
            "redis" => BackendKind::Redis,
            other => anyhow::bail!("invalid RATE_LIMIT_BACKEND={other:?}: expected memory or redis"),
        };
        let redis_url = redis_url
            .map(str::trim)
            .filter(|url| !url.is_empty())
            .map(str::to_string);
        if backend == BackendKind::Redis && redis_url.is_none() {
            anyhow::bail!("REDIS_URL must be set when RATE_LIMIT_BACKEND=redis");
        }
        let fail_mode = match fail_mode.trim().to_ascii_lowercase().as_str() {
            "open" => FailMode::Open,
            "closed" => FailMode::Closed,
            other => anyhow::bail!("invalid RATE_LIMIT_FAIL_MODE={other:?}: expected open or closed"),
        };
        Ok(Self {
            backend,
            redis_url,
            fail_mode,
        })
    }
}

impl Default for RateLimitSettings {
    fn default() -> Self {
        Self {
            backend: BackendKind::Memory,
            redis_url: None,
            fail_mode: FailMode::Open,
        }
    }
}

/// Redis backend errors since startup, exported as
/// `lumenqraph_rate_limit_backend_errors_total`.
static BACKEND_ERRORS: AtomicU64 = AtomicU64::new(0);
/// Unix seconds of the last logged backend error. Logs are throttled to one per
/// `WARN_INTERVAL_SECS`, so an outage cannot flood the log.
static LAST_BACKEND_WARN: AtomicU64 = AtomicU64::new(0);
const WARN_INTERVAL_SECS: u64 = 10;

/// Total Redis backend errors observed since startup.
pub fn backend_error_count() -> u64 {
    BACKEND_ERRORS.load(Ordering::Relaxed)
}

fn record_backend_error(err: &redis::RedisError, fail_mode: FailMode) {
    BACKEND_ERRORS.fetch_add(1, Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let last = LAST_BACKEND_WARN.load(Ordering::Relaxed);
    if now.saturating_sub(last) >= WARN_INTERVAL_SECS
        && LAST_BACKEND_WARN
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        tracing::warn!(error = %err, ?fail_mode, "rate limit Redis backend error; applying fail mode");
    }
}

/// Redis key for one identity under one limiter. Each limiter has its own
/// namespace, so budgets on different routes never share a bucket (#555).
fn redis_key(namespace: &str, identity: &str) -> String {
    format!("ratelimit:{namespace}:{identity}")
}

/// Redis backend: global limits across all replicas using a sliding window.
///
/// Holds one multiplexed `ConnectionManager`, created once at startup, which
/// reconnects on its own. Requests reuse it and never open a connection on the
/// async runtime.
pub struct RedisBackend {
    conn: redis::aio::ConnectionManager,
    namespace: String,
    fail_mode: FailMode,
}

impl RedisBackend {
    pub async fn connect(redis_url: &str, namespace: &str, fail_mode: FailMode) -> anyhow::Result<Self> {
        let client = redis::Client::open(redis_url)?;
        let conn = redis::aio::ConnectionManager::new(client).await?;
        Ok(Self {
            conn,
            namespace: namespace.to_string(),
            fail_mode,
        })
    }

    async fn check(&self, identity: &str, limit_per_min: i32) -> RateLimitStatus {
        if limit_per_min <= 0 {
            return RateLimitStatus {
                allowed: true,
                tokens_remaining: limit_per_min,
                retry_after_secs: None,
            };
        }

        let key = redis_key(&self.namespace, identity);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let window_start = now - 60;

        // Sliding window using sorted set: add current timestamp, remove old entries, count
        let script = redis::Script::new(
            r"
            local key = KEYS[1]
            local now = tonumber(ARGV[1])
            local window_start = tonumber(ARGV[2])
            local limit = tonumber(ARGV[3])
            
            redis.call('ZREMRANGEBYSCORE', key, '-inf', window_start)
            local count = redis.call('ZCARD', key)
            
            if count < limit then
                redis.call('ZADD', key, now, now .. ':' .. math.random())
                redis.call('EXPIRE', key, 60)
                return {1, limit - count - 1}
            else
                local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
                local retry_after = 61 - (now - tonumber(oldest[2]))
                return {0, 0, retry_after}
            end
            ",
        );

        let mut conn = self.conn.clone();
        match script
            .key(&key)
            .arg(now)
            .arg(window_start)
            .arg(limit_per_min)
            .invoke_async::<_, Vec<i32>>(&mut conn)
            .await
        {
            Ok(result) => {
                if result.first() == Some(&1) {
                    RateLimitStatus {
                        allowed: true,
                        tokens_remaining: result.get(1).copied().unwrap_or(0),
                        retry_after_secs: None,
                    }
                } else {
                    RateLimitStatus {
                        allowed: false,
                        tokens_remaining: 0,
                        retry_after_secs: Some(result.get(2).copied().unwrap_or(60) as u64),
                    }
                }
            }
            Err(e) => {
                record_backend_error(&e, self.fail_mode);
                failure_status(self.fail_mode, limit_per_min)
            }
        }
    }
}

/// Outcome of a request whose Redis check failed, per the configured fail mode.
fn failure_status(fail_mode: FailMode, limit_per_min: i32) -> RateLimitStatus {
    match fail_mode {
        FailMode::Open => RateLimitStatus {
            allowed: true,
            tokens_remaining: limit_per_min,
            retry_after_secs: None,
        },
        FailMode::Closed => RateLimitStatus {
            allowed: false,
            tokens_remaining: 0,
            retry_after_secs: Some(1),
        },
    }
}

enum Backend {
    Memory(MemoryBackend),
    Redis(RedisBackend),
}

pub struct RateLimiter {
    backend: Backend,
}

impl RateLimiter {
    /// Per-instance limiter using the in-memory backend.
    pub fn memory() -> Self {
        Self {
            backend: Backend::Memory(MemoryBackend::new()),
        }
    }

    /// Build the limiter described by `settings`. `namespace` prefixes this
    /// limiter's Redis keys so its budgets stay separate from the others (#555).
    /// A Redis connection failure here fails startup instead of being discovered
    /// on the first request.
    pub async fn build(settings: &RateLimitSettings, namespace: &str) -> anyhow::Result<Self> {
        let backend = match settings.backend {
            BackendKind::Memory => Backend::Memory(MemoryBackend::new()),
            BackendKind::Redis => {
                let url = settings
                    .redis_url
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("REDIS_URL must be set when RATE_LIMIT_BACKEND=redis"))?;
                let redis = RedisBackend::connect(url, namespace, settings.fail_mode)
                    .await
                    .context("connecting to Redis for rate limiting")?;
                Backend::Redis(redis)
            }
        };
        Ok(Self { backend })
    }

    pub async fn check(&self, identity: &str, limit_per_min: i32) -> RateLimitStatus {
        match &self.backend {
            Backend::Memory(memory) => memory.check(identity, limit_per_min),
            Backend::Redis(redis) => redis.check(identity, limit_per_min).await,
        }
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::memory()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn memory_limiter() -> RateLimiter {
        RateLimiter::memory()
    }

    #[tokio::test]
    async fn allows_up_to_limit_then_blocks() {
        let rl = memory_limiter();
        let limit = 3;
        for i in 0..limit {
            let status = rl.check("k", limit).await;
            assert!(status.allowed, "request {i} should be allowed");
        }
        let status = rl.check("k", limit).await;
        assert!(!status.allowed, "4th request must be blocked");
        assert!(status.retry_after_secs.is_some());
    }

    #[tokio::test]
    async fn zero_or_negative_limit_is_unlimited() {
        let rl = memory_limiter();
        for _ in 0..1000 {
            assert!(rl.check("k", 0).await.allowed);
            assert!(rl.check("k", -1).await.allowed);
        }
    }

    #[tokio::test]
    async fn identities_are_independent() {
        let rl = memory_limiter();
        assert!(rl.check("a", 1).await.allowed);
        assert!(!rl.check("a", 1).await.allowed);
        assert!(rl.check("b", 1).await.allowed);
    }

    #[tokio::test]
    async fn separate_limiters_do_not_share_buckets() {
        let general = memory_limiter();
        let rpc = memory_limiter();
        assert!(general.check("anon:1", 1).await.allowed);
        assert!(!general.check("anon:1", 1).await.allowed);
        assert!(rpc.check("anon:1", 1).await.allowed, "rpc budget must not be spent by general traffic");
    }

    #[tokio::test]
    async fn redis_keys_are_namespaced_per_limiter() {
        assert_eq!(redis_key("rpc", "anon:1"), "ratelimit:rpc:anon:1");
        assert_ne!(redis_key("rpc", "anon:1"), redis_key("main", "anon:1"));
        assert_ne!(redis_key("webhook", "key:abc"), redis_key("proxy", "key:abc"));
    }

    #[test]
    fn settings_reject_unknown_backend() {
        assert!(RateLimitSettings::parse("reddis", None, "open").is_err());
    }

    #[test]
    fn settings_require_url_for_redis() {
        assert!(RateLimitSettings::parse("redis", None, "open").is_err());
        assert!(RateLimitSettings::parse("redis", Some("  "), "open").is_err());
        assert!(RateLimitSettings::parse("redis", Some("redis://127.0.0.1:6379"), "open").is_ok());
    }

    #[test]
    fn settings_parse_backend_and_fail_mode() {
        let memory = RateLimitSettings::parse(" Memory ", None, "open").unwrap();
        assert_eq!(memory.backend, BackendKind::Memory);
        assert_eq!(memory.fail_mode, FailMode::Open);
        let closed = RateLimitSettings::parse("redis", Some("redis://h"), "CLOSED").unwrap();
        assert_eq!(closed.fail_mode, FailMode::Closed);
        assert!(RateLimitSettings::parse("memory", None, "maybe").is_err());
    }

    #[test]
    fn failure_status_follows_fail_mode() {
        let open = failure_status(FailMode::Open, 60);
        assert!(open.allowed);
        assert_eq!(open.tokens_remaining, 60);
        let closed = failure_status(FailMode::Closed, 60);
        assert!(!closed.allowed);
        assert_eq!(closed.retry_after_secs, Some(1));
    }

    #[tokio::test]
    async fn build_fails_fast_when_redis_is_unreachable() {
        let settings = RateLimitSettings::parse("redis", Some("redis://127.0.0.1:1"), "open").unwrap();
        assert!(RateLimiter::build(&settings, "test").await.is_err());
    }

    #[tokio::test]
    async fn no_burst_at_boundaries() {
        let rl = memory_limiter();
        let limit = 10;
        let mut allowed_count = 0;

        for _ in 0..20 {
            if rl.check("rapid", limit).await.allowed {
                allowed_count += 1;
            }
        }

        assert!(
            allowed_count <= limit,
            "burst requests should not exceed limit; got {allowed_count} for limit {limit}"
        );
    }

    #[tokio::test]
    async fn tokens_refill_over_time() {
        let rl = memory_limiter();
        let limit = 2;

        assert!(rl.check("refill_test", limit).await.allowed);
        assert!(rl.check("refill_test", limit).await.allowed);
        assert!(!rl.check("refill_test", limit).await.allowed);

        let status = rl.check("refill_test", limit).await;
        assert!(!status.allowed);
        assert!(status.retry_after_secs.is_some());
    }

    #[tokio::test]
    async fn retry_after_header_is_reasonable() {
        let rl = memory_limiter();
        let limit = 1;

        rl.check("retry_test", limit).await;
        let status = rl.check("retry_test", limit).await;
        assert!(!status.allowed);
        assert!(status.retry_after_secs.is_some());
        let retry = status.retry_after_secs.unwrap();
        assert!(retry > 50 && retry <= 61, "retry_after should be ~60s, got {retry}s");
    }

    #[tokio::test]
    async fn identity_map_stays_bounded_under_unique_identity_flood() {
        let backend = MemoryBackend::with_capacity(1_000);
        for i in 0..10_000 {
            assert!(backend.check(&format!("flood-{i}"), 60).allowed);
        }
        let per_shard = 1_000usize.div_ceil(SHARDS);
        assert!(backend.tracked() <= per_shard * SHARDS);
    }

    #[tokio::test]
    async fn recently_used_identity_survives_eviction() {
        let backend = MemoryBackend::with_capacity(SHARDS * 4);
        assert!(backend.check("hot", 1).allowed);
        for i in 0..(SHARDS * 16) {
            backend.check(&format!("cold-{i}"), 60);
            // Touch "hot" so it stays most-recently-used in its shard.
            assert!(!backend.check("hot", 1).allowed, "hot bucket was evicted at {i}");
        }
    }
}
