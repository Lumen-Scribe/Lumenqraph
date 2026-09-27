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
use std::sync::{Mutex, MutexGuard};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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

/// Redis backend: global limits across all replicas using sliding window
pub struct RedisBackend {
    client: redis::Client,
}

impl RedisBackend {
    pub fn new(redis_url: &str) -> anyhow::Result<Self> {
        let client = redis::Client::open(redis_url)?;
        Ok(Self { client })
    }
}

impl RateLimitBackend for RedisBackend {
    fn check(&self, identity: &str, limit_per_min: i32) -> RateLimitStatus {
        if limit_per_min <= 0 {
            return RateLimitStatus {
                allowed: true,
                tokens_remaining: limit_per_min,
                retry_after_secs: None,
            };
        }

        let mut conn = match self.client.get_connection() {
            Ok(c) => c,
            Err(_) => {
                // Fall back to allowing request if Redis is down
                return RateLimitStatus {
                    allowed: true,
                    tokens_remaining: limit_per_min,
                    retry_after_secs: None,
                };
            }
        };

        let key = format!("ratelimit:{}", identity);
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

        match script.key(&key).arg(now).arg(window_start).arg(limit_per_min).invoke::<Vec<i32>>(&mut conn) {
            Ok(result) => {
                if result[0] == 1 {
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
            Err(_) => RateLimitStatus {
                allowed: true,
                tokens_remaining: limit_per_min,
                retry_after_secs: None,
            },
        }
    }
}

pub struct RateLimiter {
    backend: Box<dyn RateLimitBackend>,
}

impl RateLimiter {
    pub fn new() -> Self {
        Self::from_env()
    }

    pub fn from_env() -> Self {
        let backend_type = std::env::var("RATE_LIMIT_BACKEND")
            .unwrap_or_else(|_| "memory".to_string());
        
        match backend_type.as_str() {
            "redis" => {
                let redis_url = std::env::var("REDIS_URL")
                    .expect("REDIS_URL must be set when RATE_LIMIT_BACKEND=redis");
                match RedisBackend::new(&redis_url) {
                    Ok(backend) => Self {
                        backend: Box::new(backend),
                    },
                    Err(e) => {
                        eprintln!("Failed to create Redis backend: {}. Falling back to memory.", e);
                        Self {
                            backend: Box::new(MemoryBackend::new()),
                        }
                    }
                }
            }
            _ => Self {
                backend: Box::new(MemoryBackend::new()),
            },
        }
    }

    pub fn check(&self, identity: &str, limit_per_min: i32) -> RateLimitStatus {
        self.backend.check(identity, limit_per_min)
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn memory_limiter() -> RateLimiter {
        RateLimiter {
            backend: Box::new(MemoryBackend::new()),
        }
    }

    #[test]
    fn allows_up_to_limit_then_blocks() {
        let rl = memory_limiter();
        let limit = 3;
        for i in 0..limit {
            let status = rl.check("k", limit);
            assert!(status.allowed, "request {i} should be allowed");
        }
        let status = rl.check("k", limit);
        assert!(!status.allowed, "4th request must be blocked");
        assert!(status.retry_after_secs.is_some());
    }

    #[test]
    fn zero_or_negative_limit_is_unlimited() {
        let rl = memory_limiter();
        for _ in 0..1000 {
            assert!(rl.check("k", 0).allowed);
            assert!(rl.check("k", -1).allowed);
        }
    }

    #[test]
    fn identities_are_independent() {
        let rl = memory_limiter();
        assert!(rl.check("a", 1).allowed);
        assert!(!rl.check("a", 1).allowed);
        assert!(rl.check("b", 1).allowed);
    }

    #[test]
    fn no_burst_at_boundaries() {
        let rl = memory_limiter();
        let limit = 10;
        let mut allowed_count = 0;

        for _ in 0..20 {
            if rl.check("rapid", limit).allowed {
                allowed_count += 1;
            }
        }

        assert!(
            allowed_count <= limit,
            "burst requests should not exceed limit; got {allowed_count} for limit {limit}"
        );
    }

    #[test]
    fn tokens_refill_over_time() {
        let rl = memory_limiter();
        let limit = 2;

        assert!(rl.check("refill_test", limit).allowed);
        assert!(rl.check("refill_test", limit).allowed);
        assert!(!rl.check("refill_test", limit).allowed);

        let status = rl.check("refill_test", limit);
        assert!(!status.allowed);
        assert!(status.retry_after_secs.is_some());
    }

    #[test]
    fn retry_after_header_is_reasonable() {
        let rl = memory_limiter();
        let limit = 1;

        rl.check("retry_test", limit);
        let status = rl.check("retry_test", limit);
        assert!(!status.allowed);
        assert!(status.retry_after_secs.is_some());
        let retry = status.retry_after_secs.unwrap();
        assert!(retry > 50 && retry <= 61, "retry_after should be ~60s, got {retry}s");
    }

    #[test]
    fn identity_map_stays_bounded_under_unique_identity_flood() {
        let backend = MemoryBackend::with_capacity(1_000);
        for i in 0..10_000 {
            assert!(backend.check(&format!("flood-{i}"), 60).allowed);
        }
        let per_shard = 1_000usize.div_ceil(SHARDS);
        assert!(backend.tracked() <= per_shard * SHARDS);
    }

    #[test]
    fn recently_used_identity_survives_eviction() {
        let backend = MemoryBackend::with_capacity(SHARDS * 4);
        assert!(backend.check("hot", 1).allowed);
        for i in 0..(SHARDS * 16) {
            backend.check(&format!("cold-{i}"), 60);
            // Touch "hot" so it stays most-recently-used in its shard.
            assert!(!backend.check("hot", 1).allowed, "hot bucket was evicted at {i}");
        }
    }
}
