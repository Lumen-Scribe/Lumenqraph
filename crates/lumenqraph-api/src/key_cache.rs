//! Short-lived in-process LRU cache for API key lookups (#430).
//!
//! Every authenticated request previously queried Postgres for the key's
//! `(revoked, rate_limit_per_min)` row. At 60 req/s that is 60 DB round-trips
//! per second on the hottest path — and random-key floods hit the DB on every
//! attempt because negative results were also not cached.
//!
//! This module provides a `KeyCache` that:
//!
//! - Stores positive entries (valid, not-revoked keys) for `TTL_SECS`.
//! - Stores negative entries (revoked **or** unknown keys) for `NEG_TTL_SECS`.
//! - Is bounded to `capacity` entries; the least-recently-used entry is evicted
//!   when the cache is full.
//! - Is safe to share across request-handling tasks (`Arc<KeyCache>`).
//!
//! **Revocation latency**: a revoked key will still be accepted for up to
//! `TTL_SECS` after the `api_keys` row is updated. This is intentional and is
//! documented in the operator guide. Use Postgres `LISTEN/NOTIFY` on the
//! `api_keys` table for near-instant invalidation (out of scope here).

use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use lru::LruCache;
use parking_lot::Mutex;
use std::num::NonZeroUsize;

/// Positive-entry TTL: how long a valid key is served from cache without
/// hitting Postgres. Default 30 s — operators can reduce this if they need
/// faster revocation.
pub const TTL_SECS: u64 = 30;

/// Negative-entry TTL: how long an unknown or revoked key lookup is cached.
/// Short so that a legitimate key just inserted becomes usable quickly, while
/// still protecting against random-key flood DoS on Postgres.
pub const NEG_TTL_SECS: u64 = 5;

/// Minimum interval between `last_used_at` writes for one key (#559).
pub const LAST_USED_TOUCH_SECS: u64 = 60;

/// The value stored for a cache hit.
#[derive(Clone, Debug)]
pub enum CachedKey {
    /// Key exists, not revoked: its per-minute rate limit, granted scopes and
    /// optional expiry (#559).
    Valid {
        rate_limit_per_min: i32,
        scopes: Vec<String>,
        expires_at: Option<DateTime<Utc>>,
    },
    /// Key is revoked.
    Revoked,
    /// Key is not found in `api_keys`.
    NotFound,
}

#[derive(Clone, Debug)]
struct Entry {
    value: CachedKey,
    expires_at: Instant,
    /// When `last_used_at` was last written for this key (#559).
    last_touch: Option<Instant>,
}

impl Entry {
    fn is_fresh(&self) -> bool {
        Instant::now() < self.expires_at
    }
}

/// Thread-safe LRU key cache.
pub struct KeyCache {
    inner: Mutex<LruCache<String, Entry>>,
}

impl KeyCache {
    /// Create a new cache with the given capacity.
    pub fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity.max(1)).expect("capacity > 0");
        Self {
            inner: Mutex::new(LruCache::new(cap)),
        }
    }

    /// Look up a key hash in the cache. Returns `None` on a miss or stale entry.
    pub fn get(&self, key_hash: &str) -> Option<CachedKey> {
        let mut cache = self.inner.lock();
        if let Some(entry) = cache.get(key_hash) {
            if entry.is_fresh() {
                return Some(entry.value.clone());
            }
            // Stale — remove and signal a miss so the caller re-fetches.
            cache.pop(key_hash);
        }
        None
    }

    /// Insert a positive (valid) entry for `key_hash`.
    pub fn insert_valid(
        &self,
        key_hash: &str,
        rate_limit_per_min: i32,
        scopes: Vec<String>,
        expires_at: Option<DateTime<Utc>>,
    ) {
        let entry = Entry {
            value: CachedKey::Valid {
                rate_limit_per_min,
                scopes,
                expires_at,
            },
            expires_at: Instant::now() + Duration::from_secs(TTL_SECS),
            last_touch: None,
        };
        self.inner.lock().put(key_hash.to_string(), entry);
    }

    /// Returns `true` when `last_used_at` should be written for `key_hash`,
    /// at most once per `LAST_USED_TOUCH_SECS`, and records the write.
    pub fn should_touch_last_used(&self, key_hash: &str) -> bool {
        let mut cache = self.inner.lock();
        let Some(entry) = cache.get_mut(key_hash) else {
            return false;
        };
        let now = Instant::now();
        let due = entry
            .last_touch
            .map_or(true, |t| now.duration_since(t) >= Duration::from_secs(LAST_USED_TOUCH_SECS));
        if due {
            entry.last_touch = Some(now);
        }
        due
    }

    /// Insert a negative (revoked) entry for `key_hash`.
    pub fn insert_revoked(&self, key_hash: &str) {
        let entry = Entry {
            value: CachedKey::Revoked,
            expires_at: Instant::now() + Duration::from_secs(NEG_TTL_SECS),
            last_touch: None,
        };
        self.inner.lock().put(key_hash.to_string(), entry);
    }

    /// Insert a negative (not-found) entry for `key_hash`.
    pub fn insert_not_found(&self, key_hash: &str) {
        let entry = Entry {
            value: CachedKey::NotFound,
            expires_at: Instant::now() + Duration::from_secs(NEG_TTL_SECS),
            last_touch: None,
        };
        self.inner.lock().put(key_hash.to_string(), entry);
    }

    /// Explicitly evict a key from the cache (e.g. on known revocation).
    pub fn invalidate(&self, key_hash: &str) {
        self.inner.lock().pop(key_hash);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_miss_on_empty() {
        let c = KeyCache::new(16);
        assert!(c.get("nonexistent").is_none());
    }

    #[test]
    fn valid_entry_is_returned() {
        let c = KeyCache::new(16);
        c.insert_valid("hash1", 100, vec!["read".into()], None);
        match c.get("hash1") {
            Some(CachedKey::Valid { rate_limit_per_min: 100, .. }) => {}
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn revoked_entry_is_returned() {
        let c = KeyCache::new(16);
        c.insert_revoked("hash2");
        assert!(matches!(c.get("hash2"), Some(CachedKey::Revoked)));
    }

    #[test]
    fn not_found_entry_is_returned() {
        let c = KeyCache::new(16);
        c.insert_not_found("hash3");
        assert!(matches!(c.get("hash3"), Some(CachedKey::NotFound)));
    }

    #[test]
    fn invalidate_removes_entry() {
        let c = KeyCache::new(16);
        c.insert_valid("hash4", 60, vec![], None);
        c.invalidate("hash4");
        assert!(c.get("hash4").is_none());
    }

    #[test]
    fn last_used_touch_is_throttled_per_key() {
        let c = KeyCache::new(16);
        assert!(!c.should_touch_last_used("missing"));
        c.insert_valid("t", 60, vec![], None);
        assert!(c.should_touch_last_used("t"));
        assert!(!c.should_touch_last_used("t"));
    }

    #[test]
    fn capacity_is_enforced_lru() {
        // Cap = 2: inserting a third entry should evict the LRU one.
        let c = KeyCache::new(2);
        c.insert_valid("a", 10, vec![], None);
        c.insert_valid("b", 20, vec![], None);
        // Touch "a" so "b" becomes LRU.
        c.get("a");
        // Insert "c" — "b" should be evicted.
        c.insert_valid("c", 30, vec![], None);
        assert!(c.get("a").is_some(), "a was recently accessed and must survive");
        assert!(c.get("c").is_some(), "c was just inserted");
        // b was LRU and should be gone.
        assert!(
            c.get("b").is_none(),
            "b was LRU and should have been evicted"
        );
    }
}
