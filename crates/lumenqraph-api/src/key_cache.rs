//! Short-lived in-process LRU cache for API key lookups (#430).
//!
//! Every authenticated request previously queried Postgres for the key's
//! `(revoked, rate_limit_per_min)` row. At 60 req/s that is 60 DB round-trips
//! per second on the hottest path — and random-key floods hit the DB on every
//! attempt because negative results were also not cached.
//!
//! This module provides a `KeyCache` that:
//!
//! - Stores positive entries (valid, not-revoked keys) for the configured TTL.
//! - Stores negative entries (revoked **or** unknown keys) for the negative TTL.
//! - Keeps the two kinds in **separate** LRUs (#649). Negative entries can be
//!   created by anyone who sends a made-up key, so they live in a smaller
//!   bounded map of their own. A flood of random keys can churn that map but
//!   can never evict a valid key, which would otherwise send every legitimate
//!   request back to Postgres.
//! - Is bounded to `capacity` valid entries (and `capacity / 4` negative ones);
//!   the least-recently-used entry is evicted when a map is full.
//! - Is safe to share across request-handling tasks (`Arc<KeyCache>`).
//!
//! **Revocation latency**: a revoked key will still be accepted for up to
//! TTL after the `api_keys` row is updated. This is intentional and is
//! documented in the operator guide. Use Postgres `LISTEN/NOTIFY` on the
//! `api_keys` table for near-instant invalidation (out of scope here).

use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use lru::LruCache;
use parking_lot::Mutex;

/// `last_used_at` is written at most once per this many seconds per key.
const LAST_USED_TOUCH_SECS: u64 = 60;

/// Negative entries get `capacity / NEGATIVE_CAPACITY_DIVISOR` slots (at least 1).
/// Kept well below the valid-key capacity so random-key churn stays confined.
const NEGATIVE_CAPACITY_DIVISOR: usize = 4;

/// Every issued API key starts with this prefix (see `scripts/gen_api_key.sh`).
pub const KEY_PREFIX: &str = "lqk_";

/// Upper bound on an accepted key's length. Longer values are rejected before
/// hashing, so they can't be used to make the request path do extra work.
const MAX_KEY_LEN: usize = 128;

/// Cheap, DB-free shape check for a presented API key (#649).
///
/// A key that does not look like one we issue (`lqk_` prefix, bounded length,
/// ASCII letters/digits/underscore only) cannot match any row, so it is
/// refused without a hash, a cache entry or a database query.
pub fn is_plausible_key(key: &str) -> bool {
    key.len() > KEY_PREFIX.len()
        && key.len() <= MAX_KEY_LEN
        && key.starts_with(KEY_PREFIX)
        && key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

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
    fn new(value: CachedKey, ttl: Duration) -> Self {
        Self {
            value,
            expires_at: Instant::now() + ttl,
            last_touch: None,
        }
    }

    fn is_fresh(&self) -> bool {
        Instant::now() < self.expires_at
    }
}

type Lru = Mutex<LruCache<String, Entry>>;

/// Thread-safe LRU key cache with separate positive and negative maps.
pub struct KeyCache {
    /// Valid keys. Only these are on the hot path, so they get the full capacity.
    valid: Lru,
    /// Revoked and unknown keys, bounded separately (see module docs, #649).
    negative: Lru,
    /// How long a valid key is served from cache without hitting Postgres.
    ttl: Duration,
    /// How long an unknown or revoked lookup is cached. Short so a legitimate
    /// key just inserted becomes usable quickly, while still protecting
    /// against random-key flood DoS on Postgres.
    neg_ttl: Duration,
}

fn lru(capacity: usize) -> Lru {
    let cap = NonZeroUsize::new(capacity.max(1)).expect("capacity > 0");
    Mutex::new(LruCache::new(cap))
}

impl KeyCache {
    /// Create a new cache. Positive entries live for `ttl`; negative entries
    /// (revoked or unknown) live for `neg_ttl`. Both come from `ApiConfig`
    /// (`API_KEY_CACHE_TTL_SECS`, `API_KEY_CACHE_NEG_TTL_SECS`), so operators
    /// can trade revocation latency against database load (#580).
    pub fn new(capacity: usize, ttl: Duration, neg_ttl: Duration) -> Self {
        Self {
            valid: lru(capacity),
            negative: lru((capacity / NEGATIVE_CAPACITY_DIVISOR).max(1)),
            ttl,
            neg_ttl,
        }
    }

    /// Look up a key hash in the cache. Returns `None` on a miss or stale entry.
    ///
    /// Valid entries are checked first, so a key that has since been added
    /// is never shadowed by an older negative entry.
    pub fn get(&self, key_hash: &str) -> Option<CachedKey> {
        Self::lookup(&self.valid, key_hash).or_else(|| Self::lookup(&self.negative, key_hash))
    }

    fn lookup(map: &Lru, key_hash: &str) -> Option<CachedKey> {
        let mut cache = map.lock();
        match cache.get(key_hash) {
            Some(entry) if entry.is_fresh() => Some(entry.value.clone()),
            Some(_) => {
                // Stale: drop it and report a miss so the caller re-fetches.
                cache.pop(key_hash);
                None
            }
            None => None,
        }
    }

    /// Insert a positive (valid) entry for `key_hash`.
    pub fn insert_valid(
        &self,
        key_hash: &str,
        rate_limit_per_min: i32,
        scopes: Vec<String>,
        expires_at: Option<DateTime<Utc>>,
    ) {
        let value = CachedKey::Valid {
            rate_limit_per_min,
            scopes,
            expires_at,
        };
        // A key that is now valid must not keep a stale negative entry.
        self.negative.lock().pop(key_hash);
        self.valid
            .lock()
            .put(key_hash.to_string(), Entry::new(value, self.ttl));
    }

    /// Returns `true` when `last_used_at` should be written for `key_hash`,
    /// at most once per `LAST_USED_TOUCH_SECS`, and records the write.
    pub fn should_touch_last_used(&self, key_hash: &str) -> bool {
        let mut cache = self.valid.lock();
        let Some(entry) = cache.get_mut(key_hash) else {
            return false;
        };
        let now = Instant::now();
        let due = entry
            .last_touch
            .is_none_or(|t| now.duration_since(t) >= Duration::from_secs(LAST_USED_TOUCH_SECS));
        if due {
            entry.last_touch = Some(now);
        }
        due
    }

    /// Insert a negative (revoked) entry for `key_hash`.
    pub fn insert_revoked(&self, key_hash: &str) {
        let entry = Entry {
            value: CachedKey::Revoked,
            expires_at: Instant::now() + self.neg_ttl,
            last_touch: None,
        };
        self.negative.lock().put(key_hash.to_string(), entry);
    }

    /// Insert a negative (not-found) entry for `key_hash`.
    pub fn insert_not_found(&self, key_hash: &str) {
        let entry = Entry {
            value: CachedKey::NotFound,
            expires_at: Instant::now() + self.neg_ttl,
            last_touch: None,
        };
        self.negative.lock().put(key_hash.to_string(), entry);
    }

    /// Explicitly evict a key from the cache (e.g. on known revocation).
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn invalidate(&self, key_hash: &str) {
        self.valid.lock().pop(key_hash);
        self.negative.lock().pop(key_hash);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TTL: Duration = Duration::from_secs(30);
    const NEG_TTL: Duration = Duration::from_secs(5);

    #[test]
    fn expired_positive_entry_is_a_miss() {
        let c = KeyCache::new(16, Duration::ZERO, NEG_TTL);
        c.insert_valid("hash5", 60, Vec::new(), None);
        assert!(c.get("hash5").is_none());
    }

    #[test]
    fn negative_entries_use_their_own_ttl() {
        let c = KeyCache::new(16, TTL, Duration::ZERO);
        c.insert_revoked("hash6");
        assert!(c.get("hash6").is_none());
        c.insert_valid("hash7", 60, Vec::new(), None);
        assert!(c.get("hash7").is_some());
    }

    #[test]
    fn cache_miss_on_empty() {
        let c = KeyCache::new(16, TTL, NEG_TTL);
        assert!(c.get("nonexistent").is_none());
    }

    #[test]
    fn valid_entry_is_returned() {
        let c = KeyCache::new(16, TTL, NEG_TTL);
        c.insert_valid("hash1", 100, Vec::new(), None);
        match c.get("hash1") {
            Some(CachedKey::Valid {
                rate_limit_per_min: 100,
                ..
            }) => {}
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn revoked_entry_is_returned() {
        let c = KeyCache::new(16, TTL, NEG_TTL);
        c.insert_revoked("hash2");
        assert!(matches!(c.get("hash2"), Some(CachedKey::Revoked)));
    }

    #[test]
    fn not_found_entry_is_returned() {
        let c = KeyCache::new(16, TTL, NEG_TTL);
        c.insert_not_found("hash3");
        assert!(matches!(c.get("hash3"), Some(CachedKey::NotFound)));
    }

    #[test]
    fn invalidate_removes_entry() {
        let c = KeyCache::new(16, TTL, NEG_TTL);
        c.insert_valid("hash4", 60, Vec::new(), None);
        c.invalidate("hash4");
        assert!(c.get("hash4").is_none());
    }

    #[test]
    fn invalidate_removes_negative_entry() {
        let c = KeyCache::new(16, TTL, NEG_TTL);
        c.insert_not_found("hash8");
        c.invalidate("hash8");
        assert!(c.get("hash8").is_none());
    }

    #[test]
    fn valid_insert_replaces_earlier_negative_entry() {
        let c = KeyCache::new(16, TTL, NEG_TTL);
        c.insert_not_found("newly-issued");
        c.insert_valid("newly-issued", 60, vec![], None);
        assert!(matches!(
            c.get("newly-issued"),
            Some(CachedKey::Valid { .. })
        ));
    }

    #[test]
    fn last_used_touch_is_throttled_per_key() {
        let c = KeyCache::new(16, TTL, NEG_TTL);
        assert!(!c.should_touch_last_used("missing"));
        c.insert_valid("t", 60, vec![], None);
        assert!(c.should_touch_last_used("t"));
        assert!(!c.should_touch_last_used("t"));
    }

    #[test]
    fn capacity_is_enforced_lru() {
        // Cap = 2: inserting a third entry should evict the LRU one.
        let c = KeyCache::new(2, TTL, NEG_TTL);
        c.insert_valid("a", 10, Vec::new(), None);
        c.insert_valid("b", 20, Vec::new(), None);
        // Touch "a" so "b" becomes LRU.
        c.get("a");
        // Insert "c" — "b" should be evicted.
        c.insert_valid("c", 30, vec![], None);
        assert!(
            c.get("a").is_some(),
            "a was recently accessed and must survive"
        );
        assert!(c.get("c").is_some(), "c was just inserted");
        assert!(
            c.get("b").is_none(),
            "b was LRU and should have been evicted"
        );
    }

    #[test]
    fn random_key_flood_does_not_evict_valid_keys() {
        // #649: 256 valid slots, so the negative map holds 64. Flood it with
        // far more distinct unknown keys than that.
        let c = KeyCache::new(256, TTL, NEG_TTL);
        c.insert_valid("valid-key", 60, vec![], None);
        for i in 0..10_000 {
            c.insert_not_found(&format!("random-{i}"));
        }
        assert!(
            matches!(c.get("valid-key"), Some(CachedKey::Valid { .. })),
            "a valid key must survive a flood of unknown keys"
        );
        // The negative map stayed bounded to its own capacity.
        let negatives = (0..10_000)
            .filter(|i| c.get(&format!("random-{i}")).is_some())
            .count();
        assert_eq!(negatives, 256 / NEGATIVE_CAPACITY_DIVISOR);
    }

    #[test]
    fn flood_of_valid_keys_is_not_limited_by_negatives() {
        // Negative churn must not shrink the valid map either.
        let c = KeyCache::new(8, TTL, NEG_TTL);
        for i in 0..8 {
            c.insert_valid(&format!("v{i}"), 60, vec![], None);
        }
        for i in 0..100 {
            c.insert_revoked(&format!("r{i}"));
        }
        for i in 0..8 {
            assert!(
                c.get(&format!("v{i}")).is_some(),
                "v{i} evicted by negatives"
            );
        }
    }

    #[test]
    fn plausible_key_shape() {
        assert!(is_plausible_key("lqk_abcDEF0123456789"));
        assert!(!is_plausible_key("lqk_"));
        assert!(!is_plausible_key("good-key"));
        assert!(!is_plausible_key("lqk_has-dash"));
        assert!(!is_plausible_key("lqk_has space"));
        assert!(!is_plausible_key(""));
        assert!(!is_plausible_key(&format!(
            "lqk_{}",
            "a".repeat(MAX_KEY_LEN)
        )));
    }
}
