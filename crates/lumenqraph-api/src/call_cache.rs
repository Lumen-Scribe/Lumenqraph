//! In-process read-through cache for `POST /contracts/:id/call` (view-function)
//! results. Identical (contract, function, args) calls within the TTL window
//! return the cached value without hitting Soroban RPC.
//!
//! Only successful `/call` results are cached — `/simulate` is intentionally
//! excluded because callers use it to preview state-changing effects where
//! staleness would be misleading.
//!
//! Configuration (env vars read in `main.rs`):
//!   `CALL_CACHE_TTL_SECS`     — TTL per entry (default 5; 0 disables caching).
//!   `CALL_CACHE_MAX_ENTRIES`  — LRU capacity (default 1000).

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use lru::LruCache;
use serde_json::Value;

#[derive(Hash, PartialEq, Eq, Clone)]
struct Key {
    contract_id: String,
    function: String,
    /// Canonical JSON representation of the call arguments, used as the cache
    /// key. serde_json preserves insertion order so identical request bodies
    /// produce identical strings.
    args: String,
}

pub struct CallCache {
    inner: Mutex<LruCache<Key, (Value, Instant)>>,
    ttl: Duration,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

impl CallCache {
    pub fn new(max_entries: usize, ttl_secs: u64) -> Self {
        let capacity = NonZeroUsize::new(max_entries.max(1)).unwrap();
        Self {
            inner: Mutex::new(LruCache::new(capacity)),
            ttl: Duration::from_secs(ttl_secs),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    /// Return a cached result if one exists and has not expired.
    pub fn get(&self, contract_id: &str, function: &str, args: &Value) -> Option<Value> {
        if self.ttl.is_zero() {
            return None;
        }
        let key = self.make_key(contract_id, function, args);
        let mut cache = self.inner.lock().unwrap();
        match cache.get(&key) {
            Some((value, inserted_at)) if inserted_at.elapsed() < self.ttl => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(value.clone())
            }
            Some(_) => {
                // Expired: evict now so the LRU capacity reflects live entries.
                cache.pop(&key);
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Store a successful call result. No-op when the cache is disabled (TTL == 0).
    pub fn insert(&self, contract_id: &str, function: &str, args: &Value, result: Value) {
        if self.ttl.is_zero() {
            return;
        }
        let key = self.make_key(contract_id, function, args);
        let evicted = self.inner
            .lock()
            .unwrap()
            .put(key, (result, Instant::now()));
        if evicted.is_some() {
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get cache hit count (for Prometheus metrics).
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Get cache miss count (for Prometheus metrics).
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Get cache eviction count (for Prometheus metrics).
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    /// Get current cache size (number of entries, for Prometheus metrics).
    pub fn size(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    fn make_key(&self, contract_id: &str, function: &str, args: &Value) -> Key {
        Key {
            contract_id: contract_id.to_string(),
            function: function.to_string(),
            args: normalize_json(args),
        }
    }
}

/// Recursively sort JSON object keys to produce a canonical representation
/// for cache key generation, ensuring {"a":1,"b":2} and {"b":2,"a":1} produce
/// identical keys.
fn normalize_json(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut sorted: Vec<_> = map.iter().collect();
            sorted.sort_by_key(|(k, _)| *k);
            let normalized_pairs: Vec<String> = sorted
                .into_iter()
                .map(|(k, v)| format!("\"{}\":{}", k, normalize_json(v)))
                .collect();
            format!("{{{}}}", normalized_pairs.join(","))
        }
        Value::Array(arr) => {
            let normalized_items: Vec<String> = arr.iter().map(normalize_json).collect();
            format!("[{}]", normalized_items.join(","))
        }
        _ => serde_json::to_string(value).unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn hit_within_ttl() {
        let cache = CallCache::new(10, 60);
        cache.insert("C1", "balance", &json!({"addr": "G1"}), json!(100));
        let result = cache.get("C1", "balance", &json!({"addr": "G1"}));
        assert_eq!(result, Some(json!(100)));
    }

    #[test]
    fn miss_on_different_args() {
        let cache = CallCache::new(10, 60);
        cache.insert("C1", "balance", &json!({"addr": "G1"}), json!(100));
        assert!(cache.get("C1", "balance", &json!({"addr": "G2"})).is_none());
    }

    #[test]
    fn disabled_when_ttl_zero() {
        let cache = CallCache::new(10, 0);
        cache.insert("C1", "fn", &json!({}), json!("result"));
        assert!(cache.get("C1", "fn", &json!({})).is_none());
    }

    #[test]
    fn lru_evicts_oldest_on_capacity() {
        let cache = CallCache::new(2, 60);
        cache.insert("C1", "a", &json!({}), json!(1));
        cache.insert("C1", "b", &json!({}), json!(2));
        cache.insert("C1", "c", &json!({}), json!(3)); // evicts "a"
        assert!(cache.get("C1", "a", &json!({})).is_none());
        assert!(cache.get("C1", "b", &json!({})).is_some());
        assert!(cache.get("C1", "c", &json!({})).is_some());
    }

    #[test]
    fn identical_args_with_different_key_ordering_produce_cache_hit() {
        let cache = CallCache::new(10, 60);
        // Insert with one key order
        cache.insert("C1", "transfer", &json!({"from": "G1", "to": "G2"}), json!(true));
        // Retrieve with different key order - should hit cache
        let result = cache.get("C1", "transfer", &json!({"to": "G2", "from": "G1"}));
        assert_eq!(result, Some(json!(true)), "cache should hit for args with different key order");
    }

    #[test]
    fn nested_objects_are_normalized() {
        let cache = CallCache::new(10, 60);
        let args1 = json!({"outer": {"b": 2, "a": 1}, "x": 10});
        let args2 = json!({"x": 10, "outer": {"a": 1, "b": 2}});
        cache.insert("C1", "fn", &args1, json!("result"));
        assert_eq!(cache.get("C1", "fn", &args2), Some(json!("result")));
    }
}
