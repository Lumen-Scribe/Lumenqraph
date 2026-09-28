//! Per-IP connection/concurrency limiter to prevent slowloris-style attacks.
//! Tracks in-flight requests per client IP and rejects excess with 503.
//!
//! Also provides a dedicated limiter for long-lived SSE streams, which are
//! capped per key/IP and globally (`SSE_MAX_STREAMS`).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// Above this many tracked IPs we drop stale entries.
const MAX_TRACKED_IPS: usize = 100_000;

#[derive(Debug, Clone)]
pub struct ConcurrencyLimitStatus {
    pub allowed: bool,
    pub current_in_flight: usize,
    pub limit: usize,
}

#[derive(Debug)]
struct IpState {
    in_flight: usize,
    last_activity_secs: f64,
}

#[derive(Default)]
pub struct ConcurrencyLimiter {
    ips: Mutex<HashMap<String, IpState>>,
}

impl ConcurrencyLimiter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if a new request is allowed for this IP. Returns the status.
    /// max_concurrent <= 0 means unlimited.
    pub fn acquire(&self, ip: &str, max_concurrent: usize) -> ConcurrencyLimitStatus {
        if max_concurrent == 0 {
            return ConcurrencyLimitStatus {
                allowed: true,
                current_in_flight: 0,
                limit: max_concurrent,
            };
        }

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);

        let mut ips = self.ips.lock().unwrap();

        // Bound memory: prune stale entries if map is too large.
        // Stale IPs with 0 in-flight are safe to evict; 10 seconds inactivity threshold.
        if ips.len() >= MAX_TRACKED_IPS {
            let cutoff = now_secs - 10.0;
            ips.retain(|_, state| state.last_activity_secs > cutoff && state.in_flight > 0);
        }

        let state = ips.entry(ip.to_string()).or_insert_with(|| IpState {
            in_flight: 0,
            last_activity_secs: now_secs,
        });

        state.last_activity_secs = now_secs;

        if state.in_flight < max_concurrent {
            state.in_flight += 1;
            ConcurrencyLimitStatus {
                allowed: true,
                current_in_flight: state.in_flight,
                limit: max_concurrent,
            }
        } else {
            ConcurrencyLimitStatus {
                allowed: false,
                current_in_flight: state.in_flight,
                limit: max_concurrent,
            }
        }
    }

    /// Release a request for this IP when it completes.
    pub fn release(&self, ip: &str) {
        let mut ips = self.ips.lock().unwrap();
        if let Some(state) = ips.get_mut(ip) {
            if state.in_flight > 0 {
                state.in_flight -= 1;
            }
        }
    }
}

/// Status returned when acquiring an SSE stream slot.
#[derive(Debug, Clone)]
pub struct SseStreamStatus {
    pub allowed: bool,
    pub current_streams: usize,
    pub limit: usize,
}

/// Caps concurrent SSE streams per key/IP and globally.
///
/// Long-lived streams hold a slot for their whole lifetime, so they are
/// tracked separately from short-lived request concurrency. `max_per_key`
/// and `max_global` of 0 mean unlimited for that dimension.
#[derive(Default)]
pub struct SseStreamLimiter {
    per_key: Mutex<HashMap<String, usize>>,
    global: AtomicUsize,
}

impl SseStreamLimiter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Try to reserve a stream slot for `key` (e.g. contract or IP).
    /// Returns the status; on `allowed == false` the caller should respond
    /// with 429/503 and must NOT call `release`.
    pub fn acquire(&self, key: &str, max_per_key: usize, max_global: usize) -> SseStreamStatus {
        let mut per_key = self.per_key.lock().unwrap();

        let current_key = per_key.get(key).copied().unwrap_or(0);
        let current_global = self.global.load(Ordering::SeqCst);

        let key_blocked = max_per_key != 0 && current_key >= max_per_key;
        let global_blocked = max_global != 0 && current_global >= max_global;

        if key_blocked || global_blocked {
            return SseStreamStatus {
                allowed: false,
                current_streams: current_global,
                limit: max_global,
            };
        }

        per_key.insert(key.to_string(), current_key + 1);
        let new_global = self.global.fetch_add(1, Ordering::SeqCst) + 1;

        SseStreamStatus {
            allowed: true,
            current_streams: new_global,
            limit: max_global,
        }
    }

    /// Release a previously acquired stream slot for `key`.
    pub fn release(&self, key: &str) {
        let mut per_key = self.per_key.lock().unwrap();
        if let Some(count) = per_key.get_mut(key) {
            if *count > 0 {
                *count -= 1;
            }
            if *count == 0 {
                per_key.remove(key);
            }
        }
        // Saturating decrement of the global counter.
        let _ = self
            .global
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
                if v > 0 {
                    Some(v - 1)
                } else {
                    None
                }
            });
    }

    /// Number of currently active SSE streams (for metrics).
    pub fn active_streams(&self) -> usize {
        self.global.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_up_to_limit_then_blocks() {
        let limiter = ConcurrencyLimiter::new();
        let limit = 3;
        for i in 0..limit {
            let status = limiter.acquire("192.168.1.1", limit);
            assert!(status.allowed, "request {i} should be allowed");
        }
        let status = limiter.acquire("192.168.1.1", limit);
        assert!(!status.allowed, "request over limit must be blocked");
        assert_eq!(status.current_in_flight, 3);
    }

    #[test]
    fn zero_limit_is_unlimited() {
        let limiter = ConcurrencyLimiter::new();
        for _ in 0..1000 {
            assert!(limiter.acquire("192.168.1.1", 0).allowed);
        }
    }

    #[test]
    fn different_ips_are_independent() {
        let limiter = ConcurrencyLimiter::new();
        assert!(limiter.acquire("192.168.1.1", 1).allowed);
        assert!(!limiter.acquire("192.168.1.1", 1).allowed);
        assert!(limiter.acquire("192.168.1.2", 1).allowed);
    }

    #[test]
    fn release_frees_slot() {
        let limiter = ConcurrencyLimiter::new();
        assert!(limiter.acquire("192.168.1.1", 1).allowed);
        assert!(!limiter.acquire("192.168.1.1", 1).allowed);
        limiter.release("192.168.1.1");
        assert!(limiter.acquire("192.168.1.1", 1).allowed);
    }

    #[test]
    fn sse_caps_per_key_and_global() {
        let limiter = SseStreamLimiter::new();
        assert!(limiter.acquire("token-a", 2, 3).allowed);
        assert!(limiter.acquire("token-a", 2, 3).allowed);
        // Per-key cap reached.
        assert!(!limiter.acquire("token-a", 2, 3).allowed);
        // Other key still allowed until global cap.
        assert!(limiter.acquire("token-b", 2, 3).allowed);
        // Global cap reached.
        assert!(!limiter.acquire("token-c", 2, 3).allowed);
        assert_eq!(limiter.active_streams(), 3);
    }

    #[test]
    fn sse_release_frees_slots() {
        let limiter = SseStreamLimiter::new();
        assert!(limiter.acquire("token-a", 1, 1).allowed);
        assert!(!limiter.acquire("token-a", 1, 1).allowed);
        limiter.release("token-a");
        assert_eq!(limiter.active_streams(), 0);
        assert!(limiter.acquire("token-a", 1, 1).allowed);
    }

    #[test]
    fn sse_zero_limits_are_unlimited() {
        let limiter = SseStreamLimiter::new();
        for _ in 0..1000 {
            assert!(limiter.acquire("token-a", 0, 0).allowed);
        }
        assert_eq!(limiter.active_streams(), 1000);
    }
}
