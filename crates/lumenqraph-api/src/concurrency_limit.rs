//! Per-IP connection/concurrency limiter to prevent slowloris-style attacks.
//! Tracks in-flight requests per client IP and rejects excess with 503.

use std::collections::HashMap;
use std::net::SocketAddr;
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
}
