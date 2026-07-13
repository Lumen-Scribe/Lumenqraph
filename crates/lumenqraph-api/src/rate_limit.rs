//! A minimal in-memory fixed-window rate limiter, keyed by identity (API key or
//! "anon"). Fine for a single API instance; swap for Redis when you run several.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// Above this many tracked identities we drop entries from earlier windows.
/// Stale entries can never permit more than the limit anyway, so evicting them
/// is safe and keeps memory bounded on a long-running instance.
const MAX_TRACKED_IDENTITIES: usize = 100_000;

#[derive(Default)]
pub struct RateLimiter {
    windows: Mutex<HashMap<String, (u64, u32)>>, // identity -> (window_minute, count)
}

impl RateLimiter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if this request is allowed under `limit_per_min`.
    pub fn check(&self, identity: &str, limit_per_min: i32) -> bool {
        if limit_per_min <= 0 {
            return true; // unlimited
        }
        let now_min = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() / 60)
            .unwrap_or(0);

        let mut windows = self.windows.lock().unwrap();
        // Bound memory: once the map is large, prune every identity that isn't
        // active in the current window before inserting a new one.
        if windows.len() >= MAX_TRACKED_IDENTITIES {
            windows.retain(|_, (window_min, _)| *window_min == now_min);
        }
        let entry = windows.entry(identity.to_string()).or_insert((now_min, 0));
        if entry.0 != now_min {
            *entry = (now_min, 0);
        }
        if entry.1 >= limit_per_min as u32 {
            false
        } else {
            entry.1 += 1;
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_up_to_limit_then_blocks() {
        let rl = RateLimiter::new();
        assert!(rl.check("k", 3));
        assert!(rl.check("k", 3));
        assert!(rl.check("k", 3));
        assert!(
            !rl.check("k", 3),
            "4th request in the window must be blocked"
        );
    }

    #[test]
    fn zero_or_negative_limit_is_unlimited() {
        let rl = RateLimiter::new();
        for _ in 0..1000 {
            assert!(rl.check("k", 0));
            assert!(rl.check("k", -1));
        }
    }

    #[test]
    fn identities_are_independent() {
        let rl = RateLimiter::new();
        assert!(rl.check("a", 1));
        assert!(!rl.check("a", 1));
        // A different identity has its own fresh budget.
        assert!(rl.check("b", 1));
    }
}
