//! A minimal in-memory fixed-window rate limiter, keyed by identity (API key or
//! "anon"). Fine for a single API instance; swap for Redis when you run several.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

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
