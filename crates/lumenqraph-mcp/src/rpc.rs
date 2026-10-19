//! Minimal Soroban RPC client: just `simulateTransaction`, for the
//! `call_contract` tool's read-only view calls.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use serde::Deserialize;

/// Per-client RPC rate limiter.
///
/// Each client (session or token) gets its own sliding-window bucket, while a
/// shared global bucket acts as an outer bound on total throughput so a single
/// noisy client cannot starve the others.
#[derive(Clone)]
pub struct RpcRateLimiter {
    limit_per_min: usize,
    global_limit_per_min: usize,
    per_client: Arc<Mutex<HashMap<String, VecDeque<Instant>>>>,
    global: Arc<Mutex<VecDeque<Instant>>>,
}

impl RpcRateLimiter {
    pub fn new(limit_per_min: usize) -> Self {
        let limit_per_min = limit_per_min.max(1);
        Self {
            limit_per_min,
            global_limit_per_min: limit_per_min,
            per_client: Arc::new(Mutex::new(HashMap::new())),
            global: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Sets the outer global cap (total requests per minute across all clients).
    pub fn with_global_limit(mut self, global_limit_per_min: usize) -> Self {
        self.global_limit_per_min = global_limit_per_min.max(1);
        self
    }

    /// Attempts to consume one request for `client_key`.
    ///
    /// Returns `Ok(())` when allowed, or `Err(retry_after)` with the number of
    /// seconds the caller should wait before retrying.
    pub fn try_consume(&self, client_key: &str) -> Result<(), u64> {
        let now = Instant::now();
        let window = Duration::from_secs(60);

        // Outer bound: total throughput across all clients.
        {
            let mut global: MutexGuard<'_, VecDeque<Instant>> = self
                .global
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            global.retain(|ts| now.duration_since(*ts) < window);
            if global.len() >= self.global_limit_per_min {
                return Err(retry_after(&global, now, window));
            }
            global.push_back(now);
        }

        // Per-client budget.
        let mut per_client: MutexGuard<'_, HashMap<String, VecDeque<Instant>>> = self
            .per_client
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let recent = per_client.entry(client_key.to_string()).or_default();
        recent.retain(|ts| now.duration_since(*ts) < window);
        if recent.len() >= self.limit_per_min {
            return Err(retry_after(recent, now, window));
        }
        recent.push_back(now);
        Ok(())
    }
}

fn retry_after(recent: &VecDeque<Instant>, now: Instant, window: Duration) -> u64 {
    match recent.front() {
        Some(oldest) => {
            let elapsed = now.duration_since(*oldest);
            window.saturating_sub(elapsed).as_secs().max(1)
        }
        None => window.as_secs().max(1),
    }
}

#[derive(Clone)]
pub struct RpcClient {
    http: reqwest::Client,
    url: String,
}

pub enum SimOutcome {
    Ok {
        result_xdr: String,
        events: Vec<String>,
        min_resource_fee: Option<String>,
        latest_ledger: i64,
    },
    Error(String),
}

#[derive(Deserialize)]
struct RpcEnvelope {
    result: Option<SimulateResult>,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    message: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SimulateResult {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    results: Option<Vec<SimResultItem>>,
    #[serde(default)]
    events: Vec<String>,
    #[serde(default)]
    min_resource_fee: Option<String>,
    #[serde(default)]
    latest_ledger: i64,
}

#[derive(Deserialize)]
struct SimResultItem {
    xdr: String,
}

impl RpcClient {
    pub fn new(url: impl Into<String>, timeout_secs: u64) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .build()
            .expect("failed to build HTTP client");
        Self {
            http,
            url: url.into(),
        }
    }

    pub async fn simulate(&self, tx_xdr: &str) -> anyhow::Result<SimOutcome> {
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "simulateTransaction",
            "params": { "transaction": tx_xdr }
        });
        let env: RpcEnvelope = self
            .http
            .post(&self.url)
            .json(&req)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        if let Some(e) = env.error {
            return Err(anyhow::anyhow!("rpc error: {}", e.message));
        }
        let result = env
            .result
            .ok_or_else(|| anyhow::anyhow!("rpc returned no result"))?;
        if let Some(err) = result.error {
            return Ok(SimOutcome::Error(err));
        }
        let events = result.events.clone();
        let min_resource_fee = result.min_resource_fee.clone();
        match result.results.and_then(|mut v| v.drain(..).next()) {
            Some(item) => Ok(SimOutcome::Ok {
                result_xdr: item.xdr,
                events,
                min_resource_fee,
                latest_ledger: result.latest_ledger,
            }),
            None => Ok(SimOutcome::Error("simulation returned no value".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_clients_have_independent_budgets() {
        let limiter = RpcRateLimiter::new(2).with_global_limit(100);

        // Client A exhausts its own budget.
        assert!(limiter.try_consume("client-a").is_ok());
        assert!(limiter.try_consume("client-a").is_ok());
        assert!(limiter.try_consume("client-a").is_err());

        // Client B is unaffected by A's usage.
        assert!(limiter.try_consume("client-b").is_ok());
        assert!(limiter.try_consume("client-b").is_ok());
        assert!(limiter.try_consume("client-b").is_err());
    }

    #[test]
    fn global_cap_bounds_total_throughput() {
        let limiter = RpcRateLimiter::new(10).with_global_limit(2);
        assert!(limiter.try_consume("client-a").is_ok());
        assert!(limiter.try_consume("client-b").is_ok());
        // Global cap reached even though each client is under its own budget.
        assert!(limiter.try_consume("client-c").is_err());
    }

    #[test]
    fn limited_request_returns_retry_after_hint() {
        let limiter = RpcRateLimiter::new(1);
        assert!(limiter.try_consume("client-a").is_ok());
        let retry_after = limiter.try_consume("client-a").unwrap_err();
        assert!(retry_after >= 1 && retry_after <= 60);
    }
}
