//! Shared application state.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use sqlx::PgPool;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::auth::IpConfig;
use crate::call_cache::CallCache;
use crate::concurrency_limit::ConcurrencyLimiter;
use crate::config::ApiConfig;
use crate::key_cache::KeyCache;
use crate::metrics_middleware::MetricsCollector;
use crate::rate_limit::RateLimiter;
use crate::read_cost_limit::ReadCostLimitConfig;
use crate::rpc::RpcClient;
use crate::specs::SpecCache;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    /// When true, data routes require a valid API key.
    pub require_auth: bool,
    /// Requests/min allowed for unauthenticated callers.
    pub anon_rate_limit: i32,
    pub limiter: Arc<RateLimiter>,
    pub http_requests: Arc<AtomicU64>,
    /// Soroban RPC client, for the read layer (`POST /contracts/:id/call`).
    pub rpc: RpcClient,
    /// Parsed contract interfaces, so the read layer doesn't re-fetch and
    /// re-parse a contract's spec section on every call.
    pub specs: Arc<SpecCache>,
    /// Sibling instances mounted under a path prefix (name, upstream URL) —
    /// see `routes::proxy`. Advertised in `/health` for client discovery.
    pub mounts: Arc<Vec<(String, String)>>,
    /// Separate rate limiter for expensive RPC-backed routes (/call, /simulate).
    /// These hit upstream Soroban RPC and must be rate-limited independently
    /// from cheap database reads.
    pub rpc_limiter: Arc<RateLimiter>,
    /// When true, RPC-backed routes require a valid API key even if other
    /// routes don't (higher protection for expensive operations).
    pub rpc_require_auth: bool,
    /// Requests/min allowed for unauthenticated callers on RPC routes.
    pub rpc_anon_rate_limit: i32,
    /// Per-route request metrics.
    pub metrics: Arc<MetricsCollector>,
    /// Short-lived read-through cache for `/call` (view-function) results.
    /// Keyed by (contract_id, function, args). Disabled when TTL is 0.
    pub call_cache: Arc<CallCache>,
    /// Build information (version, git commit, timestamp).
    pub build_info: Arc<BuildInfo>,
    /// Per-IP concurrency limiter to prevent slowloris attacks.
    pub concurrency_limiter: Arc<ConcurrencyLimiter>,
    /// Max concurrent requests per IP. 0 means unlimited.
    pub max_concurrent_per_ip: usize,
    /// Cost limiting config for read routes (/call, /simulate).
    pub read_cost_limit_config: ReadCostLimitConfig,
    /// Max ledger lag for readyz to return 200, in ledgers.
    pub readyz_lag_threshold: i64,
    /// Max age of cursor update for readyz to return 200, in seconds.
    pub readyz_max_age_secs: i64,
    /// Max ledger lag for /health to show "ok" status, in ledgers.
    pub health_max_lag_ledgers: i64,
    /// Max age of cursor update for /health to show "ok" status, in seconds.
    pub health_max_stale_secs: i64,
    /// When true, GET /metrics requires a valid API key (#213).
    pub metrics_require_auth: bool,
    /// Bounded channel for off-request-path audit writes (#367). Auth
    /// middleware pushes events here instead of awaiting an INSERT; a
    /// background task drains and batch-inserts them. `None` disables audit
    /// logging (e.g. in tests).
    pub audit_tx: Option<mpsc::Sender<AuditEvent>>,
    /// Count of audit events dropped because the channel was full (#367).
    pub audit_dropped: Arc<AtomicU64>,
    /// Per-client-IP rate limiter for sibling-instance mounts (#442).
    pub proxy_limiter: Arc<RateLimiter>,
    /// Typed configuration, parsed once at startup (#441). Handlers and
    /// middleware read settings from here, never from the environment.
    pub config: Arc<ApiConfig>,
    /// LRU cache for validated API keys (#430).
    pub key_cache: Arc<KeyCache>,
    /// Client IP resolution configuration (trusted proxy hops, platform header).
    /// Parsed once at startup from environment variables.
    pub ip_config: IpConfig,
    /// Cancelled when the process receives a shutdown signal (#436). Long-lived
    /// handlers such as the SSE stream select on this so they can end cleanly
    /// instead of blocking `with_graceful_shutdown` until SIGKILL.
    pub shutdown: CancellationToken,
}

pub struct BuildInfo {
    pub version: String,
    pub commit: String,
    pub build_time: String,
}

/// An audit-log entry pushed through the off-request-path channel (#367).
#[derive(Debug)]
pub struct AuditEvent {
    pub key_hash_prefix: String,
    pub route: String,
    pub http_method: String,
    pub status_code: u16,
}
