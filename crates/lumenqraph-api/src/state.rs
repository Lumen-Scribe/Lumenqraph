//! Shared application state.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use sqlx::PgPool;

use crate::rate_limit::RateLimiter;
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
}
