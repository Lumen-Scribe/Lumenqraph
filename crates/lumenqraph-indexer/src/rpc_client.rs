//! Thin JSON-RPC client for the Soroban RPC `getEvents` / `getLatestLedger`
//! methods. Only the fields we use are modeled.
//!
//! ## Multi-endpoint failover (#398)
//!
//! The client accepts an ordered list of RPC URLs. The first is the primary;
//! subsequent entries are failover candidates tried in priority order when the
//! primary fails.  On a retryable failure the client:
//!
//!   1. Exhausts its per-URL retry budget (3 attempts with jittered backoff).
//!   2. Rotates to the next URL in the list.
//!   3. Checks that the candidate's `latestLedger ≥ cursor` before committing to it,
//!      so a lagging secondary never silently serves stale data.
//!   4. Periodically probes the primary with `getHealth` and fails back once it
//!      recovers.
//!
//! The active endpoint (host only, no credentials) and total failover count are
//! exposed via `take_metrics()`.

use std::collections::HashSet;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use rand::Rng;
use serde::{Deserialize, Serialize};
use stellar_xdr::curr::{
    ContractDataDurability, ContractExecutable, LedgerEntryData, LedgerKey, LedgerKeyContractCode,
    LedgerKeyContractData, Limits, ReadXdr, ScAddress, ScVal, WriteXdr,
};

/// How long to wait before probing the primary again after a failover. Also the
/// minimum gap between two probes, so a dead primary is probed at most once per
/// interval (#722).
const PRIMARY_PROBE_INTERVAL: Duration = Duration::from_secs(60);

/// Timeout for a fail-back probe. Much shorter than the request timeout, so a
/// hung primary cannot hold up application traffic for a full RPC timeout (#722).
const PRIMARY_PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// How far the primary's `latestLedger` may trail the active failover endpoint
/// and still count as caught up enough to fail back to (#722).
const FAILBACK_LEDGER_TOLERANCE: i64 = 10;

/// Longest `Retry-After` the client will honour before it stops waiting and
/// fails over instead (#726). Overridable via `RPC_MAX_RETRY_AFTER_SECS`.
pub const DEFAULT_MAX_RETRY_AFTER: Duration = Duration::from_secs(60);

/// How long an endpoint that exhausted its retry budget is deprioritised when
/// choosing where to fail over next (#723). Cooling endpoints are only used
/// when every untried endpoint is cooling, so a call never gives up while
/// untried endpoints remain.
const ENDPOINT_COOLDOWN: Duration = Duration::from_secs(30);

/// Per-endpoint health used to pick failover candidates (#723).
#[derive(Clone, Copy, Default)]
struct EndpointHealth {
    /// When this endpoint last exhausted its retry budget. Cleared on success.
    last_error_at: Option<Instant>,
    /// Failed exhaustions since the last success.
    consecutive_failures: u32,
}

/// Inner mutable state that the failover logic needs to update atomically.
struct FailoverState {
    /// Index into `urls` of the currently active endpoint.
    active_idx: usize,
    /// Wall-clock time of the last failover event. Gates the first primary probe.
    last_failover_at: Option<Instant>,
    /// When the last primary probe started. Set before probing, so a probe that
    /// fails still resets the timer (#722).
    last_probe_at: Option<Instant>,
    /// Per endpoint: whether its network passphrase has been verified since the
    /// client last switched to it. Cleared on every switch (#721).
    network_verified: Vec<bool>,
    /// Per endpoint: the highest `latestLedger` seen from it since the client last
    /// switched to it. Tips only move forward, so a value that already covers a
    /// cursor answers the cursor check without another call (#720, #721).
    known_tip: Vec<Option<i64>>,
    /// Health of each endpoint, parallel to `urls` (#723).
    endpoints: Vec<EndpointHealth>,
}

pub struct RpcClient {
    http: reqwest::Client,
    /// Ordered endpoint list: `[0]` is primary, rest are failover candidates.
    urls: Vec<String>,
    /// Mutable failover state, guarded by a Mutex. The lock is held only for
    /// cheap index reads/writes — never across network calls.
    failover: Mutex<FailoverState>,
    // ── per-cycle metrics (reset by `take_metrics`) ──────────────────────
    /// Total HTTP attempts (including retries).
    pub calls_made: AtomicU64,
    /// Total call failures (all causes). Every failed attempt is counted once,
    /// including retried attempts (#727).
    pub calls_failed: AtomicU64,
    /// Failures due to RPC -32001 (processing limit exceeded).
    pub calls_failed_32001: AtomicU64,
    /// Failed attempts caused by a transport error (connect/timeout/etc.).
    pub calls_failed_transport: AtomicU64,
    /// Failed attempts caused by a retryable HTTP 5xx status.
    pub calls_failed_http_5xx: AtomicU64,
    /// Failed attempts caused by HTTP 429 (too many requests).
    pub calls_failed_http_429: AtomicU64,
    /// Failed attempts whose response body could not be decoded.
    pub calls_failed_decode: AtomicU64,
    /// Failed attempts caused by a JSON-RPC error object.
    pub calls_failed_rpc: AtomicU64,
    /// Number of whole calls that failed after every endpoint was exhausted.
    pub calls_exhausted: AtomicU64,
    /// Number of endpoint failovers since last `take_metrics`.
    pub failover_count: AtomicU64,
    /// Index of the currently active URL (snapshot for metrics, updated on
    /// failover). Stored separately from `failover.active_idx` so that
    /// `active_endpoint_host()` never needs to acquire the mutex on a hot path.
    active_url_idx: AtomicUsize,
    /// Network passphrase pinned during startup and checked before failover.
    network_passphrase: Mutex<Option<String>>,
    /// Indices of endpoints that were unreachable at startup and have not yet
    /// been verified. They are verified before their first use (#713).
    unverified: Mutex<HashSet<usize>>,
    /// Upper bound on a server-requested `Retry-After` wait (#726).
    max_retry_after: Duration,
    /// Ledger entries skipped because they could not be decoded or the ID was
    /// invalid (#724). Reset by `take_ledger_entry_decode_errors`.
    ledger_entry_decode_errors: AtomicU64,
}

/// Outcome of checking an endpoint's network identity before use.
enum NetworkCheck {
    /// The endpoint reports the pinned passphrase.
    Verified,
    /// The endpoint could not be queried right now. It is skipped, not fatal.
    Unreachable(anyhow::Error),
}

/// Identifies the sending binary's actual release to RPC operators who key off
/// `User-Agent` for debugging, so request logs can be traced back to the
/// version that sent them.
const USER_AGENT: &str = concat!("lumenqraph-indexer/", env!("CARGO_PKG_VERSION"));

/// Bounded XDR limits for decoding untrusted ledger-entry responses from the
/// RPC. These match soroban-env's own defaults (depth = 500 for recursive XDR
/// types; len = 512 KiB, well above the Soroban WASM size cap of ~256 KiB).
/// Using `Limits::none()` on untrusted input allows crafted entries to force
/// unbounded allocations or stack overflows (#400).
const XDR_LEDGER_ENTRY_LIMITS: Limits = Limits {
    depth: 500,
    len: 524_288,
};

/// Extract just the hostname (and port if non-standard) from a URL, with no
/// credentials — safe to log and use in metric labels.
fn host_of(url: &str) -> String {
    // Best-effort: strip scheme, credentials, path, and query.
    let after_scheme = url.find("://").map(|i| &url[i + 3..]).unwrap_or(url);
    // Drop userinfo (user:pass@)
    let host_and_rest = after_scheme
        .find('@')
        .map(|i| &after_scheme[i + 1..])
        .unwrap_or(after_scheme);
    // Drop path/query
    let host = host_and_rest
        .find('/')
        .map(|i| &host_and_rest[..i])
        .unwrap_or(host_and_rest);
    host.to_string()
}

/// Parse a `Retry-After` header (RFC 9110 §10.2.3): either delta-seconds or an
/// HTTP-date. A date already in the past means "retry now". Returns `None` for
/// anything unparseable, so the caller falls back to its normal backoff (#726).
fn parse_retry_after(value: &str, now: chrono::DateTime<chrono::Utc>) -> Option<Duration> {
    let value = value.trim();
    if let Ok(secs) = value.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    let when = chrono::DateTime::parse_from_rfc2822(value).ok()?;
    Some((when.with_timezone(&chrono::Utc) - now).to_std().unwrap_or(Duration::ZERO))
}

#[derive(Serialize)]
struct RpcRequest<'a, P> {
    jsonrpc: &'a str,
    id: u32,
    method: &'a str,
    params: P,
}

#[derive(Deserialize)]
struct RpcResponse<R> {
    result: Option<R>,
    error: Option<RpcError>,
}

#[derive(Deserialize, Debug)]
struct RpcError {
    code: i64,
    message: String,
}

/// Soroban RPC / JSON-RPC error codes distinguishing retryable (transient)
/// from non-retryable (permanent) errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SorobanRpcErrorCode {
    /// -32700: Parse error (invalid JSON received by the server).
    ParseError,
    /// -32600: Invalid Request (the JSON sent is not a valid Request object).
    InvalidRequest,
    /// -32601: Method not found (the method does not exist / is not available).
    MethodNotFound,
    /// -32602: Invalid params (invalid method parameter(s), e.g. invalid contract ID).
    InvalidParams,
    /// -32603: Internal JSON-RPC error.
    InternalError,
    /// -32000: Generic server error.
    ServerError,
    /// -32001: Soroban RPC resource/processing limit exceeded.
    ProcessingLimitExceeded,
    /// Any other RPC error code.
    Other(i64),
}

impl SorobanRpcErrorCode {
    pub fn from_code(code: i64) -> Self {
        match code {
            -32700 => Self::ParseError,
            -32600 => Self::InvalidRequest,
            -32601 => Self::MethodNotFound,
            -32602 => Self::InvalidParams,
            -32603 => Self::InternalError,
            -32000 => Self::ServerError,
            -32001 => Self::ProcessingLimitExceeded,
            other => Self::Other(other),
        }
    }

    /// Whether this error represents a transient, server-side condition that is safe to retry.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::ProcessingLimitExceeded | Self::InternalError | Self::ServerError
        )
    }

    /// Whether this error represents a permanent, client-side condition (e.g. invalid params, method not found).
    pub fn is_permanent(&self) -> bool {
        !self.is_retryable()
    }
}

/// Represents a classified error returned by Soroban RPC.
#[derive(Debug, Clone)]
pub struct SorobanRpcError {
    pub code: i64,
    pub message: String,
    pub method: String,
    pub error_code: SorobanRpcErrorCode,
}

impl SorobanRpcError {
    pub fn new(code: i64, message: impl Into<String>, method: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            method: method.into(),
            error_code: SorobanRpcErrorCode::from_code(code),
        }
    }

    /// `-32602` for a `startLedger` that has just fallen out of (or not yet
    /// entered) the RPC's retention window. Soroban RPC reports it as invalid
    /// params, but it is a timing race rather than a configuration error: the
    /// retention window moves forward every ledger, so the next cycle that
    /// re-reads `getHealth` will succeed (#716).
    pub fn is_out_of_retention(&self) -> bool {
        if self.code != -32602 {
            return false;
        }
        let message = self.message.to_ascii_lowercase();
        message.contains("startledger") || message.contains("start ledger")
    }

    pub fn is_retryable(&self) -> bool {
        self.error_code.is_retryable()
    }

    pub fn is_permanent(&self) -> bool {
        self.error_code.is_permanent()
    }
}

impl std::fmt::Display for SorobanRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "rpc {} error {}: {}",
            self.method, self.code, self.message
        )
    }
}

impl std::error::Error for SorobanRpcError {}

/// Returns true if the given error contains a non-retryable Soroban RPC error.
pub fn is_non_retryable_rpc_error(err: &anyhow::Error) -> bool {
    for cause in err.chain() {
        if let Some(e) = cause.downcast_ref::<SorobanRpcError>() {
            return e.is_permanent() && !e.is_out_of_retention();
        }
    }
    false
}

/// Returns true if the error is a `startLedger` that fell outside retention (#716).
pub fn is_out_of_retention(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<SorobanRpcError>()
            .is_some_and(|e| e.is_out_of_retention())
    })
}

/// Returns true if the RPC reports the method as unsupported (`-32601`).
pub fn is_method_not_found(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<SorobanRpcError>()
            .is_some_and(|e| e.code == -32601)
    })
}

// ---- getLatestLedger ----

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LatestLedgerResult {
    sequence: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NetworkResult {
    network_passphrase: String,
}

/// The `getHealth` result: the retained ledger window. Soroban RPC reports the
/// latest and oldest retained ledgers in one response, so a single call can
/// supply both the tip and the retention floor (#718). Older providers may omit
/// fields, hence the defaults.
#[derive(Debug, Deserialize, Clone, Copy, Default)]
#[serde(rename_all = "camelCase")]
pub struct RpcHealth {
    #[serde(default)]
    pub oldest_ledger: i64,
    #[serde(default)]
    pub latest_ledger: i64,
}

/// The chain tip and retention floor reported by one `getHealth` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TipSnapshot {
    pub latest: i64,
    pub oldest_available: Option<i64>,
}


// ---- getEvents ----

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GetEventsParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    start_ledger: Option<i64>,
    /// Exclusive upper bound. Only sent together with `start_ledger`.
    #[serde(skip_serializing_if = "Option::is_none")]
    end_ledger: Option<i64>,
    filters: Vec<EventFilter>,
    pagination: Pagination,
    xdr_format: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EventFilter {
    #[serde(rename = "type")]
    filter_type: &'static str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    contract_ids: Vec<String>,
    /// Topic patterns, each a list of base64 ScVal segments (`*` wildcards are
    /// passed through as-is). Omitted when empty (#728).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    topics: Vec<Vec<String>>,
}

#[derive(Serialize)]
struct Pagination {
    limit: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<String>,
}

/// getEvents allows at most this many contract IDs in one filter…
const MAX_IDS_PER_FILTER: usize = 5;
/// …and at most this many filters in one request.
const MAX_FILTERS: usize = 5;
/// …and at most this many topic segments per topic pattern.
const MAX_TOPIC_SEGMENTS: usize = 5;

/// Spread the watchlist across getEvents filters (the filters are OR'd by the
/// RPC). An empty watchlist stays a single bare `contract` filter, which
/// matches every contract's events.
fn event_filters(contract_ids: &[String]) -> anyhow::Result<Vec<EventFilter>> {
    event_filters_with_topics(contract_ids, &[])
}

/// Build getEvents filters from contract IDs and topic patterns (#728).
///
/// Topic patterns are already base64-encoded ScVal segments (`*` wildcards are
/// passed through). When both are set the filters are the cross product of the
/// contract-ID chunks and the topic patterns, within the RPC's 5-filter limit.
/// An empty watchlist with no topics stays a single bare `contract` filter,
/// which matches every contract's events.
fn event_filters_with_topics(
    contract_ids: &[String],
    topic_patterns: &[Vec<String>],
) -> anyhow::Result<Vec<EventFilter>> {
    if contract_ids.len() > MAX_IDS_PER_FILTER * MAX_FILTERS {
        return Err(anyhow!(
            "getEvents supports at most {} contract IDs ({} filters x {} IDs); got {}",
            MAX_IDS_PER_FILTER * MAX_FILTERS,
            MAX_FILTERS,
            MAX_IDS_PER_FILTER,
            contract_ids.len()
        ));
    }
    if topic_patterns.len() > MAX_FILTERS {
        return Err(anyhow!(
            "getEvents supports at most {} topic filters; got {}",
            MAX_FILTERS,
            topic_patterns.len()
        ));
    }
    for pattern in topic_patterns {
        if pattern.len() > MAX_TOPIC_SEGMENTS {
            return Err(anyhow!(
                "getEvents supports at most {} topic segments per filter; got {}",
                MAX_TOPIC_SEGMENTS,
                pattern.len()
            ));
        }
    }

    let id_chunks: Vec<Vec<String>> = if contract_ids.is_empty() {
        vec![vec![]]
    } else {
        contract_ids.chunks(MAX_IDS_PER_FILTER).map(<[String]>::to_vec).collect()
    };
    let topic_sets: Vec<Vec<String>> = if topic_patterns.is_empty() {
        vec![vec![]]
    } else {
        topic_patterns.to_vec()
    };

    let total = id_chunks.len() * topic_sets.len();
    if total > MAX_FILTERS {
        return Err(anyhow!(
            "getEvents supports at most {} filters; {} contract-ID chunks x {} topic filters = {}",
            MAX_FILTERS,
            id_chunks.len(),
            topic_sets.len(),
            total
        ));
    }

    let mut filters = Vec::with_capacity(total);
    for chunk in &id_chunks {
        for topics in &topic_sets {
            filters.push(EventFilter {
                filter_type: "contract",
                contract_ids: chunk.clone(),
                topics: if topics.is_empty() { vec![] } else { vec![topics.clone()] },
            });
        }
    }
    Ok(filters)
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct GetEventsResult {
    /// Present in the RPC response; the poller reads the tip from
    /// getLatestLedger instead, so this is retained only for completeness.
    #[allow(dead_code)]
    pub latest_ledger: i64,
    pub events: Vec<EventInfo>,
    pub cursor: Option<String>,
}

/// One event as returned by RPC. `topic` and `value` are base64 XDR.
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EventInfo {
    #[serde(rename = "type")]
    pub event_type: String,
    pub ledger: i64,
    pub ledger_closed_at: String,
    pub contract_id: String,
    pub id: String,
    /// Newer RPC responses omit `pagingToken` (the unique `id` serves the same
    /// role), so treat it as optional.
    #[serde(default)]
    pub paging_token: String,
    #[serde(default)]
    pub in_successful_contract_call: bool,
    #[serde(default)]
    pub tx_hash: String,
    #[serde(default)]
    pub topic: Vec<String>,
    #[serde(default)]
    pub value: String,
}

impl RpcClient {
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn new(url: impl Into<String>, timeout_secs: u64) -> Self {
        Self::with_urls(vec![url.into()], timeout_secs)
    }

    /// Construct a client with an ordered list of failover endpoints. The first
    /// URL is the primary; the rest are tried in order on retryable failures.
    pub fn with_urls(urls: Vec<String>, timeout_secs: u64) -> Self {
        Self::with_urls_and_headers(urls, timeout_secs, reqwest::header::HeaderMap::new())
    }

    /// Like [`with_urls`], but every request also carries `headers`, for
    /// providers that authenticate with a header such as `Authorization` or
    /// `x-api-key` (#725). Values are marked sensitive by the config parser.
    pub fn with_urls_and_headers(
        urls: Vec<String>,
        timeout_secs: u64,
        headers: reqwest::header::HeaderMap,
    ) -> Self {
        assert!(!urls.is_empty(), "at least one RPC URL is required");
        let n_urls = urls.len();
        // A request timeout is essential for a 24/7 poller: without it, a hung
        // RPC connection blocks the poll loop indefinitely and the backoff path
        // (which only fires on an error) is never reached.
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .default_headers(headers)
            .build()
            .expect("failed to build HTTP client");
        Self {
            http,
            urls,
            failover: Mutex::new(FailoverState {
                active_idx: 0,
                last_failover_at: None,
                last_probe_at: None,
                network_verified: vec![false; n_urls],
                known_tip: vec![None; n_urls],
                endpoints: vec![EndpointHealth::default(); n_urls],
            }),
            calls_made: AtomicU64::new(0),
            calls_failed: AtomicU64::new(0),
            calls_failed_32001: AtomicU64::new(0),
            calls_failed_transport: AtomicU64::new(0),
            calls_failed_http_5xx: AtomicU64::new(0),
            calls_failed_http_429: AtomicU64::new(0),
            calls_failed_decode: AtomicU64::new(0),
            calls_failed_rpc: AtomicU64::new(0),
            calls_exhausted: AtomicU64::new(0),
            failover_count: AtomicU64::new(0),
            active_url_idx: AtomicUsize::new(0),
            network_passphrase: Mutex::new(None),
            unverified: Mutex::new(HashSet::new()),
            max_retry_after: DEFAULT_MAX_RETRY_AFTER,
            ledger_entry_decode_errors: AtomicU64::new(0),
        }
    }

    /// Cap the `Retry-After` wait the client will honour; a longer request
    /// makes it fail over immediately instead of sleeping (#726).
    pub fn with_max_retry_after(mut self, max: Duration) -> Self {
        self.max_retry_after = max;
        self
    }

    /// Count one ledger entry (or batch key) skipped by a batch helper (#724).
    fn record_ledger_entry_decode_error(&self) {
        self.ledger_entry_decode_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Count one failed attempt, both in the aggregate and in its class
    /// counter (#727). `class` is a short label such as `transport`, `http_5xx`,
    /// `http_429`, `decode` or `rpc_<code>`.
    fn record_attempt_failure(&self, class: &str, endpoint: &str) {
        self.calls_failed.fetch_add(1, Ordering::Relaxed);
        match class {
            "transport" => self.calls_failed_transport.fetch_add(1, Ordering::Relaxed),
            "http_5xx" => self.calls_failed_http_5xx.fetch_add(1, Ordering::Relaxed),
            "http_429" => self.calls_failed_http_429.fetch_add(1, Ordering::Relaxed),
            "decode" => self.calls_failed_decode.fetch_add(1, Ordering::Relaxed),
            _ => self.calls_failed_rpc.fetch_add(1, Ordering::Relaxed),
        };
        tracing::debug!(
            endpoint = %endpoint,
            class,
            "rpc attempt failed"
        );
    }

    /// Return and reset the number of ledger entries skipped since the last
    /// call, for the per-cycle metrics log (`lumenqraph_ledger_entry_decode_errors`).
    pub fn take_ledger_entry_decode_errors(&self) -> u64 {
        self.ledger_entry_decode_errors.swap(0, Ordering::Acquire)
    }

    /// The hostname of the currently active endpoint (no credentials, no path),
    /// for use in log fields and metrics labels.
    pub fn active_endpoint_host(&self) -> String {
        let idx = self.active_url_idx.load(Ordering::Relaxed);
        host_of(self.urls.get(idx).unwrap_or(&self.urls[0]))
    }

    /// Ask one endpoint for its network passphrase (`getNetwork`).
    async fn fetch_network_passphrase(&self, url: &str) -> anyhow::Result<String> {
        let req = serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "getNetwork", "params": {} });
        let response = self
            .http
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(reqwest::header::USER_AGENT, USER_AGENT)
            .json(&req)
            .send()
            .await
            .with_context(|| format!("getNetwork request failed for RPC host {}", host_of(url)))?
            .error_for_status()
            .with_context(|| format!("getNetwork failed for RPC host {}", host_of(url)))?;
        let rpc: RpcResponse<NetworkResult> = response
            .json()
            .await
            .with_context(|| format!("getNetwork response decode failed for RPC host {}", host_of(url)))?;
        Ok(rpc
            .result
            .ok_or_else(|| anyhow!("getNetwork returned no result for RPC host {}", host_of(url)))?
            .network_passphrase)
    }

    /// Pin the network passphrase at startup (#713).
    ///
    /// Requires at least one reachable endpoint. Endpoints that are unreachable
    /// are logged and marked unverified, so they are checked before first use
    /// instead of blocking startup. Two reachable endpoints that disagree on the
    /// passphrase are still a hard error.
    pub async fn validate_network_passphrases(&self) -> anyhow::Result<String> {
        let mut expected: Option<String> = None;
        for url in &self.urls {
            let req = serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "getNetwork", "params": {} });
            let response = self
                .http
                .post(url)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .header(reqwest::header::USER_AGENT, USER_AGENT)
                .json(&req)
                .send()
                .await
                .with_context(|| {
                    format!("getNetwork request failed for RPC host {}", host_of(url))
                })?
                .error_for_status()
                .with_context(|| format!("getNetwork failed for RPC host {}", host_of(url)))?;
            let rpc: RpcResponse<NetworkResult> = response.json().await.with_context(|| {
                format!(
                    "getNetwork response decode failed for RPC host {}",
                    host_of(url)
                )
            })?;
            let passphrase = rpc
                .result
                .ok_or_else(|| {
                    anyhow!(
                        "getNetwork returned no result for RPC host {}",
                        host_of(url)
                    )
                })?
                .network_passphrase;
            if let Some(ref prior) = expected {
                if prior != &passphrase {
                    anyhow::bail!(
                        "RPC network mismatch: endpoint {} returned a different network passphrase",
                        host_of(url)
                    );
                }
            } else {
                expected = Some(passphrase);
            }
        }
        let passphrase = pinned.ok_or_else(|| {
            anyhow!("no RPC endpoint is reachable ({} configured)", self.urls.len())
        })?;
        *self.network_passphrase.lock().unwrap() = Some(passphrase.clone());
        Ok(passphrase)
    }

    /// Check an endpoint's network identity before it serves traffic.
    ///
    /// A wrong network is a hard error. An endpoint that cannot be reached is
    /// reported as [`NetworkCheck::Unreachable`] so the caller can skip it.
    async fn check_endpoint_network(&self, idx: usize) -> anyhow::Result<NetworkCheck> {
        let expected = self.network_passphrase.lock().unwrap().clone();
        let Some(expected) = expected else {
            return Ok(());
        };
        let url = self.url_at(idx).to_string();
        let req =
            serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "getNetwork", "params": {} });
        let rpc: RpcResponse<NetworkResult> = self
            .http
            .post(&url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(reqwest::header::USER_AGENT, USER_AGENT)
            .json(&req)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let actual = rpc
            .result
            .ok_or_else(|| {
                anyhow!(
                    "getNetwork returned no result for RPC host {}",
                    host_of(&url)
                )
            })?
            .network_passphrase;
        if actual != expected {
            anyhow::bail!("RPC failover network mismatch at {}", host_of(&url));
        }
        Ok(())
    }

    /// Return the URL at `idx` (clamped to the list length).
    fn url_at(&self, idx: usize) -> &str {
        &self.urls[idx.min(self.urls.len() - 1)]
    }

    /// Record that the endpoint at `idx` exhausted its retry budget, starting
    /// its error cooldown (#723).
    fn record_endpoint_failure(&self, idx: usize) {
        let mut state = self.failover.lock().unwrap();
        let health = &mut state.endpoints[idx];
        health.last_error_at = Some(Instant::now());
        health.consecutive_failures = health.consecutive_failures.saturating_add(1);
    }

    /// Clear the error state of the endpoint at `idx` after a successful call.
    fn record_endpoint_success(&self, idx: usize) {
        let mut state = self.failover.lock().unwrap();
        state.endpoints[idx] = EndpointHealth::default();
    }

    /// Choose the next endpoint for one call after `from_idx` gave up (#723).
    ///
    /// Walks the list round-robin from `from_idx`, skipping endpoints already
    /// `tried` in this call. Endpoints in error cooldown are passed over. If
    /// every untried endpoint is cooling, the one whose failure is oldest is
    /// chosen, since it is the most likely to have recovered. Returns `None`
    /// only when no untried endpoint remains.
    ///
    /// Wrap-around matters: with `RPC_URLS=A,B,C` a client on C still reaches
    /// B after B recovers, instead of being stuck on the last endpoint.
    fn next_endpoint(&self, from_idx: usize, tried: &[bool]) -> Option<usize> {
        let mut state = self.failover.lock().unwrap();
        let n = self.urls.len();
        let now = Instant::now();
        let mut oldest_cooling: Option<(usize, Instant)> = None;

        for step in 1..n {
            let cand = (from_idx + step) % n;
            if tried[cand] {
                continue;
            }
            match state.endpoints[cand].last_error_at {
                Some(failed_at) if now.duration_since(failed_at) < ENDPOINT_COOLDOWN => {
                    if oldest_cooling.map_or(true, |(_, t)| failed_at < t) {
                        oldest_cooling = Some((cand, failed_at));
                    }
                }
                _ => {
                    self.switch_active(&mut state, from_idx, cand);
                    return Some(cand);
                }
            }
        }

        let (cand, _) = oldest_cooling?;
        self.switch_active(&mut state, from_idx, cand);
        Some(cand)
    }

    /// Make `to` the active endpoint, recording the failover (#723).
    fn switch_active(&self, state: &mut FailoverState, from_idx: usize, to: usize) {
        if state.active_idx != to {
            tracing::warn!(
                from = %host_of(self.url_at(from_idx)),
                to   = %host_of(self.url_at(to)),
                "rpc failover: switching to next endpoint"
            );
            self.failover_count.fetch_add(1, Ordering::Relaxed);
        }
        state.active_idx = to;
        state.last_failover_at = Some(Instant::now());
        // Anything cached about the new endpoint is from before this switch as
        // far as we're concerned, so re-verify it (#721).
        state.network_verified[to] = false;
        state.known_tip[to] = None;
        self.active_url_idx.store(to, Ordering::Relaxed);
    }

    /// Probe the primary endpoint with `getHealth`. If it responds, fail back
    /// to it. Called only when we are not currently on the primary and enough
    /// time has passed since the last failover.
    async fn maybe_probe_primary(&self) {
        let (is_on_primary, should_probe) = {
            let state = self.failover.lock().unwrap();
            let on_primary = state.active_idx == 0;
            let probe = !on_primary
                && state
                    .last_failover_at
                    .map(|t| t.elapsed() >= PRIMARY_PROBE_INTERVAL)
                    .unwrap_or(false);
            (on_primary, probe)
        };
        if is_on_primary || !should_probe {
            return;
        }
        // Only the primary (index 0) is probed for fail-back.
        let primary_url = &self.urls[0];
        let req_body = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "getHealth", "params": {}
        });
        let result = self
            .http
            .post(primary_url.as_str())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(reqwest::header::USER_AGENT, USER_AGENT)
            .json(&req_body)
            .send()
            .await;
        let ok = matches!(result, Ok(r) if r.status().is_success());
        if ok {
            let mut state = self.failover.lock().unwrap();
            if state.active_idx == 0 {
                return;
            }
            let due_since_failover = state
                .last_failover_at
                .is_some_and(|t| t.elapsed() >= PRIMARY_PROBE_INTERVAL);
            let due_since_probe = state
                .last_probe_at
                .is_none_or(|t| t.elapsed() >= PRIMARY_PROBE_INTERVAL);
            if !(due_since_failover && due_since_probe) {
                return;
            }
            state.last_probe_at = Some(Instant::now());
            state.known_tip[state.active_idx]
        };

        match self.probe_primary(active_tip).await {
            Ok(()) => {
                let mut state = self.failover.lock().unwrap();
                if state.active_idx != 0 {
                    tracing::info!(
                        endpoint = %host_of(&self.urls[0]),
                        "primary rpc endpoint recovered; failing back"
                    );
                    state.active_idx = 0;
                    state.last_failover_at = None;
                    self.active_url_idx.store(0, Ordering::Relaxed);
                }
            }
            Err(e) => {
                tracing::debug!(
                    endpoint = %host_of(&self.urls[0]),
                    error = %e,
                    "primary rpc endpoint still unhealthy; staying on failover endpoint"
                );
            }
        }
    }

    /// Check that the primary is fit to serve again. It must report `healthy`
    /// from `getHealth`, pin the same network passphrase, and be no more than
    /// `FAILBACK_LEDGER_TOLERANCE` ledgers behind the endpoint we are using.
    /// Any HTTP 200 is not enough (#722).
    async fn probe_primary(&self, active_tip: Option<i64>) -> anyhow::Result<()> {
        let primary = self.urls[0].clone();
        let health: RpcHealth = self
            .raw_rpc(&primary, "getHealth", Some(PRIMARY_PROBE_TIMEOUT))
            .await?;
        anyhow::ensure!(
            health.status == "healthy",
            "primary reports status {:?}",
            health.status
        );
        self.check_endpoint_network(0, Some(PRIMARY_PROBE_TIMEOUT)).await?;
        if let Some(tip) = active_tip {
            anyhow::ensure!(
                health.latest_ledger + FAILBACK_LEDGER_TOLERANCE >= tip,
                "primary latestLedger {} trails the active endpoint ({tip})",
                health.latest_ledger
            );
        }
        Ok(())
    }

    async fn call<P: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: P,
    ) -> anyhow::Result<R> {
        self.call_with_cursor(method, params, None).await
    }

    /// Like `call`, but the caller can supply the current cursor ledger so that
    /// failover candidates are only accepted when their `latestLedger` is ≥ it.
    /// Pass `None` when there is no cursor constraint (e.g. fresh start).
    async fn call_with_cursor<P: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: P,
        cursor_ledger: Option<i64>,
    ) -> anyhow::Result<R> {
        let started = std::time::Instant::now();
        let result = self
            .call_with_cursor_untimed(method, params, cursor_ledger)
            .await;
        crate::timing::RPC_REQUEST_DURATION.observe(
            &[
                ("method", method),
                ("endpoint_host", &self.active_endpoint_host()),
            ],
            started.elapsed(),
        );
        result
    }

    async fn call_with_cursor_untimed<P: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: P,
        cursor_ledger: Option<i64>,
    ) -> anyhow::Result<R> {
        let req = RpcRequest {
            jsonrpc: "2.0",
            id: 1,
            method,
            params,
        };
        let body = serde_json::to_vec(&req).with_context(|| format!("rpc {method} serialize"))?;

        // Opportunistically probe the primary before each call; cheap no-op when
        // already on the primary or the probe interval hasn't elapsed.
        self.maybe_probe_primary().await;

        // Bounded retry with jittered exponential backoff for transient failures.
        // Per URL: up to 3 attempts. Then rotate to the next endpoint and repeat.
        const MAX_ATTEMPTS_PER_URL: u32 = 3;
        const BASE_DELAYS_MS: [u64; 2] = [1_000, 2_000];

        let mut last_err: Option<anyhow::Error> = None;
        // Try each endpoint at most once per call: the active one first, then
        // every other endpoint in round-robin order (#723).
        let mut tried = vec![false; self.urls.len()];
        let mut url_idx = self.failover.lock().unwrap().active_idx;

        loop {
            tried[url_idx] = true;
            let url = self.url_at(url_idx).to_string();

            // Never send application traffic to a failover endpoint until its
            // network identity has been re-checked, or to an endpoint that was
            // unreachable at startup and never verified (#713). An unreachable
            // endpoint is skipped; a wrong network is a hard error.
            let mut attempts = MAX_ATTEMPTS_PER_URL;
            if url_idx != 0 || self.is_unverified(url_idx) {
                match self.check_endpoint_network(url_idx).await? {
                    NetworkCheck::Verified => {}
                    NetworkCheck::Unreachable(error) => {
                        tracing::warn!(
                            endpoint = %host_of(&url),
                            %error,
                            "RPC endpoint unreachable; skipping"
                        );
                        last_err = Some(error);
                        attempts = 0;
                    }
                }
            }

            // `Retry-After` from the previous 429/503 on this endpoint (#726).
            let mut retry_after: Option<Duration> = None;
            for attempt in 0..attempts {
                self.calls_made.fetch_add(1, Ordering::Relaxed);

                if attempt > 0 {
                    let base_ms = BASE_DELAYS_MS[(attempt - 1) as usize];
                    let jitter_ms = rand::thread_rng().gen_range(0..=(base_ms / 2)) as i64
                        - (base_ms / 4) as i64;
                    let mut delay =
                        Duration::from_millis(((base_ms as i64) + jitter_ms).max(1) as u64);
                    if let Some(server_delay) = retry_after.take() {
                        if server_delay > self.max_retry_after {
                            // Waiting would exceed our cap: fail over now rather than sleep (#726).
                            tracing::warn!(
                                method,
                                endpoint = %host_of(&url),
                                retry_after_secs = server_delay.as_secs(),
                                max_retry_after_secs = self.max_retry_after.as_secs(),
                                "rpc Retry-After exceeds maximum; failing over without waiting"
                            );
                            break;
                        }
                        // Honour the server's wait when it is longer than our backoff.
                        delay = delay.max(server_delay);
                    }
                    tracing::warn!(
                        method,
                        attempt,
                        delay_ms = delay.as_millis() as u64,
                        endpoint = %host_of(&url),
                        "transient rpc failure; retrying after delay"
                    );
                    tokio::time::sleep(delay).await;
                }

                let result = self
                    .http
                    .post(url.as_str())
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .header(reqwest::header::USER_AGENT, USER_AGENT)
                    .body(body.clone())
                    .send()
                    .await;

                let response = match result {
                    Err(e) => {
                        self.record_attempt_failure("transport", &host_of(&url));
                        last_err = Some(anyhow!(e).context(format!("rpc {method} request failed")));
                        continue; // retry same URL
                    }
                    Ok(r) => r,
                };

                // Read `Retry-After` before `error_for_status` consumes the response (#726).
                let retry_after_header = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned);
                let response = match response.error_for_status() {
                    Err(e) => {
                        let status = e.status();
                        let is_retryable =
                            status.is_some_and(|s| s.is_server_error() || s.as_u16() == 429);
                        let ctx_err =
                            anyhow!(e).context(format!("rpc {method} returned http error"));
                        if is_retryable {
                            let class = if status.map(|s| s.as_u16()) == Some(429) {
                                "http_429"
                            } else {
                                "http_5xx"
                            };
                            self.record_attempt_failure(class, &host_of(&url));
                            if matches!(status.map(|s| s.as_u16()), Some(429 | 503)) {
                                retry_after = retry_after_header
                                    .as_deref()
                                    .and_then(|v| parse_retry_after(v, chrono::Utc::now()));
                            }
                            last_err = Some(ctx_err);
                            continue; // retry same URL
                        }
                        self.record_attempt_failure("http_4xx", &host_of(&url));
                        return Err(ctx_err);
                    }
                    Ok(r) => r,
                };

                let resp: RpcResponse<R> = match response.json().await {
                    Err(e) => {
                        self.record_attempt_failure("decode", &host_of(&url));
                        last_err = Some(
                            anyhow!(e).context(format!("rpc {method} response decode failed")),
                        );
                        continue; // retry same URL
                    }
                    Ok(r) => r,
                };

                if let Some(err) = resp.error {
                    let rpc_err = SorobanRpcError::new(err.code, err.message, method);
                    let class = format!("rpc_{}", rpc_err.code);
                    self.record_attempt_failure(&class, &host_of(&url));
                    if rpc_err.is_retryable() {
                        if rpc_err.code == -32001 {
                            self.calls_failed_32001.fetch_add(1, Ordering::Relaxed);
                        }
                        last_err = Some(anyhow!(rpc_err));
                        continue; // retry same URL
                    }
                    return Err(anyhow!(rpc_err));
                }

                self.record_endpoint_success(url_idx);
                return resp
                    .result
                    .ok_or_else(|| anyhow!("rpc {method} returned no result"));
            }

            // Every attempt on this endpoint failed: start its cooldown and
            // move to the best remaining candidate, if any (#723).
            self.record_endpoint_failure(url_idx);
            match self.next_endpoint(url_idx, &tried) {
                Some(next) => url_idx = next,
                None => break,
            }
        }

        // Every attempt has already been counted in `calls_failed`; this only
        // records that the whole call gave up (#727).
        self.calls_exhausted.fetch_add(1, Ordering::Relaxed);
        Err(last_err.unwrap_or_else(|| {
            anyhow!("rpc {method} failed on all {} endpoint(s)", self.urls.len())
        }))
    }

    /// Current tip ledger sequence.
    pub async fn get_latest_ledger(&self) -> anyhow::Result<i64> {
        let r: LatestLedgerResult = self.call("getLatestLedger", serde_json::json!({})).await?;
        Ok(r.sequence)
    }

    /// Return the provider's currently retained ledger window.
    pub async fn get_health(&self) -> anyhow::Result<RpcHealth> {
        self.call("getHealth", serde_json::json!({})).await
    }

    /// The chain tip and the oldest retained ledger from a single `getHealth`
    /// round-trip (#718). Falls back to `getLatestLedger` only when the provider
    /// does not support `getHealth` (`-32601`) or omits the tip. `oldest_available`
    /// is `None` when the provider does not report `oldestLedger`.
    pub async fn get_tip_snapshot(&self) -> anyhow::Result<TipSnapshot> {
        match self.get_health().await {
            Ok(health) => {
                let oldest_available = (health.oldest_ledger > 0).then_some(health.oldest_ledger);
                let latest = if health.latest_ledger > 0 {
                    health.latest_ledger
                } else {
                    self.get_latest_ledger().await?
                };
                Ok(TipSnapshot {
                    latest,
                    oldest_available,
                })
            }
            Err(e) if is_method_not_found(&e) => Ok(TipSnapshot {
                latest: self.get_latest_ledger().await?,
                oldest_available: None,
            }),
            Err(e) => Err(e),
        }
    }

    /// The oldest ledger the RPC still serves, or `None` when the provider does
    /// not report it (or does not implement `getHealth`).
    pub async fn oldest_available_ledger(&self) -> anyhow::Result<Option<i64>> {
        match self.get_health().await {
            Ok(health) => Ok((health.oldest_ledger > 0).then_some(health.oldest_ledger)),
            Err(e) if is_method_not_found(&e) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Fetch a page of events. Pass `start_ledger` on the first page of a scan,
    /// or `cursor` to continue a previous page (never both).
    ///
    /// RPC caps a getEvents filter at 5 contract IDs and a request at 5
    /// filters, so the watchlist is spread across filters — up to 25 IDs total.
    pub async fn get_events(
        &self,
        start_ledger: Option<i64>,
        contract_ids: &[String],
        cursor: Option<String>,
        limit: u32,
    ) -> anyhow::Result<GetEventsResult> {
        self.get_events_bounded(start_ledger, None, contract_ids, cursor, limit)
            .await
    }

    /// Like [`get_events`], but with an exclusive `end_ledger` so the RPC stops
    /// at the tip instead of serving whatever is newest by the time the scan
    /// reaches the end (#717). `end_ledger` is only sent on the first page,
    /// where `start_ledger` is present; continuation pages rely on the caller
    /// to stop at its own bound.
    pub async fn get_events_bounded(
        &self,
        start_ledger: Option<i64>,
        end_ledger: Option<i64>,
        contract_ids: &[String],
        cursor: Option<String>,
        limit: u32,
    ) -> anyhow::Result<GetEventsResult> {
        self.get_events_bounded_with_topics(
            start_ledger,
            end_ledger,
            contract_ids,
            &[],
            cursor,
            limit,
        )
        .await
    }

    /// Like [`get_events_bounded`], but also pushes topic filters down to the
    /// RPC (#728). `topic_patterns` are base64-encoded ScVal segments with `*`
    /// wildcards passed through.
    pub async fn get_events_bounded_with_topics(
        &self,
        start_ledger: Option<i64>,
        end_ledger: Option<i64>,
        contract_ids: &[String],
        topic_patterns: &[Vec<String>],
        cursor: Option<String>,
        limit: u32,
    ) -> anyhow::Result<GetEventsResult> {
        // A failover candidate must have reached the ledger just before the first
        // one we ask for, i.e. the cursor we are resuming from (#720). Continuation
        // pages carry a paging token instead, so there is no ledger to check.
        let cursor_ledger = if cursor.is_some() {
            None
        } else {
            start_ledger.map(|s| s.saturating_sub(1))
        };
        let start_ledger = if cursor.is_some() { None } else { start_ledger };
        let params = GetEventsParams {
            end_ledger: if start_ledger.is_some() { end_ledger } else { None },
            start_ledger,
            filters: event_filters_with_topics(contract_ids, topic_patterns)?,
            pagination: Pagination { limit, cursor },
            xdr_format: "base64",
        };
        self.call_with_cursor("getEvents", params, cursor_ledger).await
    }

    /// Fetch a contract's deployed WASM (hex hash + bytes), so its on-chain
    /// interface spec can be parsed. Returns `Ok(None)` for contracts with no
    /// WASM (e.g. a Stellar Asset Contract) or when the entries aren't found.
    ///
    /// Two hops, both via `getLedgerEntries`: the contract's *instance* entry
    /// names the WASM hash of its executable; the *code* entry holds the bytes.
    pub async fn get_contract_wasm(
        &self,
        contract_id: &str,
    ) -> anyhow::Result<Option<(String, Vec<u8>)>> {
        let addr = ScAddress::from_str(contract_id)
            .with_context(|| format!("invalid contract id {contract_id}"))?;

        let instance_key = LedgerKey::ContractData(LedgerKeyContractData {
            contract: addr,
            key: ScVal::LedgerKeyContractInstance,
            durability: ContractDataDurability::Persistent,
        });
        let Some((entry, _)) = self.get_ledger_entry(&instance_key).await? else {
            return Ok(None);
        };
        let wasm_hash = match entry {
            LedgerEntryData::ContractData(cd) => match cd.val {
                ScVal::ContractInstance(inst) => match inst.executable {
                    ContractExecutable::Wasm(hash) => hash,
                    ContractExecutable::StellarAsset => return Ok(None),
                },
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };

        let hash_hex = hex::encode(wasm_hash.0);
        let code_key = LedgerKey::ContractCode(LedgerKeyContractCode { hash: wasm_hash });
        let Some((entry, _)) = self.get_ledger_entry(&code_key).await? else {
            return Ok(None);
        };
        match entry {
            LedgerEntryData::ContractCode(cc) => Ok(Some((hash_hex, cc.code.into()))),
            _ => Ok(None),
        }
    }

    /// Fetch a contract's *instance* ledger entry: its current executable hash
    /// (`None` for a Stellar Asset Contract), its instance storage as a single
    /// `ScVal::Map`, and the ledger at which the instance last changed. Used for
    /// state snapshots and upgrade detection. `Ok(None)` if the contract's
    /// instance entry isn't found.
    pub async fn get_contract_instance(
        &self,
        contract_id: &str,
    ) -> anyhow::Result<Option<InstanceEntry>> {
        let addr = ScAddress::from_str(contract_id)
            .with_context(|| format!("invalid contract id {contract_id}"))?;
        let key = LedgerKey::ContractData(LedgerKeyContractData {
            contract: addr,
            key: ScVal::LedgerKeyContractInstance,
            durability: ContractDataDurability::Persistent,
        });
        let Some((data, last_modified_ledger)) = self.get_ledger_entry(&key).await? else {
            return Ok(None);
        };
        let LedgerEntryData::ContractData(cd) = data else {
            return Ok(None);
        };
        let ScVal::ContractInstance(inst) = cd.val else {
            return Ok(None);
        };
        let wasm_hash = match inst.executable {
            ContractExecutable::Wasm(h) => Some(hex::encode(h.0)),
            ContractExecutable::StellarAsset => None,
        };
        Ok(Some(InstanceEntry {
            wasm_hash,
            // The instance storage map (may be empty/None) as one decodable ScVal.
            storage: ScVal::Map(inst.storage),
            last_modified_ledger,
        }))
    }

    /// Fetch a batch of contract instance entries in a single `getLedgerEntries` call.
    /// The returned `Vec` is positionally aligned with `contract_ids`: `None` means the
    /// instance entry was absent. Callers should chunk large slices to avoid RPC limits.
    pub async fn get_contract_instances_batch(
        &self,
        contract_ids: &[String],
    ) -> anyhow::Result<Vec<Option<InstanceEntry>>> {
        let mut output = vec![None; contract_ids.len()];
        if contract_ids.is_empty() {
            return Ok(output);
        }

        // Encode the instance key of each valid ID. An invalid ID is logged,
        // counted and left as `None` so it cannot fail the other lookups (#724).
        // `key_to_idx` maps each encoded key back to its input position, which
        // keeps response matching O(n) instead of a scan per entry.
        let mut keys: Vec<String> = Vec::with_capacity(contract_ids.len());
        let mut key_to_idx: std::collections::HashMap<String, usize> =
            std::collections::HashMap::with_capacity(contract_ids.len());
        for (idx, contract_id) in contract_ids.iter().enumerate() {
            let addr = match ScAddress::from_str(contract_id) {
                Ok(addr) => addr,
                Err(e) => {
                    self.record_ledger_entry_decode_error();
                    tracing::warn!(
                        contract_id = %contract_id,
                        error = %e,
                        "skipping invalid contract id in instance batch"
                    );
                    continue;
                }
            };
            let key = LedgerKey::ContractData(LedgerKeyContractData {
                contract: addr,
                key: ScVal::LedgerKeyContractInstance,
                durability: ContractDataDurability::Persistent,
            });
            let key_b64 = key
                .to_xdr_base64(Limits::none())
                .context("encode ledger key")?;
            key_to_idx.entry(key_b64.clone()).or_insert(idx);
            keys.push(key_b64);
        }
        if keys.is_empty() {
            return Ok(output);
        }

        let result: LedgerEntriesResult = self
            .call("getLedgerEntries", serde_json::json!({ "keys": keys }))
            .await?;

        for item in result.entries.unwrap_or_default() {
            let data = match LedgerEntryData::from_xdr_base64(&item.xdr, XDR_LEDGER_ENTRY_LIMITS) {
                Ok(d) => d,
                Err(e) => {
                    // One undecodable entry must not discard the rest of the batch (#724).
                    self.record_ledger_entry_decode_error();
                    tracing::warn!(error = %e, "skipping undecodable instance ledger entry");
                    continue;
                }
            };
            let LedgerEntryData::ContractData(cd) = data else {
                continue;
            };
            let ScVal::ContractInstance(inst) = cd.val else {
                continue;
            };

            // Determine which contract this is by reconstructing the key.
            let entry_key = LedgerKey::ContractData(LedgerKeyContractData {
                contract: cd.contract.clone(),
                key: cd.key,
                durability: cd.durability,
            });
            if let Ok(key_b64) = entry_key.to_xdr_base64(Limits::none()) {
                if let Some(idx) = key_to_idx.get(&key_b64) {
                    let wasm_hash = match inst.executable {
                        ContractExecutable::Wasm(h) => Some(hex::encode(h.0)),
                        ContractExecutable::StellarAsset => None,
                    };
                    output[*idx] = Some(InstanceEntry {
                        wasm_hash,
                        storage: ScVal::Map(inst.storage),
                        last_modified_ledger: item.last_modified_ledger_seq,
                    });
                }
            }
        }
        Ok(output)
    }

    /// Fetch a single contract-data entry by its exact storage `key` and
    /// `durability`. Unlike instance storage, these per-key entries aren't
    /// enumerable, so the caller must know the key (e.g. a `Balance(Address)`).
    /// Returns the decoded value `ScVal` and the ledger it last changed at, or
    /// `Ok(None)` if no such entry exists (e.g. a holder with a zero balance
    /// whose entry was never written or has expired).
    ///
    /// For bulk fetches use [`get_contract_data_batch`] instead.
    #[allow(dead_code)]
    pub async fn get_contract_data(
        &self,
        contract_id: &str,
        key: &ScVal,
        durability: ContractDataDurability,
    ) -> anyhow::Result<Option<DataEntry>> {
        let addr = ScAddress::from_str(contract_id)
            .with_context(|| format!("invalid contract id {contract_id}"))?;
        let ledger_key = LedgerKey::ContractData(LedgerKeyContractData {
            contract: addr,
            key: key.clone(),
            durability,
        });
        let Some((data, last_modified_ledger)) = self.get_ledger_entry(&ledger_key).await? else {
            return Ok(None);
        };
        let LedgerEntryData::ContractData(cd) = data else {
            return Ok(None);
        };
        Ok(Some(DataEntry {
            val: cd.val,
            last_modified_ledger,
        }))
    }

    /// Reset and return the accumulated RPC metrics from this client instance.
    /// Used by the indexer to report metrics periodically.
    ///
    /// Returns `(calls_made, calls_failed, calls_failed_32001, failover_count)`.
    ///
    /// Memory ordering rationale:
    ///   - `fetch_add` on the counter paths uses `Relaxed` because counter
    ///     increments are independent — we only care about the final aggregate,
    ///     not any ordering relative to other memory operations.
    ///   - `swap(0, Acquire)` here ensures that all preceding `Relaxed`
    ///     `fetch_add` operations on *this thread* (and any thread that
    ///     synchronised with this one) are visible before the counters are
    ///     reset. This prevents a stale read where increments that happened
    ///     before the swap are not yet visible to the Prometheus reporter on
    ///     weakly-ordered architectures such as ARM.
    pub fn take_metrics(&self) -> (u64, u64, u64, u64) {
        let calls = self.calls_made.swap(0, Ordering::Acquire);
        let errors = self.calls_failed.swap(0, Ordering::Acquire);
        let errors_32001 = self.calls_failed_32001.swap(0, Ordering::Acquire);
        let failovers = self.failover_count.swap(0, Ordering::Acquire);
        (calls, errors, errors_32001, failovers)
    }

    /// Return and reset the number of whole calls that failed after every
    /// endpoint was exhausted (#727). Separate from `calls_failed`, which counts
    /// every failed attempt.
    pub fn take_calls_exhausted(&self) -> u64 {
        self.calls_exhausted.swap(0, Ordering::Acquire)
    }

    /// Return and reset the per-class failed-attempt counters (#727) as
    /// `(transport, http_5xx, http_429, decode, rpc)`.
    pub fn take_failure_classes(&self) -> (u64, u64, u64, u64, u64) {
        (
            self.calls_failed_transport.swap(0, Ordering::Acquire),
            self.calls_failed_http_5xx.swap(0, Ordering::Acquire),
            self.calls_failed_http_429.swap(0, Ordering::Acquire),
            self.calls_failed_decode.swap(0, Ordering::Acquire),
            self.calls_failed_rpc.swap(0, Ordering::Acquire),
        )
    }

    /// Fetch and XDR-decode a single ledger entry by key, with the ledger it was
    /// last modified at. `None` if absent.
    async fn get_ledger_entry(
        &self,
        key: &LedgerKey,
    ) -> anyhow::Result<Option<(LedgerEntryData, i64)>> {
        let key_b64 = key
            .to_xdr_base64(Limits::none())
            .context("encode ledger key")?;
        let result: LedgerEntriesResult = self
            .call("getLedgerEntries", serde_json::json!({ "keys": [key_b64] }))
            .await?;
        let Some(first) = result.entries.unwrap_or_default().into_iter().next() else {
            return Ok(None);
        };
        let data = LedgerEntryData::from_xdr_base64(&first.xdr, XDR_LEDGER_ENTRY_LIMITS)
            .context("decode ledger entry")?;
        Ok(Some((data, first.last_modified_ledger_seq)))
    }

    /// Fetch a batch of arbitrary ledger entries in a single `getLedgerEntries`
    /// call. Unlike [`get_contract_data_batch`], the keys may be of any
    /// `LedgerKey` variant (contract data, trustline, account, …), which is what
    /// balance snapshots for Stellar Asset Contracts need: a `G…` holder's
    /// balance lives in a trustline (or the account entry for native XLM), while
    /// a `C…` holder's balance is a `Balance(Address)` contract-data entry (#745).
    ///
    /// The returned `Vec` is positionally aligned with `keys`: `None` means the
    /// entry was absent or could not be decoded. Callers should chunk large
    /// slices to `MAX_BATCH_KEYS`.
    pub async fn get_ledger_entries_batch(
        &self,
        keys: &[LedgerKey],
    ) -> anyhow::Result<Vec<Option<LedgerEntryData>>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        // Encode all input keys and record their index for matching response entries.
        let mut key_b64s: Vec<String> = Vec::with_capacity(keys.len());
        for key in keys {
            key_b64s.push(
                key.to_xdr_base64(Limits::none())
                    .context("encode ledger key")?,
            );
        }

        let key_to_idx: std::collections::HashMap<&str, usize> = key_b64s
            .iter()
            .enumerate()
            .map(|(i, k)| (k.as_str(), i))
            .collect();

        let result: LedgerEntriesResult = self
            .call("getLedgerEntries", serde_json::json!({ "keys": key_b64s }))
            .await?;

        let mut output = vec![None; keys.len()];
        for item in result.entries.into_iter().flatten() {
            let data = match LedgerEntryData::from_xdr_base64(&item.xdr, XDR_LEDGER_ENTRY_LIMITS) {
                Ok(d) => d,
                Err(e) => {
                    // An oversized or malformed entry only loses its own slot;
                    // the other keys in the batch are still returned (#724).
                    self.record_ledger_entry_decode_error();
                    tracing::warn!(error = %e, "skipping undecodable ledger entry");
                    continue;
                }
            };
            // Re-derive the LedgerKey from the returned entry so we can match it
            // back to its position in the input slice.
            let entry_key = match &data {
                LedgerEntryData::ContractData(cd) => LedgerKey::ContractData(LedgerKeyContractData {
                    contract: cd.contract.clone(),
                    key: cd.key.clone(),
                    durability: cd.durability,
                }),
                LedgerEntryData::Trustline(tl) => LedgerKey::Trustline(
                    stellar_xdr::curr::LedgerKeyTrustLine {
                        account_id: tl.account_id.clone(),
                        asset: tl.asset.clone(),
                    },
                ),
                LedgerEntryData::Account(acct) => {
                    LedgerKey::Account(stellar_xdr::curr::LedgerKeyAccount {
                        account_id: acct.account_id.clone(),
                    })
                }
                _ => continue,
            };
            if let Ok(k) = entry_key.to_xdr_base64(Limits::none()) {
                if let Some(&idx) = key_to_idx.get(k.as_str()) {
                    output[idx] = Some(data);
                }
            }
        }

        Ok(output)
    }

    /// Fetch a batch of contract-data entries in a single `getLedgerEntries` call.
    /// The returned `Vec` is positionally aligned with `keys`: `None` means the
    /// entry was absent (e.g. a zero-balance holder whose entry never existed or
    /// has expired). Callers should chunk large slices to `MAX_BATCH_KEYS`.
    pub async fn get_contract_data_batch(
        &self,
        keys: &[(String, ScVal, ContractDataDurability)],
    ) -> anyhow::Result<Vec<Option<DataEntry>>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        // Encode all input keys and record their index for matching response entries.
        let mut key_b64s: Vec<String> = Vec::with_capacity(keys.len());
        for (contract_id, key, durability) in keys {
            let addr = ScAddress::from_str(contract_id)
                .with_context(|| format!("invalid contract id {contract_id}"))?;
            let lkey = LedgerKey::ContractData(LedgerKeyContractData {
                contract: addr,
                key: key.clone(),
                durability: *durability,
            });
            key_b64s.push(
                lkey.to_xdr_base64(Limits::none())
                    .context("encode ledger key")?,
            );
        }

        let key_to_idx: std::collections::HashMap<&str, usize> = key_b64s
            .iter()
            .enumerate()
            .map(|(i, k)| (k.as_str(), i))
            .collect();

        let result: LedgerEntriesResult = self
            .call("getLedgerEntries", serde_json::json!({ "keys": key_b64s }))
            .await?;

        let mut output = vec![None; keys.len()];
        for item in result.entries.into_iter().flatten() {
            let data = match LedgerEntryData::from_xdr_base64(&item.xdr, XDR_LEDGER_ENTRY_LIMITS) {
                Ok(d) => d,
                Err(e) => {
                    // An oversized or malformed entry only loses its own slot;
                    // the other keys in the batch are still returned (#724).
                    self.record_ledger_entry_decode_error();
                    tracing::warn!(error = %e, "skipping undecodable contract data ledger entry");
                    continue;
                }
            };
            // Re-derive the LedgerKey from the returned entry so we can match it
            // back to its position in the input slice.
            if let LedgerEntryData::ContractData(ref cd) = data {
                let entry_key = LedgerKey::ContractData(LedgerKeyContractData {
                    contract: cd.contract.clone(),
                    key: cd.key.clone(),
                    durability: cd.durability,
                });
                if let Ok(k) = entry_key.to_xdr_base64(Limits::none()) {
                    if let Some(&idx) = key_to_idx.get(k.as_str()) {
                        if let LedgerEntryData::ContractData(cd) = data {
                            output[idx] = Some(DataEntry {
                                val: cd.val,
                                last_modified_ledger: item.last_modified_ledger_seq,
                            });
                        }
                    }
                }
            }
        }

        Ok(output)
    }
}

/// A contract's instance entry: executable hash, instance storage, and the
/// ledger it last changed at.
#[derive(Clone)]
pub struct InstanceEntry {
    pub wasm_hash: Option<String>,
    pub storage: ScVal,
    pub last_modified_ledger: i64,
}

/// A single contract-data entry: its decoded value and the ledger it last
/// changed at.
#[derive(Clone)]
pub struct DataEntry {
    pub val: ScVal,
    pub last_modified_ledger: i64,
}

#[derive(Deserialize)]
struct LedgerEntriesResult {
    #[serde(default)]
    entries: Option<Vec<LedgerEntryItem>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LedgerEntryItem {
    xdr: String,
    #[serde(default)]
    last_modified_ledger_seq: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── #398 Failover tests ────────────────────────────────────────────────────

    #[tokio::test]
    async fn failover_to_secondary_when_primary_down() {
        let primary_count = Arc::new(AtomicUsize::new(0));
        let secondary_count = Arc::new(AtomicUsize::new(0));

        let primary_url = spawn_counting_mock(primary_count.clone(), Arc::new(|_| (500, ""))).await;
        let secondary_url =
            spawn_counting_mock(secondary_count.clone(), Arc::new(|_| (200, OK_SEQ_2000))).await;

        let client = RpcClient::with_urls(vec![primary_url, secondary_url], 5);
        let seq: i64 = client
            .get_latest_ledger()
            .await
            .expect("should succeed via secondary");
        assert_eq!(seq, 2000);
        assert_eq!(
            primary_count.load(Ordering::SeqCst),
            3,
            "primary must exhaust 3 attempts before failover"
        );
        assert!(
            secondary_count.load(Ordering::SeqCst) >= 1,
            "secondary must be tried at least once"
        );
    }

    #[tokio::test]
    async fn failover_count_metric_increments() {
        let primary_url =
            spawn_counting_mock(Arc::new(AtomicUsize::new(0)), Arc::new(|_| (500, ""))).await;
        let secondary_url = spawn_counting_mock(
            Arc::new(AtomicUsize::new(0)),
            Arc::new(|_| (200, OK_SEQ_1000)),
        )
        .await;

        let client = RpcClient::with_urls(vec![primary_url, secondary_url], 5);
        client.get_latest_ledger().await.expect("should succeed");

        let (_, _, _, failovers) = client.take_metrics();
        assert_eq!(failovers, 1, "one failover should be recorded");
    }

    #[test]
    fn active_endpoint_host_returns_host_only() {
        let client = RpcClient::new("https://soroban-testnet.stellar.org/rpc", 5);
        assert_eq!(client.active_endpoint_host(), "soroban-testnet.stellar.org");
    }

    #[test]
    fn host_of_strips_credentials_and_path() {
        assert_eq!(
            host_of("https://user:pass@host.example.com/path"),
            "host.example.com"
        );
        assert_eq!(host_of("http://localhost:8080/rpc"), "localhost:8080");
        assert_eq!(host_of("https://rpc.example.com"), "rpc.example.com");
    }

    fn ids(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("C{i}")).collect()
    }

    #[test]
    fn empty_watchlist_is_one_bare_filter() {
        let filters = event_filters(&[]).unwrap();
        assert_eq!(filters.len(), 1);
        assert!(filters[0].contract_ids.is_empty());
        assert!(filters[0].topics.is_empty());
    }

    #[test]
    fn topic_filters_are_emitted_with_wildcards() {
        let topics = vec![vec!["transfer".to_string(), "*".to_string(), "*".to_string()]];
        let filters = event_filters_with_topics(&[], &topics).unwrap();
        assert_eq!(filters.len(), 1);
        assert!(filters[0].contract_ids.is_empty());
        assert_eq!(filters[0].topics, topics);
    }

    #[test]
    fn contract_ids_and_topics_form_a_cross_product() {
        let topics = vec![vec!["swap".to_string()], vec!["transfer".to_string()]];
        let filters = event_filters_with_topics(&ids(7), &topics).unwrap();
        // 2 ID chunks x 2 topic patterns = 4 filters.
        assert_eq!(filters.len(), 4);
        assert!(filters.iter().all(|f| f.topics.len() == 1));
    }

    #[test]
    fn topic_filter_limits_are_validated() {
        let too_many = vec![vec!["a".to_string()]; MAX_FILTERS + 1];
        let err = event_filters_with_topics(&[], &too_many).err().unwrap().to_string();
        assert!(err.contains("topic filters"), "unexpected error: {err}");

        let too_deep = vec![vec!["a".to_string(); MAX_TOPIC_SEGMENTS + 1]];
        let err = event_filters_with_topics(&[], &too_deep).err().unwrap().to_string();
        assert!(err.contains("topic segments"), "unexpected error: {err}");

        let cross = vec![vec!["a".to_string()]; 3];
        let err = event_filters_with_topics(&ids(25), &cross).err().unwrap().to_string();
        assert!(err.contains("filters"), "unexpected error: {err}");
    }

    #[test]
    fn topic_filters_serialize_into_get_events_params() {
        let topics = vec![vec!["transfer".to_string(), "*".to_string()]];
        let filters = event_filters_with_topics(&[], &topics).unwrap();
        let json = serde_json::to_value(&filters[0]).unwrap();
        assert_eq!(json["topics"][0][0], "transfer");
        assert_eq!(json["topics"][0][1], "*");
        assert!(json.get("contractIds").is_none(), "empty contractIds are omitted");
    }

    #[test]
    fn five_ids_fit_in_one_filter() {
        let filters = event_filters(&ids(5)).unwrap();
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].contract_ids.len(), 5);
    }

    #[test]
    fn seven_ids_split_across_two_filters() {
        // The regression that stalled the live indexer: >5 IDs in one filter
        // is rejected by RPC with -32602.
        let filters = event_filters(&ids(7)).unwrap();
        assert_eq!(filters.len(), 2);
        assert_eq!(filters[0].contract_ids.len(), 5);
        assert_eq!(filters[1].contract_ids.len(), 2);
    }

    #[test]
    fn twenty_five_ids_are_the_ceiling() {
        let filters = event_filters(&ids(25)).unwrap();
        assert_eq!(filters.len(), 5);
        assert!(filters.iter().all(|f| f.contract_ids.len() == 5));

        let err = event_filters(&ids(26)).err().unwrap().to_string();
        assert!(err.contains("at most 25"), "unexpected error: {err}");
    }

    // ── Retry logic tests ─────────────────────────────────────────────────
    //
    // Spin up a tiny axum server that counts requests and returns a controlled
    // sequence of responses; verify the RpcClient retries on transient errors
    // and surfaces them correctly when all attempts are exhausted.

    use axum::extract::State;
    use axum::routing::post;
    use axum::Router;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::net::TcpListener;

    // ── helpers ───────────────────────────────────────────────────────────

    /// Spin up a mock server whose handler decides what to return based on
    /// `attempt_count` (incremented each request). Returns the server URL.
    async fn spawn_counting_mock(
        count: Arc<AtomicUsize>,
        make_response: Arc<dyn Fn(usize) -> (u16, &'static str) + Send + Sync + 'static>,
    ) -> String {
        #[derive(Clone)]
        struct S {
            count: Arc<AtomicUsize>,
            responder: Arc<dyn Fn(usize) -> (u16, &'static str) + Send + Sync + 'static>,
        }
        async fn handler(State(s): State<S>) -> axum::response::Response {
            let n = s.count.fetch_add(1, Ordering::SeqCst);
            let (status, body) = (s.responder)(n);
            axum::response::Response::builder()
                .status(status)
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body))
                .unwrap()
        }
        let state = S {
            count,
            responder: make_response,
        };
        let app = Router::new().route("/", post(handler)).with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    // Success response body for getLatestLedger.
    const OK_SEQ_1000: &str = r#"{"jsonrpc":"2.0","id":1,"result":{"sequence":1000}}"#;
    const OK_SEQ_2000: &str = r#"{"jsonrpc":"2.0","id":1,"result":{"sequence":2000}}"#;
    const ERR_32001: &str =
        r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32001,"message":"processing limit reached"}}"#;
    const ERR_32602: &str =
        r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid params"}}"#;

    // getNetwork bodies for the network-identity tests (#713).
    const NET_PUBLIC: &str = r#"{"jsonrpc":"2.0","id":1,"result":{"networkPassphrase":"Public Global Stellar Network ; September 2015"}}"#;
    const NET_TESTNET: &str = r#"{"jsonrpc":"2.0","id":1,"result":{"networkPassphrase":"Test SDF Network ; September 2015"}}"#;
    /// Port 1 is never listening, so connections are refused immediately.
    const DEAD_URL: &str = "http://127.0.0.1:1";

    #[tokio::test]
    async fn startup_succeeds_when_a_secondary_is_unreachable() {
        let primary = spawn_counting_mock(
            Arc::new(AtomicUsize::new(0)),
            Arc::new(|_| (200, NET_PUBLIC)),
        )
        .await;
        let client = RpcClient::with_urls(vec![primary, DEAD_URL.to_string()], 5);

        let passphrase = client
            .validate_network_passphrases()
            .await
            .expect("one healthy endpoint is enough to start");
        assert_eq!(passphrase, "Public Global Stellar Network ; September 2015");
        assert!(client.is_unverified(1), "dead secondary must be marked unverified");
        assert!(!client.is_unverified(0), "healthy primary is verified");
    }

    #[tokio::test]
    async fn startup_fails_when_no_endpoint_is_reachable() {
        let client = RpcClient::with_urls(vec![DEAD_URL.to_string(), "http://127.0.0.1:2".to_string()], 5);
        let err = client.validate_network_passphrases().await.unwrap_err();
        assert!(err.to_string().contains("no RPC endpoint is reachable"), "{err}");
    }

    #[tokio::test]
    async fn startup_rejects_reachable_endpoints_that_disagree() {
        let a = spawn_counting_mock(Arc::new(AtomicUsize::new(0)), Arc::new(|_| (200, NET_PUBLIC))).await;
        let b = spawn_counting_mock(Arc::new(AtomicUsize::new(0)), Arc::new(|_| (200, NET_TESTNET))).await;
        let client = RpcClient::with_urls(vec![a, b], 5);
        let err = client.validate_network_passphrases().await.unwrap_err();
        assert!(err.to_string().contains("network mismatch"), "{err}");
    }

    #[tokio::test]
    async fn secondary_on_another_network_is_rejected_on_first_use() {
        // Primary answers getNetwork at startup, then fails every call so the
        // client fails over to the secondary. The secondary is down at startup
        // (so it is unverified) and reports another network once it is used.
        let primary = spawn_counting_mock(
            Arc::new(AtomicUsize::new(0)),
            Arc::new(|n| if n == 0 { (200, NET_PUBLIC) } else { (500, "") }),
        )
        .await;
        let secondary = spawn_counting_mock(
            Arc::new(AtomicUsize::new(0)),
            Arc::new(|n| if n == 0 { (500, "") } else { (200, NET_TESTNET) }),
        )
        .await;
        let client = RpcClient::with_urls(vec![primary, secondary], 5);
        client.validate_network_passphrases().await.expect("startup");

        let err = client.get_latest_ledger().await.unwrap_err();
        assert!(err.to_string().contains("network mismatch"), "{err}");
    }

    #[tokio::test]
    async fn secondary_is_verified_before_serving_after_failover() {
        // Secondary is down at startup, so it is unverified. On first use the
        // network check (request 1) runs before the real call (request 2).
        let primary = spawn_counting_mock(
            Arc::new(AtomicUsize::new(0)),
            Arc::new(|n| if n == 0 { (200, NET_PUBLIC) } else { (500, "") }),
        )
        .await;
        let secondary_count = Arc::new(AtomicUsize::new(0));
        let secondary = spawn_counting_mock(
            secondary_count.clone(),
            Arc::new(|n| match n {
                0 => (500, ""),
                1 => (200, NET_PUBLIC),
                _ => (200, OK_SEQ_2000),
            }),
        )
        .await;
        let client = RpcClient::with_urls(vec![primary, secondary], 5);
        client.validate_network_passphrases().await.expect("startup");

        let seq = client.get_latest_ledger().await.expect("served by verified secondary");
        assert_eq!(seq, 2000);
        assert!(
            secondary_count.load(Ordering::SeqCst) >= 2,
            "secondary must be checked (getNetwork) before the call"
        );
    }

    #[tokio::test]
    async fn retries_on_5xx_and_eventually_succeeds() {
        // One 500 then a 200 — should succeed on the second attempt.
        let count = Arc::new(AtomicUsize::new(0));
        let url = spawn_counting_mock(
            count.clone(),
            Arc::new(|n| if n < 1 { (500, "") } else { (200, OK_SEQ_1000) }),
        )
        .await;

        let client = RpcClient::new(&url, 5);
        let seq: i64 = client
            .get_latest_ledger()
            .await
            .expect("should succeed after retry");
        assert_eq!(seq, 1000);
        assert_eq!(
            count.load(Ordering::SeqCst),
            2,
            "must have made exactly 2 attempts"
        );
    }

    #[tokio::test]
    async fn retried_attempts_are_counted_as_errors() {
        // Issue #727: 502, 502, 200 must report 3 calls, 2 errors, 0 exhausted.
        let count = Arc::new(AtomicUsize::new(0));
        let url = spawn_counting_mock(
            count.clone(),
            Arc::new(|n| if n < 2 { (502, "") } else { (200, OK_SEQ_1000) }),
        )
        .await;

        let client = RpcClient::new(&url, 5);
        let seq: i64 = client.get_latest_ledger().await.expect("third attempt succeeds");
        assert_eq!(seq, 1000);

        let (calls, errors, _, _) = client.take_metrics();
        assert_eq!(calls, 3, "every attempt counts as a call");
        assert_eq!(errors, 2, "every failed attempt counts as an error");
        assert_eq!(client.take_calls_exhausted(), 0, "the call did not exhaust");

        let (transport, http_5xx, http_429, decode, rpc) = client.take_failure_classes();
        assert_eq!((transport, http_429, decode, rpc), (0, 0, 0, 0));
        assert_eq!(http_5xx, 2, "both failures are classified as 5xx");
    }

    #[tokio::test]
    async fn exhausted_calls_are_counted_separately() {
        let count = Arc::new(AtomicUsize::new(0));
        let url = spawn_counting_mock(count.clone(), Arc::new(|_| (500, ""))).await;

        let client = RpcClient::new(&url, 5);
        assert!(client.get_latest_ledger().await.is_err());

        let (calls, errors, _, _) = client.take_metrics();
        assert_eq!(calls, 3);
        assert_eq!(errors, 3, "each failed attempt is counted once");
        assert_eq!(client.take_calls_exhausted(), 1, "the whole call failed once");
    }

    #[tokio::test]
    async fn topics_reach_get_events() {
        // Issue #728: the `topics` field must be sent to getEvents.
        let (url, log) = spawn_rpc_mock(Arc::new(|_m: &str, _n: usize, _r: &serde_json::Value| {
            (200, EMPTY_EVENTS.to_string())
        }))
        .await;
        let client = RpcClient::new(&url, 5);
        let topics = vec![vec!["transfer".to_string(), "*".to_string()]];
        client
            .get_events_bounded_with_topics(Some(10), None, &[], &topics, None, 10)
            .await
            .expect("getEvents");

        let requests = log.lock().unwrap().clone();
        let filters = &requests[0]["params"]["filters"];
        assert_eq!(filters[0]["topics"][0][0], "transfer");
        assert_eq!(filters[0]["topics"][0][1], "*");
    }

    #[tokio::test]
    async fn exhausts_all_attempts_on_persistent_5xx() {
        // Always 500 — all MAX_ATTEMPTS (3) should be exhausted.
        let count = Arc::new(AtomicUsize::new(0));
        let url = spawn_counting_mock(count.clone(), Arc::new(|_| (500, ""))).await;

        let client = RpcClient::new(&url, 5);
        let err = client
            .get_latest_ledger()
            .await
            .expect_err("should fail after all attempts exhausted");
        assert!(
            err.to_string().contains("http error"),
            "unexpected error message: {err}"
        );
        assert_eq!(
            count.load(Ordering::SeqCst),
            3,
            "must attempt exactly MAX_ATTEMPTS=3 times"
        );
    }

    #[tokio::test]
    async fn does_not_retry_on_4xx() {
        // HTTP 400 is a client error — should fail immediately without retrying.
        let count = Arc::new(AtomicUsize::new(0));
        let url = spawn_counting_mock(count.clone(), Arc::new(|_| (400, ""))).await;

        let client = RpcClient::new(&url, 5);
        let err = client
            .get_latest_ledger()
            .await
            .expect_err("4xx should fail immediately");
        assert!(
            err.to_string().contains("http error"),
            "unexpected error message: {err}"
        );
        assert_eq!(count.load(Ordering::SeqCst), 1, "must not retry on 4xx");
    }

    #[tokio::test]
    async fn does_not_retry_on_non_transient_rpc_error() {
        // RPC error -32602 (bad params) should surface immediately without retrying.
        let count = Arc::new(AtomicUsize::new(0));
        let url = spawn_counting_mock(count.clone(), Arc::new(|_| (200, ERR_32602))).await;

        let client = RpcClient::new(&url, 5);
        let err = client
            .get_latest_ledger()
            .await
            .expect_err("non-transient rpc error should fail immediately");
        assert!(
            err.to_string().contains("-32602"),
            "unexpected error message: {err}"
        );
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "must not retry on non-transient rpc error codes"
        );
        assert!(is_non_retryable_rpc_error(&err));
    }

    #[tokio::test]
    async fn does_not_retry_on_method_not_found_rpc_error() {
        // RPC error -32601 (method not found) should surface immediately without retrying.
        const ERR_32601: &str =
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"Method not found"}}"#;
        let count = Arc::new(AtomicUsize::new(0));
        let url = spawn_counting_mock(count.clone(), Arc::new(|_| (200, ERR_32601))).await;

        let client = RpcClient::new(&url, 5);
        let err = client
            .get_latest_ledger()
            .await
            .expect_err("method not found rpc error should fail immediately");
        assert!(
            err.to_string().contains("-32601"),
            "unexpected error message: {err}"
        );
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "must not retry on method not found"
        );
        assert!(is_non_retryable_rpc_error(&err));
    }

    #[test]
    fn soroban_rpc_error_code_classification() {
        assert!(SorobanRpcErrorCode::ProcessingLimitExceeded.is_retryable());
        assert!(SorobanRpcErrorCode::InternalError.is_retryable());
        assert!(SorobanRpcErrorCode::ServerError.is_retryable());

        assert!(SorobanRpcErrorCode::InvalidParams.is_permanent());
        assert!(SorobanRpcErrorCode::MethodNotFound.is_permanent());
        assert!(SorobanRpcErrorCode::InvalidRequest.is_permanent());
        assert!(SorobanRpcErrorCode::ParseError.is_permanent());
        assert!(SorobanRpcErrorCode::Other(-99999).is_permanent());

        assert!(!SorobanRpcErrorCode::InvalidParams.is_retryable());
        assert!(!SorobanRpcErrorCode::MethodNotFound.is_retryable());
    }

    #[tokio::test]
    async fn retries_on_rpc_32001_processing_limit() {
        // -32001 twice then success — should succeed on the third attempt.
        let count = Arc::new(AtomicUsize::new(0));
        let url = spawn_counting_mock(
            count.clone(),
            Arc::new(|n| {
                if n < 2 {
                    (200, ERR_32001)
                } else {
                    (200, OK_SEQ_2000)
                }
            }),
        )
        .await;

        let client = RpcClient::new(&url, 5);
        let seq: i64 = client
            .get_latest_ledger()
            .await
            .expect("should succeed on third attempt after two -32001 errors");
        assert_eq!(seq, 2000);
        assert_eq!(
            count.load(Ordering::SeqCst),
            3,
            "must have made exactly 3 attempts"
        );
    }

    #[tokio::test]
    async fn retries_on_http_429_too_many_requests() {
        // Issue #395: HTTP 429 should be treated as retryable.
        let count = Arc::new(AtomicUsize::new(0));
        let url = spawn_counting_mock(
            count.clone(),
            Arc::new(|n| if n < 1 { (429, "") } else { (200, OK_SEQ_1000) }),
        )
        .await;

        let client = RpcClient::new(&url, 5);
        let seq: i64 = client
            .get_latest_ledger()
            .await
            .expect("should retry and succeed after 429");
        assert_eq!(seq, 1000);
        assert_eq!(
            count.load(Ordering::SeqCst),
            2,
            "must have made 2 attempts (one failed, one succeeded)"
        );
    }

    /// Spawn a mock whose responder returns `(status, body, Retry-After)` for
    /// each request index. Returns the URL and the request counter.
    async fn spawn_retry_after_mock(
        responder: Arc<dyn Fn(usize) -> (u16, String, Option<String>) + Send + Sync + 'static>,
    ) -> (String, Arc<AtomicUsize>) {
        #[derive(Clone)]
        struct S {
            count: Arc<AtomicUsize>,
            responder: Arc<dyn Fn(usize) -> (u16, String, Option<String>) + Send + Sync + 'static>,
        }
        async fn handler(State(s): State<S>) -> axum::response::Response {
            let n = s.count.fetch_add(1, Ordering::SeqCst);
            let (status, body, retry_after) = (s.responder)(n);
            let mut builder = axum::response::Response::builder()
                .status(status)
                .header("content-type", "application/json");
            if let Some(v) = retry_after {
                builder = builder.header("retry-after", v);
            }
            builder.body(axum::body::Body::from(body)).unwrap()
        }
        let count = Arc::new(AtomicUsize::new(0));
        let state = S { count: count.clone(), responder };
        let app = Router::new().route("/", post(handler)).with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), count)
    }

    #[tokio::test]
    async fn respects_retry_after_header() {
        // Issue #726 / #395: `Retry-After: 2` on a 429 must delay the retry by
        // at least 2s. The normal jittered backoff is at most 1.25s here, so a
        // fast retry would fail the lower bound.
        let (url, count) = spawn_retry_after_mock(Arc::new(|n| {
            if n < 1 {
                (429, String::new(), Some("2".to_string()))
            } else {
                (200, OK_SEQ_1000.to_string(), None)
            }
        }))
        .await;

        let client = RpcClient::new(&url, 5);
        let start = std::time::Instant::now();
        let seq: i64 = client.get_latest_ledger().await.expect("should succeed after 429");
        let elapsed = start.elapsed();

        assert_eq!(seq, 1000);
        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert!(
            elapsed >= Duration::from_millis(1_900),
            "retry came after {elapsed:?}, before the 2s Retry-After elapsed"
        );
        assert!(elapsed < Duration::from_secs(4), "retry waited too long: {elapsed:?}");
    }

    #[tokio::test]
    async fn respects_retry_after_http_date() {
        // Issue #726: the HTTP-date form of Retry-After is honoured too. The
        // date is truncated to whole seconds, so allow some slack below 3s.
        let when = chrono::Utc::now() + chrono::Duration::seconds(3);
        let date = when.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        let (url, count) = spawn_retry_after_mock(Arc::new(move |n| {
            if n < 1 {
                (503, String::new(), Some(date.clone()))
            } else {
                (200, OK_SEQ_1000.to_string(), None)
            }
        }))
        .await;

        let client = RpcClient::new(&url, 5);
        let start = std::time::Instant::now();
        let seq: i64 = client.get_latest_ledger().await.expect("should succeed after 503");
        let elapsed = start.elapsed();

        assert_eq!(seq, 1000);
        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert!(elapsed >= Duration::from_millis(1_500), "retry came too early: {elapsed:?}");
        assert!(elapsed < Duration::from_secs(5), "retry waited too long: {elapsed:?}");
    }

    #[tokio::test]
    async fn retry_after_above_cap_fails_over_without_waiting() {
        // Issue #726: a `Retry-After` beyond the cap must not be slept on. The
        // client fails over to the next endpoint immediately.
        let (url_a, a_count) = spawn_retry_after_mock(Arc::new(|_| {
            (429, String::new(), Some("3600".to_string()))
        }))
        .await;
        let (url_b, _) = spawn_retry_after_mock(Arc::new(|_| {
            (200, OK_SEQ_1000.to_string(), None)
        }))
        .await;

        let client = RpcClient::with_urls(vec![url_a, url_b.clone()], 5)
            .with_max_retry_after(Duration::from_secs(60));
        let start = std::time::Instant::now();
        let seq: i64 = client.get_latest_ledger().await.expect("should fail over to B");
        let elapsed = start.elapsed();

        assert_eq!(seq, 1000);
        assert_eq!(a_count.load(Ordering::SeqCst), 1, "A is not retried after a capped wait");
        assert!(elapsed < Duration::from_secs(2), "must not sleep the 3600s: {elapsed:?}");
        assert_eq!(client.active_endpoint_host(), host_of(&url_b));
    }

    #[test]
    fn parse_retry_after_handles_seconds_dates_and_garbage() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(parse_retry_after("10", now), Some(Duration::from_secs(10)));
        assert_eq!(parse_retry_after(" 7 ", now), Some(Duration::from_secs(7)));
        assert_eq!(
            parse_retry_after("Thu, 01 Jan 2026 00:00:30 GMT", now),
            Some(Duration::from_secs(30))
        );
        // A date in the past means "retry now".
        assert_eq!(
            parse_retry_after("Wed, 31 Dec 2025 23:59:00 GMT", now),
            Some(Duration::ZERO)
        );
        assert_eq!(parse_retry_after("soon", now), None);
        assert_eq!(parse_retry_after("", now), None);
    }

    // ── #725: configured RPC headers reach every method ───────────────────

    #[tokio::test]
    async fn rpc_headers_are_sent_on_every_rpc_method() {
        let (url, log) = spawn_recording_mock(Arc::new(|method| match method {
            "getHealth" => r#"{"jsonrpc":"2.0","id":1,"result":{"oldestLedger":1,"latestLedger":2}}"#.to_string(),
            "getNetwork" => r#"{"jsonrpc":"2.0","id":1,"result":{"networkPassphrase":"Test SDF Network ; September 2015"}}"#.to_string(),
            "getEvents" => r#"{"jsonrpc":"2.0","id":1,"result":{"latestLedger":2,"events":[]}}"#.to_string(),
            "getLedgerEntries" => r#"{"jsonrpc":"2.0","id":1,"result":{"entries":[]}}"#.to_string(),
            _ => r#"{"jsonrpc":"2.0","id":1,"result":null}"#.to_string(),
        }))
        .await;

        let headers = crate::config::parse_rpc_headers("Authorization=Bearer tok;X-Api-Key=key-1").unwrap();
        let client = RpcClient::with_urls_and_headers(vec![url], 5, headers);

        client.get_health().await.expect("getHealth");
        client.validate_network_passphrases().await.expect("getNetwork");
        client.get_events(Some(1), &[], None, 10).await.expect("getEvents");
        client
            .get_contract_data_batch(&[(
                CONTRACT_A.to_string(),
                ScVal::U32(1),
                ContractDataDurability::Persistent,
            )])
            .await
            .expect("getLedgerEntries");

        let log = log.lock().unwrap();
        for method in ["getHealth", "getNetwork", "getEvents", "getLedgerEntries"] {
            let req = log
                .iter()
                .find(|r| r.method == method)
                .unwrap_or_else(|| panic!("{method} was never sent"));
            assert_eq!(req.headers.get("authorization").unwrap(), "Bearer tok", "{method}");
            assert_eq!(req.headers.get("x-api-key").unwrap(), "key-1", "{method}");
        }
    }

    // ── #723: failover wraps around all endpoints ─────────────────────────

    #[test]
    fn next_endpoint_wraps_round_robin_and_skips_tried() {
        let client = RpcClient::with_urls(
            vec!["http://a".into(), "http://b".into(), "http://c".into()],
            5,
        );
        // From the last endpoint, the walk wraps back to the first.
        assert_eq!(client.next_endpoint(2, &[false, false, true]), Some(0));
        // Nothing left untried in this call.
        assert_eq!(client.next_endpoint(2, &[true, true, true]), None);
    }

    #[test]
    fn next_endpoint_defers_cooling_endpoints() {
        let client = RpcClient::with_urls(
            vec!["http://a".into(), "http://b".into(), "http://c".into()],
            5,
        );
        client.record_endpoint_failure(0);
        // A is cooling, so from C the client picks B.
        assert_eq!(client.next_endpoint(2, &[false, false, true]), Some(1));
        assert_eq!(client.active_url_idx.load(Ordering::Relaxed), 1);

        // Once every untried endpoint is cooling, one is still chosen so the
        // call does not give up early.
        // Only untried endpoint C is cooling too: it is still chosen.
        client.record_endpoint_failure(2);
        assert_eq!(client.next_endpoint(1, &[true, true, false]), Some(2));
    }

    #[tokio::test]
    async fn failover_wraps_back_to_recovered_secondary() {
        // Issue #723: A, B, C. A is down throughout, B is down for its first
        // retry budget and then recovers, C is down throughout. The client
        // must reach B rather than staying stuck on the last endpoint.
        let a_count = Arc::new(AtomicUsize::new(0));
        let b_count = Arc::new(AtomicUsize::new(0));
        let c_count = Arc::new(AtomicUsize::new(0));
        let url_a = spawn_counting_mock(a_count.clone(), Arc::new(|_| (500, ""))).await;
        let url_b = spawn_counting_mock(
            b_count.clone(),
            Arc::new(|n| if n < 3 { (500, "") } else { (200, OK_SEQ_1000) }),
        )
        .await;
        let url_c = spawn_counting_mock(c_count.clone(), Arc::new(|_| (500, ""))).await;

        let client = RpcClient::with_urls(vec![url_a, url_b.clone(), url_c.clone()], 5);

        // Call 1: every endpoint exhausts its budget, so the call fails and
        // the client ends up on C, the last endpoint it tried.
        assert!(client.get_latest_ledger().await.is_err());
        assert_eq!(client.active_endpoint_host(), host_of(&url_c));

        // Call 2: starting from C, the cooling endpoints are tried and B has
        // recovered, so the call succeeds on B.
        let seq: i64 = client
            .get_latest_ledger()
            .await
            .expect("should fail over back to the recovered secondary");
        assert_eq!(seq, 1000);
        assert_eq!(client.active_endpoint_host(), host_of(&url_b));
        assert_eq!(b_count.load(Ordering::SeqCst), 4, "B: three failures then one success");
    }

    // ── recording mock (used by #724 and #725 tests) ──────────────────────

    /// One request as seen by the mock: JSON-RPC method, raw body, headers.
    #[derive(Clone)]
    struct RecordedRequest {
        method: String,
        body: String,
        headers: axum::http::HeaderMap,
    }

    /// Spawn an RPC mock that records every request and answers with
    /// `respond(method)` as a 200 JSON body. Returns the URL and the log.
    async fn spawn_recording_mock(
        respond: Arc<dyn Fn(&str) -> String + Send + Sync + 'static>,
    ) -> (String, Arc<std::sync::Mutex<Vec<RecordedRequest>>>) {
        #[derive(Clone)]
        struct S {
            log: Arc<std::sync::Mutex<Vec<RecordedRequest>>>,
            respond: Arc<dyn Fn(&str) -> String + Send + Sync + 'static>,
        }
        async fn handler(
            State(s): State<S>,
            headers: axum::http::HeaderMap,
            body: String,
        ) -> axum::response::Response {
            let method = serde_json::from_str::<serde_json::Value>(&body)
                .ok()
                .and_then(|v| v["method"].as_str().map(str::to_owned))
                .unwrap_or_default();
            let reply = (s.respond)(&method);
            s.log.lock().unwrap().push(RecordedRequest { method, body, headers });
            axum::response::Response::builder()
                .status(200)
                .header("content-type", "application/json")
                .body(axum::body::Body::from(reply))
                .unwrap()
        }
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let state = S { log: log.clone(), respond };
        let app = Router::new().route("/", post(handler)).with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), log)
    }

    /// Base64 XDR for a contract-data ledger entry with the given key and value.
    fn contract_data_xdr(contract_id: &str, key: ScVal, val: ScVal) -> String {
        LedgerEntryData::ContractData(stellar_xdr::curr::ContractDataEntry {
            ext: stellar_xdr::curr::ExtensionPoint::V0,
            contract: ScAddress::from_str(contract_id).unwrap(),
            key,
            durability: ContractDataDurability::Persistent,
            val,
        })
        .to_xdr_base64(Limits::none())
        .unwrap()
    }

    const CONTRACT_A: &str = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4";
    const CONTRACT_B: &str = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM";

    // ── #724: batch helpers isolate per-entry failures ────────────────────

    #[tokio::test]
    async fn contract_data_batch_skips_oversized_entry_and_keeps_the_rest() {
        let keys = vec![
            (CONTRACT_A.to_string(), ScVal::U32(1), ContractDataDurability::Persistent),
            (CONTRACT_A.to_string(), ScVal::U32(2), ContractDataDurability::Persistent),
            (CONTRACT_A.to_string(), ScVal::U32(3), ContractDataDurability::Persistent),
        ];
        // Entry 2 encodes fine but exceeds XDR_LEDGER_ENTRY_LIMITS when decoded.
        let oversized = ScVal::Bytes(
            vec![0u8; XDR_LEDGER_ENTRY_LIMITS.len as usize + 1].try_into().unwrap(),
        );
        let response = serde_json::json!({
            "jsonrpc": "2.0", "id": 1,
            "result": { "entries": [
                { "key": "k1", "xdr": contract_data_xdr(CONTRACT_A, ScVal::U32(1), ScVal::U32(10)), "lastModifiedLedgerSeq": 5 },
                { "key": "k2", "xdr": contract_data_xdr(CONTRACT_A, ScVal::U32(2), oversized), "lastModifiedLedgerSeq": 6 },
                { "key": "k3", "xdr": contract_data_xdr(CONTRACT_A, ScVal::U32(3), ScVal::U32(30)), "lastModifiedLedgerSeq": 7 },
            ]}
        })
        .to_string();
        let (url, _log) = spawn_recording_mock(Arc::new(move |_| response.clone())).await;

        let client = RpcClient::new(&url, 5);
        let out = client.get_contract_data_batch(&keys).await.expect("batch must succeed");

        assert_eq!(out.len(), 3);
        assert!(matches!(out[0].as_ref().map(|e| &e.val), Some(ScVal::U32(10))));
        assert!(out[1].is_none(), "the oversized entry is dropped, not the batch");
        assert!(matches!(out[2].as_ref().map(|e| &e.val), Some(ScVal::U32(30))));
        assert_eq!(client.take_ledger_entry_decode_errors(), 1);
    }

    #[tokio::test]
    async fn instance_batch_isolates_invalid_id_and_returns_the_rest() {
        let instance = ScVal::ContractInstance(stellar_xdr::curr::ScContractInstance {
            executable: ContractExecutable::StellarAsset,
            storage: None,
        });
        let ids = vec![
            CONTRACT_A.to_string(),
            "not-a-contract-id".to_string(),
            CONTRACT_B.to_string(),
        ];
        let response = serde_json::json!({
            "jsonrpc": "2.0", "id": 1,
            "result": { "entries": [
                { "key": "k2", "xdr": contract_data_xdr(CONTRACT_B, ScVal::LedgerKeyContractInstance, instance.clone()), "lastModifiedLedgerSeq": 22 },
                { "key": "k1", "xdr": contract_data_xdr(CONTRACT_A, ScVal::LedgerKeyContractInstance, instance.clone()), "lastModifiedLedgerSeq": 11 },
            ]}
        })
        .to_string();
        let (url, log) = spawn_recording_mock(Arc::new(move |_| response.clone())).await;

        let client = RpcClient::new(&url, 5);
        let out = client.get_contract_instances_batch(&ids).await.expect("batch must succeed");

        assert_eq!(out.len(), 3);
        assert_eq!(out[0].as_ref().map(|e| e.last_modified_ledger), Some(11));
        assert!(out[1].is_none(), "invalid id is left as None");
        assert_eq!(out[2].as_ref().map(|e| e.last_modified_ledger), Some(22));

        // Only the two valid IDs reach the RPC.
        let log = log.lock().unwrap();
        assert_eq!(log.len(), 1);
        let sent: serde_json::Value = serde_json::from_str(&log[0].body).unwrap();
        assert_eq!(sent["params"]["keys"].as_array().unwrap().len(), 2);
        assert_eq!(client.take_ledger_entry_decode_errors(), 1);
    }

    #[tokio::test]
    async fn exhausts_attempts_on_persistent_429() {
        // Issue #395: 429 on all attempts should eventually fail.
        let count = Arc::new(AtomicUsize::new(0));
        let url = spawn_counting_mock(count.clone(), Arc::new(|_| (429, ""))).await;

        let client = RpcClient::new(&url, 5);
        let err = client
            .get_latest_ledger()
            .await
            .expect_err("should fail after all attempts exhausted");
        assert!(
            err.to_string().contains("http error"),
            "unexpected error message: {err}"
        );
        assert_eq!(
            count.load(Ordering::SeqCst),
            3,
            "must attempt exactly MAX_ATTEMPTS=3 times"
        );
    }

    // ── #720–#722 failover checks ─────────────────────────────────────────

    const PASSPHRASE: &str = "Test SDF Network ; September 2015";

    type Responder = Arc<dyn Fn(&str) -> (u16, String) + Send + Sync>;
    type MethodLog = Arc<std::sync::Mutex<Vec<String>>>;

    fn responder(f: impl Fn(&str) -> (u16, String) + Send + Sync + 'static) -> Responder {
        Arc::new(f)
    }

    /// Mock RPC that records each JSON-RPC method it receives and answers with
    /// `respond(method)` as `(http_status, body)`. Returns the URL and the log.
    async fn spawn_method_mock(respond: Responder) -> (String, MethodLog) {
        #[derive(Clone)]
        struct S {
            log: MethodLog,
            respond: Responder,
        }
        async fn handler(State(s): State<S>, body: String) -> axum::response::Response {
            let v: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
            let method = v["method"].as_str().unwrap_or("").to_string();
            s.log.lock().unwrap().push(method.clone());
            let (status, payload) = (s.respond)(&method);
            axum::response::Response::builder()
                .status(status)
                .header("content-type", "application/json")
                .body(axum::body::Body::from(payload))
                .unwrap()
        }
        let log: MethodLog = Arc::new(std::sync::Mutex::new(Vec::new()));
        let app = Router::new()
            .route("/", post(handler))
            .with_state(S { log: log.clone(), respond });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), log)
    }

    fn method_count(log: &MethodLog, method: &str) -> usize {
        log.lock().unwrap().iter().filter(|m| m.as_str() == method).count()
    }

    fn network_body() -> String {
        format!(r#"{{"jsonrpc":"2.0","id":1,"result":{{"networkPassphrase":"{PASSPHRASE}"}}}}"#)
    }

    fn latest_body(seq: i64) -> String {
        format!(r#"{{"jsonrpc":"2.0","id":1,"result":{{"sequence":{seq}}}}}"#)
    }

    fn health_body(status: &str, latest: i64) -> String {
        format!(
            r#"{{"jsonrpc":"2.0","id":1,"result":{{"status":"{status}","latestLedger":{latest},"oldestLedger":1}}}}"#
        )
    }

    fn events_body(latest: i64) -> String {
        format!(
            r#"{{"jsonrpc":"2.0","id":1,"result":{{"latestLedger":{latest},"events":[],"cursor":null}}}}"#
        )
    }

    /// Pin the network passphrase directly, as `validate_network_passphrases`
    /// would at startup, without calling a primary that these tests break on purpose.
    fn pin_passphrase(client: &RpcClient) {
        *client.network_passphrase.lock().unwrap() = Some(PASSPHRASE.to_string());
    }

    /// Put the client on failover endpoint `idx`, as if it failed over at `failed_over_at`.
    fn force_failover_state(client: &RpcClient, idx: usize, failed_over_at: Instant) {
        let mut st = client.failover.lock().unwrap();
        st.active_idx = idx;
        st.last_failover_at = Some(failed_over_at);
        st.last_probe_at = None;
        drop(st);
        client.active_url_idx.store(idx, Ordering::Relaxed);
    }

    #[tokio::test]
    async fn failover_endpoint_network_checked_once_per_switch() {
        // #721: primary down. Ten calls must send exactly one getNetwork to the
        // secondary, not one before every call.
        let (primary, _) = spawn_method_mock(responder(|_| (500, String::new()))).await;
        let (secondary, log) = spawn_method_mock(responder(|m| match m {
            "getNetwork" => (200, network_body()),
            _ => (200, latest_body(2000)),
        }))
        .await;

        let client = RpcClient::with_urls(vec![primary, secondary], 5);
        pin_passphrase(&client);
        for _ in 0..10 {
            assert_eq!(client.get_latest_ledger().await.expect("secondary serves"), 2000);
        }
        assert_eq!(method_count(&log, "getNetwork"), 1, "one network check per switch");
        assert_eq!(method_count(&log, "getLatestLedger"), 10);
    }

    #[tokio::test]
    async fn failed_network_check_moves_on_to_next_candidate() {
        // #721: a failing getNetwork on one secondary must not fail the call;
        // the next secondary should be tried.
        let (primary, _) = spawn_method_mock(responder(|_| (500, String::new()))).await;
        let (bad, bad_log) = spawn_method_mock(responder(|_| (500, String::new()))).await;
        let (good, _) = spawn_method_mock(responder(|m| match m {
            "getNetwork" => (200, network_body()),
            _ => (200, latest_body(2000)),
        }))
        .await;

        let client = RpcClient::with_urls(vec![primary, bad, good], 5);
        pin_passphrase(&client);
        assert_eq!(client.get_latest_ledger().await.expect("third endpoint serves"), 2000);
        assert_eq!(
            method_count(&bad_log, "getLatestLedger"),
            0,
            "an endpoint that fails its network check must receive no application traffic"
        );
    }

    #[tokio::test]
    async fn lagging_failover_candidate_is_rejected_for_cursor() {
        // #720: primary down, secondary tip = cursor - 50. The secondary must not
        // be used for the page, and the rejection must be reported.
        let (primary, _) = spawn_method_mock(responder(|_| (500, String::new()))).await;
        let (secondary, log) = spawn_method_mock(responder(|m| match m {
            "getNetwork" => (200, network_body()),
            "getLatestLedger" => (200, latest_body(950)),
            _ => (200, events_body(950)),
        }))
        .await;

        let client = RpcClient::with_urls(vec![primary, secondary], 5);
        pin_passphrase(&client);
        // Resuming from cursor 1000 means the first wanted ledger is 1001.
        let err = client
            .get_events(Some(1001), &[], None, 10)
            .await
            .expect_err("lagging secondary must be rejected");
        assert!(err.to_string().contains("behind the cursor"), "unexpected error: {err}");
        assert_eq!(method_count(&log, "getEvents"), 0, "no page may come from a lagging endpoint");
    }

    #[tokio::test]
    async fn failover_candidate_at_cursor_is_accepted() {
        // #720 boundary: tip exactly equal to the cursor is enough.
        let (primary, _) = spawn_method_mock(responder(|_| (500, String::new()))).await;
        let (secondary, log) = spawn_method_mock(responder(|m| match m {
            "getNetwork" => (200, network_body()),
            "getLatestLedger" => (200, latest_body(1000)),
            _ => (200, events_body(1000)),
        }))
        .await;

        let client = RpcClient::with_urls(vec![primary, secondary], 5);
        pin_passphrase(&client);
        let page = client.get_events(Some(1001), &[], None, 10).await.expect("tip == cursor is accepted");
        assert!(page.events.is_empty());
        assert_eq!(method_count(&log, "getEvents"), 1);
    }

    #[tokio::test]
    async fn primary_probe_runs_at_most_once_per_interval() {
        // #722: a failed probe still resets the timer. Twenty calls while the
        // primary is still down must produce one getHealth, not twenty.
        let (primary, log) = spawn_method_mock(responder(|_| (500, String::new()))).await;
        let (secondary, _) = spawn_method_mock(responder(|_| (200, latest_body(2000)))).await;

        let client = RpcClient::with_urls(vec![primary, secondary], 5);
        force_failover_state(&client, 1, Instant::now() - PRIMARY_PROBE_INTERVAL - Duration::from_secs(1));
        for _ in 0..20 {
            client.get_latest_ledger().await.expect("secondary serves");
        }
        assert_eq!(method_count(&log, "getHealth"), 1);
        assert_eq!(client.active_url_idx.load(Ordering::Relaxed), 1, "still failed over");
    }

    #[tokio::test]
    async fn http_200_with_rpc_error_is_not_failed_back_to() {
        // #722: an HTTP 200 whose body is a JSON-RPC error is not a healthy primary.
        let err_body = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"boom"}}"#;
        let (primary, _) = spawn_method_mock(responder(move |_| (200, err_body.to_string()))).await;
        let (secondary, _) = spawn_method_mock(responder(|m| match m {
            "getNetwork" => (200, network_body()),
            _ => (200, latest_body(2000)),
        }))
        .await;

        let client = RpcClient::with_urls(vec![primary, secondary], 5);
        pin_passphrase(&client);
        force_failover_state(&client, 1, Instant::now() - PRIMARY_PROBE_INTERVAL - Duration::from_secs(1));
        client.get_latest_ledger().await.expect("secondary serves");
        assert_eq!(client.active_url_idx.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn unhealthy_primary_status_is_not_failed_back_to() {
        let (primary, _) = spawn_method_mock(responder(|m| match m {
            "getHealth" => (200, health_body("unhealthy", 2000)),
            "getNetwork" => (200, network_body()),
            _ => (200, latest_body(2000)),
        }))
        .await;
        let (secondary, _) = spawn_method_mock(responder(|m| match m {
            "getNetwork" => (200, network_body()),
            _ => (200, latest_body(2000)),
        }))
        .await;

        let client = RpcClient::with_urls(vec![primary, secondary], 5);
        pin_passphrase(&client);
        force_failover_state(&client, 1, Instant::now() - PRIMARY_PROBE_INTERVAL - Duration::from_secs(1));
        client.get_latest_ledger().await.expect("secondary serves");
        assert_eq!(client.active_url_idx.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn healthy_primary_fails_back() {
        let (primary, _) = spawn_method_mock(responder(|m| match m {
            "getHealth" => (200, health_body("healthy", 2000)),
            "getNetwork" => (200, network_body()),
            _ => (200, latest_body(2000)),
        }))
        .await;
        let (secondary, _) = spawn_method_mock(responder(|_| (200, latest_body(2000)))).await;

        let client = RpcClient::with_urls(vec![primary, secondary], 5);
        pin_passphrase(&client);
        force_failover_state(&client, 1, Instant::now() - PRIMARY_PROBE_INTERVAL - Duration::from_secs(1));
        assert_eq!(client.get_latest_ledger().await.expect("primary serves"), 2000);
        assert_eq!(client.active_url_idx.load(Ordering::Relaxed), 0, "fails back to primary");
    }

    // ── Method-routing mock (#715–#718) ───────────────────────────────────

    type RequestLog = Arc<std::sync::Mutex<Vec<serde_json::Value>>>;
    type RpcResponder = Arc<dyn Fn(&str, usize, &serde_json::Value) -> (u16, String) + Send + Sync + 'static>;

    /// Mock RPC that routes on the JSON-RPC `method`. `respond` receives the
    /// method, how many times it has been called before (0-based) and the whole
    /// request, and returns `(http_status, body)`. Every request is logged.
    async fn spawn_rpc_mock(respond: RpcResponder) -> (String, RequestLog) {
        #[derive(Clone)]
        struct S {
            respond: RpcResponder,
            log: RequestLog,
            counts: Arc<std::sync::Mutex<std::collections::HashMap<String, usize>>>,
        }
        async fn handler(State(s): State<S>, body: axum::body::Bytes) -> axum::response::Response {
            let req: serde_json::Value = serde_json::from_slice(&body).expect("json-rpc request");
            let method = req["method"].as_str().unwrap_or_default().to_string();
            let nth = {
                let mut counts = s.counts.lock().unwrap();
                let entry = counts.entry(method.clone()).or_insert(0);
                let nth = *entry;
                *entry += 1;
                nth
            };
            s.log.lock().unwrap().push(req.clone());
            let (status, text) = (s.respond)(&method, nth, &req);
            axum::response::Response::builder()
                .status(status)
                .header("content-type", "application/json")
                .body(axum::body::Body::from(text))
                .unwrap()
        }
        let log: RequestLog = Arc::new(std::sync::Mutex::new(Vec::new()));
        let state = S {
            respond,
            log: log.clone(),
            counts: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        };
        let app = Router::new().route("/", post(handler)).with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), log)
    }

    fn methods_called(log: &RequestLog) -> Vec<String> {
        log.lock()
            .unwrap()
            .iter()
            .map(|r| r["method"].as_str().unwrap_or_default().to_string())
            .collect()
    }

    const HEALTH_2000_1500: &str = r#"{"jsonrpc":"2.0","id":1,"result":{"status":"healthy","latestLedger":2000,"oldestLedger":1500,"ledgerRetentionWindow":500}}"#;
    const HEALTH_NO_OLDEST: &str = r#"{"jsonrpc":"2.0","id":1,"result":{"status":"healthy","latestLedger":1000}}"#;
    const ERR_32601_HEALTH: &str = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"Method not found"}}"#;
    const ERR_OUT_OF_RETENTION: &str = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"startLedger must be between the oldest ledger: 1500 and the latest ledger: 2000"}}"#;
    const EMPTY_EVENTS: &str = r#"{"jsonrpc":"2.0","id":1,"result":{"latestLedger":2000,"events":[],"cursor":null}}"#;

    // #718: one getHealth per cycle supplies both the tip and the retention floor.

    #[tokio::test]
    async fn tip_snapshot_uses_a_single_get_health_call() {
        let (url, log) = spawn_rpc_mock(Arc::new(|method: &str, _n: usize, _r: &serde_json::Value| match method {
            "getHealth" => (200, HEALTH_2000_1500.to_string()),
            other => (500, format!("unexpected method {other}")),
        }))
        .await;

        let client = RpcClient::new(&url, 5);
        let snap = client.get_tip_snapshot().await.expect("snapshot");
        assert_eq!(snap, TipSnapshot { latest: 2000, oldest_available: Some(1500) });
        assert_eq!(methods_called(&log), vec!["getHealth"], "exactly one tip-related call per cycle");
    }

    #[tokio::test]
    async fn tip_snapshot_falls_back_to_get_latest_ledger_when_health_is_unsupported() {
        let (url, log) = spawn_rpc_mock(Arc::new(|method: &str, _n: usize, _r: &serde_json::Value| match method {
            "getHealth" => (200, ERR_32601_HEALTH.to_string()),
            "getLatestLedger" => (200, OK_SEQ_1000.to_string()),
            other => (500, format!("unexpected method {other}")),
        }))
        .await;

        let client = RpcClient::new(&url, 5);
        let snap = client.get_tip_snapshot().await.expect("snapshot");
        assert_eq!(snap, TipSnapshot { latest: 1000, oldest_available: None });
        assert_eq!(methods_called(&log), vec!["getHealth", "getLatestLedger"]);
    }

    #[tokio::test]
    async fn tip_snapshot_without_oldest_ledger_does_not_make_a_second_call() {
        let (url, log) = spawn_rpc_mock(Arc::new(|method: &str, _n: usize, _r: &serde_json::Value| match method {
            "getHealth" => (200, HEALTH_NO_OLDEST.to_string()),
            other => (500, format!("unexpected method {other}")),
        }))
        .await;

        let client = RpcClient::new(&url, 5);
        let snap = client.get_tip_snapshot().await.expect("snapshot");
        assert_eq!(snap, TipSnapshot { latest: 1000, oldest_available: None });
        assert_eq!(methods_called(&log), vec!["getHealth"]);
    }

    // #716: an out-of-retention startLedger is a timing race, not a permanent error.

    #[tokio::test]
    async fn out_of_retention_start_ledger_is_not_treated_as_permanent() {
        let (url, log) = spawn_rpc_mock(Arc::new(|_m: &str, _n: usize, _r: &serde_json::Value| {
            (200, ERR_OUT_OF_RETENTION.to_string())
        }))
        .await;

        let client = RpcClient::new(&url, 5);
        let err = client
            .get_events(Some(10), &[], None, 10)
            .await
            .expect_err("range error is surfaced to the caller");
        assert!(is_out_of_retention(&err));
        assert!(
            !is_non_retryable_rpc_error(&err),
            "must not count towards the permanent-error budget"
        );
        assert_eq!(methods_called(&log).len(), 1, "a range error is not retried on the same URL");
    }

    #[tokio::test]
    async fn other_invalid_params_stay_permanent() {
        let err_contract = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid contract id"}}"#;
        let (url, _log) = spawn_rpc_mock(Arc::new(move |_m: &str, _n: usize, _r: &serde_json::Value| {
            (200, err_contract.to_string())
        }))
        .await;

        let client = RpcClient::new(&url, 5);
        let err = client.get_events(Some(10), &[], None, 10).await.expect_err("bad contract id");
        assert!(is_non_retryable_rpc_error(&err));
        assert!(!is_out_of_retention(&err));
    }

    #[tokio::test]
    async fn get_events_bounded_sends_end_ledger_only_with_start_ledger() {
        let (url, log) = spawn_rpc_mock(Arc::new(|_m: &str, _n: usize, _r: &serde_json::Value| {
            (200, EMPTY_EVENTS.to_string())
        }))
        .await;
        let client = RpcClient::new(&url, 5);

        client.get_events_bounded(Some(10), Some(21), &[], None, 10).await.unwrap();
        client
            .get_events_bounded(Some(10), Some(21), &[], Some("cursor-1".into()), 10)
            .await
            .unwrap();

        let requests = log.lock().unwrap().clone();
        let first = &requests[0]["params"];
        assert_eq!(first["startLedger"], 10);
        assert_eq!(first["endLedger"], 21, "first page is bounded at the tip");
        let continuation = &requests[1]["params"];
        assert!(continuation.get("startLedger").is_none(), "cursor pages carry no startLedger");
        assert!(continuation.get("endLedger").is_none(), "endLedger is not sent with a cursor");
    }
}
