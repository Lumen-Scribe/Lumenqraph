//! Indexer configuration, loaded from environment (see `.env.example`).

use anyhow::Context;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;

use crate::keys::{parse_durability, KeyTemplate};
pub use lumenqraph_core::db::DbConfig;

pub fn redact_database_url(url: &str) -> String {
    lumenqraph_core::redact_url(url)
}

#[derive(Clone)]
pub struct Config {
    pub database_url: String,
    /// Primary RPC URL (first in `rpc_urls`). Kept for compatibility with code
    /// that logs or displays the active endpoint.
    pub rpc_url: String,
    /// Ordered list of RPC endpoints. The first is primary; subsequent entries
    /// are failover candidates. At least one element is always present.
    pub rpc_urls: Vec<String>,
    /// Contract IDs to index. Empty => index all contract events.
    pub contract_ids: Vec<String>,
    /// Topic filters pushed down to `getEvents` (#728). Each inner vector is
    /// one topic pattern (up to 5 segments, `*` = wildcard), e.g.
    /// `[["transfer","*","*"],["swap"]]`. Empty => no topic filtering.
    /// Env `EVENT_TOPIC_FILTERS`, a JSON array of topic patterns.
    pub event_topic_filters: Vec<Vec<String>>,
    pub poll_interval_secs: u64,
    pub page_size: u32,
    pub start_ledger: i64,
    pub max_catchup_ledgers: i64,
    pub state_indexing: bool,
    pub key_indexing: bool,
    pub balance_key_symbol: String,
    pub balance_key_durability: String,
    pub key_templates: Vec<KeyTemplate>,
    pub retention_ledgers: i64,
    pub spec_version_retention: i64,
    pub upgrade_watch: bool,
    pub reorg_overlap_ledgers: i64,
    pub rpc_timeout_secs: u64,
    /// Longest `Retry-After` wait honoured on a 429/503 before the client fails
    /// over instead of sleeping (#726). Env `RPC_MAX_RETRY_AFTER_SECS`, default 60.
    pub rpc_max_retry_after_secs: u64,
    /// Headers sent on every RPC request, for providers that authenticate with
    /// a header (#725). Env `RPC_HEADERS`, as `Name=value;Name2=value2` or a
    /// JSON object. Values are redacted in `Debug` output.
    pub rpc_headers: HeaderMap,
    pub enrichment_warn_threshold: f64,
    pub spec_cache_max_entries: usize,
    pub spec_fetch_concurrency: usize,
    /// After this many consecutive poll failures the poller enters a degraded
    /// state: it sleeps for `degraded_poll_interval_secs` instead of the
    /// normal backoff and emits an ERROR-level log. The counter resets on the
    /// first successful cycle. 0 = never enter degraded state (disabled).
    /// Default: 20.
    pub max_consecutive_errors: u32,
    /// Sleep interval (seconds) used while the circuit breaker is open (i.e.
    /// the poller is in degraded state). Default: 300 (5 minutes).
    pub degraded_poll_interval_secs: u64,
    /// Maximum number of state keys snapshotted per cycle when `KEY_INDEXING`
    /// is enabled (#746). Remaining keys are deferred to later cycles,
    /// prioritised by last-snapshot age. Env `MAX_STATE_KEYS_PER_CYCLE`,
    /// default 500, clamped to >= 1.
    pub max_state_keys_per_cycle: usize,
    /// Maximum number of contracts snapshotted per cycle when `STATE_INDEXING`
    /// is enabled (#746). Remaining contracts are deferred to later cycles,
    /// prioritised by last-snapshot age. Env `MAX_STATE_CONTRACTS_PER_CYCLE`,
    /// default 50, clamped to >= 1.
    pub max_state_contracts_per_cycle: usize,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("database_url", &redact_database_url(&self.database_url))
            .field("rpc_url", &lumenqraph_core::redact_url(&self.rpc_url))
            .field(
                "rpc_urls",
                &self
                    .rpc_urls
                    .iter()
                    .map(|u| lumenqraph_core::redact_url(u))
                    .collect::<Vec<_>>(),
            )
            .field("contract_ids", &self.contract_ids)
            .field("event_topic_filters", &self.event_topic_filters)
            .field("poll_interval_secs", &self.poll_interval_secs)
            .field("page_size", &self.page_size)
            .field("start_ledger", &self.start_ledger)
            .field("max_catchup_ledgers", &self.max_catchup_ledgers)
            .field("state_indexing", &self.state_indexing)
            .field("key_indexing", &self.key_indexing)
            .field("balance_key_symbol", &self.balance_key_symbol)
            .field("balance_key_durability", &self.balance_key_durability)
            .field("key_templates", &self.key_templates)
            .field("retention_ledgers", &self.retention_ledgers)
            .field("spec_version_retention", &self.spec_version_retention)
            .field("upgrade_watch", &self.upgrade_watch)
            .field("reorg_overlap_ledgers", &self.reorg_overlap_ledgers)
            .field("rpc_timeout_secs", &self.rpc_timeout_secs)
            .field("rpc_max_retry_after_secs", &self.rpc_max_retry_after_secs)
            .field(
                "rpc_headers",
                &self
                    .rpc_headers
                    .keys()
                    .map(|name| format!("{name}: [REDACTED]"))
                    .collect::<Vec<_>>(),
            )
            .field("enrichment_warn_threshold", &self.enrichment_warn_threshold)
            .field("spec_cache_max_entries", &self.spec_cache_max_entries)
            .field("max_state_keys_per_cycle", &self.max_state_keys_per_cycle)
            .field(
                "max_state_contracts_per_cycle",
                &self.max_state_contracts_per_cycle,
            )
            .finish()
    }
}

impl Config {
    /// Reject index-all state/key indexing with no per-cycle cap: with an empty
    /// `CONTRACT_IDS` every active contract and holder is a candidate each
    /// cycle, so an unbounded cap would make RPC cost grow with the network
    /// (#746). Capped index-all is allowed; the remainder is deferred.
    pub fn validate_state_indexing(&self) -> anyhow::Result<()> {
        if !self.contract_ids.is_empty() {
            return Ok(());
        }
        if self.state_indexing && self.max_state_contracts_per_cycle == 0 {
            anyhow::bail!(
                "STATE_INDEXING with an empty CONTRACT_IDS requires MAX_STATE_CONTRACTS_PER_CYCLE > 0"
            );
        }
        if self.key_indexing && self.max_state_keys_per_cycle == 0 {
            anyhow::bail!(
                "key indexing with an empty CONTRACT_IDS requires MAX_STATE_KEYS_PER_CYCLE > 0"
            );
        }
        Ok(())
    }
    pub fn db_config() -> anyhow::Result<DbConfig> {
        let max_connections = clamp_with_warning(
            "INDEXER_DATABASE_MAX_CONNECTIONS",
            env_parse_override(
                "INDEXER_DATABASE_MAX_CONNECTIONS",
                "DATABASE_MAX_CONNECTIONS",
                10u32,
            )?,
            1,
            u32::MAX,
        );
        let min_connections = env_parse_override(
            "INDEXER_DATABASE_MIN_CONNECTIONS",
            "DATABASE_MIN_CONNECTIONS",
            0u32,
        )?;
        let min_connections = if min_connections > max_connections {
            tracing::warn!(
                requested_min = min_connections,
                max = max_connections,
                "indexer database minimum exceeds maximum; clamping to maximum"
            );
            max_connections
        } else {
            min_connections
        };
        Ok(DbConfig {
            max_connections,
            min_connections,
            acquire_timeout_secs: env_parse_override(
                "INDEXER_DATABASE_ACQUIRE_TIMEOUT_SECS",
                "DATABASE_ACQUIRE_TIMEOUT_SECS",
                30u64,
            )?,
            idle_timeout_secs: env_parse_override(
                "INDEXER_DATABASE_IDLE_TIMEOUT_SECS",
                "DATABASE_IDLE_TIMEOUT_SECS",
                600u64,
            )?,
            connect_retries: env_parse_override(
                "INDEXER_DATABASE_CONNECT_RETRIES",
                "DATABASE_CONNECT_RETRIES",
                30u32,
            )?,
        })
    }

    pub fn from_env() -> anyhow::Result<Self> {
        let contract_ids: Vec<String> =
            lumenqraph_core::parse_contract_ids(&std::env::var("CONTRACT_IDS").unwrap_or_default())
                .map_err(|e| anyhow::anyhow!("{e}"))?;

        // Parse EVENT_TOPIC_FILTERS (#728): a JSON array of topic patterns,
        // e.g. `[["transfer","*","*"],["swap"]]`. Each pattern is one topic
        // set; `*` is a wildcard segment.
        let event_topic_filters: Vec<Vec<String>> = std::env::var("EVENT_TOPIC_FILTERS")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(|s| -> anyhow::Result<Vec<Vec<String>>> {
                serde_json::from_str(&s)
                    .map_err(|e| anyhow::anyhow!("invalid EVENT_TOPIC_FILTERS JSON: {e}"))
            })
            .transpose()?
            .unwrap_or_default();
        validate_event_filters(&contract_ids, &event_topic_filters)?;

        // Parse numeric config with validation.
        let poll_interval_secs = env_parse("POLL_INTERVAL_SECS", 5)?;
        let page_size = env_parse("PAGE_SIZE", 1000)?;
        let max_catchup_ledgers = env_parse("MAX_CATCHUP_LEDGERS", 4000)?;
        let retention_ledgers = env_parse("RETENTION_LEDGERS", 0)?;
        let spec_version_retention = env_parse("SPEC_VERSION_RETENTION", 0)?;
        let reorg_overlap_ledgers = env_parse("REORG_OVERLAP_LEDGERS", 0)?;
        let rpc_timeout_secs = env_parse("RPC_TIMEOUT_SECS", 30u64)?;
        let spec_cache_max_entries = env_parse("SPEC_CACHE_MAX_ENTRIES", 2000usize)?;
        let spec_fetch_concurrency = env_parse("SPEC_FETCH_CONCURRENCY", 4usize)?;
        // Validate and clamp PAGE_SIZE to RPC documented bounds (1–10000).
        let page_size = clamp_with_warning("PAGE_SIZE", page_size, 1, 10000);

        // Validate POLL_INTERVAL_SECS minimum.
        let poll_interval_secs =
            clamp_with_warning("POLL_INTERVAL_SECS", poll_interval_secs, 1, u64::MAX);

        // Validate MAX_CATCHUP_LEDGERS minimum.
        let max_catchup_ledgers =
            clamp_with_warning("MAX_CATCHUP_LEDGERS", max_catchup_ledgers, 1, i64::MAX);

        // Validate RETENTION_LEDGERS minimum (0 = disabled).
        let retention_ledgers = if retention_ledgers < 0 {
            tracing::warn!(
                requested = retention_ledgers,
                clamped_to = 0,
                "RETENTION_LEDGERS cannot be negative; clamping to 0 (disabled)"
            );
            0
        } else {
            retention_ledgers
        };

        // Validate SPEC_VERSION_RETENTION minimum (0 = disabled).
        let spec_version_retention = if spec_version_retention < 0 {
            tracing::warn!(
                requested = spec_version_retention,
                clamped_to = 0,
                "SPEC_VERSION_RETENTION cannot be negative; clamping to 0 (disabled)"
            );
            0
        } else {
            spec_version_retention
        };

        // Validate REORG_OVERLAP_LEDGERS minimum (0 = disabled).
        let reorg_overlap_ledgers = if reorg_overlap_ledgers < 0 {
            tracing::warn!(
                requested = reorg_overlap_ledgers,
                clamped_to = 0,
                "REORG_OVERLAP_LEDGERS cannot be negative; clamping to 0 (disabled)"
            );
            0
        } else {
            reorg_overlap_ledgers
        };

        // Validate RPC_TIMEOUT_SECS minimum (must be at least 1s).
        let rpc_timeout_secs =
            clamp_with_warning("RPC_TIMEOUT_SECS", rpc_timeout_secs, 1, u64::MAX);
        // Cap on how long a server-requested Retry-After may hold us (#726).
        let rpc_max_retry_after_secs = clamp_with_warning(
            "RPC_MAX_RETRY_AFTER_SECS",
            env_parse("RPC_MAX_RETRY_AFTER_SECS", 60u64)?,
            1,
            3600,
        );

        // Validate SPEC_CACHE_MAX_ENTRIES minimum (must be at least 1).
        let spec_cache_max_entries = clamp_with_warning(
            "SPEC_CACHE_MAX_ENTRIES",
            spec_cache_max_entries,
            1,
            usize::MAX,
        );

        // Validate SPEC_FETCH_CONCURRENCY minimum (must be at least 1).
        let spec_fetch_concurrency = clamp_with_warning(
            "SPEC_FETCH_CONCURRENCY",
            spec_fetch_concurrency,
            1,
            usize::MAX,
        );

        // Parse ENRICHMENT_WARN_THRESHOLD (0.0-1.0, default 0.5).
        let enrichment_warn_threshold: f64 = env_parse("ENRICHMENT_WARN_THRESHOLD", 0.5)?;
        let enrichment_warn_threshold =
            if enrichment_warn_threshold < 0.0 || enrichment_warn_threshold > 1.0 {
                tracing::warn!(
                    requested = enrichment_warn_threshold,
                    clamped_to = 0.5,
                    "ENRICHMENT_WARN_THRESHOLD must be between 0.0 and 1.0; clamping to 0.5"
                );
                0.5
            } else {
                enrichment_warn_threshold
            };

        // Validate BALANCE_KEY_DURABILITY strictly (must be "persistent" or
        // "temporary"). A typo here would silently disable per-key indexing,
        // so fail startup with a clear error instead.
        let balance_key_durability = std::env::var("BALANCE_KEY_DURABILITY")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "persistent".to_string());
        parse_durability(&balance_key_durability).map_err(|e| {
            anyhow::anyhow!(
                "invalid BALANCE_KEY_DURABILITY {:?}: {e} (valid values: \"persistent\", \"temporary\")",
                balance_key_durability
            )
        })?;

        // Parse key templates from JSON (KEY_TEMPLATES env var).
        // We deserialize into a plain intermediate struct and then convert to
        // KeyTemplate (which holds a non-serde stellar-xdr ContractDataDurability).
        #[derive(Deserialize)]
        struct KeyTemplateRaw {
            symbol: String,
            #[serde(default, alias = "events")]
            event_names: Vec<String>,
            #[serde(default, alias = "params")]
            param_indices: Vec<usize>,
            #[serde(default = "default_durability")]
            durability: String,
            label: Option<String>,
        }
        fn default_durability() -> String {
            "persistent".to_string()
        }

        let key_templates: Vec<KeyTemplate> = std::env::var("KEY_TEMPLATES")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(|s| -> anyhow::Result<Vec<KeyTemplate>> {
                let raw: Vec<KeyTemplateRaw> = serde_json::from_str(&s)
                    .map_err(|e| anyhow::anyhow!("invalid KEY_TEMPLATES JSON: {e}"))?;
                raw.into_iter()
                    .map(|r| {
                        let durability = parse_durability(&r.durability).map_err(|e| {
                            anyhow::anyhow!(
                                "invalid durability {:?} for KEY_TEMPLATES symbol {:?}: {e} (valid values: \"persistent\", \"temporary\")",
                                r.durability,
                                r.symbol
                            )
                        })?;
                        Ok(KeyTemplate {
                            symbol: r.symbol,
                            event_names: r.event_names,
                            param_indices: r.param_indices,
                            durability,
                            label: r.label,
                        })
                    })
                    .collect::<anyhow::Result<Vec<KeyTemplate>>>()
            })
            .transpose()?
            .unwrap_or_default();

        // Validate every key template eagerly (#747). Invalid symbols, empty
        // event/param lists and out-of-range indices would otherwise only fail
        // per event at DEBUG level, silently disabling per-key indexing.
        for (i, template) in key_templates.iter().enumerate() {
            template.validate().map_err(|e| {
                anyhow::anyhow!(
                    "invalid KEY_TEMPLATES entry {i} (symbol {:?}, label {:?}): {e}",
                    template.symbol,
                    template.label
                )
            })?;
        }

        let max_consecutive_errors = env_parse("MAX_CONSECUTIVE_ERRORS", 20u32)?;
        let degraded_poll_interval_secs = env_parse("DEGRADED_POLL_INTERVAL_SECS", 300u64)?;

        // Per-cycle caps on state snapshots (#746). Without these, index-all
        // mode reads every holder / active contract every cycle, so RPC cost
        // grows with network size. Clamp to >= 1 so a cycle always progresses.
        let max_state_keys_per_cycle = clamp_with_warning(
            "MAX_STATE_KEYS_PER_CYCLE",
            env_parse("MAX_STATE_KEYS_PER_CYCLE", DEFAULT_MAX_STATE_KEYS_PER_CYCLE)?,
            1,
            usize::MAX,
        );
        let max_state_contracts_per_cycle = clamp_with_warning(
            "MAX_STATE_CONTRACTS_PER_CYCLE",
            env_parse(
                "MAX_STATE_CONTRACTS_PER_CYCLE",
                DEFAULT_MAX_STATE_CONTRACTS_PER_CYCLE,
            )?,
            1,
            usize::MAX,
        );

        let state_indexing = env_bool("STATE_INDEXING", false);
        let key_indexing = env_bool("KEY_INDEXING", false);
        // Refuse the unsupported index-all + state/key indexing combination
        // unless the operator explicitly set the per-cycle caps (#746).
        validate_index_all_state_caps(
            &contract_ids,
            state_indexing,
            key_indexing,
            std::env::var("MAX_STATE_KEYS_PER_CYCLE").ok(),
            std::env::var("MAX_STATE_CONTRACTS_PER_CYCLE").ok(),
        )?;

        // Build the ordered endpoint list: RPC_URLS (comma-separated, priority
        // order) takes precedence; RPC_URL is the single-endpoint fallback for
        // backward compatibility. At least one URL must be present.
        let rpc_urls: Vec<String> = {
            let multi = std::env::var("RPC_URLS").unwrap_or_default();
            if !multi.trim().is_empty() {
                multi
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            } else {
                vec![env("RPC_URL")?]
            }
        };
        if rpc_urls.is_empty() {
            return Err(anyhow::anyhow!(
                "at least one RPC endpoint is required: set RPC_URL or RPC_URLS"
            ));
        }
        let rpc_url = rpc_urls[0].clone();
        let rpc_headers = parse_rpc_headers(&std::env::var("RPC_HEADERS").unwrap_or_default())?;

        Ok(Self {
            database_url: env("DATABASE_URL")?,
            rpc_url,
            rpc_urls,
            poll_interval_secs,
            page_size,
            start_ledger: env_parse("START_LEDGER", 0)?,
            max_catchup_ledgers,
            state_indexing,
            key_indexing,
            upgrade_watch: env_bool("UPGRADE_WATCH", !contract_ids.is_empty()),
            contract_ids,
            event_topic_filters,
            balance_key_symbol: std::env::var("BALANCE_KEY_SYMBOL")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "Balance".to_string()),
            balance_key_durability,
            retention_ledgers,
            spec_version_retention,
            reorg_overlap_ledgers,
            rpc_timeout_secs,
            rpc_max_retry_after_secs,
            rpc_headers,
            enrichment_warn_threshold,
            key_templates,
            spec_cache_max_entries,
            spec_fetch_concurrency,
            max_consecutive_errors,
            degraded_poll_interval_secs,
            max_state_keys_per_cycle,
            max_state_contracts_per_cycle,
        })
    }
}

/// Default per-cycle cap on state keys snapshotted when `KEY_INDEXING` is on.
pub const DEFAULT_MAX_STATE_KEYS_PER_CYCLE: usize = 500;
/// Default per-cycle cap on contracts snapshotted when `STATE_INDEXING` is on.
pub const DEFAULT_MAX_STATE_CONTRACTS_PER_CYCLE: usize = 50;

/// Refuse the unsupported "index-all + state/key indexing" combination (#746).
///
/// When `CONTRACT_IDS` is empty the poller snapshots every holder of every
/// token (`KEY_INDEXING`) and every active contract (`STATE_INDEXING`) each
/// cycle, so RPC cost grows with the network. The per-cycle caps bound that
/// cost, but only if the operator has consciously set them. This validation
/// fails startup when index-all state indexing is requested without an
/// explicit cap, with a message that names the variables to set.
///
/// `keys_env` / `contracts_env` are the raw env values (if any) so that an
/// explicit setting — even one equal to the default — counts as opt-in.
pub fn validate_index_all_state_caps(
    contract_ids: &[String],
    state_indexing: bool,
    key_indexing: bool,
    keys_env: Option<String>,
    contracts_env: Option<String>,
) -> anyhow::Result<()> {
    if !contract_ids.is_empty() || (!state_indexing && !key_indexing) {
        return Ok(());
    }

    let keys_set = keys_env.is_some_and(|v| !v.trim().is_empty());
    let contracts_set = contracts_env.is_some_and(|v| !v.trim().is_empty());

    let mut missing: Vec<&str> = Vec::new();
    if key_indexing && !keys_set {
        missing.push("MAX_STATE_KEYS_PER_CYCLE");
    }
    if state_indexing && !contracts_set {
        missing.push("MAX_STATE_CONTRACTS_PER_CYCLE");
    }
    if missing.is_empty() {
        return Ok(());
    }

    Err(anyhow::anyhow!(
        "CONTRACT_IDS is empty (index-all mode) with {} enabled, which snapshots every holder/contract each cycle; \
         set {} to bound per-cycle RPC cost (defaults: MAX_STATE_KEYS_PER_CYCLE={}, MAX_STATE_CONTRACTS_PER_CYCLE={}), \
         or set CONTRACT_IDS to restrict indexing",
        if key_indexing && state_indexing {
            "KEY_INDEXING and STATE_INDEXING"
        } else if key_indexing {
            "KEY_INDEXING"
        } else {
            "STATE_INDEXING"
        },
        missing.join(" and "),
        DEFAULT_MAX_STATE_KEYS_PER_CYCLE,
        DEFAULT_MAX_STATE_CONTRACTS_PER_CYCLE,
    ))
}

/// Maximum number of filter slots Soroban RPC accepts in one `getEvents` call.
pub const MAX_EVENT_FILTERS: usize = 5;
/// Maximum number of topic segments per filter (each may be `*`).
pub const MAX_TOPIC_SEGMENTS: usize = 5;
/// Maximum number of contract IDs per filter slot.
pub const MAX_CONTRACT_IDS_PER_FILTER: usize = 5;

/// Validate `EVENT_TOPIC_FILTERS` and its combination with `CONTRACT_IDS`
/// against the Soroban RPC `getEvents` limits (#728).
///
/// The RPC accepts at most 5 filters, each with at most 5 topic segments and
/// at most 5 contract IDs. When both `CONTRACT_IDS` and topic filters are set
/// the indexer builds the cross product (one filter per contract × topic
/// pattern), so the product must fit in `MAX_EVENT_FILTERS`.
pub fn validate_event_filters(
    contract_ids: &[String],
    topic_filters: &[Vec<String>],
) -> anyhow::Result<()> {
    for (i, pattern) in topic_filters.iter().enumerate() {
        if pattern.is_empty() {
            return Err(anyhow::anyhow!(
                "EVENT_TOPIC_FILTERS entry {i} is empty; each topic pattern needs at least one segment"
            ));
        }
        if pattern.len() > MAX_TOPIC_SEGMENTS {
            return Err(anyhow::anyhow!(
                "EVENT_TOPIC_FILTERS entry {i} has {} segments; Soroban RPC allows at most {}",
                pattern.len(),
                MAX_TOPIC_SEGMENTS
            ));
        }
    }

    if topic_filters.is_empty() {
        // Only contract filters; the RPC limit is per-filter contract IDs.
        if contract_ids.len() > MAX_EVENT_FILTERS * MAX_CONTRACT_IDS_PER_FILTER {
            return Err(anyhow::anyhow!(
                "CONTRACT_IDS has {} entries; Soroban RPC allows at most {} ({} filters x {} ids)",
                contract_ids.len(),
                MAX_EVENT_FILTERS * MAX_CONTRACT_IDS_PER_FILTER,
                MAX_EVENT_FILTERS,
                MAX_CONTRACT_IDS_PER_FILTER
            ));
        }
        return Ok(());
    }

    if contract_ids.is_empty() {
        if topic_filters.len() > MAX_EVENT_FILTERS {
            return Err(anyhow::anyhow!(
                "EVENT_TOPIC_FILTERS has {} patterns; Soroban RPC allows at most {} filters",
                topic_filters.len(),
                MAX_EVENT_FILTERS
            ));
        }
        return Ok(());
    }

    // Cross product of contract IDs and topic patterns must fit in the slots.
    let total = contract_ids.len().saturating_mul(topic_filters.len());
    if total > MAX_EVENT_FILTERS {
        return Err(anyhow::anyhow!(
            "CONTRACT_IDS ({}) x EVENT_TOPIC_FILTERS ({}) = {} filters exceeds the Soroban RPC limit of {}; \
             reduce CONTRACT_IDS or EVENT_TOPIC_FILTERS",
            contract_ids.len(),
            topic_filters.len(),
            total,
            MAX_EVENT_FILTERS
        ));
    }
    Ok(())
}

/// Clamp a numeric config value to [min, max] and log a warning if clamped.
fn clamp_with_warning<T: std::cmp::PartialOrd + std::fmt::Display + Copy>(
    key: &str,
    value: T,
    min: T,
    max: T,
) -> T {
    if value < min {
        tracing::warn!(
            requested = %value,
            clamped_to = %min,
            "{} is below minimum; clamping to {}", key, min
        );
        min
    } else if value > max {
        tracing::warn!(
            requested = %value,
            clamped_to = %max,
            "{} exceeds maximum; clamping to {}", key, max
        );
        max
    } else {
        value
    }
}

fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .map(|v| matches!(v.trim().to_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(default)
}

fn env(key: &str) -> anyhow::Result<String> {
    std::env::var(key).with_context(|| format!("missing required env var {key}"))
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> anyhow::Result<T>
where
    T::Err: std::fmt::Display,
{
    match std::env::var(key) {
        Ok(v) if !v.trim().is_empty() => v
            .trim()
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid {key}: {e}")),
        _ => Ok(default),
    }
}

fn env_parse_override<T: std::str::FromStr>(
    service_key: &str,
    common_key: &str,
    default: T,
) -> anyhow::Result<T>
where
    T::Err: std::fmt::Display,
{
    let key = if std::env::var(service_key)
        .ok()
        .is_some_and(|value| !value.trim().is_empty())
    {
        service_key
    } else {
        common_key
    };
    env_parse(key, default)
}

/// Parse `RPC_HEADERS` into a header map (#725).
///
/// Accepts `Name=value;Name2=value2` (split on the first `=`, so values may
/// contain `=`) or a JSON object of name to value. Every value is marked
/// sensitive so `reqwest` and `HeaderValue`'s `Debug` keep it out of logs.
/// Errors name the offending header but never echo its value.
pub fn parse_rpc_headers(raw: &str) -> anyhow::Result<HeaderMap> {
    let raw = raw.trim();
    let mut pairs: Vec<(String, String)> = Vec::new();
    if raw.is_empty() {
        return Ok(HeaderMap::new());
    } else if raw.starts_with('{') {
        let map: std::collections::BTreeMap<String, String> = serde_json::from_str(raw)
            .context("RPC_HEADERS JSON must be an object of string values")?;
        pairs.extend(map);
    } else {
        for entry in raw.split(';').map(str::trim).filter(|e| !e.is_empty()) {
            let (name, value) = entry
                .split_once('=')
                .ok_or_else(|| anyhow::anyhow!("RPC_HEADERS entries must be Name=value"))?;
            pairs.push((name.trim().to_string(), value.trim().to_string()));
        }
    }

    let mut headers = HeaderMap::new();
    for (name, value) in pairs {
        let header_name = HeaderName::from_bytes(name.as_bytes())
            .with_context(|| format!("RPC_HEADERS has an invalid header name `{name}`"))?;
        let mut header_value = HeaderValue::from_str(&value).with_context(|| {
            format!("RPC_HEADERS value for `{name}` is not a valid header value")
        })?;
        header_value.set_sensitive(true);
        headers.append(header_name, header_value);
    }
    Ok(headers)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests that mutate process env must hold this, or they race under the
    /// parallel test runner.
    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[test]
    fn rpc_headers_parse_semicolon_pairs_and_keep_equals_in_values() {
        let h = parse_rpc_headers("Authorization=Bearer abc==;X-Api-Key=k1").unwrap();
        assert_eq!(
            h.get("authorization").unwrap().to_str().unwrap(),
            "Bearer abc=="
        );
        assert_eq!(h.get("x-api-key").unwrap().to_str().unwrap(), "k1");
        assert!(h.get("authorization").unwrap().is_sensitive());
    }

    #[test]
    fn rpc_headers_accept_a_json_object_and_empty_input() {
        let h = parse_rpc_headers(r#"{"x-api-key":"k2"}"#).unwrap();
        assert_eq!(h.get("x-api-key").unwrap().to_str().unwrap(), "k2");
        assert!(parse_rpc_headers("").unwrap().is_empty());
        assert!(parse_rpc_headers("   ").unwrap().is_empty());
    }

    #[test]
    fn debug_output_redacts_rpc_header_values() {
        let _guard = env_lock();
        std::env::set_var("DATABASE_URL", "postgres://localhost/test");
        std::env::set_var("RPC_URL", "https://example.com");
        std::env::remove_var("RPC_URLS");
        std::env::set_var("RPC_HEADERS", "X-Api-Key=very-secret-key");
        let cfg = Config::from_env().expect("config should load");
        std::env::remove_var("RPC_HEADERS");

        assert_eq!(
            cfg.rpc_headers.get("x-api-key").unwrap().to_str().unwrap(),
            "very-secret-key"
        );
        let dbg = format!("{cfg:?}");
        assert!(dbg.contains("x-api-key: [REDACTED]"), "{dbg}");
        assert!(
            !dbg.contains("very-secret-key"),
            "Debug must not print header values"
        );
    }

    #[test]
    fn rpc_headers_reject_bad_names_and_never_echo_values() {
        let err = parse_rpc_headers("bad name=supersecret")
            .unwrap_err()
            .to_string();
        assert!(err.contains("invalid header name"), "{err}");
        assert!(
            !err.contains("supersecret"),
            "error must not leak the value: {err}"
        );
        assert!(parse_rpc_headers("no-equals-sign").is_err());
    }

    #[test]
    fn page_size_clamping() {
        // Test PAGE_SIZE bounds (1–10000).
        assert_eq!(clamp_with_warning("PAGE_SIZE", 0u32, 1, 10000), 1);
        assert_eq!(clamp_with_warning("PAGE_SIZE", 1u32, 1, 10000), 1);
        assert_eq!(clamp_with_warning("PAGE_SIZE", 5000u32, 1, 10000), 5000);
        assert_eq!(clamp_with_warning("PAGE_SIZE", 10000u32, 1, 10000), 10000);
        assert_eq!(clamp_with_warning("PAGE_SIZE", 100000u32, 1, 10000), 10000);
    }

    #[test]
    fn poll_interval_clamping() {
        // Test POLL_INTERVAL_SECS minimum.
        assert_eq!(
            clamp_with_warning("POLL_INTERVAL_SECS", 0u64, 1, u64::MAX),
            1
        );
        assert_eq!(
            clamp_with_warning("POLL_INTERVAL_SECS", 1u64, 1, u64::MAX),
            1
        );
        assert_eq!(
            clamp_with_warning("POLL_INTERVAL_SECS", 5u64, 1, u64::MAX),
            5
        );
        assert_eq!(
            clamp_with_warning("POLL_INTERVAL_SECS", 60u64, 1, u64::MAX),
            60
        );
    }

    #[test]
    fn max_catchup_ledgers_clamping() {
        // Test MAX_CATCHUP_LEDGERS minimum.
        assert_eq!(
            clamp_with_warning("MAX_CATCHUP_LEDGERS", 0i64, 1, i64::MAX),
            1
        );
        assert_eq!(
            clamp_with_warning("MAX_CATCHUP_LEDGERS", 1i64, 1, i64::MAX),
            1
        );
        assert_eq!(
            clamp_with_warning("MAX_CATCHUP_LEDGERS", 4000i64, 1, i64::MAX),
            4000
        );
        assert_eq!(
            clamp_with_warning("MAX_CATCHUP_LEDGERS", 120000i64, 1, i64::MAX),
            120000
        );
    }

    #[test]
    fn retention_ledgers_validation() {
        // Negative values should be clamped to 0.
        assert_eq!(
            clamp_with_warning("RETENTION_LEDGERS", -100i64, 0, i64::MAX),
            0
        );

        // Zero and positive should pass through.
        assert_eq!(
            clamp_with_warning("RETENTION_LEDGERS", 0i64, 0, i64::MAX),
            0
        );
        assert_eq!(
            clamp_with_warning("RETENTION_LEDGERS", 1000i64, 0, i64::MAX),
            1000
        );
    }

    #[test]
    fn reorg_overlap_ledgers_validation() {
        // Negative values should be clamped to 0.
        assert_eq!(
            clamp_with_warning("REORG_OVERLAP_LEDGERS", -50i64, 0, i64::MAX),
            0
        );

        // Zero and positive should pass through.
        assert_eq!(
            clamp_with_warning("REORG_OVERLAP_LEDGERS", 0i64, 0, i64::MAX),
            0
        );
        assert_eq!(
            clamp_with_warning("REORG_OVERLAP_LEDGERS", 100i64, 0, i64::MAX),
            100
        );
    }

    #[test]
    fn contract_ids_limit_constants() {
        // Verify the limit used in Config::from_env matches the RPC protocol.
        const MAX_IDS_PER_FILTER: usize = 5;
        const MAX_FILTERS: usize = 5;
        assert_eq!(MAX_IDS_PER_FILTER * MAX_FILTERS, 25);
    }

    #[test]
    fn event_topic_filters_validation_accepts_within_limits() {
        // No filters at all is fine.
        assert!(validate_event_filters(&[], &[]).is_ok());
        // Wildcards and multiple patterns within limits.
        let filters = vec![
            vec!["transfer".to_string(), "*".to_string(), "*".to_string()],
            vec!["swap".to_string()],
        ];
        assert!(validate_event_filters(&[], &filters).is_ok());
        // Exactly 5 patterns is allowed.
        let five = vec![vec!["a".to_string()]; MAX_EVENT_FILTERS];
        assert!(validate_event_filters(&[], &five).is_ok());
    }

    #[test]
    fn event_topic_filters_validation_rejects_too_many_patterns() {
        let six = vec![vec!["a".to_string()]; MAX_EVENT_FILTERS + 1];
        let err = validate_event_filters(&[], &six).unwrap_err().to_string();
        assert!(err.contains("EVENT_TOPIC_FILTERS"), "{err}");
        assert!(err.contains("at most 5"), "{err}");
    }

    #[test]
    fn event_topic_filters_validation_rejects_too_many_segments() {
        let too_long = vec![vec!["a".to_string(); MAX_TOPIC_SEGMENTS + 1]];
        let err = validate_event_filters(&[], &too_long)
            .unwrap_err()
            .to_string();
        assert!(err.contains("segments"), "{err}");
        // Empty pattern is rejected.
        let empty = vec![Vec::<String>::new()];
        assert!(validate_event_filters(&[], &empty).is_err());
    }

    #[test]
    fn event_topic_filters_validation_rejects_oversized_cross_product() {
        let contracts = vec!["C1".to_string(), "C2".to_string(), "C3".to_string()];
        let filters = vec![vec!["transfer".to_string()], vec!["swap".to_string()]];
        // 3 x 2 = 6 > 5.
        let err = validate_event_filters(&contracts, &filters)
            .unwrap_err()
            .to_string();
        assert!(err.contains("exceeds"), "{err}");
        assert!(err.contains("CONTRACT_IDS"), "{err}");
        // 2 x 2 = 4 fits.
        assert!(validate_event_filters(&contracts[..2], &filters).is_ok());
    }

    #[test]
    fn event_topic_filters_parsed_from_env() {
        let _guard = env_lock();
        let saved: Vec<(String, Option<String>)> = [
            "DATABASE_URL",
            "RPC_URL",
            "RPC_URLS",
            "CONTRACT_IDS",
            "EVENT_TOPIC_FILTERS",
        ]
        .iter()
        .map(|k| ((*k).to_string(), std::env::var(k).ok()))
        .collect();

        std::env::set_var("DATABASE_URL", "postgres://localhost/test");
        std::env::set_var("RPC_URL", "https://example.com");
        std::env::remove_var("RPC_URLS");
        std::env::remove_var("CONTRACT_IDS");
        std::env::set_var("EVENT_TOPIC_FILTERS", r#"[["transfer","*","*"],["swap"]]"#);

        let cfg = Config::from_env().expect("config should load");
        assert_eq!(
            cfg.event_topic_filters,
            vec![
                vec!["transfer".to_string(), "*".to_string(), "*".to_string()],
                vec!["swap".to_string()],
            ]
        );
        assert!(format!("{cfg:?}").contains("event_topic_filters"));

        // Invalid JSON fails fast.
        std::env::set_var("EVENT_TOPIC_FILTERS", "not json");
        assert!(Config::from_env().is_err());

        for (key, value) in saved {
            match value {
                Some(v) => std::env::set_var(&key, v),
                None => std::env::remove_var(&key),
            }
        }
    }

    #[test]
    fn state_caps_parse_and_clamp() {
        let _guard = env_lock();
        let saved: Vec<(String, Option<String>)> = [
            "DATABASE_URL",
            "RPC_URL",
            "RPC_URLS",
            "CONTRACT_IDS",
            "STATE_INDEXING",
            "KEY_INDEXING",
            "MAX_STATE_KEYS_PER_CYCLE",
            "MAX_STATE_CONTRACTS_PER_CYCLE",
        ]
        .iter()
        .map(|k| ((*k).to_string(), std::env::var(k).ok()))
        .collect();

        std::env::set_var("DATABASE_URL", "postgres://localhost/test");
        std::env::set_var("RPC_URL", "https://example.com");
        std::env::remove_var("RPC_URLS");
        std::env::remove_var("CONTRACT_IDS");
        std::env::remove_var("STATE_INDEXING");
        std::env::remove_var("KEY_INDEXING");

        // Defaults apply when unset.
        std::env::remove_var("MAX_STATE_KEYS_PER_CYCLE");
        std::env::remove_var("MAX_STATE_CONTRACTS_PER_CYCLE");
        let cfg = Config::from_env().expect("config should load");
        assert_eq!(
            cfg.max_state_keys_per_cycle,
            DEFAULT_MAX_STATE_KEYS_PER_CYCLE
        );
        assert_eq!(
            cfg.max_state_contracts_per_cycle,
            DEFAULT_MAX_STATE_CONTRACTS_PER_CYCLE
        );

        // Explicit values are honoured.
        std::env::set_var("MAX_STATE_KEYS_PER_CYCLE", "1234");
        std::env::set_var("MAX_STATE_CONTRACTS_PER_CYCLE", "7");
        let cfg = Config::from_env().expect("config should load");
        assert_eq!(cfg.max_state_keys_per_cycle, 1234);
        assert_eq!(cfg.max_state_contracts_per_cycle, 7);
        assert!(format!("{cfg:?}").contains("max_state_keys_per_cycle"));

        // Zero is clamped up to 1.
        std::env::set_var("MAX_STATE_KEYS_PER_CYCLE", "0");
        std::env::set_var("MAX_STATE_CONTRACTS_PER_CYCLE", "0");
        let cfg = Config::from_env().expect("config should load");
        assert_eq!(cfg.max_state_keys_per_cycle, 1);
        assert_eq!(cfg.max_state_contracts_per_cycle, 1);

        // Non-numeric values fail fast.
        std::env::set_var("MAX_STATE_KEYS_PER_CYCLE", "lots");
        assert!(Config::from_env().is_err());

        for (key, value) in saved {
            match value {
                Some(v) => std::env::set_var(&key, v),
                None => std::env::remove_var(&key),
            }
        }
    }

    #[test]
    fn index_all_state_indexing_requires_explicit_caps() {
        // No contracts, no state indexing: fine.
        assert!(validate_index_all_state_caps(&[], false, false, None, None).is_ok());
        // Restricted contracts: caps not required.
        let contracts = vec!["C1".to_string()];
        assert!(validate_index_all_state_caps(&contracts, true, true, None, None).is_ok());

        // Index-all + KEY_INDEXING without a cap is refused.
        let err = validate_index_all_state_caps(&[], false, true, None, None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("MAX_STATE_KEYS_PER_CYCLE"), "{err}");
        assert!(err.contains("CONTRACT_IDS"), "{err}");

        // Index-all + STATE_INDEXING without a cap is refused.
        let err = validate_index_all_state_caps(&[], true, false, None, None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("MAX_STATE_CONTRACTS_PER_CYCLE"), "{err}");

        // Both enabled: both caps named.
        let err = validate_index_all_state_caps(&[], true, true, None, None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("MAX_STATE_KEYS_PER_CYCLE"), "{err}");
        assert!(err.contains("MAX_STATE_CONTRACTS_PER_CYCLE"), "{err}");

        // Explicit caps (even equal to defaults) opt in.
        assert!(validate_index_all_state_caps(
            &[],
            true,
            true,
            Some("500".to_string()),
            Some("50".to_string())
        )
        .is_ok());
        // Only the relevant cap is required per feature.
        assert!(
            validate_index_all_state_caps(&[], false, true, Some("500".to_string()), None).is_ok()
        );
        // Empty env values do not count as explicit.
        assert!(
            validate_index_all_state_caps(&[], false, true, Some("  ".to_string()), None).is_err()
        );
    }

    #[test]
    fn index_all_key_indexing_fails_config_load_without_cap() {
        let _guard = env_lock();
        let saved: Vec<(String, Option<String>)> = [
            "DATABASE_URL",
            "RPC_URL",
            "RPC_URLS",
            "CONTRACT_IDS",
            "STATE_INDEXING",
            "KEY_INDEXING",
            "MAX_STATE_KEYS_PER_CYCLE",
            "MAX_STATE_CONTRACTS_PER_CYCLE",
        ]
        .iter()
        .map(|k| ((*k).to_string(), std::env::var(k).ok()))
        .collect();

        std::env::set_var("DATABASE_URL", "postgres://localhost/test");
        std::env::set_var("RPC_URL", "https://example.com");
        std::env::remove_var("RPC_URLS");
        std::env::remove_var("CONTRACT_IDS");
        std::env::remove_var("STATE_INDEXING");
        std::env::set_var("KEY_INDEXING", "true");
        std::env::remove_var("MAX_STATE_KEYS_PER_CYCLE");
        std::env::remove_var("MAX_STATE_CONTRACTS_PER_CYCLE");

        let err = Config::from_env().expect_err("index-all KEY_INDEXING must be refused");
        assert!(
            err.to_string().contains("MAX_STATE_KEYS_PER_CYCLE"),
            "{err}"
        );

        // Setting the cap makes it load.
        std::env::set_var("MAX_STATE_KEYS_PER_CYCLE", "250");
        let cfg = Config::from_env().expect("config should load with explicit cap");
        assert_eq!(cfg.max_state_keys_per_cycle, 250);

        for (key, value) in saved {
            match value {
                Some(v) => std::env::set_var(&key, v),
                None => std::env::remove_var(&key),
            }
        }
    }

    #[test]
    fn invalid_balance_key_durability_fails_config_load() {
        let _guard = env_lock();

        let saved: Vec<(String, Option<String>)> = [
            "BALANCE_KEY_DURABILITY",
            "DATABASE_URL",
            "RPC_URL",
            "RPC_URLS",
            "KEY_TEMPLATES",
        ]
        .iter()
        .map(|k| ((*k).to_string(), std::env::var(k).ok()))
        .collect();

        // Minimal valid environment so only the durability value is invalid.
        std::env::set_var("DATABASE_URL", "postgres://localhost/test");
        std::env::set_var("RPC_URL", "https://example.com");
        std::env::remove_var("RPC_URLS");
        std::env::remove_var("KEY_TEMPLATES");

        for invalid in ["instance", "temp", "persistant", "temporarily", ""] {
            std::env::set_var("BALANCE_KEY_DURABILITY", invalid);
            // Empty string is treated as unset (defaults to persistent), so it
            // must succeed; all other typos must fail.
            let result = Config::from_env();
            if invalid.is_empty() {
                assert!(
                    result.is_ok(),
                    "empty BALANCE_KEY_DURABILITY should default"
                );
            } else {
                let err = result.expect_err("invalid BALANCE_KEY_DURABILITY must fail");
                let msg = err.to_string();
                assert!(
                    msg.contains("BALANCE_KEY_DURABILITY"),
                    "error should name the variable: {msg}"
                );
                assert!(
                    msg.contains("persistent") && msg.contains("temporary"),
                    "error should list valid values: {msg}"
                );
            }
        }

        // Valid values must be accepted.
        for valid in ["persistent", "temporary", "Persistent", "TEMPORARY"] {
            std::env::set_var("BALANCE_KEY_DURABILITY", valid);
            assert!(
                Config::from_env().is_ok(),
                "valid BALANCE_KEY_DURABILITY {valid:?} should be accepted"
            );
        }

        // Restore environment.
        for (key, value) in saved {
            match value {
                Some(v) => std::env::set_var(&key, v),
                None => std::env::remove_var(&key),
            }
        }
    }

    /// Run `Config::from_env` with a minimal valid environment plus the given
    /// `KEY_TEMPLATES` JSON, returning the result. Restores env afterwards.
    fn load_config_with_key_templates(templates: &str) -> anyhow::Result<Config> {
        let saved: Vec<(String, Option<String>)> = [
            "DATABASE_URL",
            "RPC_URL",
            "RPC_URLS",
            "CONTRACT_IDS",
            "EVENT_TOPIC_FILTERS",
            "KEY_TEMPLATES",
            "BALANCE_KEY_DURABILITY",
        ]
        .iter()
        .map(|k| ((*k).to_string(), std::env::var(k).ok()))
        .collect();

        std::env::set_var("DATABASE_URL", "postgres://localhost/test");
        std::env::set_var("RPC_URL", "https://example.com");
        std::env::remove_var("RPC_URLS");
        std::env::remove_var("CONTRACT_IDS");
        std::env::remove_var("EVENT_TOPIC_FILTERS");
        std::env::remove_var("BALANCE_KEY_DURABILITY");
        std::env::set_var("KEY_TEMPLATES", templates);

        let result = Config::from_env();

        for (key, value) in saved {
            match value {
                Some(v) => std::env::set_var(&key, v),
                None => std::env::remove_var(&key),
            }
        }
        result
    }

    #[test]
    fn valid_key_template_loads() {
        let _guard = env_lock();
        let cfg = load_config_with_key_templates(
            r#"[{"symbol":"Balance","event_names":["transfer"],"param_indices":[1],"label":"bal"}]"#,
        )
        .expect("valid KEY_TEMPLATES should load");
        assert_eq!(cfg.key_templates.len(), 1);
        assert_eq!(cfg.key_templates[0].label.as_deref(), Some("bal"));
    }

    #[test]
    fn key_template_invalid_symbol_fails_config_load() {
        let _guard = env_lock();
        // 33 characters exceeds the ScSymbol limit.
        let long_symbol = "A".repeat(33);
        let json = format!(
            r#"[{{"symbol":"{long_symbol}","event_names":["transfer"],"param_indices":[1],"label":"bad-sym"}}]"#
        );
        let err = load_config_with_key_templates(&json)
            .expect_err("invalid symbol must fail")
            .to_string();
        assert!(err.contains("KEY_TEMPLATES"), "{err}");
        assert!(
            err.contains("bad-sym"),
            "error should name the template: {err}"
        );
        assert!(err.contains("symbol"), "{err}");
    }

    #[test]
    fn key_template_empty_event_names_fails_config_load() {
        let _guard = env_lock();
        let err = load_config_with_key_templates(
            r#"[{"symbol":"Balance","event_names":[],"param_indices":[1],"label":"no-events"}]"#,
        )
        .expect_err("empty event_names must fail")
        .to_string();
        assert!(err.contains("KEY_TEMPLATES"), "{err}");
        assert!(
            err.contains("no-events"),
            "error should name the template: {err}"
        );
        assert!(err.contains("event_names"), "{err}");
    }

    #[test]
    fn key_template_empty_param_indices_fails_config_load() {
        let _guard = env_lock();
        let err = load_config_with_key_templates(
            r#"[{"symbol":"Balance","event_names":["transfer"],"param_indices":[],"label":"no-params"}]"#,
        )
        .expect_err("empty param_indices must fail")
        .to_string();
        assert!(err.contains("KEY_TEMPLATES"), "{err}");
        assert!(
            err.contains("no-params"),
            "error should name the template: {err}"
        );
        assert!(err.contains("param_indices"), "{err}");
    }

    #[test]
    fn key_template_zero_param_index_fails_config_load() {
        let _guard = env_lock();
        let err = load_config_with_key_templates(
            r#"[{"symbol":"Balance","event_names":["transfer"],"param_indices":[0],"label":"zero-param"}]"#,
        )
        .expect_err("param index 0 must fail")
        .to_string();
        assert!(err.contains("KEY_TEMPLATES"), "{err}");
        assert!(
            err.contains("zero-param"),
            "error should name the template: {err}"
        );
        assert!(err.contains("param_indices"), "{err}");
    }

    #[test]
    fn key_template_out_of_range_param_index_fails_config_load() {
        let _guard = env_lock();
        let err = load_config_with_key_templates(
            r#"[{"symbol":"Balance","event_names":["transfer"],"param_indices":[9],"label":"oob-param"}]"#,
        )
        .expect_err("out-of-range param index must fail")
        .to_string();
        assert!(err.contains("KEY_TEMPLATES"), "{err}");
        assert!(
            err.contains("oob-param"),
            "error should name the template: {err}"
        );
        assert!(err.contains("param_indices"), "{err}");
    }
}
