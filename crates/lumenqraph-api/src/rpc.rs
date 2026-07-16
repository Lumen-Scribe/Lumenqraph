//! Minimal Soroban RPC client for the read layer: `simulateTransaction` (to
//! execute contract view functions read-only) and `getNetwork` (so the API can
//! report which network it indexes).

use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use tokio::sync::OnceCell;

#[derive(Clone)]
pub struct RpcClient {
    http: reqwest::Client,
    url: String,
    /// The RPC's network passphrase, fetched once on first use. A deployment
    /// never changes network mid-flight, so a success is cached for good;
    /// failures are not cached, so a flaky first probe gets retried.
    passphrase: Arc<OnceCell<String>>,
}

/// The outcome of a simulation: either the result (base64 `ScVal`) plus the
/// events the call would emit and its estimated resource fee, or a contract- or
/// host-level error message.
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
    /// base64 `DiagnosticEvent`s the call would emit.
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
    pub fn new(url: impl Into<String>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("failed to build HTTP client");
        Self {
            http,
            url: url.into(),
            passphrase: Arc::new(OnceCell::new()),
        }
    }

    /// The network passphrase this RPC serves, e.g.
    /// `"Public Global Stellar Network ; September 2015"`. `None` if the RPC
    /// can't be reached right now (in which case a later call retries).
    pub async fn network_passphrase(&self) -> Option<String> {
        self.passphrase
            .get_or_try_init(|| async {
                #[derive(Deserialize)]
                struct Network {
                    passphrase: String,
                }
                let req = serde_json::json!({
                    "jsonrpc": "2.0", "id": 1, "method": "getNetwork"
                });
                #[derive(Deserialize)]
                struct Env {
                    result: Option<Network>,
                }
                let env: Env = self
                    .http
                    .post(&self.url)
                    .json(&req)
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                env.result
                    .map(|n| n.passphrase)
                    .ok_or_else(|| anyhow::anyhow!("rpc returned no network"))
            })
            .await
            .ok()
            .cloned()
    }

    /// Simulate a base64 transaction envelope. `Ok(SimOutcome::Error)` carries a
    /// client-facing message (e.g. the function trapped); `Err` is transport.
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
            None => Ok(SimOutcome::Error(
                "simulation returned no result value".to_string(),
            )),
        }
    }
}
