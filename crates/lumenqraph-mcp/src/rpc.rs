//! Minimal Soroban RPC client: just `simulateTransaction`, for the
//! `call_contract` tool's read-only view calls.

use std::time::Duration;

use serde::Deserialize;

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
    pub fn new(url: impl Into<String>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
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
