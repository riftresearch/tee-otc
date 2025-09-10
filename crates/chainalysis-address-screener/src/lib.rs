use reqwest::{
    header::{HeaderMap, HeaderValue, USER_AGENT},
    Client, StatusCode,
};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use tracing::{debug, instrument};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("failed to build reqwest client: {source}"))]
    BuildClient { source: reqwest::Error },

    #[snafu(display("request failed: {source}"))]
    Request { source: reqwest::Error },

    #[snafu(display("unexpected HTTP status {status}: {body}"))]
    HttpStatus { status: StatusCode, body: String },

    #[snafu(display("failed to decode response body as JSON: {source}. Body: {body}"))]
    Decode {
        source: serde_json::Error,
        body: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct ChainalysisAddressScreener {
    host: String,
    token: String,
    http: Client,
}

impl ChainalysisAddressScreener {
    pub fn new(host: impl Into<String>, token: impl Into<String>) -> Result<Self> {
        let host = host.into().trim_end_matches('/').to_string();
        let token = token.into();

        let mut headers = HeaderMap::new();
        headers.insert(
            USER_AGENT,
            HeaderValue::from_static("chainalysis-address-screener/0.1"),
        );

        let http = Client::builder()
            .default_headers(headers)
            .use_rustls_tls()
            .build()
            .context(BuildClientSnafu)?;

        Ok(Self { host, token, http })
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn get_address_risk(&self, address: &str) -> Result<AddressRiskResponse> {
        let url = format!("{}/api/risk/v2/entities/{}", self.host, address);
        debug!(%url, "fetching entity");

        let resp = self
            .http
            .get(url)
            .header("Token", &self.token)
            .send()
            .await
            .context(RequestSnafu)?;

        let status = resp.status();
        let bytes = resp.bytes().await.context(RequestSnafu)?;

        if !status.is_success() {
            let body = String::from_utf8_lossy(&bytes).to_string();
            return Err(Error::HttpStatus { status, body });
        }

        serde_json::from_slice::<AddressRiskResponse>(&bytes).map_err(|source| Error::Decode {
            source,
            body: String::from_utf8_lossy(&bytes).to_string(),
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddressRiskResponse {
    pub address: String,
    pub risk: RiskLevel,
    pub cluster: Option<Cluster>,
    pub risk_reason: Option<String>,
    pub address_type: AddressType,
    pub address_identifications: Vec<serde_json::Value>,
    pub exposures: Vec<Exposure>,
    pub triggers: Vec<serde_json::Value>,
    pub status: Status,
    pub pool_metadata: Option<PoolMetadata>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Severe,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AddressType {
    PrivateWallet,
    LiquidityPool,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum Status {
    Complete,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Cluster {
    pub name: Option<String>,
    pub category: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Exposure {
    pub category: String,
    pub value: f64,
    pub exposure_type: ExposureType,
    pub direction: ExposureDirection,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ExposureType {
    Direct,
    Indirect,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExposureDirection {
    BothDirections,
    Inbound,
    Outbound,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolMetadata {
    pub pool_name: Option<String>,
    pub protocol: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn deserializes_sample() {
        let body = r#"{
            "address":"0x0038AC785dfB6C82b2c9A7B3B6854e08a10cb9f1",
            "risk":"Low",
            "cluster":{"name":null,"category":"unnamed service"},
            "riskReason":null,
            "addressType":"PRIVATE_WALLET",
            "addressIdentifications":[],
            "exposures":[{"category":"exchange","value":419806.06569,"exposureType":"direct","direction":"both_directions"}],
            "triggers":[],
            "status":"COMPLETE"
        }"#;

        let parsed: AddressRiskResponse = serde_json::from_str(body).unwrap();
        assert_eq!(parsed.risk, RiskLevel::Low);
        assert_eq!(parsed.address_type, AddressType::PrivateWallet);
        assert_eq!(parsed.status, Status::Complete);
    }

    #[tokio::test]
    async fn test_get_address_risk() {
        let host = std::env::var("CHAINALYSIS_HOST").unwrap_or_default();
        let token = std::env::var("CHAINALYSIS_TOKEN").unwrap_or_default();
        if host.is_empty() || token.is_empty() {
            println!("CHAINALYSIS_HOST or CHAINALYSIS_TOKEN is not set, skipping test");
            return;
        }
        let screener = ChainalysisAddressScreener::new(host, token).unwrap();
        let entity = screener
            .get_address_risk("0x0038AC785dfB6C82b2c9A7B3B6854e08a10cb9f1")
            .await
            .unwrap();
        println!("{:#?}", entity);
    }
}
