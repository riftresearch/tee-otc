use alloy::primitives::{Address, B256, U256};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build HTTP client: {source:?}"))]
    BuildClient { source: reqwest::Error },

    #[snafu(display("Failed to send request: {source:?} at {loc}"))]
    Request {
        source: reqwest::Error,
        #[snafu(implicit)]
        loc: snafu::Location,
    },

    #[snafu(display("Failed to parse response: {source:?}"))]
    ParseResponse {
        source: reqwest::Error,
        #[snafu(implicit)]
        loc: snafu::Location,
    },

    #[snafu(display("Invalid base URL: {source:?}"))]
    InvalidUrl {
        source: url::ParseError,
        #[snafu(implicit)]
        loc: snafu::Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableCounts {
    pub account: u64,
    pub transfer_event: u64,
    pub timestamp: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Account {
    pub address: Address,
    pub balance: String, // BigInt serialized as string
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferEvent {
    pub id: String,
    pub amount: String, // BigInt serialized as string
    pub timestamp: u64,
    pub from: Address,
    pub to: Address,
    pub transaction_hash: B256,
    pub block_number: String, // BigInt serialized as string
    pub block_hash: B256,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Pagination {
    pub page: u32,
    pub limit: u32,
    pub total: u64,
    #[serde(rename = "totalPages")]
    pub total_pages: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TransfersResponse {
    pub transfers: Vec<TransferEvent>,
    pub pagination: Pagination,
}

pub struct TokenIndexerClient {
    client: Client,
    base_url: Url,
}

impl TokenIndexerClient {
    pub fn new(base_url: impl AsRef<str>) -> Result<Self> {
        let client = Client::builder().build().context(BuildClientSnafu)?;

        tracing::info!(
            "Creating TokenIndexerClient with base URL: {}",
            base_url.as_ref()
        );

        let base_url = Url::parse(base_url.as_ref()).context(InvalidUrlSnafu)?;

        Ok(Self { client, base_url })
    }

    pub async fn get_balance(&self, address: Address) -> Result<Vec<Account>> {
        let url = self
            .base_url
            .join(&format!("balance/{address:?}"))
            .context(InvalidUrlSnafu)?;

        tracing::info!("Getting balance for address: {}", address);

        let response = self
            .client
            .get(url)
            .send()
            .await
            .context(RequestSnafu)?
            .json::<Vec<Account>>()
            .await
            .context(ParseResponseSnafu)?;

        Ok(response)
    }

    pub async fn get_transfers_to(
        &self,
        address: Address,
        page: Option<u32>,
        min_amount: Option<U256>,
    ) -> Result<TransfersResponse> {
        let mut url = self
            .base_url
            .join(&format!("transfers/to/{address:?}"))
            .context(InvalidUrlSnafu)?;

        {
            let mut query_pairs = url.query_pairs_mut();

            if let Some(page) = page {
                query_pairs.append_pair("page", &page.to_string());
            }

            if let Some(amount) = min_amount {
                query_pairs.append_pair("amount", &amount.to_string());
            }
        }

        let response = self.client.get(url).send().await.context(RequestSnafu)?;

        let response = response
            .json::<TransfersResponse>()
            .await
            .context(ParseResponseSnafu)?;

        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let client = TokenIndexerClient::new("http://localhost:3000");
        assert!(client.is_ok());
    }

    #[test]
    fn test_path_building() {
        let client = TokenIndexerClient::new("http://localhost:3000/erc20-indexer/").unwrap();

        let url = client.base_url.join("balance/0x123").unwrap();
        assert_eq!(
            url.to_string(),
            "http://localhost:3000/erc20-indexer/balance/0x123"
        );
    }
}
