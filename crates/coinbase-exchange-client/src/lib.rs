use std::time::Duration;

use base64::{engine::general_purpose, Engine};
use hmac::{Hmac, Mac};
use otc_models::ChainType;
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Sha256;

use snafu::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum CoinbaseExchangeClientError {
    #[snafu(display("Failed to build HTTP client: {source}"))]
    HttpClientBuild { source: reqwest::Error },

    #[snafu(display("HTTP request failed: {source}"))]
    HttpRequest { source: reqwest::Error },

    #[snafu(display("Invalid JSON response: {source}"))]
    JsonDecode { source: serde_json::Error },

    #[snafu(display("Invalid request: {reason} at {loc}"))]
    InvalidRequest {
        reason: String,
        #[snafu(implicit)]
        loc: snafu::Location,
    },
}

pub type Result<T> = std::result::Result<T, CoinbaseExchangeClientError>;

#[derive(Debug, Deserialize)]
struct ProductTickerResponse {
    price: String,
}

#[derive(Debug, Deserialize)]
struct CoinbaseAddressWarning {
    title: String,
    details: String,
}

#[derive(Debug, Deserialize)]
struct GeneratedCryptoAddressResponse {
    address: String,
    network: String,
    warnings: Vec<CoinbaseAddressWarning>,
    deposit_uri: String,
    exchange_deposit_address: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DepositAddressKind {
    Bitcoin,
    EvmToken,
    NativeEvm,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct CoinbaseAuthHeaders {
    access_key: String,
    access_passphrase: String,
    access_signature: String,
    access_timestamp: String,
}

#[derive(Debug, Clone)]
pub struct CoinbaseClient {
    http_client: Client,
    api_key: String,
    api_secret_bytes: Vec<u8>,
    api_passphrase: String,
    coinbase_base_url: Url,
}

const USER_AGENT: &str = "rift-market-maker/1.0";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WithdrawalStatus {
    Pending,
    Completed(String),
    Cancelled,
}

fn chain_type_to_coinbase_network(chain: ChainType) -> &'static str {
    match chain {
        ChainType::Bitcoin => "bitcoin",
        ChainType::Ethereum => "ethereum",
        ChainType::Base => "base",
    }
}

fn is_evm_network(chain: ChainType) -> bool {
    matches!(chain, ChainType::Ethereum | ChainType::Base)
}

fn warnings_mention_expected_asset(
    warnings: &[CoinbaseAddressWarning],
    expected_keywords: &[&str],
) -> bool {
    warnings.iter().any(|warning| {
        let text = format!("{} {}", warning.title, warning.details).to_ascii_lowercase();
        expected_keywords
            .iter()
            .any(|keyword| text.contains(&keyword.to_ascii_lowercase()))
    })
}

fn deposit_uri_contains_recipient(deposit_uri: &str, address: &str) -> bool {
    deposit_uri
        .to_ascii_lowercase()
        .contains(&format!("address={}", address.to_ascii_lowercase()))
}

fn deposit_uri_targets_address_directly(deposit_uri: &str, address: &str) -> bool {
    let Some((_, remainder)) = deposit_uri.split_once(':') else {
        return false;
    };

    let path = remainder.split('?').next().unwrap_or(remainder);
    let direct_target = path
        .strip_prefix("//")
        .unwrap_or(path)
        .split('/')
        .next()
        .unwrap_or(path);
    let direct_target = direct_target.split('@').next().unwrap_or(direct_target);
    let direct_target = direct_target.strip_prefix("pay-").unwrap_or(direct_target);

    direct_target.eq_ignore_ascii_case(address)
}

impl CoinbaseClient {
    /// Create a new CoinbaseClient with the given base URL
    /// Default would be https://api.exchange.coinbase.com
    pub fn new(
        coinbase_base_url: Url,
        api_key: String,
        api_passphrase: String,
        api_secret: String,
    ) -> Result<Self> {
        let http_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .context(HttpClientBuildSnafu)?;

        // Decode the API secret from base64 once in constructor
        let api_secret_bytes = general_purpose::STANDARD.decode(&api_secret).map_err(|_| {
            InvalidRequestSnafu {
                reason: "Failed to decode API secret".to_string(),
            }
            .build()
        })?;

        Ok(Self {
            http_client,
            api_key,
            api_secret_bytes,
            api_passphrase,
            coinbase_base_url,
        })
    }

    /// Build the Coinbase Exchange signature
    /// prehash = timestamp + METHOD + requestPath + body_json
    /// key = base64-decoded `secret`
    /// sig = base64(HMAC-SHA256(prehash, key))
    fn create_coinbase_auth_headers(
        &self,
        method: &str,   // the HTTP method (e.g. "POST")
        path: &str,     // the request path (e.g. "/orders")
        body_str: &str, // json stringified body
    ) -> Result<CoinbaseAuthHeaders> {
        let access_timestamp = utc::now().timestamp();

        let method_up = method.to_ascii_uppercase();

        let prehash = format!("{access_timestamp}{method_up}{path}{body_str}");

        let mut hmac = Hmac::<Sha256>::new_from_slice(&self.api_secret_bytes).unwrap();
        hmac.update(prehash.as_bytes());
        let sig = hmac.finalize().into_bytes();
        let b64_sig = general_purpose::STANDARD.encode(sig);

        Ok(CoinbaseAuthHeaders {
            access_key: self.api_key.to_string(),
            access_passphrase: self.api_passphrase.to_string(),
            access_signature: b64_sig,
            access_timestamp: access_timestamp.to_string(),
        })
    }

    pub async fn get_product_ticker_price(&self, product_id: &str) -> Result<f64> {
        let endpoint_path = format!("/products/{product_id}/ticker");
        let endpoint_url = self.coinbase_base_url.join(&endpoint_path).map_err(|_| {
            InvalidRequestSnafu {
                reason: format!("Failed to construct ticker URL for product {product_id}"),
            }
            .build()
        })?;

        let response = self
            .http_client
            .get(endpoint_url)
            .header("User-Agent", USER_AGENT)
            .send()
            .await
            .context(HttpRequestSnafu)?;

        let status = response.status();
        let text = response.text().await.context(HttpRequestSnafu)?;

        if !status.is_success() {
            return InvalidRequestSnafu {
                reason: format!(
                    "Ticker request failed for {product_id}: HTTP {status} | Response: {text}"
                ),
            }
            .fail();
        }

        let ticker: ProductTickerResponse = serde_json::from_str(&text).context(JsonDecodeSnafu)?;

        ticker.price.parse::<f64>().map_err(|error| {
            InvalidRequestSnafu {
                reason: format!(
                    "Failed to parse ticker price '{}' for {product_id}: {error}",
                    ticker.price
                ),
            }
            .build()
        })
    }

    pub async fn get_btc_account_id(&self) -> Result<String> {
        let endpoint_path = "/coinbase-accounts";
        let endpoint_url = self.coinbase_base_url.join(endpoint_path).map_err(|_| {
            InvalidRequestSnafu {
                reason: "Failed to construct coinbase-accounts URL".to_string(),
            }
            .build()
        })?;

        let empty_body = "";
        let auth_headers = self.create_coinbase_auth_headers("GET", endpoint_path, empty_body)?;

        let response = self
            .http_client
            .get(endpoint_url)
            .header("User-Agent", USER_AGENT)
            .header("CB-ACCESS-KEY", &auth_headers.access_key)
            .header("CB-ACCESS-PASSPHRASE", &auth_headers.access_passphrase)
            .header("CB-ACCESS-SIGN", &auth_headers.access_signature)
            .header("CB-ACCESS-TIMESTAMP", &auth_headers.access_timestamp)
            .send()
            .await
            .context(HttpRequestSnafu)?;

        let text = response.text().await.context(HttpRequestSnafu)?;
        let wallets: serde_json::Value = serde_json::from_str(&text).context(JsonDecodeSnafu)?;

        let wallets_array = wallets.as_array().context(InvalidRequestSnafu {
            reason: format!("Response is not an array | Actual response: {text}"),
        })?;

        let btc_wallet = wallets_array
            .iter()
            .find(|wallet| wallet["currency"] == "BTC")
            .context(InvalidRequestSnafu {
                reason: "No BTC wallet found",
            })?;

        let id = btc_wallet["id"]
            .as_str()
            .context(InvalidRequestSnafu {
                reason: "BTC wallet id missing or not string",
            })?
            .to_string();

        Ok(id)
    }

    pub async fn get_eth_account_id(&self) -> Result<String> {
        let endpoint_path = "/coinbase-accounts";
        let endpoint_url = self.coinbase_base_url.join(endpoint_path).map_err(|_| {
            InvalidRequestSnafu {
                reason: "Failed to construct coinbase-accounts URL".to_string(),
            }
            .build()
        })?;

        let empty_body = "";
        let auth_headers = self.create_coinbase_auth_headers("GET", endpoint_path, empty_body)?;

        let response = self
            .http_client
            .get(endpoint_url)
            .header("User-Agent", USER_AGENT)
            .header("CB-ACCESS-KEY", &auth_headers.access_key)
            .header("CB-ACCESS-PASSPHRASE", &auth_headers.access_passphrase)
            .header("CB-ACCESS-SIGN", &auth_headers.access_signature)
            .header("CB-ACCESS-TIMESTAMP", &auth_headers.access_timestamp)
            .send()
            .await
            .context(HttpRequestSnafu)?;

        let text = response.text().await.context(HttpRequestSnafu)?;
        let wallets: serde_json::Value = serde_json::from_str(&text).context(JsonDecodeSnafu)?;

        let wallets_array = wallets.as_array().context(InvalidRequestSnafu {
            reason: format!("Response is not an array | Actual response: {text}"),
        })?;

        let eth_wallet = wallets_array
            .iter()
            .find(|wallet| wallet["currency"] == "ETH")
            .context(InvalidRequestSnafu {
                reason: "No ETH wallet found",
            })?;

        let id = eth_wallet["id"]
            .as_str()
            .context(InvalidRequestSnafu {
                reason: "ETH wallet id missing or not string",
            })?
            .to_string();

        Ok(id)
    }

    fn validate_generated_crypto_address(
        &self,
        response: GeneratedCryptoAddressResponse,
        requested_network: &str,
        expected_kind: DepositAddressKind,
    ) -> Result<String> {
        if response.network != requested_network {
            return InvalidRequestSnafu {
                reason: format!(
                    "Response network '{}' does not match requested network '{}'",
                    response.network, requested_network
                ),
            }
            .fail();
        }

        if !response.exchange_deposit_address {
            return InvalidRequestSnafu {
                reason: "Coinbase returned a non-exchange deposit address".to_string(),
            }
            .fail();
        }

        if response.address.trim().is_empty() {
            return InvalidRequestSnafu {
                reason: "Coinbase deposit address response was empty".to_string(),
            }
            .fail();
        }

        if response.deposit_uri.trim().is_empty() {
            return InvalidRequestSnafu {
                reason: "Coinbase deposit URI response was empty".to_string(),
            }
            .fail();
        }

        let is_token_transfer_uri = response.deposit_uri.contains("/transfer?");

        match expected_kind {
            DepositAddressKind::Bitcoin => {
                if is_token_transfer_uri {
                    return InvalidRequestSnafu {
                        reason: format!(
                            "Bitcoin deposit URI unexpectedly requires token transfer semantics: {}",
                            response.deposit_uri
                        ),
                    }
                    .fail();
                }
                if !response.warnings.is_empty()
                    && !warnings_mention_expected_asset(&response.warnings, &["bitcoin", "btc"])
                {
                    return InvalidRequestSnafu {
                        reason: format!(
                            "Bitcoin deposit address returned warning(s) that do not mention BTC: {:?}",
                            response.warnings
                        ),
                    }
                    .fail();
                }
                if !deposit_uri_targets_address_directly(&response.deposit_uri, &response.address) {
                    return InvalidRequestSnafu {
                        reason: format!(
                            "Bitcoin deposit URI does not directly target returned address: {}",
                            response.deposit_uri
                        ),
                    }
                    .fail();
                }
            }
            DepositAddressKind::EvmToken => {
                if !is_token_transfer_uri {
                    return InvalidRequestSnafu {
                        reason: format!(
                            "Token deposit URI is missing transfer semantics: {}",
                            response.deposit_uri
                        ),
                    }
                    .fail();
                }
                if response.warnings.is_empty()
                    || !warnings_mention_expected_asset(
                        &response.warnings,
                        &["bitcoin", "btc", "cbbtc"],
                    )
                {
                    return InvalidRequestSnafu {
                        reason: format!(
                            "Token deposit address is missing BTC/cbBTC warning(s): {:?}",
                            response.warnings
                        ),
                    }
                    .fail();
                }
                if !deposit_uri_contains_recipient(&response.deposit_uri, &response.address) {
                    return InvalidRequestSnafu {
                        reason: format!(
                            "Token deposit URI does not target returned recipient address: {}",
                            response.deposit_uri
                        ),
                    }
                    .fail();
                }
            }
            DepositAddressKind::NativeEvm => {
                if is_token_transfer_uri {
                    return InvalidRequestSnafu {
                        reason: format!(
                            "Native EVM deposit URI unexpectedly requires token transfer semantics: {}",
                            response.deposit_uri
                        ),
                    }
                    .fail();
                }
                if !response.warnings.is_empty()
                    && !warnings_mention_expected_asset(
                        &response.warnings,
                        &["ethereum", "ether", "eth"],
                    )
                {
                    return InvalidRequestSnafu {
                        reason: format!(
                            "Native EVM deposit address returned warning(s) that do not mention ETH: {:?}",
                            response.warnings
                        ),
                    }
                    .fail();
                }
                if !deposit_uri_targets_address_directly(&response.deposit_uri, &response.address) {
                    return InvalidRequestSnafu {
                        reason: format!(
                            "Native EVM deposit URI does not directly target returned address: {}",
                            response.deposit_uri
                        ),
                    }
                    .fail();
                }
            }
        }

        Ok(response.address)
    }

    async fn get_validated_deposit_address(
        &self,
        account_id: &str,
        network: ChainType,
        expected_kind: DepositAddressKind,
    ) -> Result<String> {
        let network = chain_type_to_coinbase_network(network);

        let endpoint_path = format!("/coinbase-accounts/{account_id}/addresses");
        let endpoint_url = self.coinbase_base_url.join(&endpoint_path).map_err(|_| {
            InvalidRequestSnafu {
                reason: "Failed to construct addresses URL".to_string(),
            }
            .build()
        })?;
        let body = json!({
            "network": network
        });
        let body_str = serde_json::to_string(&body).unwrap();
        let auth_headers = self.create_coinbase_auth_headers("POST", &endpoint_path, &body_str)?;

        let text = self
            .http_client
            .post(endpoint_url)
            .body(body_str)
            .header("Content-Type", "application/json")
            .header("User-Agent", USER_AGENT)
            .header("CB-ACCESS-KEY", &auth_headers.access_key)
            .header("CB-ACCESS-PASSPHRASE", &auth_headers.access_passphrase)
            .header("CB-ACCESS-SIGN", &auth_headers.access_signature)
            .header("CB-ACCESS-TIMESTAMP", &auth_headers.access_timestamp)
            .send()
            .await
            .context(HttpRequestSnafu)?
            .text()
            .await
            .context(HttpRequestSnafu)?;

        let response_data: GeneratedCryptoAddressResponse =
            serde_json::from_str(&text).context(JsonDecodeSnafu)?;

        self.validate_generated_crypto_address(response_data, network, expected_kind)
    }

    pub async fn get_bitcoin_deposit_address(&self, btc_account_id: &str) -> Result<String> {
        self.get_validated_deposit_address(
            btc_account_id,
            ChainType::Bitcoin,
            DepositAddressKind::Bitcoin,
        )
        .await
    }

    pub async fn get_cbbtc_deposit_address(
        &self,
        btc_account_id: &str,
        network: ChainType,
    ) -> Result<String> {
        if !is_evm_network(network) {
            return InvalidRequestSnafu {
                reason: format!("cbBTC deposits require an EVM network, got {network:?}"),
            }
            .fail();
        }

        self.get_validated_deposit_address(btc_account_id, network, DepositAddressKind::EvmToken)
            .await
    }

    pub async fn get_eth_deposit_address(
        &self,
        eth_account_id: &str,
        network: ChainType,
    ) -> Result<String> {
        if !is_evm_network(network) {
            return InvalidRequestSnafu {
                reason: format!("Native ETH deposits require an EVM network, got {network:?}"),
            }
            .fail();
        }

        self.get_validated_deposit_address(eth_account_id, network, DepositAddressKind::NativeEvm)
            .await
    }

    pub async fn withdraw_bitcoin(
        &self,
        recipient_address: &str,
        amount_sats: &u64,
        network: ChainType,
    ) -> Result<String> {
        let endpoint_path = "/withdrawals/crypto";
        let endpoint_url = self.coinbase_base_url.join(endpoint_path).map_err(|_| {
            InvalidRequestSnafu {
                reason: "Failed to construct withdrawals/crypto URL".to_string(),
            }
            .build()
        })?;

        let network_str = chain_type_to_coinbase_network(network);

        // Convert satoshis to BTC (8 decimal places)
        let btc_amount = format!("{:.8}", *amount_sats as f64 / 100_000_000.0);

        let body = json!({
            "amount": btc_amount,
            "currency": "BTC",
            "crypto_address": recipient_address,
            "network": network_str
        });

        let body_str = serde_json::to_string(&body).unwrap();
        let auth_headers = self.create_coinbase_auth_headers("POST", endpoint_path, &body_str)?;

        let response = self
            .http_client
            .post(endpoint_url)
            .body(body_str.clone())
            .header("Content-Type", "application/json")
            .header("User-Agent", USER_AGENT)
            .header("CB-ACCESS-KEY", &auth_headers.access_key)
            .header("CB-ACCESS-PASSPHRASE", &auth_headers.access_passphrase)
            .header("CB-ACCESS-SIGN", &auth_headers.access_signature)
            .header("CB-ACCESS-TIMESTAMP", &auth_headers.access_timestamp)
            .send()
            .await
            .context(HttpRequestSnafu)?;

        let text = response.text().await.context(HttpRequestSnafu)?;
        tracing::info!("Withdrawal response: {text}");
        let response_data: serde_json::Value =
            serde_json::from_str(&text).context(JsonDecodeSnafu)?;

        let withdrawal_id = response_data["id"]
            .as_str()
            .context(InvalidRequestSnafu {
                reason: "Withdrawal id missing or not string",
            })
            .map_err(|e| {
                tracing::error!(
                    "Failed to parse withdrawal response: {e}, response: {text}, body: {body_str}"
                );
                e
            })?
            .to_string();

        Ok(withdrawal_id)
    }

    /// Get the status and details of a single transfer/withdrawal
    pub async fn get_withdrawal_status(&self, withdrawal_id: &str) -> Result<WithdrawalStatus> {
        let endpoint_path = format!("/transfers/{withdrawal_id}");
        let endpoint_url = self.coinbase_base_url.join(&endpoint_path).map_err(|_| {
            InvalidRequestSnafu {
                reason: "Failed to construct transfer status URL".to_string(),
            }
            .build()
        })?;

        // Create authentication headers for GET request with empty body
        let empty_body = "";
        let auth_headers = self.create_coinbase_auth_headers("GET", &endpoint_path, empty_body)?;

        let response = self
            .http_client
            .get(endpoint_url)
            .header("User-Agent", USER_AGENT)
            .header("CB-ACCESS-KEY", &auth_headers.access_key)
            .header("CB-ACCESS-PASSPHRASE", &auth_headers.access_passphrase)
            .header("CB-ACCESS-SIGN", &auth_headers.access_signature)
            .header("CB-ACCESS-TIMESTAMP", &auth_headers.access_timestamp)
            .send()
            .await
            .context(HttpRequestSnafu)?;

        let text = response.text().await.context(HttpRequestSnafu)?;
        let withdrawal_data: serde_json::Value =
            serde_json::from_str(&text).context(JsonDecodeSnafu)?;

        if !withdrawal_data["cancelled"].is_null() {
            Ok(WithdrawalStatus::Cancelled)
        } else if withdrawal_data["completed_at"].is_null() {
            Ok(WithdrawalStatus::Pending)
        } else {
            Ok(WithdrawalStatus::Completed(
                withdrawal_data["details"]["crypto_transaction_hash"]
                    .as_str()
                    .unwrap()
                    .to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_client() -> CoinbaseClient {
        CoinbaseClient::new(
            Url::parse("http://127.0.0.1:1").unwrap(),
            String::new(),
            String::new(),
            String::new(),
        )
        .unwrap()
    }

    #[test]
    fn validate_native_eth_deposit_rejects_token_transfer_uri() {
        let client = test_client();
        let response = GeneratedCryptoAddressResponse {
            address: "0x00000000000000000000000000000000000000cc".to_string(),
            network: "ethereum".to_string(),
            warnings: vec![CoinbaseAddressWarning {
                title: "Only send cbBTC to this address".to_string(),
                details: "Sending any other digital asset, including Ethereum (ETH), will result in permanent loss.".to_string(),
            }],
            deposit_uri: "ethereum:0x0000000000000000000000000000000000000cb7/transfer?address=0x00000000000000000000000000000000000000cc".to_string(),
            exchange_deposit_address: true,
        };

        let error = client
            .validate_generated_crypto_address(response, "ethereum", DepositAddressKind::NativeEvm)
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("unexpectedly requires token transfer semantics"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validate_bitcoin_deposit_accepts_btc_warning() {
        let client = test_client();
        let address = "bcrt1qexample0000000000000000000000000000000000000".to_string();
        let response = GeneratedCryptoAddressResponse {
            address: address.clone(),
            network: "bitcoin".to_string(),
            warnings: vec![CoinbaseAddressWarning {
                title: "Only send Bitcoin (BTC) to this address".to_string(),
                details: "Sending any other asset, including Bitcoin Cash (BCH), will result in permanent loss.".to_string(),
            }],
            deposit_uri: format!("bitcoin:{address}"),
            exchange_deposit_address: true,
        };

        let validated = client
            .validate_generated_crypto_address(response, "bitcoin", DepositAddressKind::Bitcoin)
            .unwrap();

        assert_eq!(validated, address);
    }

    #[test]
    fn validate_cbbtc_deposit_requires_token_transfer_uri_and_warning() {
        let client = test_client();
        let response = GeneratedCryptoAddressResponse {
            address: "0x00000000000000000000000000000000000000aa".to_string(),
            network: "ethereum".to_string(),
            warnings: Vec::new(),
            deposit_uri: "ethereum:0x00000000000000000000000000000000000000aa".to_string(),
            exchange_deposit_address: true,
        };

        let error = client
            .validate_generated_crypto_address(response, "ethereum", DepositAddressKind::EvmToken)
            .unwrap_err();

        assert!(
            error.to_string().contains("missing transfer semantics"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validate_native_eth_deposit_rejects_btc_warning() {
        let client = test_client();
        let response = GeneratedCryptoAddressResponse {
            address: "0x00000000000000000000000000000000000000cc".to_string(),
            network: "ethereum".to_string(),
            warnings: vec![CoinbaseAddressWarning {
                title: "Only send Bitcoin (BTC) to this address".to_string(),
                details: "Sending any other asset, including Bitcoin Cash (BCH), will result in permanent loss.".to_string(),
            }],
            deposit_uri: "ethereum:0x00000000000000000000000000000000000000cc".to_string(),
            exchange_deposit_address: true,
        };

        let error = client
            .validate_generated_crypto_address(response, "ethereum", DepositAddressKind::NativeEvm)
            .unwrap_err();

        assert!(
            error.to_string().contains("do not mention ETH"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validate_native_eth_deposit_accepts_direct_uri_without_warnings() {
        let client = test_client();
        let address = "0x00000000000000000000000000000000000000cc".to_string();
        let response = GeneratedCryptoAddressResponse {
            address: address.clone(),
            network: "ethereum".to_string(),
            warnings: Vec::new(),
            deposit_uri: format!("ethereum:{address}"),
            exchange_deposit_address: true,
        };

        let validated = client
            .validate_generated_crypto_address(response, "ethereum", DepositAddressKind::NativeEvm)
            .unwrap();

        assert_eq!(validated, address);
    }

    #[test]
    fn validate_native_eth_deposit_accepts_chain_id_suffix() {
        let client = test_client();
        let address = "0x00000000000000000000000000000000000000cc".to_string();
        let response = GeneratedCryptoAddressResponse {
            address: address.clone(),
            network: "base".to_string(),
            warnings: vec![CoinbaseAddressWarning {
                title: "Only send Ether (ETH) to this address".to_string(),
                details: "Sending any other asset will result in permanent loss.".to_string(),
            }],
            deposit_uri: format!("ethereum:{address}@8453"),
            exchange_deposit_address: true,
        };

        let validated = client
            .validate_generated_crypto_address(response, "base", DepositAddressKind::NativeEvm)
            .unwrap();

        assert_eq!(validated, address);
    }

    #[tokio::test]
    #[ignore = "requires COINBASE_* environment variables and live API access"]
    async fn test_cb_account_api_coverage() {
        let coinbase_exchange_api_key = std::env::var("COINBASE_EXCHANGE_API_KEY").unwrap();
        let coinbase_exchange_api_passphrase =
            std::env::var("COINBASE_EXCHANGE_API_PASSPHRASE").unwrap();
        let coinbase_exchange_api_secret = std::env::var("COINBASE_EXCHANGE_API_SECRET").unwrap();
        let coinbase_base_url = Url::parse("https://api.exchange.coinbase.com").unwrap();

        let coinbase_client = CoinbaseClient::new(
            coinbase_base_url,
            coinbase_exchange_api_key,
            coinbase_exchange_api_passphrase,
            coinbase_exchange_api_secret,
        )
        .unwrap();

        let btc_account_id = coinbase_client.get_btc_account_id().await.unwrap();
        println!("btc_account_id: {btc_account_id}");
        let eth_account_id = coinbase_client.get_eth_account_id().await.unwrap();
        println!("eth_account_id: {eth_account_id}");

        // now test the btc deposit address
        let btc_deposit_address = coinbase_client
            .get_bitcoin_deposit_address(&btc_account_id)
            .await
            .unwrap();
        println!("btc_deposit_address: {btc_deposit_address}");

        // now test the cbbtc deposit address
        let cbbtc_deposit_address = coinbase_client
            .get_cbbtc_deposit_address(&btc_account_id, ChainType::Ethereum)
            .await
            .unwrap();
        println!("cbbtc_deposit_address: {cbbtc_deposit_address}");

        let eth_deposit_address = coinbase_client
            .get_eth_deposit_address(&eth_account_id, ChainType::Ethereum)
            .await
            .unwrap();
        println!("eth_deposit_address: {eth_deposit_address}");
    }

    #[tokio::test]
    #[ignore = "requires COINBASE_* environment variables and Node.js runtime"]
    async fn test_javascript_hmac_signature() {
        use std::fs;
        use std::process::Command;

        // Extract API credentials from environment variables like test_get_portfolio
        let coinbase_exchange_api_passphrase =
            std::env::var("COINBASE_EXCHANGE_API_PASSPHRASE").unwrap();
        let coinbase_exchange_api_secret = std::env::var("COINBASE_EXCHANGE_API_SECRET").unwrap();
        let coinbase_base_url = Url::parse("https://api.exchange.coinbase.com").unwrap();
        let coinbase_exchange_api_key = std::env::var("COINBASE_EXCHANGE_API_KEY").unwrap();

        let coinbase_client = CoinbaseClient::new(
            coinbase_base_url,
            coinbase_exchange_api_key,
            coinbase_exchange_api_passphrase.clone(),
            coinbase_exchange_api_secret.clone(),
        )
        .unwrap();

        let body = json!({"hello": "world"});
        let body_str = serde_json::to_string(&body).unwrap();
        let request_path = "/orders";
        let method = "POST";

        let auth_headers_rust = coinbase_client
            .create_coinbase_auth_headers(method, request_path, &body_str)
            .unwrap();
        println!("auth_headers_rust: {auth_headers_rust:?}");

        let access_timestamp = auth_headers_rust.access_timestamp;

        // Create a temporary JavaScript file with real API credentials
        let js_code = format!(
            r#"
// import crypto library
var crypto = require("crypto");

// create the json request object
var cb_access_timestamp = {access_timestamp}; 
var cb_access_passphrase = "{coinbase_exchange_api_passphrase}";
var secret = "{coinbase_exchange_api_secret}";
var requestPath = "{request_path}";
var body = JSON.stringify({body_str});
var method = "{method}";

// create the prehash string by concatenating required parts
var message = cb_access_timestamp + method + requestPath + body;

// decode the base64 secret
var key = Buffer.from(secret, "base64");

// create a sha256 hmac with the secret
var hmac = crypto.createHmac("sha256", key);

// sign the require message with the hmac and base64 encode the result
var cb_access_sign = hmac.update(message).digest("base64");

// Print out the signature
console.log("cb_access_sign:", cb_access_sign);
console.log("timestamp:", cb_access_timestamp);
console.log("message:", message);
"#
        );

        // Write JavaScript to a temporary file
        fs::write("/tmp/coinbase_hmac_test.js", js_code).expect("Failed to write JS file");

        // Execute the JavaScript file with Node.js
        let output = Command::new("node")
            .arg("/tmp/coinbase_hmac_test.js")
            .output()
            .expect("Failed to execute Node.js");

        // Print the output
        println!("JavaScript execution stdout:");
        println!("{}", String::from_utf8_lossy(&output.stdout));

        if !output.stderr.is_empty() {
            println!("JavaScript execution stderr:");
            println!("{}", String::from_utf8_lossy(&output.stderr));
        }

        // Clean up the temporary file
        let _ = fs::remove_file("/tmp/coinbase_hmac_test.js");

        // Ensure the command executed successfully
        assert!(output.status.success(), "JavaScript execution failed");
    }
}
