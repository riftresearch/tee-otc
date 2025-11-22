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

    pub async fn get_btc_deposit_address(
        &self,
        btc_account_id: &str,
        network: ChainType,
    ) -> Result<String> {
        let network = chain_type_to_coinbase_network(network);

        let endpoint_path = format!("/coinbase-accounts/{btc_account_id}/addresses");
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

        let response_data: serde_json::Value =
            serde_json::from_str(&text).context(JsonDecodeSnafu)?;

        let addr = response_data["address"]
            .as_str()
            .context(InvalidRequestSnafu {
                reason: "Address missing or not string",
            })?
            .to_string();

        let response_network = response_data["network"]
            .as_str()
            .context(InvalidRequestSnafu {
                reason: "Response network missing or not string",
            })?
            .to_string();

        if response_network != network {
            return InvalidRequestSnafu {
                reason: "Response network is not the same as the request network",
            }
            .fail();
        }

        Ok(addr)
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

        // now test the btc deposit address
        let btc_deposit_address = coinbase_client
            .get_btc_deposit_address(&btc_account_id, ChainType::Bitcoin)
            .await
            .unwrap();
        println!("btc_deposit_address: {btc_deposit_address}");

        // now test the cbbtc deposit address
        let cbbtc_deposit_address = coinbase_client
            .get_btc_deposit_address(&btc_account_id, ChainType::Ethereum)
            .await
            .unwrap();
        println!("cbbtc_deposit_address: {cbbtc_deposit_address}");
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

