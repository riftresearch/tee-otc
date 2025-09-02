use std::time::{Duration, Instant};

use alloy::primitives::U256;
use base64::{engine::general_purpose, Engine};
use hmac::{Hmac, Mac};
use otc_models::{ChainType, Currency, Lot, TokenIdentifier, CB_BTC_CONTRACT_ADDRESS};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Sha256;
use tracing::{info, warn};

use snafu::prelude::*;

use crate::wallet::Wallet;

#[derive(Debug, Snafu)]
pub enum ConversionError {
    #[snafu(display("Failed to build HTTP client: {source}"))]
    HttpClientBuild { source: reqwest::Error },

    #[snafu(display("HTTP request failed: {source}"))]
    HttpRequest { source: reqwest::Error },

    #[snafu(display("Invalid JSON response: {source}"))]
    JsonDecode { source: serde_json::Error },

    #[snafu(display("Invalid request: {reason}"))]
    InvalidRequest {
        reason: String,
        #[snafu(implicit)]
        loc: snafu::Location,
    },

    #[snafu(display("Wallet error: {source}"))]
    WalletError {
        source: crate::wallet::WalletError,
        #[snafu(implicit)]
        loc: snafu::Location,
    },

    #[snafu(display(
        "Insufficient balance: requested {}, available {}",
        requested_amount,
        available_amount
    ))]
    InsufficientBalance {
        requested_amount: U256,
        available_amount: U256,
        #[snafu(implicit)]
        loc: snafu::Location,
    },
}

pub type Result<T> = std::result::Result<T, ConversionError>;

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

const USER_AGENT: &str = "rift-tee-otc-market-maker/1.0";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WithdrawalStatus {
    Pending,
    Completed(String),
    Cancelled,
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
        let access_timestamp = chrono::Utc::now().timestamp();

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

    async fn get_btc_account_id(&self) -> Result<String> {
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
            reason: "Response is not an array",
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

    async fn get_btc_deposit_address(
        &self,
        btc_account_id: &str,
        network: ChainType,
    ) -> Result<String> {
        let network = match network {
            ChainType::Bitcoin => "bitcoin",
            ChainType::Ethereum => "ethereum",
        };

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

    async fn withdraw_bitcoin(
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

        let network_str = match network {
            ChainType::Bitcoin => "bitcoin",
            ChainType::Ethereum => "ethereum",
        };

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
            .body(body_str)
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
        let response_data: serde_json::Value =
            serde_json::from_str(&text).context(JsonDecodeSnafu)?;

        let withdrawal_id = response_data["id"]
            .as_str()
            .context(InvalidRequestSnafu {
                reason: "Withdrawal id missing or not string",
            })?
            .to_string();

        Ok(withdrawal_id)
    }

    /// Get the status and details of a single transfer/withdrawal
    async fn get_withdrawal_status(&self, withdrawal_id: &str) -> Result<WithdrawalStatus> {
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

        if withdrawal_data["cancelled"].is_null() {
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

/// Send BTC from the sender's bitcoin wallet
pub async fn convert_btc_to_cbbtc(
    coinbase_client: &CoinbaseClient,
    sender_wallet: &dyn Wallet,
    amount_sats: u64,
    recipient_address: &str,
) -> Result<String> {
    info!("Starting BTC -> cbBTC conversion for {} sats", amount_sats);
    if sender_wallet.chain_type() != ChainType::Bitcoin {
        return InvalidRequestSnafu {
            reason: "Sender wallet is not a bitcoin wallet".to_string(),
        }
        .fail();
    }

    // check if the sender wallet can fill the lot
    let lot = Lot {
        currency: Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        },
        amount: U256::from(amount_sats),
    };

    let available_balance = sender_wallet
        .balance(&TokenIdentifier::Native)
        .await
        .context(WalletSnafu)?;

    if available_balance < lot.amount {
        return InsufficientBalanceSnafu {
            requested_amount: lot.amount,
            available_amount: available_balance,
        }
        .fail();
    }

    let btc_account_id = coinbase_client.get_btc_account_id().await?;

    // get btc deposit address
    let btc_deposit_address = coinbase_client
        .get_btc_deposit_address(&btc_account_id, ChainType::Bitcoin)
        .await?;

    // send btc to the deposit address
    let btc_tx_hash = coinbase_client
        .withdraw_bitcoin(&btc_deposit_address, &amount_sats, ChainType::Bitcoin)
        .await?;

    // wait for the btc to be confirmed
    sender_wallet
        .guarantee_confirmations(&btc_tx_hash, 3) // 2 confirmations for btc transactions to credit on coinbase we do + 1 to be safe
        .await
        .context(WalletSnafu)?;

    // at this point, the btc is confirmed and should be credited to our coinbase btc account
    // now we can send the btc to eth which will implicitly convert it to cbbtc

    let withdrawal_id = coinbase_client
        .withdraw_bitcoin(recipient_address, &amount_sats, ChainType::Ethereum)
        .await?;

    let start_time = Instant::now();
    loop {
        let withdrawal_status = coinbase_client
            .get_withdrawal_status(&withdrawal_id)
            .await?;
        match withdrawal_status {
            WithdrawalStatus::Completed(tx_hash) => {
                return Ok(tx_hash);
            }
            WithdrawalStatus::Pending => {
                if start_time.elapsed() > Duration::from_secs(60 * 60) {
                    warn!(
                        "Coinbase withdrawal with id {} has been pending for more than 1 hour",
                        withdrawal_id
                    );
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            WithdrawalStatus::Cancelled => {
                return InvalidRequestSnafu {
                    reason: "Withdrawal cancelled".to_string(),
                }
                .fail();
            }
        }
    }
}

pub async fn convert_cbbtc_to_btc(
    coinbase_client: &CoinbaseClient,
    sender_wallet: &dyn Wallet,
    amount_sats: u64,
    recipient_address: &str,
) -> Result<String> {
    info!("Starting cbBTC -> BTC conversion for {} sats", amount_sats);
    if sender_wallet.chain_type() != ChainType::Ethereum {
        return InvalidRequestSnafu {
            reason: "Sender wallet is not an ethereum wallet".to_string(),
        }
        .fail();
    }

    let cbbtc = TokenIdentifier::Address(CB_BTC_CONTRACT_ADDRESS.to_string());

    // check if the sender wallet can fill the lot
    let lot = Lot {
        currency: Currency {
            chain: ChainType::Ethereum,
            token: cbbtc.clone(),
            decimals: 8,
        },
        amount: U256::from(amount_sats),
    };

    let available_balance = sender_wallet
        .balance(&cbbtc)
        .await
        .context(WalletSnafu)?;

    if available_balance < lot.amount {
        return InsufficientBalanceSnafu {
            requested_amount: lot.amount,
            available_amount: available_balance,
        }
        .fail();
    }

    let btc_account_id = coinbase_client.get_btc_account_id().await?;

    // get the cbbtc deposit address
    let cbbtc_deposit_address = coinbase_client
        .get_btc_deposit_address(&btc_account_id, ChainType::Ethereum)
        .await?;

    // send the cbbtc to the deposit address
    let cbbtc_tx_hash = sender_wallet
        .create_payment(&lot, &cbbtc_deposit_address, None)
        .await
        .context(WalletSnafu)?;

    // wait for the cbbtc to be confirmed
    sender_wallet
        .guarantee_confirmations(&cbbtc_tx_hash, 36) // 35 confirmations for cbbtc transactions to credit on coinbase we do + 1 to be safe
        .await
        .context(WalletSnafu)?;

    // at this point, the cbbtc is confirmed and should be credited to our coinbase btc account
    // now we can send the btc to eth which will implicitly convert it to cbbtc
    let withdrawal_id = coinbase_client
        .withdraw_bitcoin(recipient_address, &amount_sats, ChainType::Bitcoin)
        .await?;
    let start_time = Instant::now();
    loop {
        let withdrawal_status = coinbase_client
            .get_withdrawal_status(&withdrawal_id)
            .await?;
        match withdrawal_status {
            WithdrawalStatus::Completed(tx_hash) => {
                return Ok(tx_hash);
            }
            WithdrawalStatus::Pending => {
                if start_time.elapsed() > Duration::from_secs(60 * 60) {
                    warn!(
                        "Coinbase withdrawal with id {} has been pending for more than 1 hour",
                        withdrawal_id
                    );
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            WithdrawalStatus::Cancelled => {
                return InvalidRequestSnafu {
                    reason: "Withdrawal cancelled".to_string(),
                }
                .fail();
            }
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
