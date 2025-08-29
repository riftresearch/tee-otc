use std::time::Duration;

use alloy::primitives::U256;
use jsonwebtoken::{encode, Algorithm, Header};
use otc_models::{ChainType, Currency, Lot, TokenIdentifier, CB_BTC_CONTRACT_ADDRESS};
use p256::pkcs8::EncodePrivateKey;
use p256::SecretKey;
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::info;

use snafu::prelude::*;

use crate::wallet::Wallet;

#[derive(Debug, Snafu)]
pub enum ConversionError {
    // Config / construction
    #[snafu(display("Failed to build HTTP client: {source}"))]
    HttpClientBuild { source: reqwest::Error },

    #[snafu(display("Failed to parse Coinbase API private key: {parse_step}"))]
    JwtKeyParse { parse_step: String },

    #[snafu(display("Failed to encode JWT: {source}"))]
    JwtEncode { source: jsonwebtoken::errors::Error },

    // Network / HTTP
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

    #[snafu(display("Transaction failed on-chain"))]
    TxFailed,

    #[snafu(display("Invalid Ethereum address: {addr}"))]
    InvalidEthAddress { addr: String },

    // Logic & conversions
    #[snafu(display("Overflow or invalid amount conversion from U256"))]
    AmountConversion,

    #[snafu(display("Provider error: {source}"))]
    AlloyContractError {
        source: alloy::contract::Error,
        #[snafu(implicit)]
        loc: snafu::Location,
    },

    #[snafu(display("Alloy transport error: {source}"))]
    AlloyTransportError {
        source: alloy::transports::TransportError,
        #[snafu(implicit)]
        loc: snafu::Location,
    },

    #[snafu(display("Alloy pending transaction error: {source}"))]
    AlloyPendingTransactionError {
        source: alloy::providers::PendingTransactionError,
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

    // Async plumbing
    #[snafu(display("Trigger channel closed"))]
    TriggerChannelClosed,
}

pub type Result<T> = std::result::Result<T, ConversionError>;

#[derive(Debug, Serialize, Deserialize)]
struct JwtClaims {
    sub: String,
    iss: String,
    nbf: i64,
    exp: i64,
    uri: String,
}

#[derive(Debug, Clone)]
pub struct CoinbaseClient {
    http_client: Client,
    api_key: String,
    api_secret: String,
    coinbase_base_url: Url,
}

impl CoinbaseClient {
    /// Create a new CoinbaseClient with the given base URL
    /// Default would be https://api.coinbase.com
    pub fn new(coinbase_base_url: Url, api_key: String, api_secret: String) -> Result<Self> {
        let http_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .context(HttpClientBuildSnafu)?;

        Ok(Self {
            http_client,
            api_key,
            api_secret,
            coinbase_base_url,
        })
    }

    fn build_jwt(&self, uri: &str) -> Result<String> {
        let now = chrono::Utc::now().timestamp();

        let claims = JwtClaims {
            sub: self.api_key.clone(),
            iss: "cdp".into(),
            nbf: now,
            exp: now + 120,
            uri: uri.to_string(),
        };

        let header = Header {
            kid: Some(self.api_key.clone()),
            alg: Algorithm::ES256,
            ..Default::default()
        };

        let sk = SecretKey::from_sec1_pem(&self.api_secret).map_err(|_| {
            ConversionError::JwtKeyParse {
                parse_step: "from_sec1_pem".to_string(),
            }
        })?;

        let pkcs8_pem =
            sk.to_pkcs8_pem(Default::default())
                .map_err(|_| ConversionError::JwtKeyParse {
                    parse_step: "to_pkcs8_pem".to_string(),
                })?;

        let jwt_key =
            jsonwebtoken::EncodingKey::from_ec_pem(pkcs8_pem.as_bytes()).map_err(|_| {
                ConversionError::JwtKeyParse {
                    parse_step: "from_ec_pem".to_string(),
                }
            })?;

        encode(&header, &claims, &jwt_key).context(JwtEncodeSnafu)
    }

    async fn get_btc_account_id(&self) -> Result<String> {
        let endpoint_url = self.coinbase_base_url.join("/v2/accounts").map_err(|_| {
            InvalidRequestSnafu {
                reason: "Failed to construct accounts URL".to_string(),
            }
            .build()
        })?;
        let uri = format!("GET {}", endpoint_url.path());
        let jwt = self.build_jwt(&uri)?;

        let text = self
            .http_client
            .get(endpoint_url)
            .header("Authorization", format!("Bearer {jwt}"))
            .send()
            .await
            .context(HttpRequestSnafu)?
            .text()
            .await
            .context(HttpRequestSnafu)?;

        let v: serde_json::Value = serde_json::from_str(&text).context(JsonDecodeSnafu)?;

        let data = v["data"].as_array().context(InvalidRequestSnafu {
            reason: "accounts.data is not an array",
        })?;

        let btc = data
            .iter()
            .find(|a| a["currency"]["code"] == "BTC")
            .context(InvalidRequestSnafu {
                reason: "No BTC account found",
            })?;

        let id = btc["id"]
            .as_str()
            .context(InvalidRequestSnafu {
                reason: "BTC account id missing or not string",
            })?
            .to_string();

        Ok(id)
    }

    async fn get_cbbtc_deposit_address(&self, btc_account_id: &str) -> Result<String> {
        let endpoint_path = format!("/v2/accounts/{btc_account_id}/addresses");
        let endpoint_url = self.coinbase_base_url.join(&endpoint_path).map_err(|_| {
            InvalidRequestSnafu {
                reason: "Failed to construct addresses URL".to_string(),
            }
            .build()
        })?;
        let uri = format!("GET {}", endpoint_url.path());
        let jwt = self.build_jwt(&uri)?;

        let text = self
            .http_client
            .get(endpoint_url)
            .header("Authorization", format!("Bearer {jwt}"))
            .send()
            .await
            .context(HttpRequestSnafu)?
            .text()
            .await
            .context(HttpRequestSnafu)?;

        let v: serde_json::Value = serde_json::from_str(&text).context(JsonDecodeSnafu)?;

        let data = v["data"].as_array().context(InvalidRequestSnafu {
            reason: "addresses.data is not an array",
        })?;

        let eth = data
            .iter()
            .find(|addr| addr["network"] == "ethereum")
            .context(InvalidRequestSnafu {
                reason: "No Ethereum address found",
            })?;

        let addr = eth["address"]
            .as_str()
            .context(InvalidRequestSnafu {
                reason: "Ethereum address missing or not string",
            })?
            .to_string();

        Ok(addr)
    }

    async fn get_btc_deposit_address(&self, btc_account_id: &str) -> Result<String> {
        let endpoint_path = format!("/v2/accounts/{btc_account_id}/addresses");
        let endpoint_url = self.coinbase_base_url.join(&endpoint_path).map_err(|_| {
            InvalidRequestSnafu {
                reason: "Failed to construct addresses URL".to_string(),
            }
            .build()
        })?;
        let uri = format!("GET {}", endpoint_url.path());
        let jwt = self.build_jwt(&uri)?;

        let text = self
            .http_client
            .get(endpoint_url)
            .header("Authorization", format!("Bearer {jwt}"))
            .send()
            .await
            .context(HttpRequestSnafu)?
            .text()
            .await
            .context(HttpRequestSnafu)?;

        let v: serde_json::Value = serde_json::from_str(&text).context(JsonDecodeSnafu)?;

        let data = v["data"].as_array().context(InvalidRequestSnafu {
            reason: "addresses.data is not an array",
        })?;

        let btc = data
            .iter()
            .find(|addr| addr["network"] == "bitcoin")
            .context(InvalidRequestSnafu {
                reason: "No Bitcoin address found",
            })?;

        let addr = btc["address"]
            .as_str()
            .context(InvalidRequestSnafu {
                reason: "Bitcoin address missing or not string",
            })?
            .to_string();

        Ok(addr)
    }

    async fn send_bitcoin(
        &self,
        recipient_address: &str,
        amount: &u64,
        network: ChainType,
        btc_account_id: &str,
    ) -> Result<String> {
        let endpoint_path = format!("/v2/accounts/{btc_account_id}/transactions");
        let endpoint_url = self.coinbase_base_url.join(&endpoint_path).map_err(|_| {
            InvalidRequestSnafu {
                reason: "Failed to construct transactions URL".to_string(),
            }
            .build()
        })?;
        let uri = format!("POST {}", endpoint_url.path());
        let jwt = self.build_jwt(&uri)?;

        let network = match network {
            ChainType::Bitcoin => "bitcoin",
            ChainType::Ethereum => "ethereum",
        };

        let payload = json!({
            "type": "send",
            "to": recipient_address,
            "amount": amount,
            "currency": "BTC",
            "network": network,
        });

        let text = self
            .http_client
            .post(endpoint_url)
            .header("Authorization", format!("Bearer {jwt}"))
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .context(HttpRequestSnafu)?
            .text()
            .await
            .context(HttpRequestSnafu)?;

        let v: serde_json::Value = serde_json::from_str(&text).context(JsonDecodeSnafu)?;

        let tx_id = v["data"]["id"]
            .as_str()
            .context(InvalidRequestSnafu {
                reason: "transaction id missing or not string",
            })?
            .to_string();

        Ok(tx_id)
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
        .get_btc_deposit_address(&btc_account_id)
        .await?;

    // send btc to the deposit address
    let btc_tx_hash = coinbase_client
        .send_bitcoin(
            &btc_deposit_address,
            &amount_sats,
            ChainType::Bitcoin,
            &btc_account_id,
        )
        .await?;

    // wait for the btc to be confirmed
    sender_wallet
        .guarantee_confirmations(&btc_tx_hash, 2) // coinbase needs 3 confirmations for btc transactions to credit
        .await
        .context(WalletSnafu)?;

    // at this point, the btc is confirmed and should be credited to our coinbase btc account
    // now we can send the btc to eth which will implicitly convert it to cbbtc

    let evm_tx_hash = coinbase_client
        .send_bitcoin(
            recipient_address,
            &amount_sats,
            ChainType::Ethereum,
            &btc_account_id,
        )
        .await?;

    Ok(evm_tx_hash)
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

    let available_balance = sender_wallet.balance(&cbbtc).await.context(WalletSnafu)?;

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
        .get_cbbtc_deposit_address(&btc_account_id)
        .await?;

    // send the cbbtc to the deposit address
    let cbbtc_tx_hash = sender_wallet
        .create_payment(&lot, &cbbtc_deposit_address, None)
        .await
        .context(WalletSnafu)?;

    // wait for the cbbtc to be confirmed
    sender_wallet
        .guarantee_confirmations(&cbbtc_tx_hash, 14) // coinbase needs 3 confirmations for cbbtc transactions to credit
        .await
        .context(WalletSnafu)?;

    // at this point, the cbbtc is confirmed and should be credited to our coinbase btc account
    // now we can send the btc to eth which will implicitly convert it to cbbtc

    let evm_tx_hash = coinbase_client
        .send_bitcoin(
            recipient_address,
            &amount_sats,
            ChainType::Bitcoin,
            &btc_account_id,
        )
        .await?;

    Ok(evm_tx_hash)
}

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn test_get_portfolio() {
        let coinbase_api_key = std::env::var("COINBASE_API_KEY").unwrap();
        let coinbase_api_secret = std::env::var("COINBASE_API_SECRET").unwrap();
        let coinbase_base_url = Url::parse("https://api.coinbase.com").unwrap();

        let coinbase_client =
            CoinbaseClient::new(coinbase_base_url, coinbase_api_key, coinbase_api_secret).unwrap();

        let btc_account_id = coinbase_client.get_btc_account_id().await.unwrap();
        println!("btc_account_id: {btc_account_id}");

        let btc_deposit_address = coinbase_client
            .get_btc_deposit_address(&btc_account_id)
            .await
            .unwrap();
        println!("btc_deposit_address: {btc_deposit_address}");
    }
}
