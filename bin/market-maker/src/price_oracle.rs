use std::time::Duration;
use std::{sync::Arc, time::Instant};

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use snafu::{prelude::*, ResultExt};
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tokio::time;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info};

#[derive(Debug, Snafu)]
pub enum PriceOracleError {
    #[snafu(display("WebSocket connection error: {}", source))]
    WebSocketConnection {
        source: tokio_tungstenite::tungstenite::Error,
    },

    #[snafu(display("Failed to send WebSocket message: {}", source))]
    WebSocketSend {
        source: tokio_tungstenite::tungstenite::Error,
    },

    #[snafu(display("Failed to parse JSON: {}", source))]
    JsonParse { source: serde_json::Error },

    #[snafu(display("No price data available"))]
    NoPriceData,

    #[snafu(display("Connection timed out"))]
    ConnectionTimedOut,
}

pub type Result<T, E = PriceOracleError> = std::result::Result<T, E>;

#[derive(Debug, Clone, Serialize)]
struct CoinbaseSubscribeMessage {
    #[serde(rename = "type")]
    msg_type: String,
    channels: Vec<ChannelSubscription>,
}

#[derive(Debug, Clone, Serialize)]
struct ChannelSubscription {
    name: String,
    product_ids: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct CoinbaseTickerMessage {
    #[serde(rename = "type")]
    msg_type: String,
    product_id: Option<String>,
    best_bid: Option<String>,
    best_ask: Option<String>,
    price: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BitcoinEtherPriceOracle {
    inner: Arc<BitcoinEtherPriceOracleInner>,
}

#[derive(Debug)]
struct BitcoinEtherPriceOracleInner {
    btc_per_eth: RwLock<Option<f64>>,
}

impl BitcoinEtherPriceOracle {
    pub fn new(join_set: &mut JoinSet<crate::Result<()>>) -> Self {
        let oracle = Self {
            inner: Arc::new(BitcoinEtherPriceOracleInner {
                btc_per_eth: RwLock::new(None),
            }),
        };

        let oracle_clone = oracle.clone();
        join_set.spawn(async move {
            oracle_clone
                .run_price_feed()
                .await
                .map_err(|e| crate::Error::BackgroundThread {
                    source: Box::new(e),
                })
        });

        oracle
    }

    async fn wait_for_connection(&self) -> Result<()> {
        let start_time = Instant::now();
        let timeout = Duration::from_secs(5);
        loop {
            if self.inner.btc_per_eth.read().await.is_some() {
                return Ok(());
            }
            if start_time.elapsed() > timeout {
                return Err(PriceOracleError::ConnectionTimedOut);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn get_btc_per_eth(&self) -> Result<f64> {
        self.wait_for_connection().await?;
        self.inner
            .btc_per_eth
            .read()
            .await
            .ok_or(PriceOracleError::NoPriceData)
    }

    pub async fn get_eth_per_btc(&self) -> Result<f64> {
        let btc_per_eth = self.get_btc_per_eth().await?;
        Ok(1.0 / btc_per_eth)
    }

    async fn run_price_feed(&self) -> Result<()> {
        const WS_URI: &str = "wss://ws-feed.exchange.coinbase.com";
        const PRODUCT_ID: &str = "ETH-BTC";
        const RECONNECT_DELAY: Duration = Duration::from_secs(1);

        loop {
            #[cfg(not(feature = "mock-price-oracle"))]
            {
                match self.connect_and_stream(WS_URI, PRODUCT_ID).await {
                    Ok(_) => {
                        tracing::warn!("WebSocket stream ended unexpectedly, reconnecting...");
                    }
                    Err(e) => {
                        tracing::error!(
                            "WebSocket error: {}, reconnecting in {:?}...",
                            e,
                            RECONNECT_DELAY
                        );
                    }
                }
            }

            #[cfg(feature = "mock-price-oracle")]
            {
                #[cfg(all(feature = "mock-price-oracle", not(test), not(debug_assertions)))]
                compile_error!(
                    "mock-price-oracle feature must not be enabled in production builds"
                );
                let mut price_writer = self.inner.btc_per_eth.write().await;
                *price_writer = Some(30.7894736842);
            }
            time::sleep(RECONNECT_DELAY).await;
        }
    }

    async fn connect_and_stream(&self, ws_uri: &str, product_id: &str) -> Result<()> {
        let (ws_stream, _) = connect_async(ws_uri)
            .await
            .context(WebSocketConnectionSnafu)?;

        let (mut write, mut read) = ws_stream.split();

        let subscribe_msg = CoinbaseSubscribeMessage {
            msg_type: "subscribe".to_string(),
            channels: vec![ChannelSubscription {
                name: "ticker".to_string(),
                product_ids: vec![product_id.to_string()],
            }],
        };

        let subscribe_json = serde_json::to_string(&subscribe_msg).context(JsonParseSnafu)?;
        write
            .send(Message::Text(subscribe_json))
            .await
            .context(WebSocketSendSnafu)?;

        info!("Subscribed to {} ticker feed", product_id);

        while let Some(message) = read.next().await {
            match message {
                Ok(Message::Text(text)) => {
                    if let Err(e) = self.process_ticker_message(&text).await {
                        debug!("Failed to process ticker message: {}", e);
                    }
                }
                Ok(Message::Ping(data)) => {
                    write
                        .send(Message::Pong(data))
                        .await
                        .context(WebSocketSendSnafu)?;
                }
                Ok(Message::Close(_)) => {
                    info!("WebSocket closed by server");
                    break;
                }
                Err(e) => {
                    error!("WebSocket error: {}", e);
                    break;
                }
                _ => {}
            }
        }

        Ok(())
    }

    async fn process_ticker_message(&self, text: &str) -> Result<()> {
        let msg: CoinbaseTickerMessage = serde_json::from_str(text).context(JsonParseSnafu)?;

        if msg.msg_type != "ticker" || msg.product_id.as_deref() != Some("ETH-BTC") {
            return Ok(());
        }

        let mid_price = self.calculate_mid_price(&msg)?;

        if let Some(price) = mid_price {
            let mut btc_per_eth = self.inner.btc_per_eth.write().await;
            let old_price = *btc_per_eth;
            *btc_per_eth = Some(price);

            if old_price.is_none_or(|old| (price - old).abs() > 1e-10) {
                let eth_per_btc = 1.0 / price;
                debug!(
                    "Price update: 1 ETH = {:.8} BTC | 1 BTC = {:.6} ETH",
                    price, eth_per_btc
                );
            }
        }

        Ok(())
    }

    fn calculate_mid_price(&self, msg: &CoinbaseTickerMessage) -> Result<Option<f64>> {
        let bid = msg.best_bid.as_deref().and_then(|s| s.parse::<f64>().ok());
        let ask = msg.best_ask.as_deref().and_then(|s| s.parse::<f64>().ok());
        let price = msg.price.as_deref().and_then(|s| s.parse::<f64>().ok());

        let mid = if let (Some(b), Some(a)) = (bid, ask) {
            if b > 0.0 && a > 0.0 {
                Some((b + a) / 2.0)
            } else {
                None
            }
        } else {
            price.filter(|&p| p > 0.0)
        };

        Ok(mid)
    }
}
