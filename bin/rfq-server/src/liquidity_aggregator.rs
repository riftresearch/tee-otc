use crate::mm_registry::RfqMMRegistry;
use chrono::{DateTime, Utc};
use futures_util::future;
use otc_protocols::rfq::{RFQResponse, TradingPairLiquidity};
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum LiquidityAggregatorError {
    #[snafu(display("No market makers connected"))]
    NoMarketMakersConnected,

    #[snafu(display("Liquidity aggregation timeout"))]
    AggregationTimeout,
}

type Result<T, E = LiquidityAggregatorError> = std::result::Result<T, E>;

pub struct LiquidityAggregator {
    mm_registry: Arc<RfqMMRegistry>,
    timeout_duration: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketMakerLiquidity {
    pub market_maker_id: Uuid,
    pub trading_pairs: Vec<TradingPairLiquidity>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidityAggregatorResult {
    pub market_makers: Vec<MarketMakerLiquidity>,
    pub timestamp: DateTime<Utc>,
}

impl LiquidityAggregator {
    #[must_use]
    pub fn new(mm_registry: Arc<RfqMMRegistry>, timeout_milliseconds: u64) -> Self {
        Self {
            mm_registry,
            timeout_duration: Duration::from_millis(timeout_milliseconds),
        }
    }

    /// Request liquidity information from all connected market makers
    pub async fn request_liquidity(&self) -> Result<LiquidityAggregatorResult> {
        let request_id = Uuid::new_v4();

        info!(
            request_id = %request_id,
            "Starting liquidity aggregation"
        );

        // Broadcast liquidity request to all connected MMs
        let receivers = self
            .mm_registry
            .broadcast_liquidity_request(&request_id)
            .await;

        if receivers.is_empty() {
            return Err(LiquidityAggregatorError::NoMarketMakersConnected);
        }

        let market_makers_contacted = receivers.len();

        // Collect liquidity responses with timeout
        let collection_result = timeout(
            self.timeout_duration,
            self.collect_liquidity(receivers, request_id),
        )
        .await;

        let market_makers = match collection_result {
            Ok(collected) => collected,
            Err(_) => return Err(LiquidityAggregatorError::AggregationTimeout),
        };

        info!(
            request_id = %request_id,
            responses_received = market_makers.len(),
            market_makers_contacted = market_makers_contacted,
            "Collected liquidity from market makers"
        );

        Ok(LiquidityAggregatorResult {
            market_makers,
            timestamp: utc::now(),
        })
    }

    /// Collect liquidity responses from market makers
    async fn collect_liquidity(
        &self,
        receivers: Vec<(Uuid, mpsc::Receiver<RFQResponse>)>,
        _request_id: Uuid,
    ) -> Vec<MarketMakerLiquidity> {
        let mut market_makers = Vec::new();

        // Convert receivers into futures
        let mut futures = Vec::new();
        for (mm_id, mut rx) in receivers {
            let future = async move {
                match rx.recv().await {
                    Some(response) => match response {
                        RFQResponse::LiquidityResponse { liquidity, .. } => {
                            Some((mm_id, liquidity))
                        }
                        _ => None,
                    },
                    None => {
                        warn!(
                            market_maker_id = %mm_id,
                            "Market maker channel closed without liquidity response"
                        );
                        None
                    }
                }
            };
            futures.push(future);
        }

        // Wait for all futures to complete
        let results = future::join_all(futures).await;

        for result in results.into_iter().flatten() {
            let (mm_id, trading_pairs) = result;
            market_makers.push(MarketMakerLiquidity {
                market_maker_id: mm_id,
                trading_pairs,
            });
        }

        market_makers
    }
}

