use crate::mm_registry::RfqMMRegistry;
use futures_util::future;
use otc_models::{Quote, QuoteRequest, SwapMode};
use otc_protocols::rfq::{RFQResponse, RFQResult};
use snafu::Snafu;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum QuoteAggregatorError {
    #[snafu(display("No market makers connected"))]
    NoMarketMakersConnected,

    #[snafu(display("No quotes received from market makers"))]
    NoQuotesReceived,

    #[snafu(display("Quote aggregation timeout"))]
    AggregationTimeout,
}

type Result<T, E = QuoteAggregatorError> = std::result::Result<T, E>;

pub struct QuoteAggregator {
    mm_registry: Arc<RfqMMRegistry>,
    timeout_duration: Duration,
}

#[derive(Debug, Clone)]
pub struct QuoteRequestResult {
    pub request_id: Uuid,
    pub best_quote: Option<RFQResult<Quote>>,
    pub total_quotes_received: usize,
    pub market_makers_contacted: usize,
}

impl QuoteAggregator {
    #[must_use]
    pub fn new(mm_registry: Arc<RfqMMRegistry>, timeout_milliseconds: u64) -> Self {
        Self {
            mm_registry,
            timeout_duration: Duration::from_millis(timeout_milliseconds),
        }
    }

    /// Request quotes from all connected market makers and return the best one
    pub async fn request_quotes(&self, request: QuoteRequest) -> Result<QuoteRequestResult> {
        let request_id = Uuid::now_v7();

        info!(
            request_id = %request_id,
            from_chain = ?request.from.chain,
            to_chain = ?request.to.chain,
            mode = ?request.mode,
            "Starting quote aggregation"
        );

        // Broadcast quote request to all connected MMs
        let receivers = self
            .mm_registry
            .broadcast_quote_request(&request_id, &request)
            .await;

        if receivers.is_empty() {
            return Err(QuoteAggregatorError::NoMarketMakersConnected);
        }

        let market_makers_contacted = receivers.len();

        // Collect quotes with timeout
        let collection_result = timeout(
            self.timeout_duration,
            self.collect_quotes(receivers, request_id),
        )
        .await;

        let quotes = match collection_result {
            Ok(collected_quotes) => collected_quotes,
            Err(_) => return Err(QuoteAggregatorError::AggregationTimeout),
        };

        if quotes.is_empty() {
            return Err(QuoteAggregatorError::NoQuotesReceived);
        }

        let total_quotes = quotes.len();

        info!(
            request_id = %request_id,
            quotes_received = total_quotes,
            market_makers_contacted = market_makers_contacted,
            "Collected quotes from market makers"
        );

        // Select the best quote based on mode:
        // - ExactInput: user sends fixed amount, best = highest output
        // - ExactOutput: user wants fixed output, best = lowest input
        let best_success_quote = select_best_quote(&quotes, &request.mode);

        // Relevant fail quote - prioritize InvalidRequest over MakerUnavailable
        // Note: Unsupported responses are silently ignored as they're not actionable by the user
        let best_fail_quote: Option<RFQResult<Quote>> = quotes
            .iter()
            .find_map(|q| match q {
                RFQResult::InvalidRequest(e) => Some(RFQResult::InvalidRequest(e.clone())),
                _ => None,
            })
            .or_else(|| {
                quotes.iter().find_map(|q| match q {
                    RFQResult::MakerUnavailable(e) => Some(RFQResult::MakerUnavailable(e.clone())),
                    _ => None,
                })
            });

        // Notify the winning market maker
        if let Some(best_quote) = best_success_quote {
            if let Err(e) = self
                .mm_registry
                .notify_quote_selected(best_quote.market_maker_id, request_id, best_quote.id)
                .await
            {
                warn!(
                    market_maker_id = %best_quote.market_maker_id,
                    quote_id = %best_quote.id,
                    error = %e,
                    "Failed to notify market maker of quote selection"
                );
            }
            Ok(QuoteRequestResult {
                request_id,
                best_quote: Some(RFQResult::Success(best_quote.clone())),
                total_quotes_received: total_quotes,
                market_makers_contacted,
            })
        } else {
            Ok(QuoteRequestResult {
                request_id,
                best_quote: best_fail_quote,
                total_quotes_received: total_quotes,
                market_makers_contacted,
            })
        }
    }

    /// Collect quotes from market makers
    async fn collect_quotes(
        &self,
        receivers: Vec<(Uuid, mpsc::Receiver<RFQResponse>)>,
        _request_id: Uuid,
    ) -> Vec<RFQResult<Quote>> {
        let mut quotes = Vec::new();

        // Convert receivers into futures
        let mut futures = Vec::new();
        for (mm_id, mut rx) in receivers {
            let future = async move {
                match rx.recv().await {
                    Some(response) => match response {
                        RFQResponse::QuoteResponse { quote, .. } => {
                            // We don't check request_id since each MM gets a unique ID
                            Some((mm_id, quote))
                        }
                        _ => None,
                    },
                    None => {
                        warn!(
                            market_maker_id = %mm_id,
                            "Market maker channel closed without response"
                        );
                        None
                    }
                }
            };
            futures.push(future);
        }

        // Wait for all futures to complete
        let results = future::join_all(futures).await;

        // TODO: We should be validating that the returned market maker id is the same as the one we sent the request to
        for (_, quote) in results.into_iter().flatten() {
            quotes.push(quote);
        }

        quotes
    }
}

/// Select the best quote based on the swap mode.
///
/// - ExactInput: User sends a fixed amount, best quote = highest output (to.amount)
/// - ExactOutput: User wants a fixed output, best quote = lowest input (from.amount)
fn select_best_quote<'a>(quotes: &'a [RFQResult<Quote>], mode: &SwapMode) -> Option<&'a Quote> {
    let success_quotes = quotes.iter().filter_map(|q| match q {
        RFQResult::Success(quote) => Some(quote),
        _ => None,
    });

    match mode {
        SwapMode::ExactInput(_) => {
            // User sends fixed amount, best = highest output
            success_quotes.max_by_key(|q| q.to.amount)
        }
        SwapMode::ExactOutput(_) => {
            // User wants fixed output, best = lowest input
            success_quotes.min_by_key(|q| q.from.amount)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otc_models::Currency;
    use otc_models::{ChainType, TokenIdentifier};

    #[tokio::test]
    async fn test_no_market_makers() {
        let registry = Arc::new(RfqMMRegistry::new(None));
        let aggregator = QuoteAggregator::new(registry, 5);

        let request = QuoteRequest {
            from: Currency {
                chain: ChainType::Bitcoin,
                token: TokenIdentifier::Native,
                decimals: 8,
            },
            to: Currency {
                chain: ChainType::Ethereum,
                token: TokenIdentifier::Native,
                decimals: 18,
            },
            mode: SwapMode::ExactInput(100_000),
            affiliate: None,
        };

        let result = aggregator.request_quotes(request).await;
        assert!(matches!(
            result,
            Err(QuoteAggregatorError::NoMarketMakersConnected)
        ));
    }
}
