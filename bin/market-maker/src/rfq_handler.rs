use async_trait::async_trait;
use otc_protocols::rfq::{ProtocolMessage, RFQRequest, RFQResponse, RFQResult};
use std::{sync::Arc, time::Instant};
use tracing::{error, info};
use uuid::Uuid;

use crate::db::QuoteRepository;
use crate::liquidity_cache::LiquidityCache;
use crate::websocket_client::MessageHandler;
use crate::wrapped_bitcoin_quoter::WrappedBitcoinQuoter;
use crate::QUOTE_LATENCY_METRIC;

#[derive(Clone)]
pub struct RFQMessageHandler {
    market_maker_id: Uuid,
    wrapped_bitcoin_quoter: Arc<WrappedBitcoinQuoter>,
    quote_repository: Arc<QuoteRepository>,
    liquidity_cache: Arc<LiquidityCache>,
}

impl RFQMessageHandler {
    pub fn new(
        market_maker_id: Uuid,
        wrapped_bitcoin_quoter: Arc<WrappedBitcoinQuoter>,
        quote_repository: Arc<QuoteRepository>,
        liquidity_cache: Arc<LiquidityCache>,
    ) -> Self {
        Self {
            market_maker_id,
            wrapped_bitcoin_quoter,
            quote_repository,
            liquidity_cache,
        }
    }

    pub async fn handle_request(
        &self,
        msg: &ProtocolMessage<RFQRequest>,
    ) -> Option<ProtocolMessage<RFQResponse>> {
        match &msg.payload {
            RFQRequest::QuoteRequested {
                request_id,
                request,
                timestamp: _,
            } => {
                let start = Instant::now();
                info!(
                    "Received RFQ quote request: request_id={}, mode={:?}, from_chain={:?}, amount={}, to_chain={:?}",
                    request_id, request.mode, request.from.chain, request.amount, request.to.chain
                );

                let quote = self
                    .wrapped_bitcoin_quoter
                    .compute_quote(self.market_maker_id, request)
                    .await;
                let rfq_result = match quote {
                    Ok(quote) => quote,
                    Err(error) => {
                        error!("Failed to compute quote: {error:?}");
                        record_quote_latency(&start, "error", "quote_computation_failed");
                        return None;
                    }
                };

                let quote = match &rfq_result {
                    RFQResult::Success(quote) => Some(quote.clone()),
                    RFQResult::MakerUnavailable(_) => None,
                    RFQResult::InvalidRequest(_) => None,
                };

                let (status, reason) = match &rfq_result {
                    RFQResult::Success(_) => ("ok", "success"),
                    RFQResult::MakerUnavailable(_) => ("error", "maker_unavailable"),
                    RFQResult::InvalidRequest(_) => ("error", "invalid_request"),
                };

                record_quote_latency(&start, status, reason);

                // TODO: consider deferring the execution of the following to a seperate async task to prevent blocking?
                if let Some(quote) = quote {
                    info!(
                        "Generated quote: id={}, from_chain={:?}, from_amount={}, to_chain={:?}, to_amount={}",
                        quote.id, quote.from.currency.chain, quote.from.amount, quote.to.currency.chain , quote.to.amount
                    );
                    if let Err(e) = self.quote_repository.store_quote(&quote).await {
                        error!("Failed to store quote {}: {}", quote.id, e);
                    } else {
                        info!("Stored quote {} in database", quote.id);
                        if let Err(e) = self.quote_repository.mark_sent_to_rfq(quote.id).await {
                            error!("Failed to mark quote {} as sent to RFQ: {}", quote.id, e);
                        }
                    }
                }

                let response = RFQResponse::QuoteResponse {
                    request_id: *request_id,
                    quote: rfq_result,
                    timestamp: utc::now(),
                };

                Some(ProtocolMessage {
                    version: msg.version.clone(),
                    sequence: msg.sequence,
                    payload: response,
                })
            }
            RFQRequest::QuoteSelected {
                request_id,
                quote_id,
                timestamp: _,
            } => {
                info!(
                    "Our quote {} was selected! Request ID: {}",
                    quote_id, request_id
                );
                // Mark the quote as sent to OTC since it was selected
                if let Err(e) = self.quote_repository.mark_sent_to_otc(*quote_id).await {
                    error!("Failed to mark quote {} as sent to OTC: {}", quote_id, e);
                }
                None
            }
            RFQRequest::LiquidityRequest {
                request_id,
                timestamp: _,
            } => {
                info!("Received liquidity request: request_id={}", request_id);

                // Get liquidity from cache
                let liquidity = self.liquidity_cache.get_liquidity().await;

                info!(
                    "Responding with {} trading pairs for liquidity request {}",
                    liquidity.len(),
                    request_id
                );

                let response = RFQResponse::LiquidityResponse {
                    request_id: *request_id,
                    liquidity,
                    timestamp: utc::now(),
                };

                Some(ProtocolMessage {
                    version: msg.version.clone(),
                    sequence: msg.sequence,
                    payload: response,
                })
            }
        }
    }
}

fn record_quote_latency(start: &Instant, status: &'static str, reason: &'static str) {
    let elapsed = start.elapsed().as_secs_f64();
    metrics::histogram!(
        QUOTE_LATENCY_METRIC,
        "endpoint" => "quotes_request",
        "status" => status,
        "reason" => reason
    )
    .record(elapsed);
}

#[async_trait]
impl MessageHandler for RFQMessageHandler {
    async fn handle_text(&self, text: &str) -> Option<String> {
        match serde_json::from_str::<ProtocolMessage<RFQRequest>>(text) {
            Ok(msg) => {
                if let Some(response) = self.handle_request(&msg).await {
                    match serde_json::to_string(&response) {
                        Ok(json) => Some(json),
                        Err(e) => {
                            error!("Failed to serialize RFQ response: {}", e);
                            None
                        }
                    }
                } else {
                    None
                }
            }
            Err(e) => {
                error!("Failed to parse RFQ message: {}", e);
                None
            }
        }
    }
}
