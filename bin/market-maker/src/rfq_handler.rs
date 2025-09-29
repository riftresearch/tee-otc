use otc_protocols::rfq::{ProtocolMessage, RFQRequest, RFQResponse, RFQResult};
use std::{sync::Arc, time::Instant};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::balance_strat::QuoteBalanceStrategy;
use crate::quote_storage::QuoteStorage;
use crate::wallet::WalletManager;
use crate::wrapped_bitcoin_quoter::WrappedBitcoinQuoter;
use crate::QUOTE_LATENCY_METRIC;

#[derive(Clone)]
pub struct RFQMessageHandler {
    market_maker_id: Uuid,
    wrapped_bitcoin_quoter: Arc<WrappedBitcoinQuoter>,
    quote_storage: Arc<QuoteStorage>,
    wallet_manager: Arc<WalletManager>,
    balance_strategy: QuoteBalanceStrategy,
}

impl RFQMessageHandler {
    pub fn new(
        market_maker_id: Uuid,
        wrapped_bitcoin_quoter: Arc<WrappedBitcoinQuoter>,
        quote_storage: Arc<QuoteStorage>,
        wallet_manager: Arc<WalletManager>,
        balance_strategy: QuoteBalanceStrategy,
    ) -> Self {
        Self {
            market_maker_id,
            wrapped_bitcoin_quoter,
            quote_storage,
            wallet_manager,
            balance_strategy,
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
                let mut rfq_result = match quote {
                    Ok(quote) => quote,
                    Err(error) => {
                        error!("Failed to compute quote: {error:?}");
                        record_quote_latency(&start, "error", "quote_computation_failed");
                        return None;
                    }
                };

                // Check if we have sufficient balance to fulfill the quote
                if let RFQResult::Success(ref quote_with_fees) = rfq_result {
                    let wallet = self
                        .wallet_manager
                        .get(quote_with_fees.quote.to.currency.chain);

                    // TODO: consider getting balance more efficiently?
                    let validated_rfq_result = if let Some(wallet) = wallet {
                        let balance = wallet
                            .balance(&quote_with_fees.quote.to.currency.token)
                            .await;

                        if balance.is_err() {
                            warn!("Failed to check wallet balance: {:?}", balance.err());
                            RFQResult::MakerUnavailable(
                                "Failed to check wallet balance".to_string(),
                            )
                        } else {
                            let balance = balance.unwrap().total_balance;
                            info!("Wallet balance: {:?}", balance);
                            if !self
                                .balance_strategy
                                .can_fill_quote(&quote_with_fees.quote, balance)
                            {
                                RFQResult::MakerUnavailable(
                                    "Insufficient balance to fulfill quote".to_string(),
                                )
                            } else {
                                RFQResult::Success(quote_with_fees.clone())
                            }
                        }
                    } else {
                        warn!(
                            "No wallet configured for chain {:?}",
                            quote_with_fees.quote.to.currency.chain
                        );
                        RFQResult::MakerUnavailable("No wallet configured for chain".to_string())
                    };
                    rfq_result = validated_rfq_result;
                }

                let quote = match &rfq_result {
                    RFQResult::Success(quote) => Some(quote.quote.clone()),
                    RFQResult::MakerUnavailable(_) => None,
                    RFQResult::InvalidRequest(_) => None,
                };

                let (status, reason) = match &rfq_result {
                    RFQResult::Success(_) => ("ok", "none"),
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
                    if let Err(e) = self.quote_storage.store_quote(&quote).await {
                        error!("Failed to store quote {}: {}", quote.id, e);
                    } else {
                        info!("Stored quote {} in database", quote.id);
                        if let Err(e) = self.quote_storage.mark_sent_to_rfq(quote.id).await {
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
                if let Err(e) = self.quote_storage.mark_sent_to_otc(*quote_id).await {
                    error!("Failed to mark quote {} as sent to OTC: {}", quote_id, e);
                }
                None
            }
            RFQRequest::Ping {
                request_id,
                timestamp: _,
            } => {
                let response = RFQResponse::Pong {
                    request_id: *request_id,
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
