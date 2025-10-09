use std::{error::Error as StdError, sync::Arc, time::Instant};

use anyhow::{anyhow, Context, Result};
use otc_models::{ChainType, Currency, Lot, Quote, QuoteRequest, TokenIdentifier};
use otc_protocols::rfq::RFQResult;
use otc_server::api::swaps::{CreateSwapRequest, CreateSwapResponse, SwapResponse};
use reqwest::{Client, Url};
use rfq_server::server::QuoteResponse;
use tokio::{sync::mpsc::UnboundedSender, time::sleep};
use tracing::{debug, error};
use uuid::Uuid;

use crate::{
    args::Config,
    status::{SwapStage, SwapUpdate, UiEvent},
    wallets::PaymentWallet,
};

pub struct RunSummary {
    pub succeeded: usize,
    pub failed: usize,
}

pub async fn run_load_test(
    config: Arc<Config>,
    wallet: PaymentWallet,
    update_tx: UnboundedSender<UiEvent>,
) -> Result<RunSummary> {
    let client = Client::builder()
        .pool_max_idle_per_host(config.total_swaps) // Allow more idle connections
        .pool_idle_timeout(std::time::Duration::from_secs(90))
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .context("failed to build HTTP client")?;

    let create_swap_url = join_path(&config.otc_url, "/api/v1/swaps")?;
    let swap_status_base = join_path(&config.otc_url, "/api/v1/swaps/")?;

    let mut handles = Vec::with_capacity(config.total_swaps);
    for idx in 0..config.total_swaps {
        if idx > 0 && idx % config.swaps_per_interval == 0 {
            sleep(config.interval).await;
        }
        let ctx = SwapContext {
            client: client.clone(),
            config: config.clone(),
            quote_url: config.quote_url.clone(),
            create_swap_url: create_swap_url.clone(),
            swap_status_base: swap_status_base.clone(),
            wallet: wallet.clone(),
            update_tx: update_tx.clone(),
            index: idx,
        };
        handles.push(tokio::spawn(async move {
            match run_single_swap(ctx).await {
                Ok(res) => Ok(res),
                Err(err) => {
                    error!(error = %err, "swap task failed");
                    Err(err)
                }
            }
        }));
    }

    let mut succeeded = 0usize;
    let mut failed = 0usize;

    for handle in handles {
        match handle.await {
            Ok(Ok(())) => succeeded += 1,
            Ok(Err(err)) => {
                failed += 1;
                error!(error = %err, "swap task failed");
            }
            Err(join_err) => {
                failed += 1;
                error!(error = %join_err, "swap task panicked or was cancelled");
            }
        }
    }

    Ok(RunSummary { succeeded, failed })
}

struct SwapContext {
    client: Client,
    config: Arc<Config>,
    quote_url: Url,
    create_swap_url: Url,
    swap_status_base: Url,
    wallet: PaymentWallet,
    update_tx: UnboundedSender<UiEvent>,
    index: usize,
}

async fn run_single_swap(ctx: SwapContext) -> Result<()> {
    let SwapContext {
        client,
        config,
        quote_url,
        create_swap_url,
        swap_status_base,
        wallet,
        update_tx,
        index,
    } = ctx;

    send_update(
        &update_tx,
        SwapUpdate::new(index, SwapStage::QuoteRequested),
    );

    let quote = match request_quote(&client, &quote_url, &config.quote_request).await {
        Ok(q) => q,
        Err(err) => {
            send_update(
                &update_tx,
                SwapUpdate::new(
                    index,
                    SwapStage::QuoteFailed {
                        reason: err.to_string(),
                    },
                ),
            );
            return Err(err);
        }
    };
    let quote_id = quote.id;
    send_update(
        &update_tx,
        SwapUpdate::new(index, SwapStage::QuoteReceived { quote_id }),
    );

    let create_swap_request = CreateSwapRequest {
        quote: quote.clone(),
        user_destination_address: config.user_destination_address.clone(),
        user_evm_account_address: config.user_evm_account_address,
        metadata: None,
    };

    let swap_response = match create_swap(&client, &create_swap_url, &create_swap_request).await {
        Ok(resp) => resp,
        Err(err) => {
            send_update(
                &update_tx,
                SwapUpdate::new(
                    index,
                    SwapStage::FinishedWithError {
                        swap_id: None,
                        reason: format!("swap creation failed: {err}"),
                    },
                ),
            );
            return Err(err);
        }
    };

    let swap_id = swap_response.swap_id;
    send_update(
        &update_tx,
        SwapUpdate::new(index, SwapStage::SwapSubmitted { swap_id }),
    );

    let deposit_lot =
        lot_from_response(&swap_response).context("invalid deposit lot in response")?;

    let tx_hash = match wallet
        .create_payment(&deposit_lot, &swap_response.deposit_address)
        .await
    {
        Ok(hash) => hash,
        Err(err) => {
            send_update(
                &update_tx,
                SwapUpdate::new(
                    index,
                    SwapStage::PaymentFailed {
                        swap_id: Some(swap_id),
                        reason: err.to_string(),
                    },
                ),
            );
            return Err(err);
        }
    };

    send_update(
        &update_tx,
        SwapUpdate::new(
            index,
            SwapStage::PaymentBroadcast {
                swap_id,
                tx_hash: tx_hash.clone(),
            },
        ),
    );

    match poll_swap_status(
        &client,
        &swap_status_base,
        swap_id,
        config.poll_interval,
        config.swap_timeout,
        &update_tx,
        index,
    )
    .await
    {
        Ok(()) => {
            send_update(
                &update_tx,
                SwapUpdate::new(index, SwapStage::Settled { swap_id }),
            );
            Ok(())
        }
        Err(err) => {
            send_update(
                &update_tx,
                SwapUpdate::new(
                    index,
                    SwapStage::FinishedWithError {
                        swap_id: Some(swap_id),
                        reason: err.to_string(),
                    },
                ),
            );
            Err(err)
        }
    }
}

async fn request_quote(client: &Client, url: &Url, request: &QuoteRequest) -> Result<Quote> {
    let response = client
        .post(url.clone())
        .json(request)
        .send()
        .await
        .map_err(|e| {
            // Detailed error inspection to diagnose resource exhaustion
            tracing::error!(
                error = %e,
                is_timeout = e.is_timeout(),
                is_connect = e.is_connect(),
                is_request = e.is_request(),
                source = ?e.source(),
                "quote request failed with detailed error info"
            );
            e
        })
        .with_context(|| format!("failed to send quote request to {}", url))?;

    if !response.status().is_success() {
        let response_text = response
            .text()
            .await
            .context("failed to get response text")?;
        return Err(anyhow!(
            "quote request returned error text: {}",
            response_text
        ));
    }

    let response_text = response.text().await.unwrap();

    tracing::info!("quote request response: {}", response_text);
    /*

       {"request_id":"30968d0d-9c30-47b5-8192-69c641927b58","quote":{"type":"success","data":{"quote":{"id":"a253750b-0aa1-4ef7-8d72-916efb5ed1ff","market_maker_id":"a4c6da0d-a071-40ea-b69c-e23d49327d42","from":{"currency":{"chain":"ethereum","token":{"type":"│
    │Address","data":"0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf"},"decimals":8},"amount":"0x2711"},"to":{"currency":{"chain":"bitcoin","token":{"type":"Native"},"decimals":8},"amount":"0x2425"},"expires_at":"2025-10-03T19:16:48.257931300Z","created_at":"202│
    │5-10-03T19:11:48.257932966Z"},"fees":{"network_fee_sats":448,"liquidity_fee_sats":0,"protocol_fee_sats":300}}},"total_quotes_received":1,"market_makers_contacted":1}                                                                                        │
         */

    let body: QuoteResponse = serde_json::from_str(&response_text).unwrap();
    match body.quote {
        Some(RFQResult::Success(quote)) => Ok(quote),
        Some(RFQResult::MakerUnavailable(reason)) => {
            Err(anyhow!("no market maker available for request: {reason}"))
        }
        Some(RFQResult::InvalidRequest(reason)) => {
            Err(anyhow!("quote request rejected as invalid: {reason}"))
        }
        None => Err(anyhow!("quote response did not include a quote")),
    }
}

async fn create_swap(
    client: &Client,
    url: &Url,
    request: &CreateSwapRequest,
) -> Result<CreateSwapResponse> {
    tracing::info!(request = ?request, "creating swap");
    let response = client
        .post(url.clone())
        .json(request)
        .send()
        .await
        .map_err(|e| {
            // Detailed error inspection to diagnose resource exhaustion
            tracing::error!(
                error = %e,
                is_timeout = e.is_timeout(),
                is_connect = e.is_connect(),
                is_request = e.is_request(),
                is_body = e.is_body(),
                is_decode = e.is_decode(),
                source = ?e.source(),
                "swap creation request failed with detailed error info"
            );
            anyhow!(format!(
                "failed to send swap creation request to {}: {}",
                url, e
            ))
        })?;
    if !response.status().is_success() {
        let response_text = response
            .text()
            .await
            .context("failed to get response text")?;
        return Err(anyhow!(
            "swap creation returned error status: {}",
            response_text
        ));
    }

    response
        .json()
        .await
        .context("failed to parse swap creation response")
}

fn lot_from_response(response: &CreateSwapResponse) -> Result<Lot> {
    let chain = parse_chain_type(&response.deposit_chain)?;
    let token = parse_token_identifier(&response.token);

    if chain == ChainType::Ethereum && matches!(token, TokenIdentifier::Native) {
        return Err(anyhow!(
            "native Ethereum deposits are not currently supported by the load tester"
        ));
    }

    Ok(Lot {
        currency: Currency {
            chain,
            token,
            decimals: response.decimals,
        },
        amount: response.expected_amount,
    })
}

fn parse_chain_type(value: &str) -> Result<ChainType> {
    match value.to_ascii_lowercase().as_str() {
        "bitcoin" => Ok(ChainType::Bitcoin),
        "ethereum" => Ok(ChainType::Ethereum),
        _ => Err(anyhow!("unsupported chain type in response: {}", value)),
    }
}

fn parse_token_identifier(token: &str) -> TokenIdentifier {
    if token.eq_ignore_ascii_case("native") {
        TokenIdentifier::Native
    } else {
        TokenIdentifier::Address(token.to_string())
    }
}

async fn poll_swap_status(
    client: &Client,
    status_base: &Url,
    swap_id: Uuid,
    poll_interval: std::time::Duration,
    timeout: std::time::Duration,
    update_tx: &UnboundedSender<UiEvent>,
    index: usize,
) -> Result<()> {
    let status_url = status_base
        .join(&swap_id.to_string())
        .with_context(|| format!("failed to build swap status URL for {}", swap_id))?;

    let start = Instant::now();
    let mut last_status: Option<String> = None;

    loop {
        if start.elapsed() > timeout {
            return Err(anyhow!("swap {} timed out after {:?}", swap_id, timeout));
        }

        let response = client
            .get(status_url.clone())
            .send()
            .await
            .with_context(|| format!("failed to query swap status at {}", status_url))?
            .error_for_status()
            .with_context(|| format!("swap status request failed for {}", swap_id))?;

        let body: SwapResponse = response
            .json()
            .await
            .context("failed to parse swap status response")?;

        if last_status.as_deref() != Some(body.status.as_str()) {
            last_status = Some(body.status.clone());
            send_update(
                update_tx,
                SwapUpdate::new(
                    index,
                    SwapStage::StatusUpdated {
                        swap_id,
                        status: body.status.clone(),
                    },
                ),
            );
        }

        if body.status == "Settled" {
            return Ok(());
        }

        sleep(poll_interval).await;
    }
}

fn send_update(tx: &UnboundedSender<UiEvent>, update: SwapUpdate) {
    if let Err(err) = tx.send(UiEvent::Swap(update)) {
        debug!(error = %err, "failed to send swap update");
    }
}

fn join_path(base: &Url, path: &str) -> Result<Url> {
    base.join(path)
        .with_context(|| format!("failed to join '{}' onto '{}'", path, base))
}
