use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::liquidity_cache::LiquidityCache;
use crate::price_oracle::BitcoinEtherPriceOracle;
use crate::wallet::WalletManager;
use alloy::eips::BlockNumberOrTag;
use alloy::providers::DynProvider;
use alloy::{primitives::U256, providers::Provider};
use blockchain_utils::{compute_protocol_fee_sats, inverse_compute_protocol_fee};
use otc_models::{constants, ChainType, Lot, Quote, QuoteMode, QuoteRequest};
use otc_protocols::rfq::{FeeSchedule, RFQResult};
use snafu::{Location, Snafu};
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tracing::{info, warn};
use uuid::Uuid;

const QUOTE_EXPIRATION_TIME: Duration = Duration::from_secs(60 * 5);
const FEE_UPDATE_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Debug, Snafu)]
pub enum WrappedBitcoinQuoterError {
    #[snafu(display("Failed to get fee rate from esplora: {}, at {}", source, loc))]
    Esplora {
        source: esplora_client::Error,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Fee update timeout"))]
    FeeUpdateTimeout,
}

type Result<T, E = WrappedBitcoinQuoterError> = std::result::Result<T, E>;

pub struct WrappedBitcoinQuoter {
    trade_spread_bps: u64,
    wallet_registry: Arc<WalletManager>,
    fee_map: Arc<RwLock<HashMap<ChainType, u64>>>,
    liquidity_cache: Arc<LiquidityCache>,
}

impl WrappedBitcoinQuoter {
    pub fn new(
        wallet_registry: Arc<WalletManager>,
        liquidity_cache: Arc<LiquidityCache>,
        btc_eth_price_oracle: BitcoinEtherPriceOracle,
        esplora_client: esplora_client::AsyncClient,
        eth_provider: DynProvider,
        trade_spread_bps: u64,
        fee_safety_multiplier: f64,
        join_set: &mut JoinSet<crate::Result<()>>,
    ) -> Self {
        let fee_map = Arc::new(RwLock::new(HashMap::new()));
        let fee_map_clone = fee_map.clone();
        join_set.spawn(async move {
            Self::fee_update_loop(
                esplora_client,
                eth_provider,
                fee_safety_multiplier,
                btc_eth_price_oracle,
                fee_map_clone,
            )
            .await
            .map_err(|e| crate::Error::BackgroundThread {
                source: Box::new(e),
            })
        });

        Self {
            wallet_registry,
            trade_spread_bps,
            fee_map,
            liquidity_cache,
        }
    }

    pub async fn ensure_cache_ready(&self) -> Result<()> {
        let start_time = Instant::now();
        let timeout = Duration::from_secs(30);
        loop {
            if start_time.elapsed() > timeout {
                return FeeUpdateTimeoutSnafu.fail();
            }
            if !self.fee_map.read().await.is_empty() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn fee_update_loop(
        esplora_client: esplora_client::AsyncClient,
        eth_provider: DynProvider,
        fee_safety_multiplier: f64,
        btc_eth_price_oracle: BitcoinEtherPriceOracle,
        fee_map: Arc<RwLock<HashMap<ChainType, u64>>>,
    ) -> Result<()> {
        loop {
            let fee_history = match eth_provider
                .get_fee_history(10u64, BlockNumberOrTag::Latest, &[25.0, 50.0, 75.0])
                .await
            {
                Ok(history) => history,
                Err(e) => {
                    warn!("Failed to get fee history during fee update: {:?}, trying again later...", e);
                    tokio::time::sleep(FEE_UPDATE_INTERVAL).await;
                    continue;
                }
            };

            let base_fee_wei: u128 = match fee_history.next_block_base_fee() {
                Some(base_fee_wei) => base_fee_wei,
                None => {
                    warn!("Failed to get base fee from fee_history next block, trying again later...");
                    tokio::time::sleep(FEE_UPDATE_INTERVAL).await;
                    continue;
                }
            };
            let base_fee_gwei: f64 = (base_fee_wei as f64) / 1e9f64;

            let mid_priority_wei: u128 = match fee_history
                .reward
                .as_ref()
                .and_then(|rewards| rewards.last())
                .and_then(|percentiles| percentiles.get(1)) // 50th percentile
                .copied() { 
                    Some(mid_priority_wei) => mid_priority_wei,
                    None => {
                        warn!("Failed to get mid priority fee from fee_history, trying again later...");
                        tokio::time::sleep(FEE_UPDATE_INTERVAL).await;
                        continue;
                    }
                };

            let mut max_priority_fee_gwei: f64 = (mid_priority_wei as f64) / 1e9f64;

            max_priority_fee_gwei *= fee_safety_multiplier;

            let eth_per_btc_price = match btc_eth_price_oracle.get_eth_per_btc().await {
                Ok(p) => p,
                Err(e) => {
                    warn!("Failed to get BTC/ETH price during fee update: {:?}", e);
                    tokio::time::sleep(FEE_UPDATE_INTERVAL).await;
                    continue;
                }
            };

            let sats_per_vbyte_by_confirmations = match esplora_client
                .get_fee_estimates()
                .await
                {
                    Ok(estimates) => estimates,
                    Err(e) => {
                        warn!("Failed to get fee estimates from esplora: {:?}", e);
                        tokio::time::sleep(FEE_UPDATE_INTERVAL).await;
                        continue;
                    }
                };
            let sats_per_vbyte = sats_per_vbyte_by_confirmations.get(&1).unwrap_or(&1.5);
            let sats_per_vbyte = sats_per_vbyte * fee_safety_multiplier;

            let send_fee_sats_on_btc = calculate_fees_in_sats_for_market_maker_to_send_btc_and_receive_cbbtc_vault(
                sats_per_vbyte,
                base_fee_gwei,
                max_priority_fee_gwei,
                eth_per_btc_price,
            );

            let send_fee_sats_on_eth = calculate_fees_in_sats_for_market_maker_to_send_cbbtc_and_receive_btc_vault(
                base_fee_gwei,
                max_priority_fee_gwei,
                eth_per_btc_price,
                sats_per_vbyte,
            );

            let mut global_fee_map = fee_map.write().await;
            global_fee_map.insert(ChainType::Bitcoin, send_fee_sats_on_btc);
            global_fee_map.insert(ChainType::Ethereum, send_fee_sats_on_eth);
            drop(global_fee_map);

            tokio::time::sleep(FEE_UPDATE_INTERVAL).await;
        }
    }


    /// Compute a quote for the given amount and quote mode.
    /// Note that fill_chain is the chain that the market maker will fill the quote on.
    /// which is relevant for computing fees
    pub async fn compute_quote(
        &self,
        market_maker_id: Uuid,
        quote_request: &QuoteRequest,
    ) -> Result<RFQResult<Quote>> {
        if let Some(error_message) = is_fillable_request(quote_request) {
            info!("Unfillable quote request: {:?}", quote_request);
            return Ok(RFQResult::InvalidRequest(error_message));
        }
        if quote_request.amount > U256::from(u64::MAX) {
            return Ok(RFQResult::InvalidRequest("Amount too large".to_string()));
        }
        let amount = quote_request.amount.to::<u64>();
        let quote_id = Uuid::new_v4();
        let send_fees_in_sats = self
            .fee_map
            .read()
            .await
            .get(&quote_request.to.chain)
            .cloned();

        if send_fees_in_sats.is_none() {
            return Ok(RFQResult::InvalidRequest(
                "Network fee for chain not found".to_string(),
            ));
        }

        let send_fees_in_sats = send_fees_in_sats.unwrap();

        let rfq_result = match quote_request.mode {
            QuoteMode::ExactInput => {
                let quote_result =
                    quote_exact_input(amount, send_fees_in_sats, self.trade_spread_bps);

                match quote_result {
                    RFQResult::Success((rx_btc, fees)) => RFQResult::Success(Quote {
                        id: quote_id,
                        market_maker_id,
                        from: Lot {
                            currency: quote_request.from.clone(),
                            amount: quote_request.amount,
                        },
                        to: Lot {
                            currency: quote_request.to.clone(),
                            amount: U256::from(rx_btc),
                        },
                        fee_schedule: fees,
                        expires_at: utc::now() + QUOTE_EXPIRATION_TIME,
                        created_at: utc::now(),
                    }),
                    RFQResult::MakerUnavailable(error) => RFQResult::MakerUnavailable(error),
                    RFQResult::InvalidRequest(error) => RFQResult::InvalidRequest(error),
                }
            }
            QuoteMode::ExactOutput => {
                let quote_result =
                    quote_exact_output(amount, send_fees_in_sats, self.trade_spread_bps);
                match quote_result {
                    RFQResult::Success((tx_btc, fees)) => RFQResult::Success(Quote {
                        id: quote_id,
                        market_maker_id,
                        from: Lot {
                            currency: quote_request.from.clone(),
                            amount: U256::from(tx_btc),
                        },
                        to: Lot {
                            currency: quote_request.to.clone(),
                            amount: quote_request.amount,
                        },
                        fee_schedule: fees,
                        expires_at: utc::now() + QUOTE_EXPIRATION_TIME,
                        created_at: utc::now(),
                    }),
                    RFQResult::MakerUnavailable(error) => RFQResult::MakerUnavailable(error),
                    RFQResult::InvalidRequest(error) => RFQResult::InvalidRequest(error),
                }
            }
        };

        Ok(self.validate_quote_balance(rfq_result).await)
    }

    async fn validate_quote_balance(&self, rfq_result: RFQResult<Quote>) -> RFQResult<Quote> {
        match rfq_result {
            RFQResult::Success(quote) => match self.ensure_cached_balance_can_fill(&quote).await {
                Ok(()) => RFQResult::Success(quote),
                Err(message) => RFQResult::MakerUnavailable(message),
            },
            other => other,
        }
    }

    async fn ensure_cached_balance_can_fill(&self, quote: &Quote) -> Result<(), String> {
        let chain = quote.to.currency.chain;
        let token = &quote.to.currency.token;

        if !self.wallet_registry.is_registered(chain) {
            warn!(
                "Cannot fill quote {}: no wallet configured for chain {:?} and token {:?}",
                quote.id, chain, token
            );
            return Err("No wallet configured for chain".to_string());
        }

        // Use LiquidityCache to check if quote can be filled
        if !self
            .liquidity_cache
            .can_fill_quote_for_pair(
                &quote.from.currency,
                &quote.to.currency,
                quote.to.amount,
            )
            .await
        {
            warn!(
                "Cannot fill quote {}: insufficient balance for chain {:?} token {:?}",
                quote.id, chain, token
            );
            return Err("Insufficient balance to fulfill quote".to_string());
        }

        Ok(())
    }
}

fn is_fillable_request(quote_request: &QuoteRequest) -> Option<String> {
    if quote_request.from.chain == quote_request.to.chain {
        info!("Invalid chain selection: {:?}", quote_request);
        return Some("From and to chains cannot be the same".to_string());
    }
    match constants::SUPPORTED_TOKENS_BY_CHAIN.get(&quote_request.from.chain) {
        Some(supported_tokens) => {
            if !supported_tokens.contains(&quote_request.from.token) {
                return Some("Invalid send token".to_string());
            }
        }
        None => {
            return Some("Invalid send chain".to_string());
        }
    }

    match constants::SUPPORTED_TOKENS_BY_CHAIN.get(&quote_request.to.chain) {
        Some(supported_tokens) => {
            if !supported_tokens.contains(&quote_request.to.token) {
                return Some("Invalid receive token".to_string());
            }
        }
        None => {
            return Some("Invalid receive chain".to_string());
        }
    }

    None
}

/// P2PKH is the MOST expensive address to send BTC to, dust limit wise, so we use this as our minimum
const MIN_DUST_SATS: u64 = 546;

fn quote_exact_input(
    sent_sats: u64,
    fee_sats: u64,
    trade_spread_bps: u64,
) -> RFQResult<(u64, FeeSchedule)> {
    const BPS_DENOM: u64 = 10_000;

    let tx = sent_sats;
    let network_fee = fee_sats;
    let s = trade_spread_bps;

    if s >= BPS_DENOM {
        return RFQResult::MakerUnavailable("Profit spread is >= 100%".to_string());
    }

    let rx_before_fees = tx.saturating_mul(BPS_DENOM - s) / BPS_DENOM;

    let liquidity_fee = tx - rx_before_fees;

    let rx_after_network_fee = rx_before_fees.saturating_sub(network_fee);

    let protocol_fee = compute_protocol_fee_sats(rx_after_network_fee);
    let final_rx = rx_after_network_fee.saturating_sub(protocol_fee);

    if final_rx <= MIN_DUST_SATS {
        return RFQResult::InvalidRequest("Amount out too low net of fees".to_string());
    }

    RFQResult::Success((
        final_rx,
        FeeSchedule {
            network_fee_sats: network_fee,
            liquidity_fee_sats: liquidity_fee,
            protocol_fee_sats: protocol_fee,
        },
    ))
}

fn quote_exact_output(
    received_sats: u64,
    network_fee_sats: u64,
    trade_spread_bps: u64,
) -> RFQResult<(u64, FeeSchedule)> {
    const BPS_DENOM: u64 = 10_000;

    if received_sats < MIN_DUST_SATS {
        return RFQResult::InvalidRequest("Amount out too low".to_string());
    }

    let s = trade_spread_bps;
    if s >= BPS_DENOM {
        return RFQResult::MakerUnavailable("Profit spread is >= 100%".to_string());
    }

    let rx_after_protocol_fee = inverse_compute_protocol_fee(received_sats);
    let protocol_fee = rx_after_protocol_fee - received_sats;

    let rx_after_fees = rx_after_protocol_fee.saturating_add(network_fee_sats);

    let numerator = BPS_DENOM.saturating_mul(rx_after_fees);
    let denominator = BPS_DENOM - s;
    let tx = numerator.div_ceil(denominator);

    let liquidity_fee = tx - rx_after_fees;

    if tx < MIN_DUST_SATS {
        return RFQResult::InvalidRequest("Amount out too low net of fees".to_string());
    }

    RFQResult::Success((
        tx,
        FeeSchedule {
            network_fee_sats,
            liquidity_fee_sats: liquidity_fee,
            protocol_fee_sats: protocol_fee,
        },
    ))
}

// NOTICE: We want the following methods to include the cost for the market maker to send 
// on x chain directly to you as well as the cost to spend the deposit vault the
// user creates on y chain , all denominated in sats

fn calculate_fees_in_sats_for_market_maker_to_send_btc_and_receive_cbbtc_vault(
    sats_per_vbyte: f64,
    base_fee_gwei: f64,
    max_priority_fee_gwei: f64,
    eth_per_btc_price: f64,
) -> u64 {
    // 1 P2WPKH input + 2 P2WPKH output + 1 OP_RETURN output w/ 32 bytes
    let vbytes = (68 + (31 * 2) + 44) as f64;
    // TODO: if this method is called that means the user gave us a cbBTC deposit vault
    // so we need to add the cost to spend that cbBTC deposit vault to the quote as well
    let btc_cost_sats = (sats_per_vbyte * vbytes).ceil() as u64;
    // receiveWithAuthorization later, calldata
    let gas_limit = 57670 + 2872;
    let eth_spend_vault_cost_sats = eth_fees_in_sats(gas_limit as f64, base_fee_gwei, max_priority_fee_gwei, eth_per_btc_price);
    btc_cost_sats + eth_spend_vault_cost_sats
}

fn calculate_fees_in_sats_for_market_maker_to_send_cbbtc_and_receive_btc_vault(
    base_fee_gwei: f64,
    max_priority_fee_gwei: f64,
    eth_per_btc_price: f64,
    sats_per_vbyte: f64,
) -> u64 {
    // base tx cost, transfer incl cold-access, calldata
    let transfer_gas_limit = (21000 + 11642 + 572) as f64;
    let eth_cost_sats = eth_fees_in_sats(transfer_gas_limit, base_fee_gwei, max_priority_fee_gwei, eth_per_btc_price);
    // spend a P2WPKH input later
    let btc_spend_vault_cost_sats = (68.0 * sats_per_vbyte).ceil() as u64;
    eth_cost_sats + btc_spend_vault_cost_sats
}


fn eth_fees_in_sats(
    gas_limit: f64,
    base_fee_gwei: f64,
    max_priority_fee_gwei: f64,
    eth_per_btc_price: f64,
) -> u64 {
    let gas_cost_gwei = gas_limit * (max_priority_fee_gwei + base_fee_gwei);
    let gas_cost_wei = U256::from(gas_cost_gwei.ceil() as u64) * U256::from(1e9);
    let wei_per_sat = U256::from((eth_per_btc_price * 1e10).round() as u128);
    let eth_cost = (gas_cost_wei / wei_per_sat).to::<u64>();
    eth_cost
}

#[cfg(test)]
mod tests {
    use otc_protocols::rfq::RFQResult;

    use crate::wrapped_bitcoin_quoter::{
        calculate_fees_in_sats_for_market_maker_to_send_btc_and_receive_cbbtc_vault, quote_exact_input, quote_exact_output,
    };

    const SATS_PER_VBYTE: f64 = 1.5;
    const TRADE_SPREAD_BPS: u64 = 13;
    const BASE_FEE_GWEI: f64 = 1.5;
    const MAX_PRIORITY_FEE_GWEI: f64 = 2.5;
    const ETH_PER_BTC_PRICE: f64 = 30000.0;

    #[test]
    fn fuzz_fee_computation_symmetric() {
        let user_input_sats = [1500, 2000, 10000, 30001, 1001001];
        for user_input_sats in user_input_sats {
            println!("user_input_sats: {user_input_sats}");
            let fee_sats_to_send_btc = calculate_fees_in_sats_for_market_maker_to_send_btc_and_receive_cbbtc_vault(
                SATS_PER_VBYTE,
                BASE_FEE_GWEI,
                MAX_PRIORITY_FEE_GWEI,
                ETH_PER_BTC_PRICE,
            );
            println!("fee_sats_to_send_btc: {fee_sats_to_send_btc}");
            let output = quote_exact_input(user_input_sats, fee_sats_to_send_btc, TRADE_SPREAD_BPS);
            println!("output: {output:?}");
            let output = match output {
                RFQResult::Success((rx_btc, fees)) => (rx_btc, fees),
                _ => {
                    panic!("Failed to quote exact input");
                }
            };
            assert_eq!(output.1.network_fee_sats, fee_sats_to_send_btc);
            let input = quote_exact_output(output.0, output.1.network_fee_sats, TRADE_SPREAD_BPS);
            println!("input: {input:?}");
            let input = match input {
                RFQResult::Success((tx_btc, fees)) => (tx_btc, fees),
                _ => {
                    panic!("Failed to quote exact output");
                }
            };
            assert!(
                input.0.abs_diff(user_input_sats) <= 1,
                "Expected {} ± 1, got {}",
                user_input_sats,
                input.0
            );
        }
    }
}
