use std::time::Duration;

use crate::{
    bitcoin_wallet::BitcoinWallet, evm_wallet::EVMWallet, price_oracle::BitcoinEtherPriceOracle,
};
use alloy::eips::BlockNumberOrTag;
use alloy::providers::DynProvider;
use alloy::{primitives::U256, providers::Provider};
use bdk_wallet::bitcoin::policy::DUST_RELAY_TX_FEE;
use blockchain_utils::{compute_protocol_fee_sats, inverse_compute_protocol_fee};
use otc_models::{constants, ChainType, Lot, Quote, QuoteMode, QuoteRequest};
use otc_protocols::rfq::{FeeSchedule, QuoteWithFees, RFQResult};
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use tracing::{debug, info, warn};
use uuid::Uuid;

const QUOTE_EXPIRATION_TIME: Duration = Duration::from_secs(60 * 5);

#[derive(Debug, Snafu)]
pub enum WrappedBitcoinQuoterError {
    #[snafu(display("Failed to get fee rate from esplora: {}", source))]
    Esplora { source: esplora_client::Error },
}

impl From<esplora_client::Error> for WrappedBitcoinQuoterError {
    fn from(error: esplora_client::Error) -> Self {
        WrappedBitcoinQuoterError::Esplora { source: error }
    }
}

type Result<T, E = WrappedBitcoinQuoterError> = std::result::Result<T, E>;

/// Note this is greater than the
pub const MIN_PROTOCOL_FEE_SATS: u64 = 300;

pub struct WrappedBitcoinQuoter {
    btc_eth_price_oracle: BitcoinEtherPriceOracle,
    esplora_client: esplora_client::AsyncClient,
    eth_provider: DynProvider,
    trade_spread_bps: u64,
    fee_safety_multiplier: f64,
}

impl WrappedBitcoinQuoter {
    pub fn new(
        btc_eth_price_oracle: BitcoinEtherPriceOracle,
        esplora_client: esplora_client::AsyncClient,
        eth_provider: DynProvider,
        trade_spread_bps: u64,
        fee_safety_multiplier: f64,
    ) -> Self {
        Self {
            btc_eth_price_oracle,
            esplora_client,
            eth_provider,
            trade_spread_bps,
            fee_safety_multiplier,
        }
    }

    /// Compute a quote for the given amount and quote mode.
    /// Note that fill_chain is the chain that the market maker will fill the quote on.
    /// which is relevant for computing fees
    pub async fn compute_quote(
        &self,
        market_maker_id: Uuid,
        quote_request: &QuoteRequest,
    ) -> Result<RFQResult<QuoteWithFees>> {
        if let Some(error_message) = is_fillable_request(quote_request) {
            info!("Unfillable quote request: {:?}", quote_request);
            return Ok(RFQResult::InvalidRequest(error_message));
        }
        if quote_request.amount > U256::from(u64::MAX) {
            return Ok(RFQResult::InvalidRequest("Amount too large".to_string()));
        }
        let amount = quote_request.amount.to::<u64>();
        let send_fees_in_sats = {
            match quote_request.to.chain {
                ChainType::Bitcoin => {
                    //TODO: put updating this fee rate behind a RwLock that we cache so it's not fetched on every quote
                    let sats_per_vbyte_by_confirmations =
                        self.esplora_client.get_fee_estimates().await?;
                    let sats_per_vbyte = sats_per_vbyte_by_confirmations.get(&1).unwrap_or(&1.5);
                    let sats_per_vbyte = sats_per_vbyte * self.fee_safety_multiplier;

                    calculate_fees_in_sats_to_send_btc(sats_per_vbyte)
                }
                ChainType::Ethereum => {
                    //TODO: put updating this fee rate behind a RwLock that we cache so it's not fetched on every quote
                    let fee_history = match self
                        .eth_provider
                        .get_fee_history(10u64, BlockNumberOrTag::Latest, &[25.0, 50.0, 75.0])
                        .await
                    {
                        Ok(history) => history,
                        Err(_) => {
                            warn!("Failed to get fee history: {:?}", quote_request);
                            return Ok(RFQResult::MakerUnavailable(
                                "Failed to get fee history".to_string(),
                            ));
                        }
                    };

                    let base_fee_wei: u128 = fee_history.next_block_base_fee().unwrap_or(0u128);
                    let base_fee_gwei: f64 = (base_fee_wei as f64) / 1e9f64;

                    let mid_priority_wei: u128 = fee_history
                        .reward
                        .as_ref()
                        .and_then(|rewards| rewards.last())
                        .and_then(|percentiles| percentiles.get(1)) // 50th percentile
                        .copied()
                        .unwrap_or(1_500_000_000u128); // default 1.5 gwei
                    let mut max_priority_fee_gwei: f64 = (mid_priority_wei as f64) / 1e9f64;

                    max_priority_fee_gwei *= self.fee_safety_multiplier;

                    let eth_per_btc_price = match self.btc_eth_price_oracle.get_eth_per_btc().await
                    {
                        Ok(p) => p,
                        Err(e) => {
                            warn!("Failed to get BTC/ETH price: {:?}", e);
                            return Ok(RFQResult::MakerUnavailable(
                                "Failed to get BTC/ETH price".to_string(),
                            ));
                        }
                    };

                    calculate_fees_in_sats_to_send_cbbtc_on_eth(
                        base_fee_gwei,
                        max_priority_fee_gwei,
                        eth_per_btc_price,
                    )
                }
            }
        };

        let quote_id = Uuid::new_v4();
        match quote_request.mode {
            QuoteMode::ExactInput => {
                let quote_result =
                    quote_exact_input(amount, send_fees_in_sats, self.trade_spread_bps);

                match quote_result {
                    RFQResult::Success((rx_btc, fees)) => Ok(RFQResult::Success(QuoteWithFees {
                        quote: Quote {
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
                            expires_at: utc::now() + QUOTE_EXPIRATION_TIME,
                            created_at: utc::now(),
                        },
                        fees,
                    })),
                    RFQResult::MakerUnavailable(error) => Ok(RFQResult::MakerUnavailable(error)),
                    RFQResult::InvalidRequest(error) => Ok(RFQResult::InvalidRequest(error)),
                }
            }
            QuoteMode::ExactOutput => {
                let quote_result =
                    quote_exact_output(amount, send_fees_in_sats, self.trade_spread_bps);
                match quote_result {
                    RFQResult::Success((tx_btc, fees)) => Ok(RFQResult::Success(QuoteWithFees {
                        quote: Quote {
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
                            expires_at: utc::now() + QUOTE_EXPIRATION_TIME,
                            created_at: utc::now(),
                        },
                        fees,
                    })),
                    RFQResult::MakerUnavailable(error) => Ok(RFQResult::MakerUnavailable(error)),
                    RFQResult::InvalidRequest(error) => Ok(RFQResult::InvalidRequest(error)),
                }
            }
        }
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
    let denominator = (BPS_DENOM - s);
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

// TODO(gpt-ignore): This should be computed by the wallet
fn calculate_fees_in_sats_to_send_btc(sats_per_vbyte: f64) -> u64 {
    let vbytes = 199.0; // 3 p2wpkh outputs, 1 op return w/ 16 bytes, 1 p2wpkh input (napkin math)
    let fee = sats_per_vbyte * vbytes;
    fee.ceil() as u64
}

// TODO: This should be computed by the wallet?
fn calculate_fees_in_sats_to_send_cbbtc_on_eth(
    base_fee_gwei: f64,
    max_priority_fee_gwei: f64,
    eth_per_btc_price: f64,
) -> u64 {
    // TODO(high): compute the cost to use the EIP7702 delegator contract, for a basic transfer of CB-BTC
    // This is the gas cost to use disperse.app on ethereum mainnet w/ 2 addresses as recipients reference: https://etherscan.io/tx/0x22d7b1141273fb60ded7a910da4eb4492fd349abe927b6d1961afa7759d25644
    let transfer_gas_limit = 98_722f64;
    let gas_cost_gwei = transfer_gas_limit * (max_priority_fee_gwei + base_fee_gwei);
    let gas_cost_wei = U256::from(gas_cost_gwei.ceil() as u64) * U256::from(1e9);
    let wei_per_sat = U256::from((eth_per_btc_price * 1e10).round() as u128);
    (gas_cost_wei / wei_per_sat).to::<u64>()
}

mod tests {
    use super::*;

    const BASE_FEE_GWEI: f64 = 0.5;
    const MAX_PRIORITY_FEE_GWEI: f64 = 0.01;
    const ETH_PER_BTC: f64 = 27.15;
    const SATS_PER_VBYTE: f64 = 1.5;
    const TRADE_SPREAD_BPS: u64 = 13;

    #[test]
    fn fuzz_fee_computation_symmetric() {
        let user_input_sats = [1500, 2000, 10000, 30001, 1001001];
        for user_input_sats in user_input_sats {
            println!("user_input_sats: {user_input_sats}");
            let fee_sats_to_send_btc = calculate_fees_in_sats_to_send_btc(SATS_PER_VBYTE);
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
