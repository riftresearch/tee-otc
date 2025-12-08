use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::liquidity_cache::LiquidityCache;
use crate::price_oracle::BitcoinEtherPriceOracle;
use crate::wallet::WalletManager;
use alloy::eips::BlockNumberOrTag;
use alloy::providers::DynProvider;
use alloy::{primitives::U256, providers::Provider};
use blockchain_utils::MempoolEsploraFeeExt;
use otc_models::{constants, ChainType, Quote, QuoteRequest, SwapRates, TokenIdentifier, MIN_PROTOCOL_FEE_SATS};
use otc_protocols::rfq::RFQResult;
use snafu::{Location, Snafu};
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tracing::{debug, info, warn};
use uuid::Uuid;

const QUOTE_EXPIRATION_TIME: Duration = Duration::from_secs(60 * 5);
const FEE_UPDATE_INTERVAL: Duration = Duration::from_secs(15);

/// P2PKH is the MOST expensive address to send BTC to, dust limit wise, so we use this as our minimum
const MIN_DUST_SATS: u64 = 546;


/// Protocol fee in basis points (0.10%)
const PROTOCOL_FEE_BPS: u64 = 10;

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

/// Normalize a TokenIdentifier to lowercase for case-insensitive comparison.
///
/// Ethereum addresses can be represented in different cases (checksummed, lowercase, uppercase),
/// but they all represent the same address. This function normalizes to lowercase.
fn normalize_token(token: &TokenIdentifier) -> TokenIdentifier {
    match token {
        TokenIdentifier::Native => TokenIdentifier::Native,
        TokenIdentifier::Address(addr) => TokenIdentifier::Address(addr.to_lowercase()),
    }
}

pub struct WrappedBitcoinQuoter {
    trade_spread_bps: u64,
    wallet_registry: Arc<WalletManager>,
    fee_map: Arc<RwLock<HashMap<ChainType, u64>>>,
    liquidity_cache: Arc<LiquidityCache>,
    configured_evm_chain: ChainType,
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
        configured_evm_chain: ChainType,
        join_set: &mut JoinSet<crate::Result<()>>,
    ) -> Self {
        let fee_map = Arc::new(RwLock::new(HashMap::new()));
        let fee_map_clone = fee_map.clone();
        join_set.spawn(async move {
            Self::fee_update_loop(
                configured_evm_chain,
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
            configured_evm_chain,
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
        configured_evm_chain: ChainType,
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
                    warn!(
                        "Failed to get fee history during fee update: {:?}, trying again later...",
                        e
                    );
                    tokio::time::sleep(FEE_UPDATE_INTERVAL).await;
                    continue;
                }
            };

            let base_fee_wei: u128 = match fee_history.next_block_base_fee() {
                Some(base_fee_wei) => base_fee_wei,
                None => {
                    warn!(
                        "Failed to get base fee from fee_history next block, trying again later..."
                    );
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
                .copied()
            {
                Some(mid_priority_wei) => mid_priority_wei,
                None => {
                    warn!(
                        "Failed to get mid priority fee from fee_history, trying again later..."
                    );
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

            let fee_estimate = match esplora_client.get_mempool_fee_estimate_next_block().await {
                Ok(estimates) => estimates,
                Err(e) => {
                    warn!("Failed to get fee estimates from esplora: {:?}", e);
                    tokio::time::sleep(FEE_UPDATE_INTERVAL).await;
                    continue;
                }
            };
            let sats_per_vbyte = fee_estimate * fee_safety_multiplier;

            let send_fee_sats_on_btc =
                calculate_fees_in_sats_for_market_maker_to_send_btc_and_receive_cbbtc_vault(
                    sats_per_vbyte,
                    base_fee_gwei,
                    max_priority_fee_gwei,
                    eth_per_btc_price,
                );

            let send_fee_sats_on_eth =
                calculate_fees_in_sats_for_market_maker_to_send_cbbtc_and_receive_btc_vault(
                    base_fee_gwei,
                    max_priority_fee_gwei,
                    eth_per_btc_price,
                    sats_per_vbyte,
                );

            let mut global_fee_map = fee_map.write().await;
            global_fee_map.insert(ChainType::Bitcoin, send_fee_sats_on_btc);
            global_fee_map.insert(configured_evm_chain, send_fee_sats_on_eth);
            drop(global_fee_map);

            metrics::gauge!("mm_quote_eth_priority_fee_gwei").set(max_priority_fee_gwei);
            metrics::gauge!("mm_quote_btc_sats_per_vbyte").set(sats_per_vbyte);

            tokio::time::sleep(FEE_UPDATE_INTERVAL).await;
        }
    }

    /// Compute a rate-based quote for the given currency pair.
    /// The quote specifies rates and min/max bounds instead of fixed amounts.
    pub async fn compute_quote(
        &self,
        market_maker_id: Uuid,
        quote_request: &QuoteRequest,
    ) -> Result<RFQResult<Quote>> {
        if let Some(error_message) = self.is_fillable_request(quote_request) {
            info!("Unsupported quote request: {:?}", quote_request);
            return Ok(RFQResult::Unsupported(error_message));
        }

        let quote_id = Uuid::new_v4();

        // Get network fee for the destination chain (where MM sends funds)
        let network_fee_sats = match self.fee_map.read().await.get(&quote_request.to.chain).cloned()
        {
            Some(fee) => fee,
            None => {
                return Ok(RFQResult::InvalidRequest(
                    "Network fee for chain not found".to_string(),
                ))
            }
        };

        // Build the rates
        let rates = SwapRates::new(self.trade_spread_bps, PROTOCOL_FEE_BPS, network_fee_sats);

        // Get max liquidity from cache for this pair (with balance strategy applied)
        // This is consistent with what the liquidity endpoint reports
        let max_output = self
            .liquidity_cache
            .get_max_output_for_pair(&quote_request.from, &quote_request.to)
            .await
            .unwrap_or(U256::ZERO);

        if max_output.is_zero() {
            return Ok(RFQResult::MakerUnavailable(
                "Insufficient balance to fulfill quote".to_string(),
            ));
        }

        // Calculate max_input from max_output by reversing the fee calculation
        // max_output = (input * (1 - liquidity_bps/10000) - network_fee) * (1 - protocol_bps/10000)
        // Solving for input: input = (max_output / (1 - protocol_bps/10000) + network_fee) / (1 - liquidity_bps/10000)
        let max_input = reverse_compute_max_input(max_output.to::<u64>(), &rates);

        // Compute minimum input that produces viable output after all fees
        let min_input = compute_min_viable_input(&rates);

        if max_input <= min_input {
            return Ok(RFQResult::MakerUnavailable(
                "Insufficient liquidity for viable quote range".to_string(),
            ));
        }

        // If the user's input_hint exceeds our max_input, we can't fulfill the request
        if let Some(input_hint) = quote_request.input_hint {
            if input_hint > U256::from(max_input) {
                return Ok(RFQResult::MakerUnavailable(format!(
                    "Insufficient balance: requested {} but max available is {}",
                    input_hint, max_input
                )));
            }
        }

        let quote = Quote {
            id: quote_id,
            market_maker_id,
            from_currency: quote_request.from.clone(),
            to_currency: quote_request.to.clone(),
            rates,
            min_input: U256::from(min_input),
            max_input: U256::from(max_input),
            expires_at: utc::now() + QUOTE_EXPIRATION_TIME,
            created_at: utc::now(),
        };

        // Validate we have a wallet for the destination chain
        if !self.wallet_registry.is_registered(quote.to_currency.chain) {
            warn!(
                "Cannot fill quote {}: no wallet configured for chain {:?}",
                quote.id, quote.to_currency.chain
            );
            return Ok(RFQResult::MakerUnavailable(
                "No wallet configured for chain".to_string(),
            ));
        }

        Ok(RFQResult::Success(quote))
    }

    /// Validates if a quote request is fillable by this market maker.
    /// Only supports swaps between native Bitcoin and cbBTC on the configured EVM chain.
    fn is_fillable_request(&self, quote_request: &QuoteRequest) -> Option<String> {
        // Swaps must be between different chains
        if quote_request.from.chain == quote_request.to.chain {
            info!("Invalid chain selection: {:?}", quote_request);
            return Some("From and to chains cannot be the same".to_string());
        }

        // Get the cbBTC token address for the configured chain (normalized to lowercase)
        let cbbtc_token = normalize_token(&constants::CBBTC_TOKEN);

        // Normalize request tokens for case-insensitive comparison
        let from_token = normalize_token(&quote_request.from.token);
        let to_token = normalize_token(&quote_request.to.token);

        // Valid swap patterns:
        // 1. Bitcoin (native) -> cbBTC on configured EVM chain
        // 2. cbBTC on configured EVM chain -> Bitcoin (native)

        let is_btc_to_cbbtc = quote_request.from.chain == ChainType::Bitcoin
            && from_token == TokenIdentifier::Native
            && quote_request.to.chain == self.configured_evm_chain
            && to_token == cbbtc_token;

        let is_cbbtc_to_btc = quote_request.from.chain == self.configured_evm_chain
            && from_token == cbbtc_token
            && quote_request.to.chain == ChainType::Bitcoin
            && to_token == TokenIdentifier::Native;

        if !is_btc_to_cbbtc && !is_cbbtc_to_btc {
            return Some(format!(
                "Unsupported swap path for EVM chain: {:?}",
                self.configured_evm_chain,
            ));
        }

        None
    }
}

/// Compute the minimum input that produces at least dust output after all fees.
///
/// This accounts for the minimum protocol fee from `rates.min_protocol_fee_sats` which ensures
/// the fee output is always above Bitcoin's dust limit.
fn compute_min_viable_input(rates: &SwapRates) -> u64 {
    const BPS_DENOM: u64 = 10_000;

    // We need both:
    // 1. mm_output >= MIN_DUST_SATS (user payment must be above dust)
    // 2. protocol_fee >= rates.min_protocol_fee_sats (fee payment must be above dust)
    //
    // Since protocol_fee = max(computed_fee, min_protocol_fee_sats), and
    // mm_output = after_network - protocol_fee, we need:
    // after_network >= min_protocol_fee_sats + MIN_DUST_SATS
    //
    // This is the simpler case that applies for small inputs where the minimum fee kicks in.
    // For larger inputs where the percentage-based fee exceeds min_protocol_fee_sats,
    // the percentage-based formula would give a lower min_input, so this is conservative.

    let after_liq_bps = BPS_DENOM - rates.liquidity_fee_bps;

    // Minimum after_network needed to satisfy both dust constraints
    let min_after_network = rates.min_protocol_fee_sats + MIN_DUST_SATS;

    // Amount needed before network fee
    let before_network = min_after_network + rates.network_fee_sats;

    // Amount needed before liquidity fee (the input)
    let input = (before_network * BPS_DENOM).div_ceil(after_liq_bps);

    input
}

/// Reverse compute max input from max output using the rates.
fn reverse_compute_max_input(max_output: u64, rates: &SwapRates) -> u64 {
    const BPS_DENOM: u64 = 10_000;

    if max_output == 0 {
        return 0;
    }

    let after_proto_bps = BPS_DENOM - rates.protocol_fee_bps;
    let after_liq_bps = BPS_DENOM - rates.liquidity_fee_bps;

    // Reverse: output = (input * after_liq_bps / BPS_DENOM - network_fee) * after_proto_bps / BPS_DENOM
    // => output * BPS_DENOM / after_proto_bps = input * after_liq_bps / BPS_DENOM - network_fee
    // => output * BPS_DENOM / after_proto_bps + network_fee = input * after_liq_bps / BPS_DENOM
    // => (output * BPS_DENOM / after_proto_bps + network_fee) * BPS_DENOM / after_liq_bps = input

    let before_proto = (max_output * BPS_DENOM).div_ceil(after_proto_bps);
    let before_network = before_proto + rates.network_fee_sats;
    let input = (before_network * BPS_DENOM).div_ceil(after_liq_bps);

    input
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
    let eth_spend_vault_cost_sats = eth_fees_in_sats(
        gas_limit as f64,
        base_fee_gwei,
        max_priority_fee_gwei,
        eth_per_btc_price,
    );
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
    let eth_cost_sats = eth_fees_in_sats(
        transfer_gas_limit,
        base_fee_gwei,
        max_priority_fee_gwei,
        eth_per_btc_price,
    );
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
    (gas_cost_wei / wei_per_sat).to::<u64>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use otc_models::RealizedSwap;

    const SATS_PER_VBYTE: f64 = 1.5;
    const TRADE_SPREAD_BPS: u64 = 13;
    const BASE_FEE_GWEI: f64 = 1.5;
    const MAX_PRIORITY_FEE_GWEI: f64 = 2.5;
    const ETH_PER_BTC_PRICE: f64 = 30000.0;

    #[test]
    fn test_min_viable_input() {
        let network_fee =
            calculate_fees_in_sats_for_market_maker_to_send_btc_and_receive_cbbtc_vault(
                SATS_PER_VBYTE,
                BASE_FEE_GWEI,
                MAX_PRIORITY_FEE_GWEI,
                ETH_PER_BTC_PRICE,
            );

        let rates = SwapRates::new(TRADE_SPREAD_BPS, PROTOCOL_FEE_BPS, network_fee);
        let min_input = compute_min_viable_input(&rates);

        // Verify that min_input produces at least MIN_DUST_SATS output
        let realized = RealizedSwap::compute(min_input, &rates)
            .expect("min_input should produce valid output");
        assert!(
            realized.mm_output >= U256::from(MIN_DUST_SATS),
            "Min input {} should produce at least {} output, got {}",
            min_input,
            MIN_DUST_SATS,
            realized.mm_output
        );

        // Verify that inputs below min_input produce None (below MIN_VIABLE_OUTPUT_SATS)
        // or produce output below MIN_DUST_SATS
        if min_input > 3 {
            let realized_below = RealizedSwap::compute(min_input - 3, &rates);
            match realized_below {
                None => {
                    // Good - the input was rejected as it would produce dust
                }
                Some(r) => {
                    assert!(
                        r.mm_output < U256::from(MIN_DUST_SATS),
                        "Input {} should produce less than {} output, got {}",
                        min_input - 3,
                        MIN_DUST_SATS,
                        r.mm_output
                    );
                }
            }
        }
    }

    #[test]
    fn test_reverse_compute_max_input() {
        let network_fee =
            calculate_fees_in_sats_for_market_maker_to_send_btc_and_receive_cbbtc_vault(
                SATS_PER_VBYTE,
                BASE_FEE_GWEI,
                MAX_PRIORITY_FEE_GWEI,
                ETH_PER_BTC_PRICE,
            );

        let rates = SwapRates::new(TRADE_SPREAD_BPS, PROTOCOL_FEE_BPS, network_fee);
        let max_output = 1_000_000u64; // 1M sats available

        let max_input = reverse_compute_max_input(max_output, &rates);

        // Verify that max_input produces approximately max_output
        let realized = RealizedSwap::compute(max_input, &rates)
            .expect("max_input derived from 1M sats should produce valid output");

        // The output should be close to max_output (within rounding)
        // Since we use div_ceil in the reverse calculation, the actual output
        // should be >= max_output
        assert!(
            realized.mm_output >= U256::from(max_output),
            "Max input {} should produce at least {} output, got {}",
            max_input,
            max_output,
            realized.mm_output
        );
    }

    #[test]
    fn test_rate_based_fee_symmetry() {
        let network_fee =
            calculate_fees_in_sats_for_market_maker_to_send_btc_and_receive_cbbtc_vault(
                SATS_PER_VBYTE,
                BASE_FEE_GWEI,
                MAX_PRIORITY_FEE_GWEI,
                ETH_PER_BTC_PRICE,
            );

        let rates = SwapRates::new(TRADE_SPREAD_BPS, PROTOCOL_FEE_BPS, network_fee);

        // Test various input amounts (all large enough to produce valid outputs)
        let test_inputs = [10_000u64, 100_000, 1_000_000, 10_000_000, 100_000_000];

        for input in test_inputs {
            let realized = RealizedSwap::compute(input, &rates)
                .expect(&format!("Input {} should produce valid output", input));

            // Verify fee breakdown adds up
            let total_fees = realized.liquidity_fee.to::<u64>()
                + realized.network_fee.to::<u64>()
                + realized.protocol_fee.to::<u64>();

            let output_plus_fees = realized.mm_output.to::<u64>() + total_fees;

            assert_eq!(
                realized.user_input.to::<u64>(),
                output_plus_fees,
                "Input {} should equal output {} + fees {}",
                input,
                realized.mm_output,
                total_fees
            );
        }
    }
}
