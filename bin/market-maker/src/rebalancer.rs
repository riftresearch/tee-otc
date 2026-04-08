use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::primitives::utils::format_units;
use alloy::primitives::U256;
use otc_chains::traits::Payment;
use otc_models::{ChainType, Currency, Lot, TokenIdentifier, CB_BTC_CONTRACT_ADDRESS};
use snafu::{Location, ResultExt};
use tokio::time::sleep;
use tracing::{debug, error, info, instrument, warn};

use metrics::{counter, gauge, histogram};
use serde::{Deserialize, Serialize};

use coinbase_exchange_client::{CoinbaseClient, CoinbaseExchangeClientError, WithdrawalStatus};

use crate::wallet::{Wallet, WalletError};

const BPS_DENOM: u64 = 10_000;
const BITCOIN_CONSOLIDATION_BATCH_SIZE: usize = 100;
const EVM_CONSOLIDATION_BATCH_SIZE: usize = 200;
const TEST_MODE_BALANCE_PERCENT_DIVISOR: u64 = 100;
const TEST_MODE_MAX_USD_VALUE: f64 = 100.0;
const SATS_PER_BTC: f64 = 100_000_000.0;
const WEI_PER_ETH: f64 = 1_000_000_000_000_000_000.0;

#[derive(Debug, snafu::Snafu)]
pub enum RebalancerError {
    #[snafu(display("Failed to get balance: {}", source))]
    GetBalance {
        source: WalletError,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Wallet operation failed: {}", source))]
    Wallet {
        source: WalletError,
        #[snafu(implicit)]
        loc: Location,
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
        loc: Location,
    },

    #[snafu(display("Failed to convert: {}", source))]
    CoinbaseExchangeClientError {
        source: CoinbaseExchangeClientError,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Invalid conversion request: {reason} at {loc}"))]
    InvalidConversionRequest {
        reason: String,
        #[snafu(implicit)]
        loc: snafu::Location,
    },
}

type Result<T, E = RebalancerError> = std::result::Result<T, E>;

#[derive(Clone, Debug)]
pub struct BandsParams {
    /// target BTC share, e.g. 5000 = 50% of each asset
    pub target_bps: u64,
    /// symmetric band half-width around target, e.g. 2500 = ±25%
    pub band_width_bps: u64,
    /// loop timing
    pub poll_interval: Duration,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DrainToCoinbaseSummary {
    pub test_mode: bool,
    pub drain_eth: bool,
    pub btc_balance_sats: u64,
    pub btc_requested_sats: u64,
    pub btc_tx_hash: Option<String>,
    pub cbbtc_balance_sats: u64,
    pub cbbtc_requested_sats: u64,
    pub cbbtc_tx_hash: Option<String>,
    pub eth_balance_wei: String,
    pub eth_requested_wei: String,
    pub eth_tx_hash: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum DrainToCoinbaseMode {
    #[default]
    Full,
    Test {
        btc_usd_price: f64,
        eth_usd_price: f64,
    },
}

impl DrainToCoinbaseMode {
    fn is_test_mode(self) -> bool {
        matches!(self, Self::Test { .. })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExactDrainRequest {
    Btc { amount_sats: u64 },
    Cbbtc { amount_sats: u64 },
    Eth { amount_wei: U256 },
}

enum Side {
    BtcToCbbtc,
    CbbtcToBtc,
}

#[inline]
fn ratio_bps(btc: u64, cbbtc: u64) -> u64 {
    let total = btc.saturating_add(cbbtc);
    if total == 0 {
        return 0;
    }
    let num = ((btc as u128) * (BPS_DENOM as u128)) / (total as u128);
    num.min(BPS_DENOM as u128) as u64
}

/// Exact sats needed to move to reach target share (snap-to-target).
#[inline]
fn sats_to_target(total_sats: u64, target_bps: u64, r_bps: u64) -> u64 {
    if total_sats == 0 {
        return 0;
    }
    let target_btc = ((total_sats as u128) * (target_bps as u128)) / (BPS_DENOM as u128);
    let current_btc = ((total_sats as u128) * (r_bps as u128)) / (BPS_DENOM as u128);
    if target_btc >= current_btc {
        (target_btc - current_btc) as u64
    } else {
        (current_btc - target_btc) as u64
    }
}

fn test_mode_requested_units(balance: U256, usd_price: f64, units_per_whole: f64) -> Result<U256> {
    if balance.is_zero() {
        return Ok(U256::ZERO);
    }

    if !usd_price.is_finite() || usd_price <= 0.0 {
        return InvalidConversionRequestSnafu {
            reason: format!("Invalid spot price for test drain: {usd_price}"),
        }
        .fail();
    }

    let one_percent_units = balance / U256::from(TEST_MODE_BALANCE_PERCENT_DIVISOR);
    let hundred_usd_units = ((TEST_MODE_MAX_USD_VALUE / usd_price) * units_per_whole).floor();
    let hundred_usd_units = U256::from(hundred_usd_units.max(0.0) as u128);
    let capped_units = one_percent_units.min(hundred_usd_units);

    Ok(capped_units.max(U256::from(1u64)).min(balance))
}

fn test_mode_requested_sats(balance_sats: u64, btc_usd_price: f64) -> Result<u64> {
    Ok(test_mode_requested_units(U256::from(balance_sats), btc_usd_price, SATS_PER_BTC)?.to())
}

fn test_mode_requested_wei(balance_wei: U256, eth_usd_price: f64) -> Result<U256> {
    test_mode_requested_units(balance_wei, eth_usd_price, WEI_PER_ETH)
}

fn requested_drain_sats(balance_sats: u64, mode: DrainToCoinbaseMode) -> Result<u64> {
    match mode {
        DrainToCoinbaseMode::Full => Ok(balance_sats),
        DrainToCoinbaseMode::Test { btc_usd_price, .. } => {
            test_mode_requested_sats(balance_sats, btc_usd_price)
        }
    }
}

fn requested_drain_wei(balance_wei: U256, mode: DrainToCoinbaseMode) -> Result<U256> {
    match mode {
        DrainToCoinbaseMode::Full => Ok(balance_wei),
        DrainToCoinbaseMode::Test { eth_usd_price, .. } => {
            test_mode_requested_wei(balance_wei, eth_usd_price)
        }
    }
}

/// Continuously records market maker balance and inventory metrics.
///
/// This function runs in its own loop, periodically querying wallet balances
/// and updating Prometheus-style gauges for monitoring.
pub async fn run_inventory_metrics_reporter(
    btc_wallet: Arc<dyn Wallet>,
    evm_wallet: Arc<dyn Wallet>,
    poll_interval: Duration,
) -> Result<()> {
    let btc_token = TokenIdentifier::Native;
    let cbbtc_token = TokenIdentifier::address(CB_BTC_CONTRACT_ADDRESS.to_string());

    loop {
        counter!("mm_loop_iterations_total").increment(1);

        // Query all balances
        let btc_balance = btc_wallet
            .balance(&btc_token)
            .await
            .context(GetBalanceSnafu)?;
        let btc = btc_balance.total_balance.to::<u64>();

        let cbbtc_balance = evm_wallet
            .balance(&cbbtc_token)
            .await
            .context(GetBalanceSnafu)?;
        let cbbtc = cbbtc_balance.total_balance.to::<u64>();

        let total_sats = btc.saturating_add(cbbtc);

        let eth_balance = evm_wallet
            .balance(&TokenIdentifier::Native)
            .await
            .context(GetBalanceSnafu)?;
        let eth_native = eth_balance.total_balance;
        let eth_native_str: String = format_units(eth_native, "ether").unwrap();
        let eth_native_f64 = eth_native_str.parse::<f64>().unwrap();

        // Record primary balance gauges
        gauge!("mm_balance_sats", "asset" => "btc").set(btc as f64);
        gauge!("mm_balance_sats", "asset" => "cbbtc").set(cbbtc as f64);
        gauge!("mm_total_sats").set(total_sats as f64);
        gauge!("mm_eth_balance_ether").set(eth_native_f64);

        // Record native vs deposit key balance breakdown
        let btc_native = btc_balance.native_balance.to::<u64>();
        let btc_deposit_key = btc_balance.deposit_key_balance.to::<u64>();
        let cbbtc_native = cbbtc_balance.native_balance.to::<u64>();
        let cbbtc_deposit_key = cbbtc_balance.deposit_key_balance.to::<u64>();

        gauge!("mm_native_balance_sats", "asset" => "btc").set(btc_native as f64);
        gauge!("mm_native_balance_sats", "asset" => "cbbtc").set(cbbtc_native as f64);
        gauge!("mm_deposit_key_balance_sats", "asset" => "btc").set(btc_deposit_key as f64);
        gauge!("mm_deposit_key_balance_sats", "asset" => "cbbtc").set(cbbtc_deposit_key as f64);

        // Calculate and record inventory ratio
        let r_bps = if total_sats == 0 {
            0
        } else {
            ratio_bps(btc, cbbtc)
        };
        gauge!("mm_inventory_ratio_bps").set(r_bps as f64);

        // Log detailed inventory breakdown
        debug!(
            "inventory: btc={} sats, cbbtc={} sats, total={}, ratio_bps={}",
            btc, cbbtc, total_sats, r_bps
        );

        if total_sats > 0 {
            let btc_pct = (btc as f64 / total_sats as f64) * 100.0;
            let cbbtc_pct = (cbbtc as f64 / total_sats as f64) * 100.0;

            debug!(
                message = "detailed inventory breakdown",
                btc_total_sats = btc,
                btc_native_sats = btc_native,
                btc_deposit_key_sats = btc_deposit_key,
                btc_percentage = %format!("{:.2}%", btc_pct),
                cbbtc_total_sats = cbbtc,
                cbbtc_native_sats = cbbtc_native,
                cbbtc_deposit_key_sats = cbbtc_deposit_key,
                cbbtc_percentage = %format!("{:.2}%", cbbtc_pct),
                total_sats = total_sats
            );
        }

        sleep(poll_interval).await;
    }
}

pub async fn run_rebalancer(
    evm_chain: ChainType,
    coinbase_client: CoinbaseClient,
    btc_wallet: Arc<dyn Wallet>,
    evm_wallet: Arc<dyn Wallet>,
    params: BandsParams,
    execute_rebalance: bool,
    confirmation_poll_interval: Duration,
    btc_coinbase_confirmations: u32,
    cbbtc_coinbase_confirmations: u32,
) -> Result<()> {
    let btc_token = TokenIdentifier::Native;
    let cbbtc_token = TokenIdentifier::address(CB_BTC_CONTRACT_ADDRESS.to_string());

    loop {
        // --- balances (u64 sats) ---
        let btc_balance = btc_wallet
            .balance(&btc_token)
            .await
            .context(GetBalanceSnafu)?;
        let btc = btc_balance.total_balance.to::<u64>();
        let cbbtc_balance = evm_wallet
            .balance(&cbbtc_token)
            .await
            .context(GetBalanceSnafu)?;
        let cbbtc = cbbtc_balance.total_balance.to::<u64>();

        let total_sats = btc.saturating_add(cbbtc);

        if total_sats == 0 {
            sleep(params.poll_interval).await;
            continue;
        }

        // --- symmetric band around target ---
        let lo = params
            .target_bps
            .saturating_sub(params.band_width_bps)
            .min(BPS_DENOM);
        let hi = (params.target_bps + params.band_width_bps).min(BPS_DENOM);

        let r_bps = ratio_bps(btc, cbbtc);

        let side = if r_bps < lo {
            Some(Side::CbbtcToBtc) // below lower band → BTC ratio too low, need more BTC
        } else if r_bps > hi {
            Some(Side::BtcToCbbtc) // above upper band → BTC ratio too high, need more cBBTC
        } else {
            None
        };

        if let Some(side) = side {
            if execute_rebalance {
                // snap-to-target trade size (in sats)
                let trade_sats = sats_to_target(total_sats, params.target_bps, r_bps);
                if trade_sats == 0 {
                    sleep(params.poll_interval).await;
                    continue;
                }

                match side {
                    Side::BtcToCbbtc => {
                        let recv = evm_wallet.receive_address(&cbbtc_token);
                        info!(
                            "rebalance: BTC -> cBBTC | r_bps={} band=[{},{}] target={} sats={}",
                            r_bps, lo, hi, params.target_bps, trade_sats
                        );
                        counter!("mm_rebalances_total", "direction" => "btc_to_cbbtc").increment(1);
                        gauge!("mm_trade_sats", "direction" => "btc_to_cbbtc")
                            .set(trade_sats as f64);

                        let t0 = Instant::now();
                        convert_btc_to_cbbtc(
                            evm_chain,
                            &coinbase_client,
                            btc_wallet.as_ref(),
                            trade_sats,
                            &recv,
                            confirmation_poll_interval,
                            btc_coinbase_confirmations,
                        )
                        .await?;
                        let secs = t0.elapsed().as_secs_f64();
                        histogram!("mm_convert_duration_seconds", "direction" => "btc_to_cbbtc")
                            .record(secs);
                    }
                    Side::CbbtcToBtc => {
                        let recv = btc_wallet.receive_address(&btc_token);
                        info!(
                            "rebalance: cBBTC -> BTC | r_bps={} band=[{},{}] target={} sats={}",
                            r_bps, lo, hi, params.target_bps, trade_sats
                        );
                        counter!("mm_rebalances_total", "direction" => "cbbtc_to_btc").increment(1);
                        gauge!("mm_trade_sats", "direction" => "cbbtc_to_btc")
                            .set(trade_sats as f64);

                        let t0 = Instant::now();
                        convert_cbbtc_to_btc(
                            evm_chain,
                            &coinbase_client,
                            evm_wallet.as_ref(),
                            trade_sats,
                            &recv,
                            confirmation_poll_interval,
                            cbbtc_coinbase_confirmations,
                        )
                        .await?;
                        let secs = t0.elapsed().as_secs_f64();
                        histogram!("mm_convert_duration_seconds", "direction" => "cbbtc_to_btc")
                            .record(secs);
                    }
                }
            } else {
                info!("auto manage inventory is disabled, skipping rebalance...");
            }
        } else {
            debug!("inside band: r_bps={} ∈ [{}, {}]; no trade", r_bps, lo, hi);
        }

        // No cooldown: convert_* calls may block for minutes; this loop resumes after they return.
        sleep(params.poll_interval).await;
    }
}

pub async fn drain_inventory_to_coinbase(
    evm_chain: ChainType,
    coinbase_client: &CoinbaseClient,
    btc_wallet: &dyn Wallet,
    evm_wallet: &dyn Wallet,
    confirmation_poll_interval: Duration,
    btc_coinbase_confirmations: u32,
    cbbtc_coinbase_confirmations: u32,
    mode: DrainToCoinbaseMode,
    drain_eth: bool,
) -> Result<DrainToCoinbaseSummary> {
    let btc_token = TokenIdentifier::Native;
    let cbbtc_token = TokenIdentifier::address(CB_BTC_CONTRACT_ADDRESS.to_string());
    let eth_token = TokenIdentifier::Native;

    let btc_balance_sats = btc_wallet
        .balance(&btc_token)
        .await
        .context(GetBalanceSnafu)?
        .total_balance
        .to::<u64>();
    let cbbtc_balance_sats = evm_wallet
        .balance(&cbbtc_token)
        .await
        .context(GetBalanceSnafu)?
        .total_balance
        .to::<u64>();
    let eth_balance_wei = if drain_eth {
        evm_wallet
            .balance(&eth_token)
            .await
            .context(GetBalanceSnafu)?
            .total_balance
    } else {
        U256::ZERO
    };
    let btc_requested_sats = requested_drain_sats(btc_balance_sats, mode)?;
    let cbbtc_requested_sats = requested_drain_sats(cbbtc_balance_sats, mode)?;
    let eth_requested_wei = if drain_eth {
        requested_drain_wei(eth_balance_wei, mode)?
    } else {
        U256::ZERO
    };
    let test_mode = mode.is_test_mode();

    info!(
        test_mode,
        drain_eth,
        btc_balance_sats,
        btc_requested_sats,
        cbbtc_balance_sats,
        cbbtc_requested_sats,
        eth_balance_wei = %eth_balance_wei,
        eth_requested_wei = %eth_requested_wei,
        evm_chain = ?evm_chain,
        "Starting one-shot inventory drain to Coinbase"
    );

    let btc_drain = async {
        if btc_requested_sats == 0 {
            info!("No BTC inventory to drain to Coinbase");
            Ok(None)
        } else if matches!(mode, DrainToCoinbaseMode::Full) {
            drain_all_btc_to_coinbase(
                coinbase_client,
                btc_wallet,
                confirmation_poll_interval,
                btc_coinbase_confirmations,
                false,
            )
            .await
            .map(Some)
        } else {
            deposit_exact_btc_to_coinbase(
                coinbase_client,
                btc_wallet,
                btc_requested_sats,
                confirmation_poll_interval,
                btc_coinbase_confirmations,
                false,
            )
            .await
            .map(Some)
        }
    };

    let cbbtc_drain = async {
        if cbbtc_requested_sats == 0 {
            info!("No cbBTC inventory to drain to Coinbase");
            Ok(None)
        } else {
            deposit_cbbtc_to_coinbase(
                evm_chain,
                coinbase_client,
                evm_wallet,
                cbbtc_requested_sats,
                confirmation_poll_interval,
                cbbtc_coinbase_confirmations,
                false,
            )
            .await
            .map(Some)
        }
    };

    let eth_drain = async {
        if !drain_eth {
            Ok(None)
        } else if eth_requested_wei.is_zero() {
            info!("No ETH inventory to drain to Coinbase");
            Ok(None)
        } else if matches!(mode, DrainToCoinbaseMode::Full) {
            drain_all_eth_to_coinbase(
                evm_chain,
                coinbase_client,
                evm_wallet,
                confirmation_poll_interval,
                cbbtc_coinbase_confirmations,
                false,
            )
            .await
            .map(Some)
        } else {
            deposit_eth_to_coinbase(
                evm_chain,
                coinbase_client,
                evm_wallet,
                eth_requested_wei,
                confirmation_poll_interval,
                cbbtc_coinbase_confirmations,
                false,
            )
            .await
            .map(Some)
        }
    };

    let (btc_tx_hash, cbbtc_tx_hash) = tokio::join!(btc_drain, cbbtc_drain);
    let btc_tx_hash = btc_tx_hash?;
    let cbbtc_tx_hash = cbbtc_tx_hash?;
    let eth_tx_hash = eth_drain.await?;

    Ok(DrainToCoinbaseSummary {
        test_mode,
        drain_eth,
        btc_balance_sats,
        btc_requested_sats,
        btc_tx_hash,
        cbbtc_balance_sats,
        cbbtc_requested_sats,
        cbbtc_tx_hash,
        eth_balance_wei: eth_balance_wei.to_string(),
        eth_requested_wei: eth_requested_wei.to_string(),
        eth_tx_hash,
    })
}

pub async fn drain_exact_asset_to_coinbase(
    evm_chain: ChainType,
    coinbase_client: &CoinbaseClient,
    btc_wallet: &dyn Wallet,
    evm_wallet: &dyn Wallet,
    confirmation_poll_interval: Duration,
    btc_coinbase_confirmations: u32,
    cbbtc_coinbase_confirmations: u32,
    request: ExactDrainRequest,
) -> Result<DrainToCoinbaseSummary> {
    let btc_balance_sats = btc_wallet
        .balance(&TokenIdentifier::Native)
        .await
        .context(GetBalanceSnafu)?
        .total_balance
        .to::<u64>();
    let cbbtc_balance_sats = evm_wallet
        .balance(&TokenIdentifier::address(
            CB_BTC_CONTRACT_ADDRESS.to_string(),
        ))
        .await
        .context(GetBalanceSnafu)?
        .total_balance
        .to::<u64>();
    let eth_balance_wei = evm_wallet
        .balance(&TokenIdentifier::Native)
        .await
        .context(GetBalanceSnafu)?
        .total_balance;

    let mut summary = DrainToCoinbaseSummary {
        test_mode: false,
        drain_eth: matches!(request, ExactDrainRequest::Eth { .. }),
        btc_balance_sats,
        btc_requested_sats: 0,
        btc_tx_hash: None,
        cbbtc_balance_sats,
        cbbtc_requested_sats: 0,
        cbbtc_tx_hash: None,
        eth_balance_wei: eth_balance_wei.to_string(),
        eth_requested_wei: U256::ZERO.to_string(),
        eth_tx_hash: None,
    };

    match request {
        ExactDrainRequest::Btc { amount_sats } => {
            summary.btc_requested_sats = amount_sats;
            summary.btc_tx_hash = Some(
                deposit_exact_btc_to_coinbase(
                    coinbase_client,
                    btc_wallet,
                    amount_sats,
                    confirmation_poll_interval,
                    btc_coinbase_confirmations,
                    false,
                )
                .await?,
            );
        }
        ExactDrainRequest::Cbbtc { amount_sats } => {
            summary.cbbtc_requested_sats = amount_sats;
            summary.cbbtc_tx_hash = Some(
                deposit_cbbtc_to_coinbase(
                    evm_chain,
                    coinbase_client,
                    evm_wallet,
                    amount_sats,
                    confirmation_poll_interval,
                    cbbtc_coinbase_confirmations,
                    false,
                )
                .await?,
            );
        }
        ExactDrainRequest::Eth { amount_wei } => {
            summary.eth_requested_wei = amount_wei.to_string();
            summary.eth_tx_hash = Some(
                deposit_eth_to_coinbase(
                    evm_chain,
                    coinbase_client,
                    evm_wallet,
                    amount_wei,
                    confirmation_poll_interval,
                    cbbtc_coinbase_confirmations,
                    false,
                )
                .await?,
            );
        }
    }

    Ok(summary)
}

async fn maybe_wait_for_coinbase_deposit_confirmations(
    sender_wallet: &dyn Wallet,
    tx_hash: &str,
    confirmations: u32,
    confirmation_poll_interval: Duration,
    log_label: &str,
    wait_for_confirmations: bool,
) -> Result<()> {
    if !wait_for_confirmations {
        info!(
            %tx_hash,
            confirmations,
            action = log_label,
            "Skipping confirmation wait for Coinbase drain transaction"
        );
        return Ok(());
    }

    let t0 = Instant::now();
    sender_wallet
        .guarantee_confirmations(tx_hash, confirmations as u64, confirmation_poll_interval)
        .await
        .context(WalletSnafu)?;
    info!(
        %tx_hash,
        confirmations,
        action = log_label,
        duration = ?t0.elapsed(),
        "Guaranteed Coinbase deposit confirmations"
    );

    Ok(())
}

async fn deposit_exact_btc_to_coinbase(
    coinbase_client: &CoinbaseClient,
    sender_wallet: &dyn Wallet,
    amount_sats: u64,
    confirmation_poll_interval: Duration,
    btc_coinbase_confirmations: u32,
    wait_for_confirmations: bool,
) -> Result<String> {
    if sender_wallet.chain_type() != ChainType::Bitcoin {
        return InvalidConversionRequestSnafu {
            reason: "Sender wallet is not a bitcoin wallet".to_string(),
        }
        .fail();
    }

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
        .context(WalletSnafu)?
        .total_balance;

    if available_balance < lot.amount {
        return InsufficientBalanceSnafu {
            requested_amount: lot.amount,
            available_amount: available_balance,
        }
        .fail();
    }

    info!("Consolidating bitcoin deposits before Coinbase deposit");
    let t0 = Instant::now();
    let consolidation_summary = sender_wallet
        .consolidate(
            &Lot {
                currency: Currency {
                    chain: ChainType::Bitcoin,
                    token: TokenIdentifier::Native,
                    decimals: 8,
                },
                amount: U256::MAX,
            },
            BITCOIN_CONSOLIDATION_BATCH_SIZE,
        )
        .await
        .context(WalletSnafu)?;
    info!(
        "Bitcoin consolidation summary before Coinbase deposit: {:?}, duration: {:?}",
        consolidation_summary,
        t0.elapsed()
    );

    let btc_account_id = coinbase_client
        .get_btc_account_id()
        .await
        .context(CoinbaseExchangeClientSnafu)?;

    let btc_deposit_address = coinbase_client
        .get_bitcoin_deposit_address(&btc_account_id)
        .await
        .context(CoinbaseExchangeClientSnafu)?;
    info!("Got Coinbase BTC deposit address: {}", btc_deposit_address);

    let payments = vec![Payment {
        lot,
        to_address: btc_deposit_address,
    }];
    let t0 = Instant::now();
    let btc_tx_hash = sender_wallet
        .create_batch_payment(payments, None)
        .await
        .context(WalletSnafu)?;
    info!(
        "Created BTC deposit transaction for Coinbase: {}, duration: {:?}",
        btc_tx_hash,
        t0.elapsed()
    );

    maybe_wait_for_coinbase_deposit_confirmations(
        sender_wallet,
        &btc_tx_hash,
        btc_coinbase_confirmations,
        confirmation_poll_interval,
        "btc_coinbase_deposit",
        wait_for_confirmations,
    )
    .await?;

    Ok(btc_tx_hash)
}

async fn drain_all_btc_to_coinbase(
    coinbase_client: &CoinbaseClient,
    sender_wallet: &dyn Wallet,
    confirmation_poll_interval: Duration,
    btc_coinbase_confirmations: u32,
    wait_for_confirmations: bool,
) -> Result<String> {
    info!("Consolidating bitcoin deposits before full drain to Coinbase");
    let t0 = Instant::now();
    let consolidation_summary = sender_wallet
        .consolidate(
            &Lot {
                currency: Currency {
                    chain: ChainType::Bitcoin,
                    token: TokenIdentifier::Native,
                    decimals: 8,
                },
                amount: U256::MAX,
            },
            BITCOIN_CONSOLIDATION_BATCH_SIZE,
        )
        .await
        .context(WalletSnafu)?;
    info!(
        "Bitcoin consolidation summary before full drain: {:?}, duration: {:?}",
        consolidation_summary,
        t0.elapsed()
    );

    let btc_account_id = coinbase_client
        .get_btc_account_id()
        .await
        .context(CoinbaseExchangeClientSnafu)?;

    let btc_deposit_address = coinbase_client
        .get_bitcoin_deposit_address(&btc_account_id)
        .await
        .context(CoinbaseExchangeClientSnafu)?;
    info!(
        "Got Coinbase BTC deposit address for full drain: {}",
        btc_deposit_address
    );

    let t0 = Instant::now();
    let btc_tx_hash = sender_wallet
        .drain_to_address(&btc_deposit_address)
        .await
        .context(WalletSnafu)?;
    info!(
        "Created BTC drain transaction for Coinbase: {}, duration: {:?}",
        btc_tx_hash,
        t0.elapsed()
    );

    maybe_wait_for_coinbase_deposit_confirmations(
        sender_wallet,
        &btc_tx_hash,
        btc_coinbase_confirmations,
        confirmation_poll_interval,
        "btc_coinbase_drain",
        wait_for_confirmations,
    )
    .await?;

    Ok(btc_tx_hash)
}

async fn deposit_eth_to_coinbase(
    evm_chain: ChainType,
    coinbase_client: &CoinbaseClient,
    sender_wallet: &dyn Wallet,
    amount_wei: U256,
    confirmation_poll_interval: Duration,
    eth_coinbase_confirmations: u32,
    wait_for_confirmations: bool,
) -> Result<String> {
    if sender_wallet.chain_type() != evm_chain {
        return InvalidConversionRequestSnafu {
            reason: "Sender wallet is not the configured EVM wallet".to_string(),
        }
        .fail();
    }

    let available_balance = sender_wallet
        .balance(&TokenIdentifier::Native)
        .await
        .context(WalletSnafu)?
        .total_balance;
    if available_balance < amount_wei {
        return InsufficientBalanceSnafu {
            requested_amount: amount_wei,
            available_amount: available_balance,
        }
        .fail();
    }

    let eth_account_id = coinbase_client
        .get_eth_account_id()
        .await
        .context(CoinbaseExchangeClientSnafu)?;
    let eth_deposit_address = coinbase_client
        .get_eth_deposit_address(&eth_account_id, evm_chain)
        .await
        .context(CoinbaseExchangeClientSnafu)?;
    info!(
        "Got Coinbase ETH deposit address on {:?}: {}",
        evm_chain, eth_deposit_address
    );

    let payment = Payment {
        lot: Lot {
            currency: Currency {
                chain: evm_chain,
                token: TokenIdentifier::Native,
                decimals: 18,
            },
            amount: amount_wei,
        },
        to_address: eth_deposit_address,
    };

    let t0 = Instant::now();
    let eth_tx_hash = sender_wallet
        .create_batch_payment(vec![payment], None)
        .await
        .context(WalletSnafu)?;
    info!(
        "Created ETH deposit transaction for Coinbase: {}, duration: {:?}",
        eth_tx_hash,
        t0.elapsed()
    );

    maybe_wait_for_coinbase_deposit_confirmations(
        sender_wallet,
        &eth_tx_hash,
        eth_coinbase_confirmations,
        confirmation_poll_interval,
        "eth_coinbase_deposit",
        wait_for_confirmations,
    )
    .await?;

    Ok(eth_tx_hash)
}

async fn drain_all_eth_to_coinbase(
    evm_chain: ChainType,
    coinbase_client: &CoinbaseClient,
    sender_wallet: &dyn Wallet,
    confirmation_poll_interval: Duration,
    eth_coinbase_confirmations: u32,
    wait_for_confirmations: bool,
) -> Result<String> {
    if sender_wallet.chain_type() != evm_chain {
        return InvalidConversionRequestSnafu {
            reason: "Sender wallet is not the configured EVM wallet".to_string(),
        }
        .fail();
    }

    let eth_account_id = coinbase_client
        .get_eth_account_id()
        .await
        .context(CoinbaseExchangeClientSnafu)?;
    let eth_deposit_address = coinbase_client
        .get_eth_deposit_address(&eth_account_id, evm_chain)
        .await
        .context(CoinbaseExchangeClientSnafu)?;
    info!(
        "Got Coinbase ETH deposit address for full drain on {:?}: {}",
        evm_chain, eth_deposit_address
    );

    let t0 = Instant::now();
    let eth_tx_hash = sender_wallet
        .drain_to_address(&eth_deposit_address)
        .await
        .context(WalletSnafu)?;
    info!(
        "Created ETH drain transaction for Coinbase: {}, duration: {:?}",
        eth_tx_hash,
        t0.elapsed()
    );

    maybe_wait_for_coinbase_deposit_confirmations(
        sender_wallet,
        &eth_tx_hash,
        eth_coinbase_confirmations,
        confirmation_poll_interval,
        "eth_coinbase_drain",
        wait_for_confirmations,
    )
    .await?;

    Ok(eth_tx_hash)
}

async fn deposit_cbbtc_to_coinbase(
    evm_chain: ChainType,
    coinbase_client: &CoinbaseClient,
    sender_wallet: &dyn Wallet,
    amount_sats: u64,
    confirmation_poll_interval: Duration,
    cbbtc_coinbase_confirmations: u32,
    wait_for_confirmations: bool,
) -> Result<String> {
    if sender_wallet.chain_type() != evm_chain {
        return InvalidConversionRequestSnafu {
            reason: "Sender wallet is not the configured EVM wallet".to_string(),
        }
        .fail();
    }

    let cbbtc = TokenIdentifier::address(CB_BTC_CONTRACT_ADDRESS.to_string());
    let lot = Lot {
        currency: Currency {
            chain: evm_chain,
            token: cbbtc.clone(),
            decimals: 8,
        },
        amount: U256::from(amount_sats),
    };

    let t0 = Instant::now();
    let available_balance = sender_wallet
        .balance(&cbbtc)
        .await
        .context(WalletSnafu)?
        .total_balance;
    info!(
        "Got cbBTC balance before Coinbase deposit: {}, duration: {:?}",
        available_balance,
        t0.elapsed()
    );
    if available_balance < lot.amount {
        return InsufficientBalanceSnafu {
            requested_amount: lot.amount,
            available_amount: available_balance,
        }
        .fail();
    }

    info!("Consolidating cbBTC deposits before Coinbase deposit");
    let t0 = Instant::now();
    let consolidation_summary = sender_wallet
        .consolidate(
            &Lot {
                currency: Currency {
                    chain: evm_chain,
                    token: cbbtc.clone(),
                    decimals: 8,
                },
                amount: U256::MAX,
            },
            EVM_CONSOLIDATION_BATCH_SIZE,
        )
        .await
        .context(WalletSnafu)?;
    info!(
        "cbBTC consolidation summary before Coinbase deposit: {:?}, duration: {:?}",
        consolidation_summary,
        t0.elapsed()
    );

    let t0 = Instant::now();
    let btc_account_id = coinbase_client
        .get_btc_account_id()
        .await
        .context(CoinbaseExchangeClientSnafu)?;
    info!(
        "Got Coinbase BTC account id: {}, duration: {:?}",
        btc_account_id,
        t0.elapsed()
    );

    let cbbtc_deposit_address = coinbase_client
        .get_cbbtc_deposit_address(&btc_account_id, evm_chain)
        .await
        .context(CoinbaseExchangeClientSnafu)?;
    info!(
        "Got Coinbase cbBTC deposit address: {}, duration: {:?}",
        cbbtc_deposit_address,
        t0.elapsed()
    );

    let payments = vec![Payment {
        lot,
        to_address: cbbtc_deposit_address,
    }];
    let t0 = Instant::now();
    let cbbtc_tx_hash = sender_wallet
        .create_batch_payment(payments, None)
        .await
        .context(WalletSnafu)?;
    info!(
        "Created cbBTC deposit transaction for Coinbase: {}, duration: {:?}",
        cbbtc_tx_hash,
        t0.elapsed()
    );

    maybe_wait_for_coinbase_deposit_confirmations(
        sender_wallet,
        &cbbtc_tx_hash,
        cbbtc_coinbase_confirmations,
        confirmation_poll_interval,
        "cbbtc_coinbase_deposit",
        wait_for_confirmations,
    )
    .await?;

    Ok(cbbtc_tx_hash)
}

/// Send BTC from the sender's bitcoin wallet
#[instrument(
    name = "convert_btc_to_cbbtc",
    skip(
        coinbase_client,
        sender_wallet,
        amount_sats,
        recipient_address,
        confirmation_poll_interval,
        btc_coinbase_confirmations
    )
)]
pub async fn convert_btc_to_cbbtc(
    evm_chain: ChainType,
    coinbase_client: &CoinbaseClient,
    sender_wallet: &dyn Wallet,
    amount_sats: u64,
    recipient_address: &str,
    confirmation_poll_interval: Duration,
    btc_coinbase_confirmations: u32,
) -> Result<String> {
    info!("Starting BTC -> cbBTC conversion for {} sats", amount_sats);
    deposit_exact_btc_to_coinbase(
        coinbase_client,
        sender_wallet,
        amount_sats,
        confirmation_poll_interval,
        btc_coinbase_confirmations,
        true,
    )
    .await?;

    // at this point, the btc is confirmed and should be credited to our coinbase btc account
    // now we can send the btc to eth which will implicitly convert it to cbbtc
    let t0 = Instant::now();
    let withdrawal_id = loop {
        let withdrawal_id = coinbase_client
            .withdraw_bitcoin(recipient_address, &amount_sats, evm_chain)
            .await
            .context(CoinbaseExchangeClientSnafu);
        if let Ok(withdrawal_id) = withdrawal_id {
            break withdrawal_id;
        } else {
            error!("Failed to submit withdrawal: {:?}", withdrawal_id.err());
            tokio::time::sleep(Duration::from_secs(60)).await; //retry once a minute forever
            error!("Retrying withdrawal...");
        }
    };
    info!(
        "Withdraw cbbtc request submitted successfully: {}, duration: {:?}",
        withdrawal_id,
        t0.elapsed()
    );
    let start_time = Instant::now();
    loop {
        let withdrawal_status = coinbase_client
            .get_withdrawal_status(&withdrawal_id)
            .await
            .context(CoinbaseExchangeClientSnafu)?;
        match withdrawal_status {
            WithdrawalStatus::Completed(tx_hash) => {
                info!(
                    "Withdrew cbbtc complete! tx hash: {}, duration: {:?}",
                    tx_hash,
                    start_time.elapsed()
                );
                return Ok(tx_hash);
            }
            WithdrawalStatus::Pending => {
                if start_time.elapsed() > Duration::from_secs(60 * 60) {
                    warn!(
                        "Coinbase withdrawal with id {} has been pending for more than 1 hour",
                        withdrawal_id
                    );
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            WithdrawalStatus::Cancelled => {
                return InvalidConversionRequestSnafu {
                    reason: "Withdrawal cancelled".to_string(),
                }
                .fail();
            }
        }
    }
}

#[instrument(
    name = "convert_cbbtc_to_btc",
    skip(
        coinbase_client,
        sender_wallet,
        amount_sats,
        recipient_address,
        confirmation_poll_interval,
        cbbtc_coinbase_confirmations
    )
)]
pub async fn convert_cbbtc_to_btc(
    evm_chain: ChainType,
    coinbase_client: &CoinbaseClient,
    sender_wallet: &dyn Wallet,
    amount_sats: u64,
    recipient_address: &str,
    confirmation_poll_interval: Duration,
    cbbtc_coinbase_confirmations: u32,
) -> Result<String> {
    info!("Starting cbBTC -> BTC conversion for {} sats", amount_sats);
    deposit_cbbtc_to_coinbase(
        evm_chain,
        coinbase_client,
        sender_wallet,
        amount_sats,
        confirmation_poll_interval,
        cbbtc_coinbase_confirmations,
        true,
    )
    .await?;

    // at this point, the cbbtc is confirmed and should be credited to our coinbase btc account
    // now we can send the btc to eth which will implicitly convert it to cbbtc
    let t0 = Instant::now();
    let withdrawal_id = loop {
        let withdrawal_id = coinbase_client
            .withdraw_bitcoin(recipient_address, &amount_sats, ChainType::Bitcoin)
            .await
            .context(CoinbaseExchangeClientSnafu);
        if let Ok(withdrawal_id) = withdrawal_id {
            break withdrawal_id;
        } else {
            error!("Failed to submit withdrawal: {:?}", withdrawal_id.err());
            tokio::time::sleep(Duration::from_secs(60)).await; //retry once a minute forever
            error!("Retrying withdrawal...");
        }
    };
    info!(
        "Withdraw btc request submitted successfully: {}, duration: {:?}",
        withdrawal_id,
        t0.elapsed()
    );
    let start_time = Instant::now();
    loop {
        let withdrawal_status = coinbase_client
            .get_withdrawal_status(&withdrawal_id)
            .await
            .context(CoinbaseExchangeClientSnafu)?;
        match withdrawal_status {
            WithdrawalStatus::Completed(tx_hash) => {
                info!(
                    "Withdrew btc complete! tx hash: {}, duration: {:?}",
                    tx_hash,
                    start_time.elapsed()
                );
                return Ok(tx_hash);
            }
            WithdrawalStatus::Pending => {
                if start_time.elapsed() > Duration::from_secs(60 * 60) {
                    warn!(
                        "Coinbase withdrawal with id {} has been pending for more than 1 hour",
                        withdrawal_id
                    );
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            WithdrawalStatus::Cancelled => {
                error!("Withdrawal cancelled: {}", withdrawal_id);
                return InvalidConversionRequestSnafu {
                    reason: "Withdrawal cancelled".to_string(),
                }
                .fail();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{drain_inventory_to_coinbase, test_mode_requested_sats, DrainToCoinbaseMode};
    use crate::wallet::{ConsolidationSummary, Wallet, WalletBalance, WalletError, WalletResult};
    use alloy::primitives::U256;
    use async_trait::async_trait;
    use axum::{
        extract::Path,
        response::IntoResponse,
        routing::{get, post},
        Json as AxumJson, Router,
    };
    use coinbase_exchange_client::CoinbaseClient;
    use otc_chains::traits::{MarketMakerPaymentVerification, Payment};
    use otc_models::{ChainType, Lot, TokenIdentifier, CB_BTC_CONTRACT_ADDRESS};
    use reqwest::Url;
    use serde_json::json;
    use std::{collections::HashMap, sync::Mutex, time::Duration};

    #[derive(Default)]
    struct BitcoinTestWallet {
        balances: HashMap<TokenIdentifier, WalletBalance>,
    }

    impl BitcoinTestWallet {
        fn with_balance(
            mut self,
            token: TokenIdentifier,
            total_balance: U256,
            native_balance: U256,
            deposit_key_balance: U256,
        ) -> Self {
            self.balances.insert(
                token,
                WalletBalance {
                    total_balance,
                    native_balance,
                    deposit_key_balance,
                },
            );
            self
        }
    }

    #[async_trait]
    impl Wallet for BitcoinTestWallet {
        async fn create_batch_payment(
            &self,
            _payments: Vec<Payment>,
            _mm_payment_validation: Option<MarketMakerPaymentVerification>,
        ) -> WalletResult<String> {
            Err(WalletError::TransactionCreationFailed {
                reason: "unexpected create_batch_payment on bitcoin test wallet".to_string(),
            })
        }

        async fn drain_to_address(&self, _to_address: &str) -> WalletResult<String> {
            Ok("btc-drain-tx".to_string())
        }

        async fn guarantee_confirmations(
            &self,
            _tx_hash: &str,
            _confirmations: u64,
            _poll_interval: Duration,
        ) -> WalletResult<()> {
            Ok(())
        }

        async fn balance(&self, token: &TokenIdentifier) -> WalletResult<WalletBalance> {
            self.balances.get(token).cloned().ok_or_else(|| {
                WalletError::TransactionCreationFailed {
                    reason: format!("unsupported bitcoin test token: {token:?}"),
                }
            })
        }

        async fn consolidate(
            &self,
            lot: &Lot,
            _max_deposits_per_iteration: usize,
        ) -> WalletResult<ConsolidationSummary> {
            Ok(ConsolidationSummary {
                total_amount: lot.amount,
                iterations: 1,
                tx_hashes: vec!["btc-consolidation-tx".to_string()],
            })
        }

        fn receive_address(&self, _token: &TokenIdentifier) -> String {
            "btc-receive-address".to_string()
        }

        async fn cancel_tx(&self, _tx_hash: &str) -> WalletResult<String> {
            Ok("cancelled".to_string())
        }

        async fn check_tx_confirmations(&self, _tx_hash: &str) -> WalletResult<u64> {
            Ok(0)
        }

        fn chain_type(&self) -> ChainType {
            ChainType::Bitcoin
        }
    }

    struct EvmOrderingWallet {
        chain: ChainType,
        balances: HashMap<TokenIdentifier, WalletBalance>,
        events: Mutex<Vec<String>>,
    }

    impl EvmOrderingWallet {
        fn new(chain: ChainType) -> Self {
            Self {
                chain,
                balances: HashMap::new(),
                events: Mutex::new(Vec::new()),
            }
        }

        fn with_balance(
            mut self,
            token: TokenIdentifier,
            total_balance: U256,
            native_balance: U256,
            deposit_key_balance: U256,
        ) -> Self {
            self.balances.insert(
                token,
                WalletBalance {
                    total_balance,
                    native_balance,
                    deposit_key_balance,
                },
            );
            self
        }

        fn events(&self) -> Vec<String> {
            self.events.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Wallet for EvmOrderingWallet {
        async fn create_batch_payment(
            &self,
            payments: Vec<Payment>,
            _mm_payment_validation: Option<MarketMakerPaymentVerification>,
        ) -> WalletResult<String> {
            let payment = payments.into_iter().next().ok_or_else(|| {
                WalletError::TransactionCreationFailed {
                    reason: "expected a payment in evm ordering test wallet".to_string(),
                }
            })?;

            let event = if payment.lot.currency.token == TokenIdentifier::Native {
                "create_batch_payment:native"
            } else {
                "create_batch_payment:cbbtc"
            };
            self.events.lock().unwrap().push(event.to_string());

            Ok(if payment.lot.currency.token == TokenIdentifier::Native {
                "eth-payment-tx".to_string()
            } else {
                "cbbtc-payment-tx".to_string()
            })
        }

        async fn drain_to_address(&self, _to_address: &str) -> WalletResult<String> {
            let mut events = self.events.lock().unwrap();
            if !events
                .iter()
                .any(|event| event == "create_batch_payment:cbbtc")
            {
                return Err(WalletError::TransactionCreationFailed {
                    reason: "ETH drain executed before cbBTC transfer".to_string(),
                });
            }

            events.push("drain_to_address:native".to_string());
            Ok("eth-drain-tx".to_string())
        }

        async fn guarantee_confirmations(
            &self,
            _tx_hash: &str,
            _confirmations: u64,
            _poll_interval: Duration,
        ) -> WalletResult<()> {
            Ok(())
        }

        async fn balance(&self, token: &TokenIdentifier) -> WalletResult<WalletBalance> {
            self.balances.get(token).cloned().ok_or_else(|| {
                WalletError::TransactionCreationFailed {
                    reason: format!("unsupported evm test token: {token:?}"),
                }
            })
        }

        async fn consolidate(
            &self,
            lot: &Lot,
            _max_deposits_per_iteration: usize,
        ) -> WalletResult<ConsolidationSummary> {
            let event = if lot.currency.token == TokenIdentifier::Native {
                "consolidate:native"
            } else {
                "consolidate:cbbtc"
            };
            self.events.lock().unwrap().push(event.to_string());

            Ok(ConsolidationSummary {
                total_amount: lot.amount,
                iterations: 1,
                tx_hashes: vec!["consolidation-tx".to_string()],
            })
        }

        fn receive_address(&self, _token: &TokenIdentifier) -> String {
            "0x00000000000000000000000000000000000000bb".to_string()
        }

        async fn cancel_tx(&self, _tx_hash: &str) -> WalletResult<String> {
            Ok("cancelled".to_string())
        }

        async fn check_tx_confirmations(&self, _tx_hash: &str) -> WalletResult<u64> {
            Ok(0)
        }

        fn chain_type(&self) -> ChainType {
            self.chain
        }
    }

    async fn spawn_coinbase_account_server() -> Url {
        async fn accounts() -> impl IntoResponse {
            AxumJson(json!([
                {
                    "id": "btc-account-id",
                    "currency": "BTC"
                },
                {
                    "id": "eth-account-id",
                    "currency": "ETH"
                }
            ]))
        }

        async fn addresses(
            Path(_account_id): Path<String>,
            AxumJson(body): AxumJson<serde_json::Value>,
        ) -> impl IntoResponse {
            let network = body["network"].as_str().unwrap();
            let (address, warnings, deposit_uri) = match (_account_id.as_str(), network) {
                ("btc-account-id", "bitcoin") => {
                    let address = "bcrt1qexample0000000000000000000000000000000000000";
                    (address, json!([]), format!("bitcoin:{address}"))
                }
                ("btc-account-id", "ethereum" | "base") => {
                    let address = "0x00000000000000000000000000000000000000aa";
                    (
                        address,
                        json!([{
                            "title": "Only send cbBTC to this address",
                            "details": "Sending any other digital asset, including Ethereum (ETH), will result in permanent loss."
                        }]),
                        format!("{network}:0x0000000000000000000000000000000000000cb7/transfer?address={address}"),
                    )
                }
                ("eth-account-id", "ethereum" | "base") => {
                    let address = "0x00000000000000000000000000000000000000cc";
                    (address, json!([]), format!("{network}:{address}"))
                }
                (_, other) => panic!("unexpected test account/network: {_account_id}/{other}"),
            };

            AxumJson(json!({
                "address": address,
                "network": network,
                "warnings": warnings,
                "deposit_uri": deposit_uri,
                "exchange_deposit_address": true,
            }))
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/coinbase-accounts", get(accounts))
            .route("/coinbase-accounts/:account_id/addresses", post(addresses));

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        Url::parse(&format!("http://{}", addr)).unwrap()
    }

    #[test]
    fn test_mode_uses_one_percent_when_it_is_below_hundred_usd() {
        let requested = test_mode_requested_sats(1_000_000, 65_000.0).unwrap();
        assert_eq!(requested, 10_000);
    }

    #[test]
    fn test_mode_uses_hundred_usd_cap_when_one_percent_is_larger() {
        let requested = test_mode_requested_sats(10 * 100_000_000, 65_000.0).unwrap();
        assert_eq!(requested, 153_846);
    }

    #[test]
    fn test_mode_keeps_a_minimum_non_zero_probe_amount() {
        let requested = test_mode_requested_sats(50, 65_000.0).unwrap();
        assert_eq!(requested, 1);
    }

    #[test]
    fn test_mode_rejects_non_positive_prices() {
        assert!(test_mode_requested_sats(100_000, 0.0).is_err());
        assert!(test_mode_requested_sats(100_000, -1.0).is_err());
    }

    #[tokio::test]
    async fn drain_inventory_drains_eth_after_cbbtc_send() {
        let coinbase_client = CoinbaseClient::new(
            spawn_coinbase_account_server().await,
            String::new(),
            String::new(),
            String::new(),
        )
        .unwrap();
        let btc_wallet = BitcoinTestWallet::default().with_balance(
            TokenIdentifier::Native,
            U256::from(75_000_000u64),
            U256::from(75_000_000u64),
            U256::ZERO,
        );
        let evm_wallet = EvmOrderingWallet::new(ChainType::Ethereum)
            .with_balance(
                TokenIdentifier::address(CB_BTC_CONTRACT_ADDRESS.to_string()),
                U256::from(25_000_000u64),
                U256::from(25_000_000u64),
                U256::ZERO,
            )
            .with_balance(
                TokenIdentifier::Native,
                U256::from(2_000_000_000_000_000u64),
                U256::from(2_000_000_000_000_000u64),
                U256::ZERO,
            );

        let summary = drain_inventory_to_coinbase(
            ChainType::Ethereum,
            &coinbase_client,
            &btc_wallet,
            &evm_wallet,
            Duration::from_millis(1),
            0,
            0,
            DrainToCoinbaseMode::Full,
            true,
        )
        .await
        .unwrap();

        assert_eq!(summary.btc_tx_hash.as_deref(), Some("btc-drain-tx"));
        assert_eq!(summary.cbbtc_tx_hash.as_deref(), Some("cbbtc-payment-tx"));
        assert_eq!(summary.eth_tx_hash.as_deref(), Some("eth-drain-tx"));
        assert_eq!(
            evm_wallet.events(),
            vec![
                "consolidate:cbbtc".to_string(),
                "create_batch_payment:cbbtc".to_string(),
                "drain_to_address:native".to_string(),
            ]
        );
    }
}
