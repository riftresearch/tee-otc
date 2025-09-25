use std::sync::Arc;
use std::time::{Duration, Instant};

use otc_models::{TokenIdentifier, CB_BTC_CONTRACT_ADDRESS};
use snafu::{Location, ResultExt};
use tokio::time::sleep;
use tracing::{debug, info};

use metrics::{counter, gauge, histogram};

use crate::cb_bitcoin_converter::coinbase_client::{
    convert_btc_to_cbbtc, convert_cbbtc_to_btc, CoinbaseClient, ConversionError,
};
use crate::wallet::{Wallet, WalletError};

const BPS_DENOM: u64 = 10_000;

#[derive(Debug, snafu::Snafu)]
pub enum ConversionActorError {
    #[snafu(display("Failed to get balance: {}", source))]
    GetBalance {
        source: WalletError,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Failed to convert: {}", source))]
    Convert {
        source: ConversionError,
        #[snafu(implicit)]
        loc: Location,
    },
}

type Result<T, E = ConversionActorError> = std::result::Result<T, E>;

#[derive(Clone, Debug)]
pub struct BandsParams {
    /// target BTC share, e.g. 5000 = 50% of each asset
    pub target_bps: u64,
    /// symmetric band half-width around target, e.g. 2500 = ±25%
    pub band_width_bps: u64,
    /// loop timing
    pub poll_interval: Duration,
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

pub async fn run_rebalancer(
    coinbase_client: CoinbaseClient,
    btc_wallet: Arc<dyn Wallet>,
    evm_wallet: Arc<dyn Wallet>,
    params: BandsParams,
    execute_rebalance: bool,
) -> Result<()> {
    let btc_token = TokenIdentifier::Native;
    let cbbtc_token = TokenIdentifier::Address(CB_BTC_CONTRACT_ADDRESS.to_string());

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

        let total = btc.saturating_add(cbbtc);

        // --- metrics: snapshot every loop, even if no rebalance ---
        counter!("mm_loop_iterations_total").increment(1);
        gauge!("mm_balance_sats", "asset" => "btc").set(btc as f64);
        gauge!("mm_balance_sats", "asset" => "cbbtc").set(cbbtc as f64);
        gauge!("mm_total_sats").set(total as f64);

        // Native vs deposit key balance metrics
        let btc_native = btc_balance.native_balance.to::<u64>();
        let btc_deposit_key = btc_balance.deposit_key_balance.to::<u64>();
        let cbbtc_native = cbbtc_balance.native_balance.to::<u64>();
        let cbbtc_deposit_key = cbbtc_balance.deposit_key_balance.to::<u64>();

        gauge!("mm_native_balance_sats", "asset" => "btc").set(btc_native as f64);
        gauge!("mm_native_balance_sats", "asset" => "cbbtc").set(cbbtc_native as f64);
        gauge!("mm_deposit_key_balance_sats", "asset" => "btc").set(btc_deposit_key as f64);
        gauge!("mm_deposit_key_balance_sats", "asset" => "cbbtc").set(cbbtc_deposit_key as f64);

        debug!(
            "inventory: btc={} sats, cbbtc={} sats, total={}",
            btc, cbbtc, total
        );
        if total == 0 {
            gauge!("mm_inventory_ratio_bps").set(0.0);
            sleep(params.poll_interval).await;
            continue;
        }

        // Log detailed balance information with human-readable percentages
        let btc_pct = (btc as f64 / total as f64) * 100.0;
        let cbbtc_pct = (cbbtc as f64 / total as f64) * 100.0;

        info!(
            message = "detailed inventory breakdown",
            btc_total_sats = btc,
            btc_native_sats = btc_native,
            btc_deposit_key_sats = btc_deposit_key,
            btc_percentage = %format!("{:.2}%", btc_pct),
            cbbtc_total_sats = cbbtc,
            cbbtc_native_sats = cbbtc_native,
            cbbtc_deposit_key_sats = cbbtc_deposit_key,
            cbbtc_percentage = %format!("{:.2}%", cbbtc_pct),
            total_sats = total
        );

        // --- symmetric band around target ---
        let lo = params
            .target_bps
            .saturating_sub(params.band_width_bps)
            .min(BPS_DENOM);
        let hi = (params.target_bps + params.band_width_bps).min(BPS_DENOM);

        let r_bps = ratio_bps(btc, cbbtc);
        gauge!("mm_inventory_ratio_bps").set(r_bps as f64);

        let side = if r_bps < lo {
            Some(Side::BtcToCbbtc) // below lower band → need more cBBTC
        } else if r_bps > hi {
            Some(Side::CbbtcToBtc) // above upper band → need more BTC
        } else {
            None
        };

        if let Some(side) = side {
            if execute_rebalance {
                // snap-to-target trade size (in sats)
                let trade_sats = sats_to_target(total, params.target_bps, r_bps);
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
                            &coinbase_client,
                            btc_wallet.as_ref(),
                            trade_sats,
                            &recv,
                        )
                        .await
                        .context(ConvertSnafu)?;
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
                            &coinbase_client,
                            evm_wallet.as_ref(),
                            trade_sats,
                            &recv,
                        )
                        .await
                        .context(ConvertSnafu)?;
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
