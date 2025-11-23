use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::primitives::U256;
use alloy::primitives::utils::format_units;
use otc_chains::traits::Payment;
use otc_models::{CB_BTC_CONTRACT_ADDRESS, ChainType, Currency, Lot, TokenIdentifier};
use snafu::{Location, ResultExt};
use tokio::time::sleep;
use tracing::{debug, error, info, instrument, warn};

use metrics::{counter, gauge, histogram};

use coinbase_exchange_client::{
    CoinbaseClient, CoinbaseExchangeClientError, WithdrawalStatus
};

use crate::wallet::{Wallet, WalletError};

const BPS_DENOM: u64 = 10_000;

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

    #[snafu(display("Insufficient balance: requested {}, available {}", requested_amount, available_amount))]
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
    let cbbtc_token = TokenIdentifier::Address(CB_BTC_CONTRACT_ADDRESS.to_string());

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


/// Send BTC from the sender's bitcoin wallet
#[instrument(name = "convert_btc_to_cbbtc", skip(coinbase_client, sender_wallet, amount_sats, recipient_address, confirmation_poll_interval, btc_coinbase_confirmations))]
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
    if sender_wallet.chain_type() != ChainType::Bitcoin {
        return InvalidConversionRequestSnafu {
            reason: "Sender wallet is not a bitcoin wallet".to_string(),
        }
        .fail();
    }

    // check if the sender wallet can fill the lot
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

    info!("Consolidating bitcoin deposits before rebalance");
    let t0 = Instant::now();
    let consolidation_summary = sender_wallet.consolidate(&Lot {
        currency: Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        },
        amount: U256::MAX,
    }, 100).await.context(WalletSnafu)?;
    info!("Consolidation summary during rebalance: {:?}, duration: {:?}", consolidation_summary, t0.elapsed());

    let btc_account_id = coinbase_client.get_btc_account_id().await.context(CoinbaseExchangeClientSnafu)?;

    // get btc deposit address
    let btc_deposit_address = coinbase_client
        .get_btc_deposit_address(&btc_account_id, ChainType::Bitcoin)
        .await.context(CoinbaseExchangeClientSnafu)?;
    info!("Got btc deposit address: {}", btc_deposit_address);

    // send the btc to the deposit address
    let payments = vec![Payment {
        lot: lot.clone(),
        to_address: btc_deposit_address,
    }];
    let t0 = Instant::now();
    let btc_tx_hash = sender_wallet
        .create_batch_payment(payments, None)
        .await
        .context(WalletSnafu)?;
    info!("Created batch payment for btc: {}, duration: {:?}", btc_tx_hash, t0.elapsed());
    let t0 = Instant::now();
    // wait for the btc to be confirmed
    sender_wallet
        .guarantee_confirmations(&btc_tx_hash, btc_coinbase_confirmations as u64, confirmation_poll_interval)
        .await
        .context(WalletSnafu)?;
    info!("Guaranteed {} confirmations for btc tx hash: {}, duration: {:?}", btc_coinbase_confirmations, btc_tx_hash, t0.elapsed());
    // at this point, the btc is confirmed and should be credited to our coinbase btc account
    // now we can send the btc to eth which will implicitly convert it to cbbtc
    let t0 = Instant::now();
    let withdrawal_id = loop {
        let withdrawal_id = coinbase_client
            .withdraw_bitcoin(recipient_address, &amount_sats, evm_chain)
            .await.context(CoinbaseExchangeClientSnafu);
        if let Ok(withdrawal_id) = withdrawal_id {
            break withdrawal_id;
        } else {
            error!("Failed to submit withdrawal: {:?}", withdrawal_id.err());
            tokio::time::sleep(Duration::from_secs(60)).await; //retry once a minute forever
            error!("Retrying withdrawal...");
        }
    };
    info!("Withdraw cbbtc request submitted successfully: {}, duration: {:?}", withdrawal_id, t0.elapsed());
    let start_time = Instant::now();
    loop {
        let withdrawal_status = coinbase_client
            .get_withdrawal_status(&withdrawal_id)
            .await.context(CoinbaseExchangeClientSnafu)?;
        match withdrawal_status {
            WithdrawalStatus::Completed(tx_hash) => {
                info!("Withdrew cbbtc complete! tx hash: {}, duration: {:?}", tx_hash, start_time.elapsed());
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

#[instrument(name = "convert_cbbtc_to_btc", skip(coinbase_client, sender_wallet, amount_sats, recipient_address, confirmation_poll_interval, cbbtc_coinbase_confirmations))]
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
    if sender_wallet.chain_type() != evm_chain {
        return InvalidConversionRequestSnafu {
            reason: "Sender wallet is not an ethereum wallet".to_string(),
        }
        .fail();
    }
    

    let cbbtc = TokenIdentifier::Address(CB_BTC_CONTRACT_ADDRESS.to_string());

    // check if the sender wallet can fill the lot
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
    info!("Got cbbtc balance: {}, duration: {:?}", available_balance, t0.elapsed());
    if available_balance < lot.amount {
        return InsufficientBalanceSnafu {
            requested_amount: lot.amount,
            available_amount: available_balance,
        }
        .fail();
    }

    info!("Consolidating cbbtc deposits before rebalance");
    let t0 = Instant::now();
    let consolidation_summary = sender_wallet.consolidate(&Lot {
        currency: Currency {
            chain: evm_chain,
            token: cbbtc.clone(),
            decimals: 8,
        },
        amount: U256::MAX,
    }, 200).await.context(WalletSnafu)?;
    info!("Consolidation summary during rebalance: {:?}, duration: {:?}", consolidation_summary, t0.elapsed());
    let t0 = Instant::now();
    let btc_account_id = coinbase_client.get_btc_account_id().await.context(CoinbaseExchangeClientSnafu)?;
    info!("Got btc account id: {}, duration: {:?}", btc_account_id, t0.elapsed());

    // get the cbbtc deposit address
    let cbbtc_deposit_address = coinbase_client
        .get_btc_deposit_address(&btc_account_id, evm_chain)
        .await.context(CoinbaseExchangeClientSnafu)?;
    info!("Got cbbtc deposit address: {}, duration: {:?}", cbbtc_deposit_address, t0.elapsed());
    // send the cbbtc to the deposit address
    let payments = vec![Payment {
        lot: lot.clone(),
        to_address: cbbtc_deposit_address,
    }];
    let t0 = Instant::now();
    let cbbtc_tx_hash = sender_wallet
        .create_batch_payment(payments, None)
        .await
        .context(WalletSnafu)?;
    info!("Created batch payment for cbbtc: {}, duration: {:?}", cbbtc_tx_hash, t0.elapsed());
    let t0 = Instant::now();
    // wait for the cbbtc to be confirmed
    sender_wallet
        .guarantee_confirmations(&cbbtc_tx_hash, cbbtc_coinbase_confirmations as u64, confirmation_poll_interval)
        .await
        .context(WalletSnafu)?;
    info!("Guaranteed {} confirmations for cbbtc tx hash: {}, duration: {:?}", cbbtc_coinbase_confirmations, cbbtc_tx_hash, t0.elapsed());
    // at this point, the cbbtc is confirmed and should be credited to our coinbase btc account
    // now we can send the btc to eth which will implicitly convert it to cbbtc
    let t0 = Instant::now();
    let withdrawal_id = loop { 
        let withdrawal_id = coinbase_client
            .withdraw_bitcoin(recipient_address, &amount_sats, ChainType::Bitcoin)
            .await.context(CoinbaseExchangeClientSnafu);
        if let Ok(withdrawal_id) = withdrawal_id {
            break withdrawal_id;
        } else {
            error!("Failed to submit withdrawal: {:?}", withdrawal_id.err());
            tokio::time::sleep(Duration::from_secs(60)).await; //retry once a minute forever
            error!("Retrying withdrawal...");
        }
    };
    info!("Withdraw btc request submitted successfully: {}, duration: {:?}", withdrawal_id, t0.elapsed());
    let start_time = Instant::now();
    loop {
        let withdrawal_status = coinbase_client
            .get_withdrawal_status(&withdrawal_id)
            .await.context(CoinbaseExchangeClientSnafu)?;
        match withdrawal_status {
            WithdrawalStatus::Completed(tx_hash) => {
                info!("Withdrew btc complete! tx hash: {}, duration: {:?}", tx_hash, start_time.elapsed());
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


