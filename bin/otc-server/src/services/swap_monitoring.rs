use crate::db::Database;
use crate::error::OtcServerError;
use crate::{config::Settings, services::mm_registry};
use alloy::primitives::U256;
use blockchain_utils::FeeCalcFromLot;
use otc_chains::traits::MarketMakerPaymentValidation;
use otc_chains::ChainRegistry;
use otc_models::{MMDepositStatus, Swap, SwapStatus, TxStatus, UserDepositStatus};
use snafu::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{error, info, warn};

#[derive(Debug, Snafu)]
pub enum MonitoringError {
    #[snafu(display("Database error: {}", source))]
    Database { source: OtcServerError },

    #[snafu(display("Chain operation error: {}", source))]
    ChainOperation { source: otc_chains::Error },

    #[snafu(display("Invalid state transition from {:?}", current_state))]
    InvalidTransition { current_state: SwapStatus },
}

pub type MonitoringResult<T> = Result<T, MonitoringError>;

/// Background service that monitors all active swaps for:
/// - Incoming deposits (user and MM)
/// - Confirmation tracking
/// - Settlement completion
pub struct SwapMonitoringService {
    db: Database,
    settings: Arc<Settings>,
    chain_registry: Arc<ChainRegistry>,
    mm_registry: Arc<mm_registry::MMRegistry>,
    chain_monitor_interval_seconds: u64,
}

impl SwapMonitoringService {
    #[must_use]
    pub fn new(
        db: Database,
        settings: Arc<Settings>,
        chain_registry: Arc<ChainRegistry>,
        mm_registry: Arc<mm_registry::MMRegistry>,
        chain_monitor_interval_seconds: u64,
    ) -> Self {
        Self {
            db,
            settings,
            chain_registry,
            mm_registry,
            chain_monitor_interval_seconds,
        }
    }

    /// Start the monitoring service
    pub async fn run(self: Arc<Self>) {
        info!("Starting swap monitoring service");

        // Check every 12 seconds
        // interval is based on the shortest confirmation time of all chains
        let chains = self.chain_registry.supported_chains();
        let interval = Duration::from_secs(self.chain_monitor_interval_seconds);
        info!(
            "Starting swap monitoring service with interval: {:?}",
            interval
        );
        let mut interval = time::interval(interval);

        loop {
            interval.tick().await;

            if let Err(e) = self.monitor_all_swaps().await {
                error!("Error monitoring swaps: {}", e);
            }
        }
    }

    /// Monitor all active swaps
    async fn monitor_all_swaps(&self) -> MonitoringResult<()> {
        // Get all active swaps
        let active_swaps = self.db.swaps().get_active().await.context(DatabaseSnafu)?;

        info!("Monitoring {} active swaps", active_swaps.len());

        // TODO: use a semaphore to limit the number of swaps we monitor in parallel (+ support for parallelization)
        for swap in active_swaps {
            if let Err(e) = self.monitor_swap(&swap).await {
                error!("Error monitoring swap {}: {}", swap.id, e);
            }
        }

        Ok(())
    }

    /// Monitor a single swap based on its current state
    async fn monitor_swap(&self, swap: &Swap) -> MonitoringResult<()> {
        info!(
            "Monitoring swap {} status: {:?}, user deposit status: {:?}",
            swap.id, swap.status, swap.user_deposit_status
        );
        // Check for timeout first
        if swap.failure_at.is_some() {
            warn!("Swap {} has failed", swap.id);
            return Ok(());
        }

        info!(
            "Monitoring swap {} status: {:?}, user deposit status: {:?}",
            swap.id, swap.status, swap.user_deposit_status
        );

        match swap.status {
            SwapStatus::WaitingUserDepositInitiated => {
                self.check_user_deposit(swap).await?;
            }
            SwapStatus::WaitingUserDepositConfirmed => {
                self.check_user_deposit_confirmation(swap).await?;
            }
            SwapStatus::WaitingMMDepositInitiated => {
                self.check_mm_deposit(swap).await?;
            }
            SwapStatus::WaitingMMDepositConfirmed => {
                self.check_mm_deposit_confirmation(swap).await?;
            }
            SwapStatus::Settled => {
                // Settlement already complete, nothing to monitor
            }
            _ => {
                // Other states don't need monitoring
            }
        }

        Ok(())
    }

    /// Check for user deposit
    async fn check_user_deposit(&self, swap: &Swap) -> MonitoringResult<()> {
        // Get the quote to know what token/chain to check
        let quote = &swap.quote;

        // Get the chain operations for the user's deposit chain (from = user sends)
        let chain_ops = self.chain_registry.get(&quote.from.currency.chain).ok_or(
            MonitoringError::ChainOperation {
                source: otc_chains::Error::ChainNotSupported {
                    chain: format!("{:?}", quote.from.currency.chain),
                },
            },
        )?;

        // Derive the user deposit address
        let user_wallet = chain_ops
            .derive_wallet(&self.settings.master_key_bytes(), &swap.user_deposit_salt)
            .context(ChainOperationSnafu)?;

        info!("User deposit wallet: {:?}", user_wallet.address);

        // Check for deposit from the user's wallet
        let deposit_info = chain_ops
            .search_for_transfer(&user_wallet.address, &quote.from, None, None)
            .await
            .context(ChainOperationSnafu)?;

        info!("Deposit info: {:?}", deposit_info);

        if let Some(deposit) = deposit_info {
            info!(
                "User deposit detected for swap {}: {} on chain {:?}",
                swap.id, deposit.tx_hash, quote.from.currency.chain
            );

            // Update swap state
            let user_deposit_status = UserDepositStatus {
                tx_hash: deposit.tx_hash.clone(),
                amount: deposit.amount,
                deposit_detected_at: utc::now(),
                confirmed_at: None,
                confirmations: 0, // Initial detection
                last_checked: utc::now(),
            };

            self.db
                .swaps()
                .user_deposit_detected(swap.id, user_deposit_status)
                .await
                .context(DatabaseSnafu)?;

            // Notify MM about user deposit
            let mm_registry = self.mm_registry.clone();
            let market_maker_id = swap.market_maker_id;
            let swap_id = swap.id;
            let quote_id = swap.quote.id;
            let user_deposit_address = swap.user_deposit_address.clone();
            let tx_hash = deposit.tx_hash.clone();
            tokio::spawn(async move {
                let _ = mm_registry
                    .notify_user_deposit(
                        &market_maker_id,
                        &swap_id,
                        &quote_id,
                        &user_deposit_address,
                        &tx_hash,
                    )
                    .await;
            });
        }

        Ok(())
    }

    /// Check user deposit confirmations
    async fn check_user_deposit_confirmation(&self, swap: &Swap) -> MonitoringResult<()> {
        let quote = &swap.quote;
        let user_deposit =
            swap.user_deposit_status
                .as_ref()
                .ok_or(MonitoringError::InvalidTransition {
                    current_state: swap.status,
                })?;

        // Get the chain operations for the user's deposit chain
        let chain_ops = self.chain_registry.get(&quote.from.currency.chain).ok_or(
            MonitoringError::ChainOperation {
                source: otc_chains::Error::ChainNotSupported {
                    chain: format!("{:?}", quote.from.currency.chain),
                },
            },
        )?;

        // Check confirmation status
        let tx_status = chain_ops
            .get_tx_status(&user_deposit.tx_hash)
            .await
            .context(ChainOperationSnafu)?;

        match tx_status {
            TxStatus::Confirmed(confirmations) => {
                info!(
                    "User deposit for swap {} has {} confirmations",
                    swap.id, confirmations
                );

                // Update confirmations
                self.db
                    .swaps()
                    .update_user_confirmations(swap.id, confirmations as u32)
                    .await
                    .context(DatabaseSnafu)?;

                // Check if we have enough confirmations
                let (required_user_confirmations, _) = swap.get_required_confirmations();
                if confirmations >= required_user_confirmations {
                    info!(
                        "User deposit for swap {} has reached required confirmations",
                        swap.id
                    );

                    // Transition to waiting for MM deposit
                    self.db
                        .swaps()
                        .user_deposit_confirmed(swap.id)
                        .await
                        .context(DatabaseSnafu)?;

                    // Notify MM to send their deposit
                    let mm_registry = self.mm_registry.clone();
                    let market_maker_id = swap.market_maker_id;
                    let swap_id = swap.id;
                    let quote_id = swap.quote.id;
                    let user_destination_address = swap.user_destination_address.clone();
                    let mm_nonce = swap.mm_nonce;
                    let expected_currency = swap.quote.to.clone();

                    tokio::spawn(async move {
                        let _ = mm_registry
                            .notify_user_deposit_confirmed(
                                &market_maker_id,
                                &swap_id,
                                &quote_id,
                                &user_destination_address,
                                mm_nonce,
                                &expected_currency,
                            )
                            .await;
                    });
                }
            }
            TxStatus::NotFound => {
                warn!(
                    "User deposit tx {} for swap {} not found on chain",
                    user_deposit.tx_hash, swap.id
                );
            }
        }

        Ok(())
    }

    /// Check for market maker deposit
    async fn check_mm_deposit(&self, swap: &Swap) -> MonitoringResult<()> {
        // Get the quote to know what token/chain to check
        let quote = &swap.quote;

        // Get the chain operations for the MM's deposit chain (to = MM sends)
        let chain_ops = self.chain_registry.get(&quote.to.currency.chain).ok_or(
            MonitoringError::ChainOperation {
                source: otc_chains::Error::ChainNotSupported {
                    chain: format!("{:?}", quote.to.currency.chain),
                },
            },
        )?;

        // Check for deposit
        let deposit_info = chain_ops
            .search_for_transfer(
                &swap.user_destination_address,
                &quote.to,
                Some(MarketMakerPaymentValidation {
                    fee_amount: U256::from(quote.to.compute_protocol_fee()),
                    embedded_nonce: swap.mm_nonce,
                }),
                None,
            )
            .await
            .context(ChainOperationSnafu)?;

        if let Some(deposit) = deposit_info {
            info!(
                "MM deposit detected for swap {}: {} on chain {:?}",
                swap.id, deposit.tx_hash, quote.to.currency.chain
            );

            // Update swap state
            let mm_deposit_status = MMDepositStatus {
                tx_hash: deposit.tx_hash.clone(),
                amount: deposit.amount,
                deposit_detected_at: utc::now(),
                confirmations: deposit.confirmations,
                last_checked: utc::now(),
            };

            self.db
                .swaps()
                .mm_deposit_detected(swap.id, mm_deposit_status)
                .await
                .context(DatabaseSnafu)?;
        }

        Ok(())
    }

    /// Check MM deposit confirmations
    async fn check_mm_deposit_confirmation(&self, swap: &Swap) -> MonitoringResult<()> {
        let quote = &swap.quote;
        let mm_deposit =
            swap.mm_deposit_status
                .as_ref()
                .ok_or(MonitoringError::InvalidTransition {
                    current_state: swap.status,
                })?;

        // Get the chain operations for the MM's deposit chain
        let chain_ops = self.chain_registry.get(&quote.to.currency.chain).ok_or(
            MonitoringError::ChainOperation {
                source: otc_chains::Error::ChainNotSupported {
                    chain: format!("{:?}", quote.to.currency.chain),
                },
            },
        )?;

        // Check confirmation status
        let tx_status = chain_ops
            .get_tx_status(&mm_deposit.tx_hash)
            .await
            .context(ChainOperationSnafu)?;

        match tx_status {
            TxStatus::Confirmed(confirmations) => {
                info!(
                    "MM deposit for swap {} has {} confirmations",
                    swap.id, confirmations
                );

                // Update confirmations
                self.db
                    .swaps()
                    .update_mm_confirmations(swap.id, confirmations as u32)
                    .await
                    .context(DatabaseSnafu)?;

                // Check if we have enough confirmations
                let (_, required_mm_confirmations) = swap.get_required_confirmations();
                if confirmations >= required_mm_confirmations {
                    info!(
                        "MM deposit for swap {} has reached required confirmations",
                        swap.id
                    );

                    // Transition to settled state
                    self.db
                        .swaps()
                        .mm_deposit_confirmed(swap.id)
                        .await
                        .context(DatabaseSnafu)?;

                    // Send private key to MM
                    let chain_ops = self.chain_registry.get(&quote.from.currency.chain).ok_or(
                        MonitoringError::ChainOperation {
                            source: otc_chains::Error::ChainNotSupported {
                                chain: format!("{:?}", quote.from.currency.chain),
                            },
                        },
                    )?;

                    let user_wallet = chain_ops
                        .derive_wallet(&self.settings.master_key_bytes(), &swap.user_deposit_salt)
                        .context(ChainOperationSnafu)?;

                    let mm_registry = self.mm_registry.clone();
                    let market_maker_id = swap.market_maker_id;
                    let swap_id = swap.id;
                    let private_key = user_wallet.private_key().to_string();
                    let user_deposit_tx_hash =
                        swap.user_deposit_status.as_ref().unwrap().tx_hash.clone();
                    let lot = swap.quote.from.clone();
                    tokio::spawn(async move {
                        let _ = mm_registry
                            .notify_swap_complete(
                                &market_maker_id,
                                &swap_id,
                                &private_key,
                                &lot,
                                &user_deposit_tx_hash,
                            )
                            .await;
                    });

                    // Mark private key as sent
                    self.db
                        .swaps()
                        .mark_private_key_sent(swap.id)
                        .await
                        .context(DatabaseSnafu)?;
                }
            }
            TxStatus::NotFound => {
                warn!(
                    "MM deposit tx {} for swap {} not found on chain",
                    mm_deposit.tx_hash, swap.id
                );
            }
        }

        Ok(())
    }
}
