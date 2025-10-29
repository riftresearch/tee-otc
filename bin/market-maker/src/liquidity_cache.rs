use crate::balance_strat::QuoteBalanceStrategy;
use crate::wallet::{WalletManager, WalletResult};
use otc_models::{constants::SUPPORTED_TOKENS_BY_CHAIN, ChainType, Currency, TokenIdentifier};
use otc_protocols::rfq::TradingPairLiquidity;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tracing::{error, warn};

const CACHE_UPDATE_INTERVAL: Duration = Duration::from_secs(60);

pub struct LiquidityCache {
    cached_data: Arc<RwLock<Vec<TradingPairLiquidity>>>,
}

impl LiquidityCache {
    /// Create a new liquidity cache and spawn a background task to keep it updated.
    ///
    /// The background task will periodically compute liquidity and update the cache.
    /// `get_liquidity()` will always return the cached data without blocking on computation.
    #[must_use]
    pub fn new(
        wallet_manager: Arc<WalletManager>,
        balance_strategy: Arc<QuoteBalanceStrategy>,
        join_set: &mut JoinSet<crate::Result<()>>,
    ) -> Self {
        let cached_data = Arc::new(RwLock::new(Vec::new()));
        let cached_data_task = cached_data.clone();

        // Spawn background task to update the cache periodically
        join_set.spawn(async move {
            let mut interval = tokio::time::interval(CACHE_UPDATE_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                match Self::compute_liquidity_static(
                    &wallet_manager,
                    &balance_strategy,
                )
                .await
                {
                    Ok(trading_pairs) => {
                        let mut cache = cached_data_task.write().await;
                        *cache = trading_pairs;
                    }
                    Err(e) => {
                        error!("Failed to compute liquidity in background task: {}", e);
                    }
                }
            }
        });

        Self {
            cached_data,
        }
    }

    /// Get the current cached liquidity data.
    ///
    /// This method always returns immediately with the cached data and never blocks on computation.
    /// The cache is updated asynchronously by a background task.
    pub async fn get_liquidity(&self) -> Vec<TradingPairLiquidity> {
        let cache = self.cached_data.read().await;
        cache.clone()
    }

    /// Static method to compute liquidity for all supported trading pairs.
    ///
    /// This is used by the background task and doesn't require `&self`.
    async fn compute_liquidity_static(
        wallet_manager: &WalletManager,
        balance_strategy: &QuoteBalanceStrategy,
    ) -> WalletResult<Vec<TradingPairLiquidity>> {
        let mut trading_pairs = Vec::new();

        // Get all registered chains
        let registered_chains = wallet_manager.registered_chains();

        // For each chain, compute liquidity for all supported tokens
        for from_chain in &registered_chains {
            if let Some(from_tokens) = SUPPORTED_TOKENS_BY_CHAIN.get(from_chain) {
                for from_token in from_tokens {
                    // For each other chain, create a trading pair
                    for to_chain in &registered_chains {
                        if from_chain == to_chain {
                            continue; // Skip same-chain swaps
                        }

                        if let Some(to_tokens) = SUPPORTED_TOKENS_BY_CHAIN.get(to_chain) {
                            for to_token in to_tokens {
                                // Get wallet for the destination chain
                                if let Some(to_wallet) = wallet_manager.get(*to_chain) {
                                    // Get balance for the TO token (what MM can provide)
                                    match to_wallet.balance(to_token).await {
                                        Ok(to_balance) => {
                                            // Compute max amount based on TO token's balance.
                                            // This represents the maximum OUTPUT the MM can provide.
                                            let max_output_capacity = balance_strategy
                                                .max_output_amount(to_balance.total_balance);

                                            // TradingPairLiquidity.max_amount represents max INPUT from user's perspective.
                                            //
                                            // Ideally: max_input should be set such that output(max_input) ≈ capacity
                                            // This would require: max_input = (capacity + network_fee) / (1 - protocol_fee_rate)
                                            //
                                            // However, for simplicity and to avoid fee calculation complexity here,
                                            // we use capacity directly as max_amount. This is conservative since:
                                            // - output(capacity) = capacity × 0.999 - 124 < capacity ✓
                                            // - Actual max_input ≈ capacity × 1.001 would be more precise
                                            //
                                            // This ensures the reported max safely stays within capacity, though it
                                            // means inputs slightly over max_amount might still succeed due to fees.
                                            let max_amount = max_output_capacity;

                                            // Get decimals for both tokens
                                            let to_decimals = get_token_decimals(*to_chain, to_token);
                                            let from_decimals =
                                                get_token_decimals(*from_chain, from_token);

                                            trading_pairs.push(TradingPairLiquidity {
                                                from: Currency {
                                                    chain: *from_chain,
                                                    token: from_token.clone(),
                                                    decimals: from_decimals,
                                                },
                                                to: Currency {
                                                    chain: *to_chain,
                                                    token: to_token.clone(),
                                                    decimals: to_decimals,
                                                },
                                                max_amount,
                                            });
                                        }
                                        Err(e) => {
                                            warn!(
                                                "Failed to get balance for {:?}/{:?}: {}",
                                                to_chain, to_token, e
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(trading_pairs)
    }
}

/// Get the standard decimals for a token on a chain
fn get_token_decimals(chain: ChainType, token: &TokenIdentifier) -> u8 {
    match (chain, token) {
        (ChainType::Bitcoin, TokenIdentifier::Native) => 8,
        (ChainType::Ethereum, TokenIdentifier::Native) => 18,
        (ChainType::Ethereum, TokenIdentifier::Address(_)) => 8, // cbBTC has 8 decimals
        _ => 18, // Default fallback
    }
}

