use crate::balance_strat::QuoteBalanceStrategy;
use crate::liquidity_lock::LiquidityLockManager;
use crate::wallet::{WalletBalance, WalletManager, WalletResult};
use alloy::primitives::U256;
use otc_models::{
    constants::{CB_BTC_CONTRACT_ADDRESS, SUPPORTED_TOKENS_BY_CHAIN},
    ChainType, Currency, TokenIdentifier,
};
use otc_protocols::rfq::TradingPairLiquidity;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tracing::{error, warn};

const BALANCE_UPDATE_INTERVAL: Duration = Duration::from_secs(15);
const LIQUIDITY_COMPUTE_INTERVAL: Duration = Duration::from_secs(1);

pub struct LiquidityCache {
    cached_data: Arc<RwLock<Vec<TradingPairLiquidity>>>,
    balance_map: Arc<RwLock<HashMap<ChainType, HashMap<TokenIdentifier, WalletBalance>>>>,
    balance_strategy: Arc<QuoteBalanceStrategy>,
    lock_manager: Arc<LiquidityLockManager>,
}

impl LiquidityCache {
    /// Create a new liquidity cache and spawn background tasks to keep it updated.
    ///
    /// Two background tasks are spawned:
    /// 1. Balance update task (every 15 seconds): Fetches fresh balances from wallets
    /// 2. Liquidity computation task (every 1 second): Computes liquidity using cached balances
    ///
    /// This separation allows liquidity to reflect lock changes quickly (every 1 second) while
    /// minimizing wallet API calls (balances only update every 15 seconds).
    /// `get_liquidity()` will always return the cached data without blocking on computation.
    #[must_use]
    pub fn new(
        wallet_manager: Arc<WalletManager>,
        balance_strategy: Arc<QuoteBalanceStrategy>,
        lock_manager: Arc<LiquidityLockManager>,
        join_set: &mut JoinSet<crate::Result<()>>,
    ) -> Self {
        let cached_data = Arc::new(RwLock::new(Vec::new()));
        let balance_map = Arc::new(RwLock::new(HashMap::new()));
        
        let balance_map_task = balance_map.clone();
        let wallet_manager_task = wallet_manager.clone();

        // Spawn background task to update balance cache (every 15 seconds)
        join_set.spawn(async move {
            let mut interval = tokio::time::interval(BALANCE_UPDATE_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                // Update balance cache by fetching fresh balances from wallets
                let mut updated_balances: HashMap<ChainType, HashMap<TokenIdentifier, WalletBalance>> =
                    HashMap::new();

                for chain in wallet_manager_task.registered_chains() {
                    let Some(wallet) = wallet_manager_task.get(chain) else {
                        continue;
                    };

                    let Some(tokens) = SUPPORTED_TOKENS_BY_CHAIN.get(&chain) else {
                        continue;
                    };

                    let mut token_balances = HashMap::new();
                    for token in tokens {
                        match wallet.balance(token).await {
                            Ok(balance) => {
                                token_balances.insert(token.clone(), balance);
                            }
                            Err(error) => {
                                warn!(
                                    "Failed to refresh cached balance for chain {:?}, token {:?}: {:?}",
                                    chain, token, error
                                );
                            }
                        }
                    }

                    if !token_balances.is_empty() {
                        updated_balances.insert(chain, token_balances);
                    }
                }

                // Update balance map
                {
                    let mut guard = balance_map_task.write().await;
                    *guard = updated_balances;
                }
            }
        });

        // Spawn separate background task to compute liquidity (every 1 second)
        // This uses cached balances, so it's fast and reflects lock changes quickly
        let cached_data_task = cached_data.clone();
        let balance_map_task = balance_map.clone();
        let balance_strategy_task = balance_strategy.clone();
        let lock_manager_task = lock_manager.clone();

        join_set.spawn(async move {
            // Compute liquidity immediately on startup (don't wait for first tick)
            // This ensures data is available as soon as balances are populated
            // Emit metrics on startup
            match Self::compute_liquidity_static(
                &balance_strategy_task,
                &lock_manager_task,
                &balance_map_task,
                true, // emit_metrics on startup
            )
            .await
            {
                Ok(trading_pairs) => {
                    let mut cache = cached_data_task.write().await;
                    *cache = trading_pairs;
                }
                Err(e) => {
                    error!("Failed to compute initial liquidity: {}", e);
                }
            }

            let mut interval = tokio::time::interval(LIQUIDITY_COMPUTE_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let mut tick_count = 0u32;

            loop {
                interval.tick().await;
                tick_count += 1;

                // Emit metrics every 10 seconds (10 ticks at 1 second interval)
                let emit_metrics = tick_count % 10 == 0;

                // Compute liquidity using cached balances (reflects lock changes quickly)
                match Self::compute_liquidity_static(
                    &balance_strategy_task,
                    &lock_manager_task,
                    &balance_map_task,
                    emit_metrics,
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
            balance_map,
            balance_strategy,
            lock_manager,
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

    /// Get the cached balance for a specific chain and token.
    ///
    /// Returns `None` if the balance is not cached or not available.
    pub async fn get_cached_balance(
        &self,
        chain: ChainType,
        token: &TokenIdentifier,
    ) -> Option<WalletBalance> {
        let guard = self.balance_map.read().await;
        guard.get(&chain)?.get(token).cloned()
    }

    /// Get the available balance for a trading pair (cached balance minus locked amount).
    ///
    /// Returns `None` if the balance is not cached or not available.
    /// Returns the raw available balance (NOT applying balance strategy).
    pub async fn get_available_balance_for_pair(
        &self,
        from: &Currency,
        to: &Currency,
    ) -> Option<U256> {
        let balance = self.get_cached_balance(to.chain, &to.token).await?;
        
        // Get fresh lock data for this trading pair
        let locked_amount = self.lock_manager.get_locked_amount(from, to).await;
        
        // Calculate available balance (what's actually available for new quotes)
        let available_balance = balance.total_balance.saturating_sub(locked_amount);
        
        Some(available_balance)
    }

    /// Check if a quote can be filled for a trading pair.
    ///
    /// Returns `true` if the quote can be filled, `false` otherwise.
    pub async fn can_fill_quote_for_pair(
        &self,
        from: &Currency,
        to: &Currency,
        quote_amount: U256,
    ) -> bool {
        let Some(available_balance) = self.get_available_balance_for_pair(from, to).await else {
            return false;
        };
        
        self.balance_strategy.can_fill_quote(quote_amount, available_balance)
    }

    /// Compute liquidity for the two supported trading pairs: BTC->cbBTC and cbBTC->BTC.
    ///
    /// This is used by the background task and doesn't require `&self`.
    /// Uses cached balances from balance_map instead of calling wallet.balance() directly.
    /// 
    /// # Arguments
    /// * `emit_metrics` - If true, emits Prometheus metrics. Set to false to reduce metric update frequency.
    async fn compute_liquidity_static(
        balance_strategy: &QuoteBalanceStrategy,
        lock_manager: &LiquidityLockManager,
        balance_map: &Arc<RwLock<HashMap<ChainType, HashMap<TokenIdentifier, WalletBalance>>>>,
        emit_metrics: bool,
    ) -> WalletResult<Vec<TradingPairLiquidity>> {
        let mut trading_pairs = Vec::new();

        // Define the two currencies we support
        let btc = Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        };
        let cbbtc = Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::Address(CB_BTC_CONTRACT_ADDRESS.to_string()),
            decimals: 8,
        };

        // Trading pair 1: BTC -> cbBTC (user sends BTC, MM provides cbBTC)
        if let Some(available_balance) =
            Self::get_available_balance_static(&btc, &cbbtc, lock_manager, balance_map).await
        {
            let max_amount = balance_strategy.max_output_amount(available_balance);

            if emit_metrics {
                metrics::gauge!(
                    "mm_available_liquidity_sats",
                    "market" => format!("bitcoin:Native-ethereum:{}", CB_BTC_CONTRACT_ADDRESS)
                )
                .set(available_balance.to::<u64>() as f64);
            }

            trading_pairs.push(TradingPairLiquidity {
                from: btc.clone(),
                to: cbbtc.clone(),
                max_amount,
            });
        }

        // Trading pair 2: cbBTC -> BTC (user sends cbBTC, MM provides BTC)
        if let Some(available_balance) =
            Self::get_available_balance_static(&cbbtc, &btc, lock_manager, balance_map).await
        {
            let max_amount = balance_strategy.max_output_amount(available_balance);

            if emit_metrics {
                metrics::gauge!(
                    "mm_available_liquidity_sats",
                    "market" => format!("ethereum:{}-bitcoin:Native", CB_BTC_CONTRACT_ADDRESS)
                )
                .set(available_balance.to::<u64>() as f64);
            }

            trading_pairs.push(TradingPairLiquidity {
                from: cbbtc,
                to: btc,
                max_amount,
            });
        }

        Ok(trading_pairs)
    }

    /// Static helper that mimics get_available_balance_for_pair but for background task use.
    ///
    /// Gets the available balance for a trading pair (total balance - locked amount).
    async fn get_available_balance_static(
        from: &Currency,
        to: &Currency,
        lock_manager: &LiquidityLockManager,
        balance_map: &Arc<RwLock<HashMap<ChainType, HashMap<TokenIdentifier, WalletBalance>>>>,
    ) -> Option<U256> {
        // Get cached balance for the "to" token (what MM provides)
        let balance_guard = balance_map.read().await;
        let balance = balance_guard
            .get(&to.chain)
            .and_then(|balances| balances.get(&to.token))?;

        // Get locked amount for this trading pair
        let locked_amount = lock_manager.get_locked_amount(from, to).await;

        // Calculate available balance
        let available_balance = balance.total_balance.saturating_sub(locked_amount);

        Some(available_balance)
    }
}
