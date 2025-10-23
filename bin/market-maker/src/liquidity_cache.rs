use crate::balance_strat::QuoteBalanceStrategy;
use crate::wallet::{WalletManager, WalletResult};
use otc_models::{constants::SUPPORTED_TOKENS_BY_CHAIN, ChainType, Currency, TokenIdentifier};
use otc_protocols::rfq::TradingPairLiquidity;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{info, warn};

const CACHE_TTL: Duration = Duration::from_secs(60);

struct CachedLiquidity {
    trading_pairs: Vec<TradingPairLiquidity>,
    computed_at: Instant,
}

impl CachedLiquidity {
    fn is_valid(&self) -> bool {
        self.computed_at.elapsed() < CACHE_TTL
    }
}

pub struct LiquidityCache {
    cached_data: Arc<RwLock<Option<CachedLiquidity>>>,
    wallet_manager: Arc<WalletManager>,
    balance_strategy: Arc<QuoteBalanceStrategy>,
}

impl LiquidityCache {
    #[must_use]
    pub fn new(wallet_manager: Arc<WalletManager>, balance_strategy: Arc<QuoteBalanceStrategy>) -> Self {
        Self {
            cached_data: Arc::new(RwLock::new(None)),
            wallet_manager,
            balance_strategy,
        }
    }

    /// Get liquidity data, using cache if valid, otherwise recomputing
    pub async fn get_liquidity(&self) -> Vec<TradingPairLiquidity> {
        // Check if cache is valid
        {
            let cache = self.cached_data.read().await;
            if let Some(cached) = cache.as_ref() {
                if cached.is_valid() {
                    info!("Returning cached liquidity data");
                    return cached.trading_pairs.clone();
                }
            }
        }

        // Cache is invalid or doesn't exist, recompute
        info!("Computing fresh liquidity data");
        match self.compute_liquidity().await {
            Ok(trading_pairs) => {
                let mut cache = self.cached_data.write().await;
                *cache = Some(CachedLiquidity {
                    trading_pairs: trading_pairs.clone(),
                    computed_at: Instant::now(),
                });
                trading_pairs
            }
            Err(e) => {
                warn!("Failed to compute liquidity: {}", e);
                // Return empty vec on error
                Vec::new()
            }
        }
    }

    /// Compute liquidity for all supported trading pairs
    async fn compute_liquidity(&self) -> WalletResult<Vec<TradingPairLiquidity>> {
        let mut trading_pairs = Vec::new();

        // Get all registered chains
        let registered_chains = self.wallet_manager.registered_chains();

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
                                if let Some(to_wallet) = self.wallet_manager.get(*to_chain) {
                                    // Get balance for the TO token (what MM can provide)
                                    match to_wallet.balance(to_token).await {
                                        Ok(to_balance) => {
                                            // Compute max amount based on TO token's balance.
                                            // This represents the maximum OUTPUT the MM can provide.
                                            let max_output_capacity = self.balance_strategy.max_output_amount(to_balance.total_balance);
                                            
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
                                            let from_decimals = get_token_decimals(*from_chain, from_token);

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

