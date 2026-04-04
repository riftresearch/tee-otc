use crate::wallet::{WalletManager, WalletResult};
use alloy::primitives::U256;
use otc_models::{constants::CB_BTC_CONTRACT_ADDRESS, ChainType, Currency, TokenIdentifier};
use otc_protocols::rfq::TradingPairLiquidity;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::warn;

const CAPACITY_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// Normalize a TokenIdentifier to lowercase for case-insensitive lookup.
#[inline]
fn normalize_token(token: &TokenIdentifier) -> TokenIdentifier {
    token.normalize()
}

pub struct LiquidityCache {
    cached_data: Arc<RwLock<Vec<TradingPairLiquidity>>>,
}

impl LiquidityCache {
    /// Create an empty boot-time route-capacity cache.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cached_data: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Populate the boot-time route-capacity snapshot and keep it cached for the process lifetime.
    ///
    /// Capacity is computed from total wallet balances at startup and does not shrink as swaps
    /// arrive. If balances are not readable yet, initialization retries until successful.
    pub async fn initialize(
        &self,
        wallet_manager: Arc<WalletManager>,
        evm_chain: ChainType,
    ) -> crate::Result<()> {
        loop {
            match Self::compute_boot_capacity_snapshot(&wallet_manager, evm_chain).await {
                Ok(trading_pairs) => {
                    Self::emit_capacity_metrics(&trading_pairs);
                    let mut cache = self.cached_data.write().await;
                    *cache = trading_pairs;
                    return Ok(());
                }
                Err(error) => {
                    warn!(
                        "Failed to compute boot-time route capacity snapshot: {}",
                        error
                    );
                    tokio::time::sleep(CAPACITY_RETRY_INTERVAL).await;
                }
            }
        }
    }

    pub async fn has_snapshot(&self) -> bool {
        !self.cached_data.read().await.is_empty()
    }

    /// Get the cached route-capacity data.
    pub async fn get_liquidity(&self) -> Vec<TradingPairLiquidity> {
        self.cached_data.read().await.clone()
    }

    /// Get the maximum payout amount for a trading pair from the boot-time snapshot.
    pub async fn get_max_output_for_pair(&self, from: &Currency, to: &Currency) -> Option<U256> {
        let cache = self.cached_data.read().await;
        cache
            .iter()
            .find(|pair| {
                pair.from.chain == from.chain
                    && normalize_token(&pair.from.token) == normalize_token(&from.token)
                    && pair.to.chain == to.chain
                    && normalize_token(&pair.to.token) == normalize_token(&to.token)
            })
            .map(|pair| pair.max_amount)
    }

    async fn compute_boot_capacity_snapshot(
        wallet_manager: &WalletManager,
        evm_chain: ChainType,
    ) -> WalletResult<Vec<TradingPairLiquidity>> {
        let btc = Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        };
        let cbbtc = Currency {
            chain: evm_chain,
            token: TokenIdentifier::address(CB_BTC_CONTRACT_ADDRESS.to_string()),
            decimals: 8,
        };

        let bitcoin_wallet = wallet_manager
            .get(ChainType::Bitcoin)
            .expect("bitcoin wallet must be registered");
        let evm_wallet = wallet_manager
            .get(evm_chain)
            .expect("configured evm wallet must be registered");

        let btc_total = bitcoin_wallet
            .balance(&TokenIdentifier::Native)
            .await?
            .total_balance;
        let cbbtc_total = evm_wallet.balance(&cbbtc.token).await?.total_balance;

        Ok(vec![
            TradingPairLiquidity {
                from: btc.clone(),
                to: cbbtc.clone(),
                max_amount: cbbtc_total,
            },
            TradingPairLiquidity {
                from: cbbtc,
                to: btc,
                max_amount: btc_total,
            },
        ])
    }

    fn emit_capacity_metrics(trading_pairs: &[TradingPairLiquidity]) {
        for pair in trading_pairs {
            metrics::gauge!(
                "mm_theoretical_route_capacity_sats",
                "from_chain" => pair.from.chain.to_db_string().to_string(),
                "to_chain" => pair.to.chain.to_db_string().to_string(),
            )
            .set(pair.max_amount.to::<u64>() as f64);
        }
    }
}
