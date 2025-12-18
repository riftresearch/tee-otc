use alloy::primitives::U256;
use chrono::{DateTime, Duration, Utc};
use otc_models::Currency;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tracing::info;
use uuid::Uuid;

/// Default TTL for liquidity locks (1 hours)
const DEFAULT_LOCK_TTL: Duration = Duration::hours(1);

/// Cleanup interval for expired locks (1 minute)
const CLEANUP_INTERVAL: StdDuration = StdDuration::from_secs(60);

/// Represents locked liquidity for a specific swap
#[derive(Debug, Clone)]
pub struct LockedLiquidity {
    pub from: Currency,
    pub to: Currency,
    pub amount: U256,
    pub created_at: DateTime<Utc>,
}

/// Normalized currency pair for use as a cache key
#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedPair {
    from: Currency,
    to: Currency,
}

impl Hash for NormalizedPair {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.from.chain.hash(state);
        self.from.token.hash(state);
        self.from.decimals.hash(state);
        self.to.chain.hash(state);
        self.to.token.hash(state);
        self.to.decimals.hash(state);
    }
}

/// Thread-safe manager for tracking locked liquidity per swap
pub struct LiquidityLockManager {
    locks: Arc<RwLock<HashMap<Uuid, LockedLiquidity>>>,
    cached_sums: Arc<RwLock<HashMap<NormalizedPair, U256>>>,
    ttl: Duration,
}

impl LiquidityLockManager {
    /// Create a new LiquidityLockManager with default TTL and spawn a background cleanup task
    ///
    /// The cleanup task runs every 10 minutes to remove expired locks from memory.
    pub fn new(join_set: &mut JoinSet<crate::Result<()>>) -> Self {
        Self::with_ttl_and_cleanup(DEFAULT_LOCK_TTL, join_set)
    }

    /// Create a new LiquidityLockManager with custom TTL and spawn a background cleanup task
    ///
    /// The cleanup task runs every 10 minutes to remove expired locks from memory.
    pub fn with_ttl_and_cleanup(ttl: Duration, join_set: &mut JoinSet<crate::Result<()>>) -> Self {
        let locks = Arc::new(RwLock::new(HashMap::new()));
        let cached_sums = Arc::new(RwLock::new(HashMap::new()));
        let ttl_clone = ttl;

        // Create a manager for the background task that shares the same state
        let manager_for_task = Self {
            locks: locks.clone(),
            cached_sums: cached_sums.clone(),
            ttl: ttl_clone,
        };

        join_set.spawn(async move {
            let mut interval = tokio::time::interval(CLEANUP_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                match manager_for_task.cleanup_expired().await {
                    0 => {
                        // No locks removed, skip logging
                    }
                    count => {
                        info!("Cleaned up {} expired liquidity lock(s)", count);
                    }
                }
            }
        });

        // Return the manager that shares the same state
        Self {
            locks,
            cached_sums,
            ttl,
        }
    }

    /// Create a new LiquidityLockManager with custom TTL (for testing)
    ///
    /// This constructor does not spawn a background cleanup task.
    /// Use `new()` or `with_ttl_and_cleanup()` in production code.
    #[allow(dead_code)]
    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            locks: Arc::new(RwLock::new(HashMap::new())),
            cached_sums: Arc::new(RwLock::new(HashMap::new())),
            ttl,
        }
    }


    /// Lock liquidity for a swap
    ///
    /// If a lock already exists for this swap_id, it will be replaced.
    pub async fn lock(&self, swap_id: Uuid, locked: LockedLiquidity) {
        let now = utc::now();
        let mut locks = self.locks.write().await;
        
        // If replacing an existing lock, subtract the old amount from cache
        if let Some(old_lock) = locks.get(&swap_id) {
            if now.signed_duration_since(old_lock.created_at) <= self.ttl {
                // Only subtract if the old lock wasn't expired
                let pair_key = Self::normalize_pair(&old_lock.from, &old_lock.to);
                let mut sums = self.cached_sums.write().await;
                if let Some(current_sum) = sums.get_mut(&pair_key) {
                    *current_sum = current_sum.saturating_sub(old_lock.amount);
                    if *current_sum == U256::ZERO {
                        sums.remove(&pair_key);
                    }
                }
            }
        }
        
        // Extract data needed for cache update before moving locked
        let should_cache = now.signed_duration_since(locked.created_at) <= self.ttl;
        let pair_key = if should_cache {
            Some(Self::normalize_pair(&locked.from, &locked.to))
        } else {
            None
        };
        let amount = locked.amount;
        
        locks.insert(swap_id, locked);
        
        // Release the locks write lock before updating cache to avoid deadlock potential
        drop(locks);
        
        // Add the new lock amount to cache if not expired
        if let Some(key) = pair_key {
            let mut sums = self.cached_sums.write().await;
            let current = sums.get(&key).copied().unwrap_or(U256::ZERO);
            sums.insert(key, current.saturating_add(amount));
        }
    }

    /// Unlock liquidity for a swap
    ///
    /// Returns the locked liquidity if it existed, None otherwise.
    pub async fn unlock(&self, swap_id: Uuid) -> Option<LockedLiquidity> {
        let now = utc::now();
        let mut locks = self.locks.write().await;
        
        if let Some(removed_lock) = locks.remove(&swap_id) {
            // Update cache if the lock wasn't expired
            if now.signed_duration_since(removed_lock.created_at) <= self.ttl {
                let pair_key = Self::normalize_pair(&removed_lock.from, &removed_lock.to);
                let mut sums = self.cached_sums.write().await;
                if let Some(current_sum) = sums.get_mut(&pair_key) {
                    *current_sum = current_sum.saturating_sub(removed_lock.amount);
                    if *current_sum == U256::ZERO {
                        sums.remove(&pair_key);
                    }
                }
            }
            Some(removed_lock)
        } else {
            None
        }
    }

    /// Get total locked amount for a trading pair, excluding expired locks
    ///
    /// Returns the cached sum for the trading pair, or zero if no locks exist.
    pub async fn get_locked_amount(&self, from: &Currency, to: &Currency) -> U256 {
        let pair_key = Self::normalize_pair(from, to);
        let sums = self.cached_sums.read().await;
        sums.get(&pair_key).copied().unwrap_or(U256::ZERO)
    }

    /// Check if a swap has locked liquidity
    #[allow(dead_code)]
    pub async fn has_lock(&self, swap_id: &Uuid) -> bool {
        let locks = self.locks.read().await;
        locks.contains_key(swap_id)
    }

    /// Clean up expired locks from the map
    ///
    /// Returns the number of locks removed.
    pub async fn cleanup_expired(&self) -> usize {
        let now = utc::now();
        let mut locks = self.locks.write().await;
        let initial_len = locks.len();
        
        // Collect expired locks with their amounts for cache updates
        let mut expired_pairs: Vec<(NormalizedPair, U256)> = Vec::new();
        
        locks.retain(|_, lock| {
            let is_expired = now.signed_duration_since(lock.created_at) > self.ttl;
            if is_expired {
                let pair_key = Self::normalize_pair(&lock.from, &lock.to);
                expired_pairs.push((pair_key, lock.amount));
            }
            !is_expired
        });
        
        drop(locks);
        
        // Update cache by subtracting expired lock amounts
        if !expired_pairs.is_empty() {
            let mut sums = self.cached_sums.write().await;
            for (pair_key, amount) in expired_pairs {
                if let Some(current_sum) = sums.get_mut(&pair_key) {
                    *current_sum = current_sum.saturating_sub(amount);
                    if *current_sum == U256::ZERO {
                        sums.remove(&pair_key);
                    }
                }
            }
        }
        
        initial_len - self.locks.read().await.len()
    }

    /// Normalize a currency pair for use as a cache key
    ///
    /// Normalizes Ethereum addresses to lowercase to handle different case representations.
    fn normalize_pair(from: &Currency, to: &Currency) -> NormalizedPair {
        NormalizedPair {
            from: Self::normalize_currency(from),
            to: Self::normalize_currency(to),
        }
    }

    /// Normalize a currency for use as a cache key
    ///
    /// Normalizes Ethereum addresses to lowercase.
    fn normalize_currency(currency: &Currency) -> Currency {
        let normalized_token = match &currency.token {
            otc_models::TokenIdentifier::Native => otc_models::TokenIdentifier::Native,
            otc_models::TokenIdentifier::Address(addr) => {
                otc_models::TokenIdentifier::Address(addr.to_lowercase())
            }
        };
        
        Currency {
            chain: currency.chain,
            token: normalized_token,
            decimals: currency.decimals,
        }
    }

}

// Note: Default impl removed because new() now requires join_set parameter
// Use new(join_set) or with_ttl() for testing instead

#[cfg(test)]
mod tests {
    use super::*;
    use otc_models::{ChainType, TokenIdentifier};

    fn create_test_currency(chain: ChainType, token: TokenIdentifier) -> Currency {
        Currency {
            chain,
            token,
            decimals: 8,
        }
    }

    #[tokio::test]
    async fn test_lock_and_unlock() {
        let manager = LiquidityLockManager::with_ttl(DEFAULT_LOCK_TTL);
        let swap_id = Uuid::now_v7();

        let from = create_test_currency(ChainType::Bitcoin, TokenIdentifier::Native);
        let to = create_test_currency(ChainType::Ethereum, TokenIdentifier::Native);
        let amount = U256::from(1000u64);

        let locked = LockedLiquidity {
            from: from.clone(),
            to: to.clone(),
            amount,
            created_at: utc::now(),
        };

        // Lock
        manager.lock(swap_id, locked).await;
        assert!(manager.has_lock(&swap_id).await);

        // Get locked amount
        let locked_amount = manager.get_locked_amount(&from, &to).await;
        assert_eq!(locked_amount, amount);

        // Unlock
        let removed = manager.unlock(swap_id).await;
        assert!(removed.is_some());
        assert!(!manager.has_lock(&swap_id).await);

        // Verify locked amount is now zero
        let locked_amount = manager.get_locked_amount(&from, &to).await;
        assert_eq!(locked_amount, U256::ZERO);
    }

    #[tokio::test]
    async fn test_multiple_locks_same_pair() {
        let manager = LiquidityLockManager::with_ttl(DEFAULT_LOCK_TTL);
        let from = create_test_currency(ChainType::Bitcoin, TokenIdentifier::Native);
        let to = create_test_currency(ChainType::Ethereum, TokenIdentifier::Native);

        // Lock multiple swaps
        for i in 0..5 {
            let swap_id = Uuid::now_v7();
            let locked = LockedLiquidity {
                from: from.clone(),
                to: to.clone(),
                amount: U256::from(100u64 * (i + 1)),
                created_at: utc::now(),
            };
            manager.lock(swap_id, locked).await;
        }

        // Total should be sum: 100 + 200 + 300 + 400 + 500 = 1500
        let total = manager.get_locked_amount(&from, &to).await;
        assert_eq!(total, U256::from(1500u64));
    }

    #[tokio::test]
    async fn test_different_trading_pairs() {
        let manager = LiquidityLockManager::with_ttl(DEFAULT_LOCK_TTL);
        let btc_to_eth = LockedLiquidity {
            from: create_test_currency(ChainType::Bitcoin, TokenIdentifier::Native),
            to: create_test_currency(ChainType::Ethereum, TokenIdentifier::Native),
            amount: U256::from(1000u64),
            created_at: utc::now(),
        };

        let eth_to_btc = LockedLiquidity {
            from: create_test_currency(ChainType::Ethereum, TokenIdentifier::Native),
            to: create_test_currency(ChainType::Bitcoin, TokenIdentifier::Native),
            amount: U256::from(2000u64),
            created_at: utc::now(),
        };

        manager.lock(Uuid::now_v7(), btc_to_eth.clone()).await;
        manager.lock(Uuid::now_v7(), eth_to_btc.clone()).await;

        // Query should only return matching pair
        let btc_eth_locked = manager.get_locked_amount(&btc_to_eth.from, &btc_to_eth.to).await;
        assert_eq!(btc_eth_locked, U256::from(1000u64));

        let eth_btc_locked = manager.get_locked_amount(&eth_to_btc.from, &eth_to_btc.to).await;
        assert_eq!(eth_btc_locked, U256::from(2000u64));
    }

    #[tokio::test]
    async fn test_ttl_expiration() {
        let manager = LiquidityLockManager::with_ttl(Duration::minutes(1));
        let swap_id = Uuid::now_v7();
        let from = create_test_currency(ChainType::Bitcoin, TokenIdentifier::Native);
        let to = create_test_currency(ChainType::Ethereum, TokenIdentifier::Native);

        // Create lock with old timestamp
        let old_lock = LockedLiquidity {
            from: from.clone(),
            to: to.clone(),
            amount: U256::from(1000u64),
            created_at: utc::now() - Duration::hours(2), // 2 hours ago
        };

        manager.lock(swap_id, old_lock).await;

        // Old lock should be filtered out
        let locked_amount = manager.get_locked_amount(&from, &to).await;
        assert_eq!(locked_amount, U256::ZERO);

        // Cleanup should remove it
        let removed = manager.cleanup_expired().await;
        assert_eq!(removed, 1);
        assert!(!manager.has_lock(&swap_id).await);
    }

    #[tokio::test]
    async fn test_case_insensitive_address_matching() {
        let manager = LiquidityLockManager::with_ttl(DEFAULT_LOCK_TTL);
        let swap_id = Uuid::now_v7();

        // Lock with checksummed address (mixed case)
        let from = Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        };
        let to_checksummed = Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::Address("0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf".to_string()),
            decimals: 8,
        };
        let amount = U256::from(1000u64);

        let locked = LockedLiquidity {
            from: from.clone(),
            to: to_checksummed.clone(),
            amount,
            created_at: utc::now(),
        };

        manager.lock(swap_id, locked).await;

        // Query with lowercase address - should match
        let to_lowercase = Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::Address("0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf".to_string()),
            decimals: 8,
        };

        let locked_amount = manager.get_locked_amount(&from, &to_lowercase).await;
        assert_eq!(locked_amount, amount, "Should match addresses regardless of case");

        // Query with uppercase address - should also match
        let to_uppercase = Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::Address("0xCBB7C0000AB88B473B1F5AFD9EF808440EED33BF".to_string()),
            decimals: 8,
        };

        let locked_amount = manager.get_locked_amount(&from, &to_uppercase).await;
        assert_eq!(locked_amount, amount, "Should match addresses regardless of case");
    }
}

