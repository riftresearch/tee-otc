use async_trait::async_trait;
use dashmap::DashMap;
use otc_models::{ChainType, Currency};
use snafu::Snafu;
use std::sync::Arc;

#[derive(Debug, Snafu)]
pub enum WalletError {
    #[snafu(display("Wallet not registered for chain type: {:?}", chain_type))]
    WalletNotRegistered { chain_type: ChainType },

    #[snafu(display("Insufficient balance: required {}, available {}", required, available))]
    InsufficientBalance { required: String, available: String },

    #[snafu(display("Transaction creation failed: {}", reason))]
    TransactionCreationFailed { reason: String },

    #[snafu(display("Balance check failed: {}", reason))]
    BalanceCheckFailed { reason: String },

    #[snafu(display("Unsupported currency: {:?}", currency))]
    UnsupportedCurrency { currency: Currency },

    #[snafu(display("Failed to parse address: {}", context))]
    ParseAddressFailed { context: String },

    #[snafu(display("Failed to get erc20 balance: {}", context))]
    GetErc20BalanceFailed { context: String },
}

pub type Result<T, E = WalletError> = std::result::Result<T, E>;

#[async_trait]
pub trait Wallet: Send + Sync {
    /// Create a transaction for the given currency to the specified address
    /// Optional nonce must be embedded in the transaction somehow
    async fn create_transaction(
        &self,
        currency: &Currency,
        to_address: &str,
        nonce: Option<[u8; 16]>,
    ) -> Result<String>;

    /// Check if the wallet can fill the specified amount of currency
    async fn can_fill(&self, currency: &Currency) -> Result<bool>;
}

#[derive(Clone)]
pub struct WalletManager {
    wallets: Arc<DashMap<ChainType, Arc<dyn Wallet>>>,
}

impl WalletManager {
    #[must_use]
    pub fn new() -> Self {
        Self {
            wallets: Arc::new(DashMap::new()),
        }
    }

    /// Register a wallet implementation for a specific chain type
    pub fn register(&self, chain_type: ChainType, wallet: Arc<dyn Wallet>) {
        self.wallets.insert(chain_type, wallet);
    }

    /// Remove a wallet implementation for a specific chain type
    pub fn remove(&self, chain_type: ChainType) -> Option<Arc<dyn Wallet>> {
        self.wallets.remove(&chain_type).map(|(_, wallet)| wallet)
    }

    /// Get a wallet implementation for a specific chain type
    pub fn get(&self, chain_type: ChainType) -> Option<Arc<dyn Wallet>> {
        self.wallets.get(&chain_type).map(|entry| entry.clone())
    }

    /// Check if a wallet is registered for a specific chain type
    #[must_use]
    pub fn is_registered(&self, chain_type: ChainType) -> bool {
        self.wallets.contains_key(&chain_type)
    }

    /// Get all registered chain types
    #[must_use]
    pub fn registered_chains(&self) -> Vec<ChainType> {
        self.wallets.iter().map(|entry| *entry.key()).collect()
    }
}

impl Default for WalletManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::U256;
    use otc_models::TokenIdentifier;

    struct MockWallet {
        can_fill_response: bool,
    }

    #[async_trait]
    impl Wallet for MockWallet {
        async fn create_transaction(
            &self,
            _currency: &Currency,
            _to_address: &str,
            _nonce: Option<[u8; 16]>,
        ) -> Result<String> {
            Ok("mock_txid_123".to_string())
        }

        async fn can_fill(&self, _currency: &Currency) -> Result<bool> {
            Ok(self.can_fill_response)
        }
    }

    #[tokio::test]
    async fn test_wallet_registration() {
        let manager = WalletManager::new();
        let mock_wallet = Arc::new(MockWallet {
            can_fill_response: true,
        });

        // Register wallet
        manager.register(ChainType::Bitcoin, mock_wallet.clone());
        assert!(manager.is_registered(ChainType::Bitcoin));
        assert!(!manager.is_registered(ChainType::Ethereum));

        // Get wallet
        let wallet = manager.get(ChainType::Bitcoin).unwrap();
        let currency = Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            amount: U256::from(100000),
            decimals: 8,
        };

        // Test wallet methods
        let can_fill = wallet.can_fill(&currency).await.unwrap();
        assert!(can_fill);

        let txid = wallet
            .create_transaction(&currency, "bc1q...", None)
            .await
            .unwrap();
        assert_eq!(txid, "mock_txid_123");

        // Remove wallet
        let removed = manager.remove(ChainType::Bitcoin);
        assert!(removed.is_some());
        assert!(!manager.is_registered(ChainType::Bitcoin));
    }

    #[test]
    fn test_registered_chains() {
        let manager = WalletManager::new();
        let mock_wallet = Arc::new(MockWallet {
            can_fill_response: true,
        });

        manager.register(ChainType::Bitcoin, mock_wallet.clone());
        manager.register(ChainType::Ethereum, mock_wallet);

        let chains = manager.registered_chains();
        assert_eq!(chains.len(), 2);
        assert!(chains.contains(&ChainType::Bitcoin));
        assert!(chains.contains(&ChainType::Ethereum));
    }
}
