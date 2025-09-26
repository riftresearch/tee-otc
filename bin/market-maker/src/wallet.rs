use alloy::primitives::{Address, U256};
use alloy::providers::PendingTransactionError;
use async_trait::async_trait;
use chrono::Duration;
use dashmap::DashMap;
use otc_chains::traits::MarketMakerPaymentValidation;
use otc_models::TokenIdentifier;
use otc_models::{ChainType, Currency, Lot};
use snafu::{Location, Snafu};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::oneshot;

use crate::bitcoin_wallet::BitcoinWalletError;

#[derive(Debug, Snafu)]
pub enum WalletError {
    #[snafu(display("Crafting receive with authorization execution failed: {}", source))]
    ReceiveAuthorizationFailed {
        source: blockchain_utils::ReceiveAuthorizationError,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Deposit key storage error: {}", source))]
    DepositKeyStorageError {
        source: crate::deposit_key_storage::Error,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Esplora client error: {}", source))]
    EsploraClientError {
        source: bdk_esplora::esplora_client::Error,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Failed to sign hash: {}", source))]
    SignatureFailed { source: alloy::signers::Error },

    #[snafu(display("Invalid sender: expected {}, actual {}", expected, actual))]
    InvalidSender { expected: Address, actual: Address },

    #[snafu(display("Wallet not registered for chain type: {:?}", chain_type))]
    WalletNotRegistered { chain_type: ChainType },

    #[snafu(display("Insufficient balance: required {}, available {}", required, available))]
    InsufficientBalance { required: String, available: String },

    #[snafu(display("Transaction creation failed: {}", reason))]
    TransactionCreationFailed { reason: String },

    #[snafu(display("Balance check failed: {}", source))]
    BalanceCheckFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unsupported token: {:?}", token))]
    UnsupportedToken {
        token: TokenIdentifier,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Failed to parse address: {}", context))]
    ParseAddressFailed { context: String },

    #[snafu(display("Failed to get erc20 balance: {}", context))]
    GetErc20BalanceFailed { context: String },

    #[snafu(display("Channel closed"))]
    ChannelClosed,

    #[snafu(display("Failed to enqueue transaction request"))]
    EnqueueFailed,

    #[snafu(display("Failed to send transaction execution result"))]
    SendResultFailed,

    #[snafu(display("Unknown simulation error: {}", message))]
    UnknownSimulationError { message: String },

    #[snafu(display("Failed to get block number: {}", source))]
    GetBlockNumber {
        source: alloy::transports::RpcError<alloy::transports::TransportErrorKind>,
    },

    #[snafu(display("Failed to receive transaction result: {}", source))]
    ReceiveResult { source: oneshot::error::RecvError },

    #[snafu(display("Failed to get code: {}", source))]
    RpcCallError {
        source: alloy::transports::RpcError<alloy::transports::TransportErrorKind>,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Failed to send transaction: {}", source))]
    PendingTransactionError {
        source: PendingTransactionError,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Bitcoin wallet client error: {}", source))]
    BitcoinWalletClient {
        source: BitcoinWalletError,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Invalid descriptor: {}", reason))]
    InvalidDescriptor {
        reason: String,
        #[snafu(implicit)]
        loc: Location,
    },
}

pub type WalletResult<T, E = WalletError> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct WalletBalance {
    // total balance of the wallet, native_balance + deposit_key_balance
    pub total_balance: U256,
    // balance of the primary wallet
    pub native_balance: U256,
    // sum of balances from all deposit keys
    pub deposit_key_balance: U256,
}

#[async_trait]
pub trait Wallet: Send + Sync {
    /// Create a transaction for the given currency to the specified address
    /// Optionally handle market maker payment validation
    async fn create_payment(
        &self,
        lot: &Lot,
        recipient: &str,
        mm_payment_validation: Option<MarketMakerPaymentValidation>,
    ) -> WalletResult<String>;

    /// Waits until the given transaction reaches the specified number of confirmations.
    ///
    /// Behavior:
    /// - If the transaction does not have at least one confirmation by the time the next block
    ///   is mined, this method will rebroadcast it and continue doing so until it is confirmed.
    /// - This function only works for transactions originally broadcast by this wallet;
    ///   externally-created transactions are not tracked.
    ///
    /// Guarantees:
    /// - Returns once the transaction has the requested number of confirmations.
    /// - May rebroadcast the transaction multiple times until confirmation is observed.
    ///
    /// Notes:
    /// - Does not guarantee confirmation if the transaction is permanently invalid (e.g., double spend).
    /// - Requires an active connection to a node that tracks the mempool and blockchain state.
    async fn guarantee_confirmations(&self, tx_hash: &str, confirmations: u64) -> WalletResult<()>;

    /// Return the available balance for the given token
    async fn balance(&self, token: &TokenIdentifier) -> WalletResult<WalletBalance>;

    fn receive_address(&self, token: &TokenIdentifier) -> String;

    /// Get the chain type of the wallet
    fn chain_type(&self) -> ChainType;
}

#[derive(Clone)]
pub struct WalletManager {
    wallets: HashMap<ChainType, Arc<dyn Wallet>>,
}

impl WalletManager {
    #[must_use]
    pub fn new() -> Self {
        Self {
            wallets: HashMap::new(),
        }
    }

    /// Register a wallet implementation for a specific chain type
    pub fn register(&mut self, chain_type: ChainType, wallet: Arc<dyn Wallet>) {
        self.wallets.insert(chain_type, wallet);
    }

    /// Remove a wallet implementation for a specific chain type
    pub fn remove(&mut self, chain_type: ChainType) -> Option<Arc<dyn Wallet>> {
        self.wallets.remove(&chain_type)
    }

    /// Get a wallet implementation for a specific chain type
    pub fn get(&self, chain_type: ChainType) -> Option<Arc<dyn Wallet>> {
        self.wallets.get(&chain_type).cloned()
    }

    /// Check if a wallet is registered for a specific chain type
    #[must_use]
    pub fn is_registered(&self, chain_type: ChainType) -> bool {
        self.wallets.contains_key(&chain_type)
    }

    /// Get all registered chain types
    #[must_use]
    pub fn registered_chains(&self) -> Vec<ChainType> {
        self.wallets.keys().cloned().collect()
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

    struct MockWallet {}

    #[async_trait]
    impl Wallet for MockWallet {
        async fn create_payment(
            &self,
            _lot: &Lot,
            _to_address: &str,
            _mm_payment_validation: Option<MarketMakerPaymentValidation>,
        ) -> WalletResult<String> {
            Ok("mock_txid_123".to_string())
        }

        fn chain_type(&self) -> ChainType {
            ChainType::Bitcoin
        }

        async fn guarantee_confirmations(
            &self,
            _tx_hash: &str,
            _confirmations: u64,
        ) -> WalletResult<()> {
            Ok(())
        }

        fn receive_address(&self, _token: &TokenIdentifier) -> String {
            "mock_address_123".to_string()
        }

        async fn balance(&self, _token: &TokenIdentifier) -> WalletResult<WalletBalance> {
            Ok(WalletBalance {
                total_balance: U256::from(1000000000000000000u64),
                native_balance: U256::from(1000000000000000000u64),
                deposit_key_balance: U256::from(0),
            })
        }
    }

    #[tokio::test]
    async fn test_wallet_registration() {
        let mut manager = WalletManager::new();
        let mock_wallet = Arc::new(MockWallet {});

        // Register wallet
        manager.register(ChainType::Bitcoin, mock_wallet.clone());
        assert!(manager.is_registered(ChainType::Bitcoin));
        assert!(!manager.is_registered(ChainType::Ethereum));

        // Get wallet
        let wallet = manager.get(ChainType::Bitcoin).unwrap();
        let lot = Lot {
            currency: Currency {
                chain: ChainType::Bitcoin,
                token: TokenIdentifier::Native,
                decimals: 8,
            },
            amount: U256::from(100000),
        };

        // Test wallet methods
        let bal = wallet
            .balance(&lot.currency.token)
            .await
            .unwrap()
            .total_balance;
        assert!(bal > U256::from(0));

        let txid = wallet.create_payment(&lot, "bc1q...", None).await.unwrap();
        assert_eq!(txid, "mock_txid_123");

        // Remove wallet
        let removed = manager.remove(ChainType::Bitcoin);
        assert!(removed.is_some());
        assert!(!manager.is_registered(ChainType::Bitcoin));
    }

    #[test]
    fn test_registered_chains() {
        let mut manager = WalletManager::new();
        let mock_wallet = Arc::new(MockWallet {});

        manager.register(ChainType::Bitcoin, mock_wallet.clone());
        manager.register(ChainType::Ethereum, mock_wallet);

        let chains = manager.registered_chains();
        assert_eq!(chains.len(), 2);
        assert!(chains.contains(&ChainType::Bitcoin));
        assert!(chains.contains(&ChainType::Ethereum));
    }
}
