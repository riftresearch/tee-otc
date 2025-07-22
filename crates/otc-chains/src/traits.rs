use crate::Result;
use alloy::primitives::U256;
use async_trait::async_trait;
use otc_models::{DepositInfo, TxStatus, Wallet};
use std::time::Duration;

#[async_trait]
pub trait ChainOperations: Send + Sync {
    /// Create a new wallet, returning the wallet and the salt used
    async fn create_wallet(&self) -> Result<(Wallet, [u8; 32])>;
    
    /// Derive a wallet deterministically from a master key and salt
    fn derive_wallet(&self, master_key: &[u8], salt: &[u8; 32]) -> Result<Wallet>;
    
    /// Get the balance of an address
    async fn get_balance(&self, address: &str) -> Result<U256>;
    
    /// Get transaction status
    async fn get_tx_status(&self, txid: &str) -> Result<TxStatus>;
    
    /// Check for deposits to an address
    async fn check_deposit(
        &self,
        address: &str,
        expected_amount: U256,
        min_confirmations: u32,
    ) -> Result<Option<DepositInfo>>;
    
    /// Send funds from a wallet
    async fn send_funds(
        &self,
        private_key: &str,
        to_address: &str,
        amount: U256,
    ) -> Result<String>; // Returns tx hash
    
    /// Validate an address format
    fn validate_address(&self, address: &str) -> bool;
    
    /// Get minimum recommended confirmations
    fn minimum_confirmations(&self) -> u32;
    
    /// Get estimated block time
    fn estimated_block_time(&self) -> Duration;
}