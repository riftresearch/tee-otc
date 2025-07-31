use crate::Result;
use async_trait::async_trait;
use otc_models::{Currency, TransferInfo, TxStatus, Wallet};
use std::time::Duration;


// implementors of this trait should be stateless
#[async_trait]
pub trait ChainOperations: Send + Sync {
    /// Create a new wallet, returning the wallet and the salt used
    fn create_wallet(&self) -> Result<(Wallet, [u8; 32])>;
    
    /// Derive a wallet deterministically from a master key and salt
    fn derive_wallet(&self, master_key: &[u8], salt: &[u8; 32]) -> Result<Wallet>;
    
    
    /// Check for transfers to an address
    async fn search_for_transfer(
        &self,
        to_address: &str,
        currency: &Currency,
        // Some callers may require a nonce to be embedded in the transaction
        embedded_nonce: Option<[u8; 16]>,
        // Before this block, the transfer was not possible/irrelevant - can be used to limit the search range
        from_block_height: Option<u64>
    )
     -> Result<Option<TransferInfo>>;

    /// Get the status of a transaction
    async fn get_tx_status(&self, tx_hash: &str) -> Result<TxStatus>;
    
    /// Send funds from a wallet
    /// TODO: Reason about how refunds will work and create a type around this
    /* 
    async fn sign_payment(
        &self,
        private_key: &str,
        to_address: &str,
        amount: U256,
    ) -> Result<String>; // Returns tx hash
     */
    
    /// Validate an address format
    fn validate_address(&self, address: &str) -> bool;
    
    /// Get minimum recommended confirmations
    fn minimum_block_confirmations(&self) -> u32;
    
    /// Get rough block time as an estimation of confirmation time
    fn estimated_block_time(&self) -> Duration;
}