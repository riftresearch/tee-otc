use crate::Result;
use alloy::primitives::U256;
use async_trait::async_trait;
use otc_models::{Lot, TokenIdentifier, TransferInfo, TxStatus, Wallet};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct MarketMakerPaymentValidation {
    pub fee_amount: U256,
    pub embedded_nonce: [u8; 16],
}

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
        recipient_address: &str,
        lot: &Lot,
        // Optional validation of the market maker payment, if that's required for this transfer
        mm_payment_validation: Option<MarketMakerPaymentValidation>,
        // Before this block, the transfer was not possible/irrelevant - can be used to limit the search range
        from_block_height: Option<u64>,
    ) -> Result<Option<TransferInfo>>;

    /// Get the status of a transaction
    async fn get_tx_status(&self, tx_hash: &str) -> Result<TxStatus>;

    /// Create a signed transaction to send all funds from a wallet to an address
    async fn dump_to_address(
        &self,
        token: &TokenIdentifier,
        private_key: &str,
        recipient_address: &str,
        fee: U256,
    ) -> Result<String>; // Returns signed transaction data as a hex string

    /// Validate an address format
    fn validate_address(&self, address: &str) -> bool;

    /// Get minimum recommended confirmations
    fn minimum_block_confirmations(&self) -> u32;

    /// Get rough block time as an estimation of confirmation time
    fn estimated_block_time(&self) -> Duration;
}
