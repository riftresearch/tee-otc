use crate::Result;
use alloy::primitives::U256;
use async_trait::async_trait;
use blockchain_utils::FeeCalcFromLot;
use otc_models::{Lot, Swap, TokenIdentifier, TransferInfo, TxStatus, Wallet};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketMakerPaymentVerification {
    pub aggregated_fee: U256,
    pub batch_nonce_digest: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Payment {
    pub lot: Lot,
    pub to_address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketMakerBatch {
    pub ordered_payments: Vec<Payment>,
    pub payment_verification: MarketMakerPaymentVerification,
}

/// A queued payment request waiting to be batched
#[derive(Debug, Clone)]
pub struct MarketMakerQueuedPayment {
    pub swap_id: Uuid,
    pub quote_id: Uuid,
    pub lot: Lot,
    pub destination_address: String,
    pub mm_nonce: [u8; 16],
}


// we can derive a MarketMakerQueuedPayment from a Swap
impl From<&Swap> for MarketMakerQueuedPayment {
    fn from(swap: &Swap) -> Self {
        MarketMakerQueuedPayment {
            swap_id: swap.id,
            quote_id: swap.quote.id,
            lot: swap.quote.to.clone(), // to is what the MM is sending to the user always
            destination_address: swap.user_destination_address.clone(),
            mm_nonce: swap.mm_nonce,
        }
    }
}


pub trait MarketMakerQueuedPaymentExt {
    fn to_market_maker_batch(&self) -> Option<MarketMakerBatch>;
}

impl MarketMakerQueuedPaymentExt for [MarketMakerQueuedPayment] {
    fn to_market_maker_batch(&self) -> Option<MarketMakerBatch> {
        if self.is_empty() {
            return None;
        }

        // Convert to Payment structs
        let ordered_payments: Vec<Payment> = self
            .iter()
            .map(|qp| Payment {
                lot: qp.lot.clone(),
                to_address: qp.destination_address.clone(),
            })
            .collect();

        // Calculate aggregated fee
        let aggregated_fee: U256 = self
            .iter()
            .map(|qp| U256::from(qp.lot.compute_protocol_fee()))
            .fold(U256::ZERO, |acc, fee| acc + fee);

        // Compute batch nonce digest by hashing all nonces together
        let mut nonce_data = Vec::new();
        for qp in self {
            // safety note: mm_nonce is 16 bytes always, so no risk of ambiguity attacks
            nonce_data.extend_from_slice(&qp.mm_nonce);
        }
        let batch_nonce_digest = alloy::primitives::keccak256(&nonce_data).0;

        Some(MarketMakerBatch {
            ordered_payments,
            payment_verification: MarketMakerPaymentVerification {
                aggregated_fee,
                batch_nonce_digest,
            },
        })
    }
}



// implementors of this trait should be stateless
#[async_trait]
pub trait ChainOperations: Send + Sync {
    /// Create a new wallet, returning the wallet and the salt used
    fn create_wallet(&self) -> Result<(Wallet, [u8; 32])>;

    /// Derive a wallet deterministically from a master key and salt
    fn derive_wallet(&self, master_key: &[u8], salt: &[u8; 32]) -> Result<Wallet>;

    /// Verifies a market maker batch and returns the number of confirmations the batch has if any
    async fn verify_market_maker_batch_transaction(
        &self,
        tx_hash: &str,
        market_maker_batch: &MarketMakerBatch,
    ) -> Result<Option<u64>>;

    /// Check for transfers to an address
    async fn search_for_transfer(
        &self,
        recipient_address: &str,
        lot: &Lot,
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

    /// Get the best hash for the chain as hex string
    async fn get_best_hash(&self) -> Result<String>;
}
