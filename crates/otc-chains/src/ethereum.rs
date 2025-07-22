use crate::{ ChainOperations, Result, key_derivation};
use alloy::primitives::{Address, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use alloy::network::{TransactionBuilder, EthereumWallet};
use alloy::rpc::types::TransactionRequest;
use async_trait::async_trait;
use otc_models::{DepositInfo, TxStatus, Wallet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info};

pub struct EthereumChain {
    provider: Arc<dyn Provider<alloy::network::Ethereum>>,
    chain_id: u64,
}

impl EthereumChain {
    pub async fn new(rpc_url: &str, chain_id: u64) -> Result<Self> {
        let provider = ProviderBuilder::new()
            .connect_http(rpc_url.parse().map_err(|_| crate::Error::Serialization {
                message: "Invalid RPC URL".to_string(),
            })?);
            
        Ok(Self {
            provider: Arc::new(provider),
            chain_id,
        })
    }
}

#[async_trait]
impl ChainOperations for EthereumChain {
    fn create_wallet(&self) -> Result<(Wallet, [u8; 32])> {
        // Generate a random salt
        let mut salt = [0u8; 32];
        getrandom::getrandom(&mut salt).map_err(|_| crate::Error::Serialization {
            message: "Failed to generate random salt".to_string(),
        })?;
        
        // Create a new random signer
        let signer = PrivateKeySigner::random();
        let address = signer.address();
        let private_key = alloy::primitives::hex::encode(signer.to_bytes());
        
        info!("Created new Ethereum wallet: {}", address);
        
        let wallet = Wallet::new(format!("{:?}", address), format!("0x{}", private_key));
        Ok((wallet, salt))
    }
    
    fn derive_wallet(&self, master_key: &[u8], salt: &[u8; 32]) -> Result<Wallet> {
        // Derive private key using HKDF
        let private_key_bytes = key_derivation::derive_private_key(
            master_key,
            salt,
            b"ethereum-wallet"
        )?;
        
        // Create signer from derived key
        let signer = PrivateKeySigner::from_bytes(&private_key_bytes.into())
            .map_err(|_| crate::Error::Serialization {
                message: "Failed to create signer from derived key".to_string(),
            })?;
        
        let address = format!("{:?}", signer.address());
        let private_key = format!("0x{}", hex::encode(private_key_bytes));
        
        debug!("Derived Ethereum wallet: {}", address);
        
        Ok(Wallet::new(address, private_key))
    }
    
    async fn get_balance(&self, address: &str) -> Result<U256> {
        let addr = Address::from_str(address)
            .map_err(|_| crate::Error::InvalidAddress)?;
        
        let balance = self.provider
            .get_balance(addr)
            .await
            .map_err(|_| crate::Error::Rpc {
                message: "Failed to get balance".to_string(),
            })?;
        
        debug!("Balance for {}: {} wei", address, balance);
        Ok(balance)
    }
    
    async fn get_tx_status(&self, txid: &str) -> Result<TxStatus> {
        let txid = txid.parse()
            .map_err(|_| crate::Error::Serialization {
                message: "Invalid transaction hash".to_string(),
            })?;
        
        // Get transaction receipt
        match self.provider.get_transaction_receipt(txid).await {
            Ok(Some(receipt)) => {
                // Get current block number
                let current_block = self.provider
                    .get_block_number()
                    .await
                    .map_err(|_| crate::Error::Rpc {
                        message: "Failed to get block number".to_string(),
                    })?;
                
                let confirmations = current_block
                    .saturating_sub(receipt.block_number.unwrap_or_default()) as u32;
                
                Ok(TxStatus::Confirmed(confirmations))
            }
            Ok(None) => {
                // Check if transaction is in mempool
                match self.provider.get_transaction_by_hash(txid).await {
                    Ok(Some(_)) => Ok(TxStatus::Confirmed(0)), // In mempool
                    _ => Ok(TxStatus::NotFound),
                }
            }
            Err(_) => Ok(TxStatus::NotFound),
        }
    }
    
    async fn check_deposit(
        &self,
        address: &str,
        _expected_amount: U256,
        _min_confirmations: u32,
    ) -> Result<Option<DepositInfo>> {
        // In production, would:
        // 1. Get recent blocks
        // 2. Filter for transactions to this address
        // 3. Check amounts and confirmations
        
        debug!("Checking deposits for address: {}", address);
        Ok(None)
    }
    
    async fn send_funds(
        &self,
        private_key: &str,
        to_address: &str,
        amount: U256,
    ) -> Result<String> {
        let signer = PrivateKeySigner::from_str(private_key.trim_start_matches("0x"))
            .map_err(|_| crate::Error::Serialization {
                message: "Invalid private key".to_string(),
            })?;
            
        let to = Address::from_str(to_address)
            .map_err(|_| crate::Error::InvalidAddress)?;
        
        let _wallet = EthereumWallet::from(signer);
        
        // Create transaction
        let _tx = TransactionRequest::default()
            .with_to(to)
            .with_value(amount)
            .with_chain_id(self.chain_id);
        
        // In production, would:
        // 1. Estimate gas
        // 2. Set gas price
        // 3. Sign and send transaction
        
        info!("Sending {} wei to {}", amount, to_address);
        Ok("0x0000000000000000000000000000000000000000000000000000000000000000".to_string())
    }
    
    fn validate_address(&self, address: &str) -> bool {
        Address::from_str(address).is_ok()
    }
    
    fn minimum_confirmations(&self) -> u32 {
        12 // Standard for Ethereum
    }
    
    fn estimated_block_time(&self) -> Duration {
        Duration::from_secs(12) // ~12 seconds
    }
}
