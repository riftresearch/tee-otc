use crate::{ChainOperations, Result, key_derivation};
use alloy::primitives::U256;
use async_trait::async_trait;
use bitcoin::secp256k1::{rand, Secp256k1, SecretKey};
use bitcoin::{Address, CompressedPublicKey, Network, PrivateKey};
use bitcoincore_rpc::{Auth, Client, RpcApi};
use otc_models::{DepositInfo, TxStatus, Wallet};
use std::str::FromStr;
use std::time::Duration;
use tracing::{debug, info};

pub struct BitcoinChain {
    rpc_client: Client,
    network: Network,
}

impl BitcoinChain {
    pub fn new(rpc_url: &str, rpc_auth: Auth, network: Network) -> Result<Self> {
        let rpc_client = Client::new(rpc_url, rpc_auth)
            .map_err(|_| crate::Error::Rpc {
                message: "Failed to create Bitcoin RPC client".to_string(),
            })?;
            
        Ok(Self {
            rpc_client,
            network,
        })
    }
}

#[async_trait]
impl ChainOperations for BitcoinChain {
    async fn create_wallet(&self) -> Result<(Wallet, [u8; 32])> {
        // Generate a random salt
        let mut salt = [0u8; 32];
        getrandom::getrandom(&mut salt).map_err(|_| crate::Error::Serialization {
            message: "Failed to generate random salt".to_string(),
        })?;
        
        // Generate a new private key
        let secp = Secp256k1::new();
        let secret_key = bitcoin::secp256k1::SecretKey::new(&mut rand::thread_rng());
        let private_key = PrivateKey::new(secret_key, self.network);
        
        // Derive public key and address
        let compressed_pk = CompressedPublicKey::from_private_key(&secp, &private_key).unwrap();
        let address = Address::p2wpkh(&compressed_pk, self.network);
        
        info!("Created new Bitcoin wallet: {}", address);
        
        let wallet = Wallet::new(address.to_string(), private_key.to_wif());
        Ok((wallet, salt))
    }
    
    fn derive_wallet(&self, master_key: &[u8], salt: &[u8; 32]) -> Result<Wallet> {
        // Derive private key using HKDF
        let private_key_bytes = key_derivation::derive_private_key(
            master_key,
            salt,
            b"bitcoin-wallet"
        );
        
        // Create secp256k1 secret key
        let secret_key = SecretKey::from_slice(&private_key_bytes)
            .map_err(|_| crate::Error::Serialization {
                message: "Failed to create secret key from derived bytes".to_string(),
            })?;
        
        let private_key = PrivateKey::new(secret_key, self.network);
        
        // Derive public key and address
        let secp = Secp256k1::new();
        let compressed_pk = CompressedPublicKey::from_private_key(&secp, &private_key).unwrap();
        let address = Address::p2wpkh(&compressed_pk, self.network);
        
        debug!("Derived Bitcoin wallet: {}", address);
        
        Ok(Wallet::new(address.to_string(), private_key.to_wif()))
    }
    
    async fn get_balance(&self, address: &str) -> Result<U256> {
        let addr = Address::from_str(address)
            .map_err(|_| crate::Error::InvalidAddress)?
            .require_network(self.network)
            .map_err(|_| crate::Error::InvalidAddress)?;
        
        // For now, return 0 - implement actual RPC call
        // In production, would query UTXOs for this address
        debug!("Getting balance for address: {}", addr);
        Ok(U256::ZERO)
    }
    
    async fn get_tx_status(&self, tx_hash: &str) -> Result<TxStatus> {
        let txid = bitcoin::Txid::from_str(tx_hash)
            .map_err(|_| crate::Error::Serialization {
                message: "Invalid transaction hash".to_string(),
            })?;
        
        // Check if transaction exists
        match self.rpc_client.get_raw_transaction_info(&txid, None) {
            Ok(tx_info) => {
                let confirmations = tx_info.confirmations.unwrap_or(0) as u32;
                Ok(TxStatus::Confirmed(confirmations))
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
        // In production, would scan recent blocks for deposits
        // For now, return None
        debug!("Checking deposits for address: {}", address);
        Ok(None)
    }
    
    async fn send_funds(
        &self,
        private_key: &str,
        to_address: &str,
        amount: U256,
    ) -> Result<String> {
        // Parse private key and destination
        let _privkey = PrivateKey::from_wif(private_key)
            .map_err(|_| crate::Error::Serialization {
                message: "Invalid private key WIF".to_string(),
            })?;
            
        let _dest = Address::from_str(to_address)
            .map_err(|_| crate::Error::InvalidAddress)?
            .require_network(self.network)
            .map_err(|_| crate::Error::InvalidAddress)?;
        
        // In production, would:
        // 1. Import private key to wallet
        // 2. Create and sign transaction
        // 3. Broadcast transaction
        // For now, return dummy txid
        
        info!("Sending {} BTC to {}", amount, to_address);
        Ok("0000000000000000000000000000000000000000000000000000000000000000".to_string())
    }
    
    fn validate_address(&self, address: &str) -> bool {
        match Address::from_str(address) {
            Ok(addr) => addr.is_valid_for_network(self.network),
            Err(_) => false,
        }
    }
    
    fn minimum_confirmations(&self) -> u32 {
        6 // Standard for Bitcoin
    }
    
    fn estimated_block_time(&self) -> Duration {
        Duration::from_secs(600) // 10 minutes
    }
}
