use crate::{ ChainOperations, Result, key_derivation};
use alloy::primitives::{Address, U256};
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use async_trait::async_trait;
use evm_token_indexer_client::TokenIndexerClient;
use otc_models::{Currency, TokenIdentifier, TransferInfo, TxStatus, Wallet};
use std::str::FromStr;
use std::time::Duration;
use tracing::{debug, info};


sol! {
    event Transfer(address indexed from, address indexed to, uint256 value);
}


const ALLOWED_TOKEN: &str = "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf";

pub struct EthereumChain {
    provider: DynProvider,
    evm_indexer_client: TokenIndexerClient,
    chain_id: u64,
    allowed_token: Address,
}

impl EthereumChain {
    pub async fn new(rpc_url: &str, evm_indexer_url: &str, chain_id: u64) -> Result<Self> {
        let provider = ProviderBuilder::new()
            .connect_http(rpc_url.parse().map_err(|_| crate::Error::Serialization {
                message: "Invalid RPC URL".to_string(),
            })?).erased();

        let evm_indexer_client = TokenIndexerClient::new(evm_indexer_url)?;
        let allowed_token = Address::from_str(ALLOWED_TOKEN).map_err(|_| crate::Error::Serialization {
            message: "Invalid allowed token address".to_string(),
        })?;

        Ok(Self {
            provider,
            evm_indexer_client,
            chain_id,
            allowed_token,
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
        
        let wallet = Wallet::new(format!("{address:?}"), format!("0x{private_key}"));
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
        let private_key = format!("0x{}", alloy::hex::encode(private_key_bytes));
        
        debug!("Derived Ethereum wallet: {}", address);
        
        Ok(Wallet::new(address, private_key))
    }


    async fn get_tx_status(&self, tx_hash: &str) -> Result<TxStatus> {
        let tx_hash_parsed = tx_hash.parse().map_err(|_| crate::Error::Serialization {
            message: "Invalid transaction hash".to_string(),
        })?;
        let tx = self.provider.get_transaction_receipt(tx_hash_parsed).await?;

        if tx.is_some() {
            let current_block_height = self.provider.get_block_number().await?;
            Ok(TxStatus::Confirmed(current_block_height - tx.unwrap().block_number.unwrap()))
        } else {
            Ok(TxStatus::NotFound)
        }
    }
    async fn search_for_transfer(
        &self,
        address: &str,
        currency: &Currency,
        embedded_nonce: Option<[u8; 16]>,
        _from_block_height: Option<u64>
    ) -> Result<Option<TransferInfo>> {
        let token_address = match &currency.token {
            TokenIdentifier::Address(address) => address,
            TokenIdentifier::Native => return Ok(None),
        };
        let token_address = Address::from_str(&token_address).map_err(|_| crate::Error::Serialization {
            message: "Invalid token address".to_string(),
        })?;

        if token_address != self.allowed_token {
            debug!("Token address {} is not allowed", token_address);
            return Ok(None);
        }

        let to_address = Address::from_str(address).map_err(|_| crate::Error::Serialization {
            message: "Invalid address".to_string(),
        })?;

        let transfer_hint = self.get_transfer(&to_address, &currency.amount, embedded_nonce).await?;
        if transfer_hint.is_none() {
            return Ok(None);
        }

        Ok(Some(transfer_hint.unwrap()))
    }
    
    fn validate_address(&self, address: &str) -> bool {
        Address::from_str(address).is_ok()
    }
    
    fn minimum_block_confirmations(&self) -> u32 {
        4 // Standard for Ethereum
    }
    
    fn estimated_block_time(&self) -> Duration {
        Duration::from_secs(12) // ~12 seconds
    }
}

impl EthereumChain {
    // Note this function's response is safe to trust, b/c it will validate the responses from the untrusted evm_indexer_client
    async fn get_transfer(
        &self,
        address: &Address,
        amount: &U256,
        embedded_nonce: Option<[u8; 16]>,
    ) -> Result<Option<TransferInfo>> {
        // use the untrusted evm_indexer_client to get the transfer hint - this will only return 50 latest transfers (TODO: how to handle this?)
        let transfers = self.evm_indexer_client.get_transfers_to(*address, None, Some(*amount)).await?;
        if transfers.transfers.len() == 0 {
            return Ok(None);
        }

        let mut transfer_hint: Option<TransferInfo> = None;
        for transfer in transfers.transfers {
            let transaction_receipt = self.provider.get_transaction_receipt(transfer.transaction_hash).await?;
            if transaction_receipt.is_none() { 
                continue;
            }
            let transaction_receipt = transaction_receipt.unwrap();
            if !transaction_receipt.status() {
                continue;
            }

            let transfer_log = transaction_receipt.decoded_log::<Transfer>();
            if transfer_log.is_none() {
                continue;
            }
            let transfer_log = transfer_log.unwrap();
            // validate the recipient
            if transfer_log.to != *address {
                continue;
            }
            // validate the amount
            if transfer_log.value < *amount {
                continue;
            }
            // validate the embedded nonce
            if let Some(embedded_nonce) = embedded_nonce {
                let transaction = self.provider.get_raw_transaction_by_hash(transfer.transaction_hash).await?;
                if transaction.is_none() {
                    continue;
                }
                let transaction = transaction.unwrap();
                let tx_hex = alloy::hex::encode(transaction);
                let nonce_hex = alloy::hex::encode(embedded_nonce);
                if !tx_hex.contains(&nonce_hex) {
                    continue;
                }
            }
            // get the current block height
            let current_block_height = self.provider.get_block_number().await?;
            let confirmations = current_block_height - transaction_receipt.block_number.unwrap();

            // only return the transfer if it has more confirmations than the previous transfer hint
            if transfer_hint.is_some() && transfer_hint.as_ref().unwrap().confirmations > confirmations {
                continue;
            }

            transfer_hint = Some(TransferInfo{ 
                tx_hash: alloy::hex::encode(transfer.transaction_hash),
                detected_at: chrono::Utc::now(),
                confirmations,
                amount: transfer_log.value
            });
        }

        Ok(transfer_hint)
    }
}

