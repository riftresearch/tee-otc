use crate::{key_derivation, ChainOperations, Result};
use alloy::hex;
use alloy::primitives::U256;
use async_trait::async_trait;
use bitcoin::secp256k1::{Secp256k1, SecretKey};
use bitcoin::{consensus, Address, CompressedPublicKey, Network, PrivateKey, Script};
use bitcoincore_rpc_async::{Auth, Client, RpcApi};
use otc_models::{ChainType, Currency, TransferInfo, TxStatus, Wallet};
use std::str::FromStr;
use std::time::Duration;
use tracing::{debug, info};

pub struct BitcoinChain {
    rpc_client: Client,
    esplora_client: esplora_client::AsyncClient,
    network: Network,
}

impl BitcoinChain {
    /// Auth (if necessary) should be embedded in the bitcoin_core_rpc_url
    pub async fn new(bitcoin_core_rpc_url: &str, esplora_url: &str, network: Network) -> Result<Self> {
        let rpc_client = Client::new(bitcoin_core_rpc_url.to_string(), Auth::None).await
            .map_err(|_| crate::Error::Rpc {
                message: "Failed to create Bitcoin RPC client".to_string(),
            })?;

        let esplora_client = esplora_client::Builder::new(esplora_url)
            .build_async()
            .map_err(|_| crate::Error::Rpc {
                message: "Failed to create Esplora client".to_string(),
            })?;

        Ok(Self {
            rpc_client,
            esplora_client,
            network,
        })
    }
}

#[async_trait]
impl ChainOperations for BitcoinChain {
    fn create_wallet(&self) -> Result<(Wallet, [u8; 32])> {
        // Generate a random salt
        let mut salt = [0u8; 32];
        getrandom::getrandom(&mut salt).map_err(|_| crate::Error::Serialization {
            message: "Failed to generate random salt".to_string(),
        })?;
        
        // Generate a new private key
        let secp = Secp256k1::new();
        let secret_key = bitcoin::secp256k1::SecretKey::from_slice(&salt).unwrap();
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
        )?;
        
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

    async fn get_tx_status(&self, tx_hash: &str) -> Result<TxStatus> {
        let tx = self.rpc_client.get_raw_transaction_verbose(&bitcoin::Txid::from_str(tx_hash).unwrap()).await?;
        if tx.confirmations.unwrap_or(0) > 0 {
            Ok(TxStatus::Confirmed(tx.confirmations.unwrap_or(0)))
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
        if !matches!(currency.chain, ChainType::Bitcoin) || !matches!(currency.token, otc_models::TokenIdentifier::Native)  {
            return Err(crate::Error::InvalidCurrency { currency: currency.clone(), network: ChainType::Bitcoin });
        }
        let address = bitcoin::Address::from_str(address)?.assume_checked(); 
        let potential_transfer = self.get_transfer_hint(address.to_string().as_str(), &currency.amount, embedded_nonce).await?;
        if potential_transfer.is_some() {
            let potential_transfer = potential_transfer.unwrap();
            // Validate the transfer hint
            let tx = self.rpc_client.get_raw_transaction_verbose(&bitcoin::Txid::from_str(&potential_transfer.tx_hash).unwrap()).await?;

            // validate it's in the longest chain
            if tx.confirmations.unwrap_or(0) > 0 && !tx.in_active_chain.unwrap_or(false) {
                tracing::debug!(message = "Transfer hint is not in the longest chain", tx_hash = potential_transfer.tx_hash);
                return Ok(None);
            }

            // did the transfer hint lie about it's confirmations (it's okay if it was outdated)
            if potential_transfer.confirmations > tx.confirmations.unwrap_or(0) as u64 {
                tracing::debug!(message = "Transfer hint lied about it's confirmations", tx_hash = potential_transfer.tx_hash, hint_confirmations = potential_transfer.confirmations, actual_confirmations = tx.confirmations.unwrap_or(0));
                return Ok(None);
            }

            // validate the embedded nonce is in the tx (if required)
            if embedded_nonce.is_some() {
                let embedded_nonce = embedded_nonce.unwrap();
                if !tx.hex.contains(hex::encode(embedded_nonce).as_str()) {
                    tracing::debug!(message = "Transfer hint lied about it's embedded nonce", tx_hash = potential_transfer.tx_hash, hint_nonce = hex::encode(embedded_nonce), actual_tx = tx.hex);
                    return Ok(None);
                }
            }
            let minimum_amount = currency.amount.to::<u64>();
            let valid_outputs = tx.outputs.iter().filter_map(|output| {
                if address.matches_script_pubkey(Script::from_bytes(&hex::decode(&output.script_pubkey.hex).unwrap())) {
                    // validate the funds have been sent to our "address"
                    if output.value >= minimum_amount as f64 {
                        // validate sufficient funds have been sent to our "to"
                        Some(output.clone())
                    }  else { 
                        None
                    }
                } else {
                    None
                }
            }).collect::<Vec<_>>();

            if valid_outputs.is_empty() {
                tracing::debug!(message = "Transfer hint did not send funds to our address", tx_hash = potential_transfer.tx_hash, address = address.to_string());
                return Ok(None);
            }

            // We dont actually have to modify the original hint, since we've validated all of it's info against our rpc
            Ok(Some(potential_transfer))
        } else {
            Ok(None)
        }
    }
    

    fn validate_address(&self, address: &str) -> bool {
        match Address::from_str(address) {
            Ok(addr) => addr.is_valid_for_network(self.network),
            Err(_) => false,
        }
    }
    
    fn minimum_block_confirmations(&self) -> u32 {
        2
    }
    
    fn estimated_block_time(&self) -> Duration {
        Duration::from_secs(600) // 10 minutes
    }
}

impl BitcoinChain {
    /// Called a hint b/c the esplora client CANNOT be trusted to return non-fradulent data (b/c it not intended to run locally)
    /// Note that if there are more than 50 utxos available to the address, this could ignore a valid transfer (TODO: how to handle this?)
    async fn get_transfer_hint(
        &self,
        address: &str,
        amount: &U256,
        embedded_nonce: Option<[u8; 16]>, 
    ) -> Result<Option<TransferInfo>> {
        let address = bitcoin::Address::from_str(address)?.assume_checked(); 
        let utxos = self.esplora_client.get_address_utxo(&address).await?;
        let current_block_height = self.esplora_client.get_height().await?;
        let mut most_confirmed_transfer: Option<TransferInfo> = None;
        for utxo in utxos {
            if utxo.value < amount.to::<u64>() {
                continue;
            }
            let cur_utxo_confirmations = current_block_height - utxo.status.block_height.unwrap_or(current_block_height);
            let most_confirmed_transfer_confirmations = most_confirmed_transfer.as_ref().unwrap().confirmations;
            if most_confirmed_transfer.is_some() && (most_confirmed_transfer_confirmations > cur_utxo_confirmations as u64) { 
                // if we already have a candidate let's do the cheap check to see if it's better confirmations wise before we fully validate it
                // before we download the full tx
                continue;
            }
            // At this point, we either have a new candidate that's more confirmed than the current candidate
            // as let's finally validate that it's the correct transfer
            if embedded_nonce.is_some() {
                // we only need to do this check if the embedded nonce is a requirement 
                let embedded_nonce = embedded_nonce.unwrap();
                let tx = self.esplora_client.get_tx(&utxo.txid).await?;
                if tx.is_none() {
                    continue;
                }
                let tx = tx.unwrap();
                let serialized_tx = consensus::encode::serialize_hex(&tx);
                if !serialized_tx.contains(hex::encode(embedded_nonce).as_str()) {
                    continue;
                }
            }
            // At this point, our new candidate is valid and the most confirmed transfer we've seen
            // so let's return it
            most_confirmed_transfer = Some(TransferInfo { 
                tx_hash: utxo.txid.to_string(),
                amount: U256::from(utxo.value),
                detected_at: chrono::Utc::now(), 
                confirmations: cur_utxo_confirmations as u64,
            });
            
        }
        Ok(most_confirmed_transfer)
    }
}
