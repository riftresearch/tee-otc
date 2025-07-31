use alloy::{
    primitives::U256,
    signers::local::LocalSigner,
    network::EthereumWallet,
};
use bitcoin::{
    Address as BtcAddress, PrivateKey as BtcPrivateKey,
    secp256k1::Secp256k1,
};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use otc_mm_protocol::{MMRequest, MMResponse, MMErrorCode};
use otc_models::{ChainType, TokenIdentifier};
use parking_lot::RwLock;
use snafu::Snafu;
use std::{str::FromStr, sync::Arc};

use tracing::{info, error};
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum SettlementError {
    #[snafu(display("Quote not found: {}", quote_id))]
    QuoteNotFound { quote_id: Uuid },
    
    #[snafu(display("Quote expired: {}", quote_id))]
    QuoteExpired { quote_id: Uuid },
    
    #[snafu(display("Invalid chain configuration"))]
    InvalidChain,
    
    #[snafu(display("Wallet error: {}", message))]
    WalletError { message: String },
    
    #[snafu(display("Transaction error: {}", source))]
    TransactionError { source: Box<dyn std::error::Error + Send + Sync> },
    
    #[snafu(display("Insufficient balance"))]
    InsufficientBalance,
    
    #[snafu(display("Network error: {}", source))]
    NetworkError { source: Box<dyn std::error::Error + Send + Sync> },
    
    #[snafu(display("Invalid request type: expected {}, received {}", expected, received))]
    InvalidRequestType { expected: String, received: String },
}

type Result<T> = std::result::Result<T, SettlementError>;

/// Represents a quote that the MM has provided
#[derive(Debug, Clone)]
pub struct ActiveQuote {
    pub quote_id: Uuid,
    pub from_chain: ChainType,
    pub from_token: TokenIdentifier,
    pub from_amount: U256,
    pub to_chain: ChainType,
    pub to_token: TokenIdentifier,
    pub to_amount: U256,
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

/// Tracks an active swap that's being settled
#[derive(Debug, Clone)]
pub struct ActiveSwap {
    pub swap_id: Uuid,
    pub quote_id: Uuid,
    pub status: SwapStatus,
    pub user_deposit_address: String,
    pub user_deposit_amount: U256,
    pub user_deposit_chain: ChainType,
    pub user_deposit_tx: Option<String>,
    pub mm_deposit_tx: Option<String>,
    pub mm_destination_address: String,
    pub confirmations_required: u32,
    pub confirmations_received: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SwapStatus {
    AwaitingUserDeposit,
    UserDepositDetected,
    MMDepositSent,
    AwaitingConfirmations,
    Completed,
    Failed(String),
}

/// Configuration for settlement
#[derive(Debug, Clone)]
pub struct SettlementConfig {
    pub eth_private_key: Option<String>,
    pub eth_rpc_url: String,
    pub btc_config: Option<BtcConfig>,
    pub eth_confirmations: u32,
    pub btc_confirmations: u32,
}

#[derive(Debug, Clone)]
pub struct BtcConfig {
    pub rpc_url: String,
    pub rpc_user: String,
    pub rpc_password: String,
    pub private_key_wif: String,
}

impl Default for SettlementConfig {
    fn default() -> Self {
        Self {
            eth_private_key: None,
            eth_rpc_url: "https://eth.llamarpc.com".to_string(),
            btc_config: None,
            eth_confirmations: 12,
            btc_confirmations: 3,
        }
    }
}

/// Optimized settlement manager with concurrent data structures
pub struct SettlementManager {
    // Active quotes using DashMap
    active_quotes: Arc<DashMap<Uuid, ActiveQuote>>,
    
    // Active swaps using DashMap
    active_swaps: Arc<DashMap<Uuid, ActiveSwap>>,
    
    // Quote expiry tracker using sorted entries
    quote_expiry: Arc<RwLock<Vec<(DateTime<Utc>, Uuid)>>>,
    
    // Ethereum wallet
    eth_wallet: Option<EthereumWallet>,
    
    // Bitcoin components
    btc_private_key: Option<BtcPrivateKey>,
    
    // Configuration
    eth_confirmations: u32,
    btc_confirmations: u32,
}



impl SettlementManager {
    pub async fn new(config: SettlementConfig) -> Result<Self> {
        // Initialize Ethereum wallet
        let eth_wallet = if let Some(eth_key) = config.eth_private_key {
            let signer = LocalSigner::from_str(&eth_key)
                .map_err(|e| SettlementError::WalletError { 
                    message: format!("Invalid ETH private key: {}", e) 
                })?;
            
            let wallet = EthereumWallet::from(signer);
            Some(wallet)
        } else {
            None
        };
        
        // Initialize Bitcoin components
        let btc_private_key = if let Some(btc_config) = config.btc_config.clone() {
            let private_key = BtcPrivateKey::from_wif(&btc_config.private_key_wif)
                .map_err(|e| SettlementError::WalletError { 
                    message: format!("Invalid BTC private key: {}", e) 
                })?;
            
            Some(private_key)
        } else {
            None
        };
        
        let active_quotes = Arc::new(DashMap::new());
        let active_swaps = Arc::new(DashMap::new());
        let quote_expiry = Arc::new(RwLock::new(Vec::new()));
        
        Ok(Self {
            active_quotes,
            active_swaps,
            quote_expiry,
            eth_wallet,

            btc_private_key,
            eth_confirmations: config.eth_confirmations,
            btc_confirmations: config.btc_confirmations,
        })
    }
    
    /// Store a quote for later validation
    pub async fn store_quote(&self, quote: ActiveQuote) {
        let quote_id = quote.quote_id;
        let expires_at = quote.expires_at;
        
        // Store in concurrent map
        self.active_quotes.insert(quote_id, quote);
        
        // Add to expiry tracker
        let mut expiry = self.quote_expiry.write();
        let insert_pos = expiry.binary_search_by_key(&expires_at, |(exp, _)| *exp)
            .unwrap_or_else(|pos| pos);
        expiry.insert(insert_pos, (expires_at, quote_id));
        
        info!("Stored quote {} (expires at {})", quote_id, expires_at);
    }
    
    /// Handle quote validation request from OTC server
    pub async fn handle_validate_quote(&self, request: MMRequest) -> Result<MMResponse> {
        match request {
            MMRequest::ValidateQuote { request_id, quote_id, quote_hash, .. } => {
                // Fast lookup from DashMap
                let quote = self.active_quotes.get(&quote_id)
                    .ok_or(SettlementError::QuoteNotFound { quote_id })?;
                
                // Check if quote is expired
                if quote.expires_at < Utc::now() {
                    return Err(SettlementError::QuoteExpired { quote_id });
                }
                
                // Validate quote hash
                let computed_hash = self.compute_quote_hash(&quote);
                if computed_hash != quote_hash {
                    return Ok(MMResponse::QuoteValidated {
                        request_id,
                        quote_id,
                        accepted: false,
                        rejection_reason: Some("Quote hash mismatch".to_string()),
                        timestamp: Utc::now(),
                    });
                }
                
                // Determine MM destination address
                let _mm_destination_address = match &quote.from_chain {
                    ChainType::Ethereum => {
                        self.get_eth_address()
                            .ok_or(SettlementError::WalletError { 
                                message: "ETH wallet not configured".to_string() 
                            })?
                    },
                    ChainType::Bitcoin => {
                        self.get_btc_address()
                            .ok_or(SettlementError::WalletError { 
                                message: "BTC wallet not configured".to_string() 
                            })?
                    },
                };
                
                info!("Validated quote {} successfully", quote_id);
                
                Ok(MMResponse::QuoteValidated {
                    request_id,
                    quote_id,
                    accepted: true,
                    rejection_reason: None,
                    timestamp: Utc::now(),
                })
            },
            _ => return Err(SettlementError::InvalidRequestType { 
                expected: "ValidateQuote".to_string(),
                received: format!("{:?}", request),
            }),
        }
    }
    
    /// Handle user deposit notification from OTC server
    pub async fn handle_user_deposited(&self, request: MMRequest) -> Result<MMResponse> {
        match request {
            MMRequest::UserDeposited { 
                request_id, 
                swap_id, 
                quote_id, 
                deposit_address, 
                user_tx_hash,
                ..
            } => {
                // Get the quote
                let quote = self.active_quotes.get(&quote_id)
                    .ok_or(SettlementError::QuoteNotFound { quote_id })?;
                
                // Get user destination address from our stored quote
                let user_destination_address = match &quote.to_chain {
                    ChainType::Ethereum => "0x0000000000000000000000000000000000000000".to_string(), // Placeholder
                    ChainType::Bitcoin => "bc1qplaceholder".to_string(), // Placeholder
                };
                
                // Create active swap
                let swap = ActiveSwap {
                    swap_id,
                    quote_id,
                    status: SwapStatus::UserDepositDetected,
                    user_deposit_address: deposit_address.clone(),
                    user_deposit_amount: quote.from_amount,
                    user_deposit_chain: quote.from_chain.clone(),
                    user_deposit_tx: Some(user_tx_hash),
                    mm_deposit_tx: None,
                    mm_destination_address: self.get_destination_address(&quote.from_chain)?,
                    confirmations_required: match quote.from_chain {
                        ChainType::Ethereum => self.eth_confirmations,
                        ChainType::Bitcoin => self.btc_confirmations,
                    },
                    confirmations_received: 0,
                };
                
                self.active_swaps.insert(swap_id, swap);
                
                // Execute settlement
                match self.execute_settlement(&quote, &user_destination_address).await {
                    Ok(tx_hash) => {
                        // Update swap status
                        if let Some(mut swap) = self.active_swaps.get_mut(&swap_id) {
                            swap.status = SwapStatus::MMDepositSent;
                            swap.mm_deposit_tx = Some(tx_hash.clone());
                        }
                        
                        info!("Settlement executed for swap {}: tx {}", swap_id, tx_hash);
                        
                        Ok(MMResponse::DepositInitiated {
                            request_id,
                            swap_id,
                            tx_hash,
                            amount_sent: quote.to_amount,
                            timestamp: Utc::now(),
                        })
                    },
                    Err(e) => {
                        error!("Settlement failed for swap {}: {}", swap_id, e);
                        
                        // Update swap status
                        if let Some(mut swap) = self.active_swaps.get_mut(&swap_id) {
                            swap.status = SwapStatus::Failed(e.to_string());
                        }
                        
                        Ok(MMResponse::Error {
                            request_id,
                            error_code: MMErrorCode::InternalError,
                            message: e.to_string(),
                            timestamp: Utc::now(),
                        })
                    }
                }
            },
            _ => return Err(SettlementError::InvalidRequestType { 
                expected: "UserDeposited".to_string(),
                received: format!("{:?}", request),
            }),
        }
    }
    
    async fn execute_settlement(
        &self,
        quote: &ActiveQuote,
        user_destination_address: &str,
    ) -> Result<String> {
        match &quote.to_chain {
            ChainType::Ethereum => {
                self.send_eth_transaction(quote, user_destination_address).await
            },
            ChainType::Bitcoin => {
                self.send_btc_transaction(quote, user_destination_address).await
            },
        }
    }
    
    async fn send_eth_transaction(
        &self,
        quote: &ActiveQuote,
        _recipient: &str,
    ) -> Result<String> {
        // For now, return mock transaction
        Ok(format!("0x{}", hex::encode(sha2::Sha256::digest(
            format!("{:?}", quote).as_bytes()
        ))))
    }
    
    async fn send_btc_transaction(
        &self,
        quote: &ActiveQuote,
        _recipient: &str,
    ) -> Result<String> {
        // For now, return mock transaction
        Ok(format!("{}", hex::encode(sha2::Sha256::digest(
            format!("{:?}", quote).as_bytes()
        ))))
    }
    
    fn compute_quote_hash(&self, quote: &ActiveQuote) -> [u8; 32] {
        let quote_data = format!(
            "{:?}:{:?}:{}:{:?}:{:?}:{}",
            quote.from_chain,
            quote.from_token,
            quote.from_amount,
            quote.to_chain,
            quote.to_token,
            quote.to_amount
        );
        sha2::Sha256::digest(quote_data.as_bytes()).into()
    }
    
    fn get_eth_address(&self) -> Option<String> {
        self.eth_wallet.as_ref().map(|w| format!("{:?}", w.default_signer().address()))
    }
    
    fn get_btc_address(&self) -> Option<String> {
        self.btc_private_key.as_ref().map(|pk| {
            let secp = Secp256k1::new();
            let compressed_pk = bitcoin::key::CompressedPublicKey::from_private_key(&secp, &pk)
                .expect("Valid public key");
            BtcAddress::p2wpkh(&compressed_pk, bitcoin::KnownHrp::Mainnet).to_string()
        })
    }
    
    fn get_destination_address(&self, chain: &ChainType) -> Result<String> {
        match chain {
            ChainType::Ethereum => self.get_eth_address()
                .ok_or(SettlementError::WalletError { 
                    message: "ETH wallet not configured".to_string() 
                }),
            ChainType::Bitcoin => self.get_btc_address()
                .ok_or(SettlementError::WalletError { 
                    message: "BTC wallet not configured".to_string() 
                }),
        }
    }
}

// Add SHA-256 import
use sha2::Digest;