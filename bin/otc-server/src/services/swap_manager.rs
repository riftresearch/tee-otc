use crate::api::swaps::{CreateSwapRequest, CreateSwapResponse, SwapResponse, DepositInfoResponse};
use crate::config::Settings;
use crate::db::Database;
use crate::error::OtcServerError;
use crate::services::MMRegistry;
use chrono::Utc;
use otc_chains::ChainRegistry;
use otc_models::{Swap, SwapStatus, TokenIdentifier};
use snafu::prelude::*;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::time::{timeout, Duration};
use tracing::{info, warn};
use uuid::Uuid;


const MARKET_MAKER_VALIDATION_TIMEOUT: Duration = Duration::from_secs(5);


#[derive(Debug, Snafu)]
pub enum SwapError {
    #[snafu(display("Quote not found: {}", quote_id))]
    QuoteNotFound { quote_id: Uuid },
    
    #[snafu(display("Quote has expired"))]
    QuoteExpired,
    
    
    #[snafu(display("Market maker rejected the quote"))]
    MarketMakerRejected,
    
    #[snafu(display("Market maker not connected: {}", market_maker_id))]
    MarketMakerNotConnected { market_maker_id: String },
    
    #[snafu(display("Market maker validation timeout"))]
    MarketMakerValidationTimeout,
    
    #[snafu(display("Database error: {}", source))]
    Database { source: OtcServerError },
    
    #[snafu(display("Chain not supported: {:?}", chain))]
    ChainNotSupported { chain: otc_models::ChainType },
    
    #[snafu(display("Failed to derive wallet: {}", source))]
    WalletDerivation { source: otc_chains::Error },
}

impl From<OtcServerError> for SwapError {
    fn from(err: OtcServerError) -> Self {
        match err {
            OtcServerError::NotFound => SwapError::QuoteNotFound { 
                quote_id: Uuid::nil() // We don't have the ID here
            },
            _ => SwapError::Database { source: err },
        }
    }
}

pub type SwapResult<T> = Result<T, SwapError>;

/// Manages the swap lifecycle from creation to settlement
pub struct SwapManager {
    db: Database,
    settings: Arc<Settings>,
    chain_registry: Arc<ChainRegistry>,
    mm_registry: Arc<MMRegistry>,
}

impl SwapManager {
    #[must_use] pub fn new(
        db: Database, 
        settings: Arc<Settings>, 
        chain_registry: Arc<ChainRegistry>,
        mm_registry: Arc<MMRegistry>,
    ) -> Self {
        Self {
            db,
            settings,
            chain_registry,
            mm_registry,
        }
    }
    
    /// Create a new swap from a quote
    /// 
    /// This will:
    /// 1. Validate the quote hasn't expired
    /// 2. Validate the market maker matches
    /// 3. Ask the market maker if they'll fill the quote (TODO)
    /// 4. Generate salts for deterministic wallet derivation
    /// 5. Create the swap record in the database
    /// 6. Return the deposit details to the user
    pub async fn create_swap(&self, request: CreateSwapRequest) -> SwapResult<CreateSwapResponse> {
        let quote = request.quote;
        // 1. Check if quote has expired
        if quote.expires_at < Utc::now() {
            return Err(SwapError::QuoteExpired);
        }
        
        // 2. Ask market maker if they'll fill this quote
        info!("Validating quote {} with market maker {}", quote.id, quote.market_maker_id);
        
        // Check if MM is connected
        if !self.mm_registry.is_connected(quote.market_maker_id) {
            warn!("Market maker {} not connected, rejecting swap", quote.market_maker_id);
            return Err(SwapError::MarketMakerNotConnected {
                market_maker_id: quote.market_maker_id.to_string(),
            });
        }
        
        // 3. Send validation request with timeout
        let (response_tx, response_rx) = oneshot::channel();
        self.mm_registry.validate_quote(
            &quote.market_maker_id,
            &quote.id,
            &quote.hash(),
            &request.user_destination_address,
            response_tx,
        ).await;
        
        // Wait for response with timeout
        let validation_result = match timeout(
            MARKET_MAKER_VALIDATION_TIMEOUT,
            response_rx
        ).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                warn!("Failed to receive validation response from market maker");
                return Err(SwapError::MarketMakerValidationTimeout);
            }
            Err(_) => {
                warn!("Market maker validation timed out after 5 seconds");
                return Err(SwapError::MarketMakerValidationTimeout);
            }
        };
        
        // Handle the validation result
        match validation_result {
            Ok(accepted) => {
                if !accepted {
                    info!("Market maker rejected quote {}", quote.id);
                    return Err(SwapError::MarketMakerRejected);
                }
                info!("Market maker accepted quote {}", quote.id);
            }
            Err(e) => {
                warn!("Market maker validation error: {:?}", e);
                return Err(SwapError::MarketMakerValidationTimeout);
            }
        }
        
        // 5. Generate random salts for wallet derivation
        let swap_id = Uuid::new_v4();
        let mut user_deposit_salt = [0u8; 32];
        let mut mm_nonce = [0u8; 16]; // 128 bits of collision resistance against an existing tx w/ a given output address && amount
        getrandom::getrandom(&mut user_deposit_salt).expect("Failed to generate random salt");
        getrandom::getrandom(&mut mm_nonce).expect("Failed to generate random nonce");
        // 7. Derive user deposit address for response
        let user_chain = self.chain_registry
        .get(&quote.from.chain)
        .ok_or(SwapError::ChainNotSupported { chain: quote.from.chain })?;
    
        let user_deposit_address = &user_chain
        .derive_wallet(&self.settings.master_key_bytes(), &user_deposit_salt)
        .map_err(|e| SwapError::WalletDerivation { source: e })?.address;
        
        // 6. Create swap record
        let now = Utc::now();
        let swap = Swap {
            id: swap_id,
            quote: quote.clone(),
            market_maker_id: quote.market_maker_id,
            user_deposit_salt,
            user_deposit_address: user_deposit_address.clone(),
            mm_nonce,
            user_destination_address: request.user_destination_address,
            user_refund_address: request.user_refund_address,
            status: SwapStatus::WaitingUserDepositInitiated,
            user_deposit_status: None,
            mm_deposit_status: None,
            settlement_status: None,
            failure_reason: None,
            failure_at: None,
            mm_notified_at: None,
            mm_private_key_sent_at: None,
            created_at: now,
            updated_at: now,
        };

        
        // Save swap to database
        self.db.swaps()
            .create(&swap)
            .await
            .context(DatabaseSnafu)?;
        
        info!("Created swap {} for quote {}", swap_id, quote.id);
        
        // 7. Derive user deposit address for response
        let user_chain = self.chain_registry
            .get(&quote.from.chain)
            .ok_or(SwapError::ChainNotSupported { chain: quote.from.chain })?;
        
        let user_wallet = user_chain
            .derive_wallet(&self.settings.master_key_bytes(), &user_deposit_salt)
            .map_err(|e| SwapError::WalletDerivation { source: e })?;
        
        // 8. Return response
        Ok(CreateSwapResponse {
            swap_id,
            deposit_address: user_wallet.address.clone(),
            deposit_chain: format!("{:?}", quote.from.chain),
            expected_amount: quote.from.amount,
            decimals: quote.from.decimals,
            token: match &quote.from.token {
                TokenIdentifier::Native => "Native".to_string(),
                TokenIdentifier::Address(addr) => addr.clone(),
            },
            expires_at: quote.expires_at,
            status: "waiting_user_deposit".to_string(),
        })
    }
    
    /// Get swap details by ID with derived wallet addresses
    pub async fn get_swap(&self, swap_id: Uuid) -> SwapResult<SwapResponse> {
        // Get swap from database
        let swap = self.db.swaps()
            .get(swap_id)
            .await
            .context(DatabaseSnafu)?;
        
        
        // Derive wallet addresses
        let master_key = self.settings.master_key_bytes();
        
        let user_chain = self.chain_registry
            .get(&swap.quote.from.chain)
            .ok_or(SwapError::ChainNotSupported { chain: swap.quote.from.chain })?;
        
        
        let user_wallet = user_chain
            .derive_wallet(&master_key, &swap.user_deposit_salt)
            .map_err(|e| SwapError::WalletDerivation { source: e })?;
        
        // Build response
        Ok(SwapResponse {
            id: swap.id,
            quote_id: swap.quote.id,
            status: format!("{:?}", swap.status),
            created_at: swap.created_at,
            updated_at: swap.updated_at,
            user_deposit: DepositInfoResponse {
                address: user_wallet.address.clone(),
                chain: format!("{:?}", swap.quote.from.chain),
                expected_amount: swap.quote.from.amount,
                decimals: swap.quote.from.decimals,
                token: match &swap.quote.from.token {
                    TokenIdentifier::Native => "Native".to_string(),
                    TokenIdentifier::Address(addr) => addr.clone(),
                },
                deposit_tx: swap.user_deposit_status.as_ref().map(|d| d.tx_hash.clone()),
                deposit_amount: swap.user_deposit_status.as_ref().map(|d| d.amount),
                deposit_detected_at: swap.user_deposit_status.as_ref().map(|d| d.detected_at),
            },
            mm_deposit: DepositInfoResponse {
                address: swap.user_destination_address.clone(),
                chain: format!("{:?}", swap.quote.to.chain),
                expected_amount: swap.quote.to.amount,
                decimals: swap.quote.to.decimals,
                token: match &swap.quote.to.token {
                    TokenIdentifier::Native => "Native".to_string(),
                    TokenIdentifier::Address(addr) => addr.clone(),
                },
                deposit_tx: swap.mm_deposit_status.as_ref().map(|d| d.tx_hash.clone()),
                deposit_amount: swap.mm_deposit_status.as_ref().map(|d| d.amount),
                deposit_detected_at: swap.mm_deposit_status.as_ref().map(|d| d.detected_at),
            },
            settlement_tx: swap.settlement_status.as_ref().map(|s| s.tx_hash.clone()),
        })
    }
}