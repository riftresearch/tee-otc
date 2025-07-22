use crate::api::swaps::{CreateSwapRequest, CreateSwapResponse, SwapResponse, DepositInfoResponse};
use crate::config::Settings;
use crate::db::{Database, DbError};
use chrono::Utc;
use otc_chains::ChainRegistry;
use otc_models::{Swap, SwapStatus, TokenIdentifier};
use snafu::prelude::*;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum SwapError {
    #[snafu(display("Quote not found: {}", quote_id))]
    QuoteNotFound { quote_id: Uuid },
    
    #[snafu(display("Quote has expired"))]
    QuoteExpired,
    
    #[snafu(display("Market maker mismatch: quote has '{}', request has '{}'", quote_mm, request_mm))]
    MarketMakerMismatch { quote_mm: String, request_mm: String },
    
    #[snafu(display("Market maker rejected the quote"))]
    MarketMakerRejected,
    
    #[snafu(display("Database error: {}", source))]
    Database { source: DbError },
    
    #[snafu(display("Chain not supported: {:?}", chain))]
    ChainNotSupported { chain: otc_models::ChainType },
    
    #[snafu(display("Failed to derive wallet: {}", source))]
    WalletDerivation { source: otc_chains::Error },
}

impl From<DbError> for SwapError {
    fn from(err: DbError) -> Self {
        match err {
            DbError::NotFound => SwapError::QuoteNotFound { 
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
    // TODO: Add market_maker_registry: Arc<MarketMakerRegistry>,
}

impl SwapManager {
    pub fn new(db: Database, settings: Arc<Settings>, chain_registry: Arc<ChainRegistry>) -> Self {
        Self {
            db,
            settings,
            chain_registry,
        }
    }
    
    /// Create a new swap from a quote
    /// 
    /// This will:
    /// 1. Validate the quote exists and hasn't expired
    /// 2. Validate the market maker matches
    /// 3. Ask the market maker if they'll fill the quote (TODO)
    /// 4. Generate salts for deterministic wallet derivation
    /// 5. Create the swap record in the database
    /// 6. Return the deposit details to the user
    pub async fn create_swap(&self, request: CreateSwapRequest) -> SwapResult<CreateSwapResponse> {
        // 1. Validate quote exists
        let quote = self.db.quotes()
            .get(request.quote_id)
            .await
            .context(DatabaseSnafu)?;
        
        // 2. Check if quote has expired
        if quote.expires_at < Utc::now() {
            return Err(SwapError::QuoteExpired);
        }
        
        // 3. Validate market maker matches
        if quote.market_maker_identifier != request.market_maker_identifier {
            return Err(SwapError::MarketMakerMismatch {
                quote_mm: quote.market_maker_identifier,
                request_mm: request.market_maker_identifier,
            });
        }

        
        // 4. TODO: Ask market maker if they'll fill this quote
        // TODO(claude): I've decided I want this to actually depend on the market maker responding in realtime
        // so once we build out the market maker <-> otc-server websocket protocol and a have a way to ask the market maker
        // from this service, we can build this
        // For now, we'll assume they always accept
        info!("TODO: Implement market maker validation for quote {}", quote.id);
        let mm_accepts = true;
        
        if !mm_accepts {
            return Err(SwapError::MarketMakerRejected);
        }
        
        // 5. Generate random salts for wallet derivation
        let swap_id = Uuid::new_v4();
        let mut user_deposit_salt = [0u8; 32];
        let mut mm_deposit_salt = [0u8; 32];
        getrandom::getrandom(&mut user_deposit_salt).expect("Failed to generate random salt");
        getrandom::getrandom(&mut mm_deposit_salt).expect("Failed to generate random salt");
        
        // 6. Create swap record
        let swap = Swap {
            id: swap_id,
            quote_id: quote.id,
            market_maker: quote.market_maker_identifier.clone(),
            user_deposit_salt,
            mm_deposit_salt,
            user_destination_address: request.user_destination_address,
            user_refund_address: request.user_refund_address,
            status: SwapStatus::WaitingUserDeposit,
            user_deposit_status: None,
            mm_deposit_status: None,
            user_withdrawal_tx: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        
        // Save to database
        self.db.swaps()
            .create(&swap)
            .await
            .context(DatabaseSnafu)?;
        
        info!("Created swap {} for quote {}", swap_id, quote.id);
        
        // TODO: Start monitoring for deposits
        // self.start_monitoring(swap_id);
        
        // 7. Derive user deposit address for response
        let master_key = self.settings.master_key_bytes();
        let user_chain = self.chain_registry
            .get(&quote.from.chain)
            .ok_or(SwapError::ChainNotSupported { chain: quote.from.chain })?;
        
        let user_wallet = user_chain
            .derive_wallet(&master_key, &user_deposit_salt)
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
        
        // Get quote for chain information
        let quote = self.db.quotes()
            .get(swap.quote_id)
            .await
            .context(DatabaseSnafu)?;
        
        // Derive wallet addresses
        let master_key = self.settings.master_key_bytes();
        
        let user_chain = self.chain_registry
            .get(&quote.from.chain)
            .ok_or(SwapError::ChainNotSupported { chain: quote.from.chain })?;
        
        let mm_chain = self.chain_registry
            .get(&quote.to.chain)
            .ok_or(SwapError::ChainNotSupported { chain: quote.to.chain })?;
        
        let user_wallet = user_chain
            .derive_wallet(&master_key, &swap.user_deposit_salt)
            .map_err(|e| SwapError::WalletDerivation { source: e })?;
        
        let mm_wallet = mm_chain
            .derive_wallet(&master_key, &swap.mm_deposit_salt)
            .map_err(|e| SwapError::WalletDerivation { source: e })?;
        
        // Build response
        Ok(SwapResponse {
            id: swap.id,
            quote_id: swap.quote_id,
            status: format!("{:?}", swap.status),
            created_at: swap.created_at,
            updated_at: swap.updated_at,
            user_deposit: DepositInfoResponse {
                address: user_wallet.address.clone(),
                chain: format!("{:?}", quote.from.chain),
                expected_amount: quote.from.amount,
                decimals: quote.from.decimals,
                token: match &quote.from.token {
                    TokenIdentifier::Native => "Native".to_string(),
                    TokenIdentifier::Address(addr) => addr.clone(),
                },
                deposit_tx: swap.user_deposit_status.as_ref().map(|d| d.tx_hash.clone()),
                deposit_amount: swap.user_deposit_status.as_ref().map(|d| d.amount),
                deposit_detected_at: swap.user_deposit_status.as_ref().map(|d| d.detected_at),
            },
            mm_deposit: DepositInfoResponse {
                address: mm_wallet.address.clone(),
                chain: format!("{:?}", quote.to.chain),
                expected_amount: quote.to.amount,
                decimals: quote.to.decimals,
                token: match &quote.to.token {
                    TokenIdentifier::Native => "Native".to_string(),
                    TokenIdentifier::Address(addr) => addr.clone(),
                },
                deposit_tx: swap.mm_deposit_status.as_ref().map(|d| d.tx_hash.clone()),
                deposit_amount: swap.mm_deposit_status.as_ref().map(|d| d.amount),
                deposit_detected_at: swap.mm_deposit_status.as_ref().map(|d| d.detected_at),
            },
            settlement_tx: swap.user_withdrawal_tx,
        })
    }
}