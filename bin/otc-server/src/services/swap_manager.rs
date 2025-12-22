use crate::api::swaps::{CreateSwapRequest, RefundSwapResponse};
use crate::config::Settings;
use crate::db::Database;
use crate::error::OtcServerError;
use crate::services::MMRegistry;
use alloy::hex::FromHexError;
use alloy::primitives::U256;
use blockchain_utils::MempoolEsploraFeeExt;
use otc_chains::ChainRegistry;
use otc_models::{ChainType, LatestRefund, Metadata, Swap, SwapStatus};
use snafu::prelude::*;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::time::{timeout, Duration};
use tracing::{info, warn};
use uuid::Uuid;

const MARKET_MAKER_VALIDATION_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Snafu)]
pub enum SwapError {
    #[snafu(display("Bad refund request: {}", reason))]
    BadRefundRequest { reason: String },

    #[snafu(display("Quote not found: {}", quote_id))]
    QuoteNotFound { quote_id: Uuid },

    #[snafu(display("Quote has expired"))]
    QuoteExpired,

    #[snafu(display("Market maker rejected the quote"))]
    MarketMakerRejected,

    #[snafu(display("Market maker not connected: {}", market_maker_id))]
    MarketMakerNotConnected { market_maker_id: String },

    #[snafu(display("Market maker not in good standing: {}", market_maker_id))]
    MarketMakerNotInGoodStanding { market_maker_id: String },

    #[snafu(display("Market maker validation timeout"))]
    MarketMakerValidationTimeout,

    #[snafu(display("Database error: {}", source))]
    Database { source: OtcServerError },

    #[snafu(display("Chain not supported: {:?}", chain))]
    ChainNotSupported { chain: otc_models::ChainType },

    #[snafu(display("Failed to derive wallet: {}", source))]
    WalletDerivation { source: otc_chains::Error },

    #[snafu(display("Invalid EVM account address: {}", source))]
    InvalidEvmAccountAddress { source: FromHexError },

    #[snafu(display("Invalid destination address: {} for chain {:?}", address, chain))]
    InvalidDestinationAddress {
        address: String,
        chain: otc_models::ChainType,
    },

    #[snafu(display("Invalid refund attempt: {}", reason))]
    InvalidRefundAttempt { reason: String },

    #[snafu(display("Invalid metadata: {}", reason))]
    InvalidMetadata { reason: String },

    #[snafu(display("Failed to dump to address: {}", err))]
    DumpToAddress { err: String },
}

impl From<OtcServerError> for SwapError {
    fn from(err: OtcServerError) -> Self {
        match err {
            OtcServerError::NotFound => SwapError::QuoteNotFound {
                quote_id: Uuid::nil(), // We don't have the ID here
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
    #[must_use]
    pub fn new(
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

    pub async fn refund_swap(
        &self,
        swap_id: Uuid,
    ) -> SwapResult<RefundSwapResponse> {
        let swap = self.db.swaps().get(swap_id).await.context(DatabaseSnafu)?;

        match swap.can_be_refunded() {
            Some(reason) => {
                // Atomically set the swap to RefundingUser before proceeding. This prevents the
                // monitoring service from progressing the swap to further states during the refund.
                // If the swap is already RefundingUser (from a previous refund attempt), skip this step.
                if !matches!(swap.status, SwapStatus::RefundingUser) { 
                    self.db.swaps().mark_swap_as_refunding_user(swap_id, swap.status).await.context(DatabaseSnafu)?;
                }
                let deposit_chain = self
                    .chain_registry
                    .get(&swap.quote.from.currency.chain)
                    .ok_or(SwapError::ChainNotSupported {
                        chain: swap.quote.from.currency.chain,
                    })?;

                let deposit_wallet = deposit_chain
                    .derive_wallet(&self.settings.master_key_bytes(), &swap.deposit_vault_salt)
                    .map_err(|e| SwapError::WalletDerivation { source: e })?;


                let fee = match swap.quote.from.currency.chain { 
                    ChainType::Bitcoin => { 
                        let esplora = deposit_chain.esplora_client().ok_or(SwapError::BadRefundRequest {
                            reason: "Esplora client not available for Bitcoin chain".to_string(),
                        })?;

                        let next_block_fee_rate = esplora
                            .get_mempool_fee_estimate_next_block()
                            .await
                            .map_err(|e| SwapError::BadRefundRequest {
                                reason: format!("Failed to query mempool fee estimate: {e}"),
                            })?;
                        // 111 estimated vbytes for a refund tx that is P2WPKH
                        (next_block_fee_rate * 111 as f64).ceil() as u64
                    },
                    ChainType::Ethereum => 0,
                    ChainType::Base => 0,
                };

                let tx_data = deposit_chain
                    .dump_to_address(
                        &swap.quote.from.currency.token,
                        deposit_wallet.private_key(),
                        &swap.refund_address,
                        U256::from(fee),
                    )
                    .await
                    .map_err(|e| SwapError::DumpToAddress { err: e.to_string() })?;

                let latest_refund = LatestRefund {
                    timestamp: utc::now(),
                    recipient_address: swap.refund_address.clone(),
                };

                self.db
                    .swaps()
                    .update_latest_refund(swap_id, &latest_refund)
                    .await
                    .context(DatabaseSnafu)?;

                Ok(RefundSwapResponse {
                    swap_id,
                    reason,
                    tx_data,
                    tx_chain: swap.quote.from.currency.chain,
                })
            }
            None => InvalidRefundAttemptSnafu {
                reason: format!(
                    "Swap is not in a refundable state, current status: {:?}",
                    swap.status
                ),
            }
            .fail(),
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
    /// 6. Return the swap directly
    pub async fn create_swap(&self, request: CreateSwapRequest) -> SwapResult<Swap> {
        let CreateSwapRequest {
            quote,
            user_destination_address,
            refund_address,
            metadata,
        } = request;

        let metadata = metadata.unwrap_or_default();
        Self::validate_metadata(&metadata)?;

        // Ensure the user destination address is valid for the "to" chain.
        let destination_chain = self.chain_registry.get(&quote.to.currency.chain).ok_or(
            SwapError::ChainNotSupported {
                chain: quote.to.currency.chain,
            },
        )?;

        if !destination_chain.validate_address(&user_destination_address) {
            return Err(SwapError::InvalidDestinationAddress {
                address: user_destination_address,
                chain: quote.to.currency.chain,
            });
        }

        // Ensure the refund address is valid for the "from" chain.
        let refund_chain = self.chain_registry.get(&quote.from.currency.chain).ok_or(
            SwapError::ChainNotSupported {
                chain: quote.from.currency.chain,
            },
        )?;

        if !refund_chain.validate_address(&refund_address) {
            // TODO: This should be InvalidRefundAddress
            return Err(SwapError::InvalidDestinationAddress {
                address: refund_address,
                chain: quote.from.currency.chain,
            });
        }

        // 1. Check if quote has expired
        if quote.expires_at < utc::now() {
            return Err(SwapError::QuoteExpired);
        }

        // 2. Ask market maker if they'll fill this quote
        info!(
            "Validating quote {} with market maker {}",
            quote.id, quote.market_maker_id
        );

        // Check if MM is connected
        if !self.mm_registry.is_connected(quote.market_maker_id) {
            warn!(
                "Market maker {} not connected, rejecting swap",
                quote.market_maker_id
            );
            return Err(SwapError::MarketMakerNotConnected {
                market_maker_id: quote.market_maker_id.to_string(),
            });
        }

        // Enforce protocol fee good-standing gate.
        let now = utc::now();
        let is_good = self
            .db
            .fees()
            .is_good_standing(quote.market_maker_id, now)
            .await
            .map_err(SwapError::from)?;
        if !is_good {
            return Err(SwapError::MarketMakerNotInGoodStanding {
                market_maker_id: quote.market_maker_id.to_string(),
            });
        }

        // 3. Send validation request with timeout
        let (response_tx, response_rx) = oneshot::channel();
        self.mm_registry
            .validate_quote(
                &quote.market_maker_id,
                &quote.id,
                &quote.hash(),
                user_destination_address.as_str(),
                response_tx,
            )
            .await;

        // Wait for response with timeout
        let validation_result = match timeout(MARKET_MAKER_VALIDATION_TIMEOUT, response_rx).await {
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
        let swap_id = Uuid::now_v7();
        let mut deposit_vault_salt = [0u8; 32];
        let mut mm_nonce = [0u8; 16]; // 128 bits of collision resistance against an existing tx w/ a given output address && amount
        getrandom::getrandom(&mut deposit_vault_salt).expect("Failed to generate random salt");
        getrandom::getrandom(&mut mm_nonce).expect("Failed to generate random nonce");
        // 7. Derive user deposit address for response
        let user_chain = self.chain_registry.get(&quote.from.currency.chain).ok_or(
            SwapError::ChainNotSupported {
                chain: quote.from.currency.chain,
            },
        )?;

        let deposit_vault_address = &user_chain
            .derive_wallet(&self.settings.master_key_bytes(), &deposit_vault_salt)
            .map_err(|e| SwapError::WalletDerivation { source: e })?
            .address;

        // 6. Create swap record
        let now = utc::now();
        let swap = Swap {
            id: swap_id,
            quote: quote.clone(),
            market_maker_id: quote.market_maker_id,
            deposit_vault_salt,
            deposit_vault_address: deposit_vault_address.clone(),
            mm_nonce,
            metadata,
            realized: None, // Populated when user deposit is detected
            user_destination_address,
            refund_address,
            status: SwapStatus::WaitingUserDepositInitiated,
            user_deposit_status: None,
            mm_deposit_status: None,
            settlement_status: None,
            latest_refund: None,
            failure_reason: None,
            failure_at: None,
            mm_notified_at: None,
            mm_private_key_sent_at: None,
            created_at: now,
            updated_at: now,
        };

        // Save swap to database
        self.db.swaps().create(&swap).await.context(DatabaseSnafu)?;

        info!("Created swap {} for quote {}", swap_id, quote.id);

        Ok(swap)
    }

    /// Get swap details by ID
    pub async fn get_swap(&self, swap_id: Uuid) -> SwapResult<Swap> {
        self.db.swaps().get(swap_id).await.context(DatabaseSnafu)
    }

    fn validate_metadata(metadata: &Metadata) -> SwapResult<()> {
        if let Some(value) = metadata.start_asset.as_ref() {
            if value.chars().count() > 1000 {
                return InvalidMetadataSnafu {
                    reason: "start_asset must be 1000 characters or fewer".to_string(),
                }
                .fail();
            }
        }

        Ok(())
    }

    #[must_use]
    pub fn master_key_bytes(&self) -> [u8; 64] {
        self.settings.master_key_bytes()
    }
}
