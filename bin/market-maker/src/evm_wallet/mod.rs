pub mod transaction_broadcaster;

use std::sync::Arc;

use alloy::{
    network::TransactionBuilder,
    primitives::{Address, U256},
    rpc::types::TransactionRequest,
};
use async_trait::async_trait;
use common::{GenericERC20::GenericERC20Instance, WebsocketWalletProvider};
use otc_models::{ChainType, Currency, TokenIdentifier};
use tokio::task::JoinSet;
use tracing::info;

use crate::wallet::{self, Wallet, WalletError};

pub struct EVMWallet {
    pub tx_broadcaster: transaction_broadcaster::EVMTransactionBroadcaster,
    provider: Arc<WebsocketWalletProvider>,
}

const BALANCE_BUFFER_PERCENT: u8 = 25; // 25% buffer

impl EVMWallet {
    pub fn new(
        provider: Arc<WebsocketWalletProvider>,
        debug_rpc_url: String,
        confirmations: u64,
        join_set: &mut JoinSet<transaction_broadcaster::Result<()>>,
    ) -> Self {
        let tx_broadcaster = transaction_broadcaster::EVMTransactionBroadcaster::new(
            provider.clone(),
            debug_rpc_url,
            confirmations,
            join_set,
        );
        Self {
            tx_broadcaster,
            provider,
        }
    }
}

#[async_trait]
impl Wallet for EVMWallet {
    async fn create_transaction(
        &self,
        currency: &Currency,
        to_address: &str,
        nonce: Option<[u8; 16]>,
    ) -> wallet::Result<String> {
        ensure_valid_currency(currency)?;
        let transaction_request =
            create_evm_transfer_transaction(&self.provider, currency, to_address, nonce)?;

        let broadcast_result = self
            .tx_broadcaster
            .broadcast_transaction(
                transaction_request,
                transaction_broadcaster::PreflightCheck::Simulate,
            )
            .await
            .map_err(|e| WalletError::TransactionCreationFailed {
                reason: e.to_string(),
            })?;
        // we need a method to get some erc20 calldata
        match broadcast_result {
            transaction_broadcaster::TransactionExecutionResult::Success(tx_receipt) => {
                Ok(tx_receipt.transaction_hash.to_string())
            }
            _ => Err(WalletError::TransactionCreationFailed {
                reason: format!("{broadcast_result:?}"),
            }),
        }
    }

    async fn can_fill(&self, currency: &Currency) -> wallet::Result<bool> {
        // TODO: This check should also include a check that we can pay for gas
        if ensure_valid_currency(currency).is_err() {
            return Ok(false);
        }
        
        let token_address = match &currency.token {
            TokenIdentifier::Native => return Ok(false), // native tokens are not supported for now
            TokenIdentifier::Address(address) => {
                address
                    .parse::<Address>()
                    .map_err(|_| WalletError::ParseAddressFailed {
                        context: "invalid token address".to_string(),
                    })?
            }
        };
        let balance =
            get_erc20_balance(&self.provider, &token_address, &self.tx_broadcaster.sender)
                .await?;
        let required_balance = balance_with_buffer(currency.amount);
        Ok(balance > required_balance)
    }
}

async fn get_erc20_balance(
    provider: &Arc<WebsocketWalletProvider>,
    token_address: &Address,
    address: &Address,
) -> wallet::Result<U256> {
    let token_contract = GenericERC20Instance::new(*token_address, provider.clone());
    let balance = token_contract
        .balanceOf(*address)
        .call()
        .await
        .map_err(|e| WalletError::GetErc20BalanceFailed {
            context: e.to_string(),
        })?;
    Ok(balance)
}

fn create_evm_transfer_transaction(
    provider: &Arc<WebsocketWalletProvider>,
    currency: &Currency,
    to_address: &str,
    nonce: Option<[u8; 16]>,
) -> Result<TransactionRequest, WalletError> {
    match &currency.token {
        TokenIdentifier::Native => unimplemented!(),
        TokenIdentifier::Address(address) => {
            let token_address =
                address
                    .parse::<Address>()
                    .map_err(|_| WalletError::ParseAddressFailed {
                        context: "invalid token address".to_string(),
                    })?;
            let to_address =
                to_address
                    .parse::<Address>()
                    .map_err(|_| WalletError::ParseAddressFailed {
                        context: "invalid to address".to_string(),
                    })?;
            let token_contract = GenericERC20Instance::new(token_address, provider);
            let transfer = token_contract.transfer(to_address, currency.amount);
            let mut transaction_request = transfer.into_transaction_request();

            // Add nonce to the end of calldata if provided
            if let Some(nonce) = nonce {
                // Audit: Consider how this could be problematic if done with arbitrary addresses (not whitelisted)
                let mut calldata_with_nonce = transaction_request
                    .input
                    .input()
                    .to_owned()
                    .unwrap()
                    .to_vec();
                calldata_with_nonce.extend_from_slice(&nonce);
                transaction_request.set_input(calldata_with_nonce);
                transaction_request.set_input_and_data();
            }
            info!("transaction_request: {:?}", transaction_request);
            Ok(transaction_request)
        }
    }
}

fn ensure_valid_currency(currency: &Currency) -> Result<(), WalletError> {
    if !matches!(currency.chain, ChainType::Ethereum)
        || !otc_models::SUPPORTED_TOKENS_BY_CHAIN
            .get(&currency.chain)
            .unwrap()
            .contains(&currency.token)
    {
        return Err(WalletError::UnsupportedCurrency {
            currency: currency.clone(),
        });
    }
    info!("currency is valid: {:?}", currency);
    Ok(())
}

fn balance_with_buffer(balance: U256) -> U256 {
    balance + (balance * U256::from(BALANCE_BUFFER_PERCENT)) / U256::from(100_u8)
}
