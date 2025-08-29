pub mod transaction_broadcaster;

use std::{str::FromStr, sync::Arc};

use alloy::{
    network::TransactionBuilder,
    primitives::{Address, U256},
    providers::Provider,
    rpc::types::TransactionRequest,
};
use async_trait::async_trait;
use blockchain_utils::{GenericERC20::GenericERC20Instance, WebsocketWalletProvider};
use disperse_contract::Disperse::DisperseInstance;
use otc_chains::traits::MarketMakerPaymentValidation;
use otc_models::{ChainType, Currency, Lot, TokenIdentifier};
use snafu::location;
use tokio::task::JoinSet;
use tracing::info;

use crate::wallet::{self, Wallet, WalletError};

pub struct EVMWallet {
    pub tx_broadcaster: transaction_broadcaster::EVMTransactionBroadcaster,
    provider: Arc<WebsocketWalletProvider>,
}

const BALANCE_BUFFER_PERCENT: u8 = 25; // 25% buffer
const DISPERSE_CONTRACT_ADDRESS: &str = "0xd152f549545093347A162DCE210e7293f1452150";

impl EVMWallet {
    pub fn new(
        provider: Arc<WebsocketWalletProvider>,
        debug_rpc_url: String,
        confirmations: u64,
        join_set: &mut JoinSet<crate::Result<()>>,
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
    pub async fn ensure_inf_approval_on_disperse(
        &self,
        token_address: &Address,
    ) -> wallet::Result<()> {
        let disperse_contract_address = Address::from_str(DISPERSE_CONTRACT_ADDRESS).unwrap();
        let token_contract = GenericERC20Instance::new(*token_address, self.provider.clone());
        let min_approval = U256::MAX / U256::from(2);
        let current_approval = token_contract
            .allowance(self.tx_broadcaster.sender, disperse_contract_address)
            .call()
            .await
            .map_err(|e| WalletError::GetErc20BalanceFailed {
                context: e.to_string(),
            })?;
        if min_approval > current_approval {
            info!(
                "Allowance on disperse contract is insufficient for token {token_address}, approving inf allowance...",
            );
            // Ensure at least
            let approve = token_contract.approve(
                Address::from_str(DISPERSE_CONTRACT_ADDRESS).unwrap(),
                U256::MAX,
            );
            let approve_tx = approve.into_transaction_request();
            let approve_tx_hash = self
                .tx_broadcaster
                .broadcast_transaction(
                    approve_tx,
                    transaction_broadcaster::PreflightCheck::Simulate,
                )
                .await?;
            info!(
                "Allowance increased to inf for contract: {token_address} for disperse contract tx hash: {:?}",
                approve_tx_hash
            );
        } else {
            info!("Allowance on disperse contract is sufficient for token {token_address}",);
        }
        Ok(())
    }
}

#[async_trait]
impl Wallet for EVMWallet {
    async fn create_payment(
        &self,
        lot: &Lot,
        to_address: &str,
        mm_payment_validation: Option<MarketMakerPaymentValidation>,
    ) -> wallet::Result<String> {
        if lot.currency.chain != ChainType::Ethereum {
            return Err(WalletError::UnsupportedToken {
                token: lot.currency.token.clone(),
                loc: location!(),
            });
        }
        ensure_valid_token(&lot.currency.token)?;
        let transaction_request = create_evm_transfer_transaction(
            &self.provider,
            lot,
            to_address,
            mm_payment_validation,
        )?;

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

    async fn balance(&self, token: &TokenIdentifier) -> wallet::Result<U256> {
        // TODO: This check should also include a check that we can pay for gas
        if ensure_valid_token(token).is_err() {
            return Err(WalletError::UnsupportedToken {
                token: token.clone(),
                loc: location!(),
            });
        }

        let token_address = match token {
            TokenIdentifier::Native => {
                return Err(WalletError::UnsupportedToken {
                    token: token.clone(),
                    loc: location!(),
                });
            }
            TokenIdentifier::Address(address) => {
                let res = address.parse::<Address>();
                if res.is_err() {
                    return Err(WalletError::UnsupportedToken {
                        token: token.clone(),
                        loc: location!(),
                    });
                }
                res.unwrap()
            }
        };
        let balance =
            get_erc20_balance(&self.provider, &token_address, &self.tx_broadcaster.sender).await?;
        Ok(balance)
    }

    fn chain_type(&self) -> ChainType {
        ChainType::Ethereum
    }

    fn receive_address(&self, _token: &TokenIdentifier) -> String {
        self.tx_broadcaster.sender.to_string()
    }

    async fn guarantee_confirmations(
        &self,
        tx_hash: &str,
        confirmations: u64,
    ) -> Result<(), WalletError> {
        // TODO(high): implement this
        Ok(())
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
    lot: &Lot,
    to_address: &str,
    mm_payment_validation: Option<MarketMakerPaymentValidation>,
) -> Result<TransactionRequest, WalletError> {
    match &lot.currency.token {
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

            let fee_address =
                Address::from_str(&otc_models::FEE_ADDRESSES_BY_CHAIN[&ChainType::Ethereum])
                    .unwrap();

            let token_contract = DisperseInstance::new(
                Address::from_str(DISPERSE_CONTRACT_ADDRESS).unwrap(),
                provider,
            );
            let recipients = match mm_payment_validation.is_some() {
                true => vec![to_address, fee_address],
                false => vec![to_address],
            };
            let amounts = match mm_payment_validation.is_some() {
                true => {
                    let fee_amount = mm_payment_validation.as_ref().unwrap().fee_amount;
                    vec![lot.amount, fee_amount]
                }
                false => vec![lot.amount],
            };
            let transfer = token_contract.disperseTokenSimple(token_address, recipients, amounts);
            let mut transaction_request = transfer.into_transaction_request();

            // Add nonce to the end of calldata if provided
            if let Some(mm_payment_validation) = &mm_payment_validation {
                let nonce = mm_payment_validation.embedded_nonce;
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

fn ensure_valid_token(token: &TokenIdentifier) -> Result<(), WalletError> {
    if !otc_models::SUPPORTED_TOKENS_BY_CHAIN
        .get(&ChainType::Ethereum)
        .unwrap()
        .contains(token)
    {
        return Err(WalletError::UnsupportedToken {
            token: token.clone(),
            loc: location!(),
        });
    }
    Ok(())
}

fn balance_with_buffer(balance: U256) -> U256 {
    balance + (balance * U256::from(BALANCE_BUFFER_PERCENT)) / U256::from(100_u8)
}
