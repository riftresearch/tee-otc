pub mod transaction_broadcaster;

use std::{str::FromStr, sync::Arc, time::Duration};

use alloy::{
    hex,
    network::{TransactionBuilder, TransactionBuilder7702},
    primitives::{Address, TxHash, U256},
    providers::{DynProvider, Provider},
    rpc::types::{Authorization, TransactionRequest},
    signers::{local::PrivateKeySigner, SignerSync},
    sol_types::SolValue,
};
use async_trait::async_trait;
use blockchain_utils::{create_receive_with_authorization_execution, WebsocketWalletProvider};
use eip3009_erc20_contract::GenericEIP3009ERC20::GenericEIP3009ERC20Instance;
use eip7702_delegator_contract::{
    EIP7702Delegator::{EIP7702DelegatorInstance, Execution},
    ModeCode, EIP7702_DELEGATOR_CROSSCHAIN_ADDRESS,
};
use otc_chains::traits::MarketMakerPaymentValidation;
use otc_models::{ChainType, Currency, Lot, TokenIdentifier};
use snafu::location;
use tokio::task::JoinSet;
use tracing::info;

use crate::{
    deposit_key_storage::{Deposit, DepositKeyStorage, DepositKeyStorageTrait, FillStatus},
    wallet::{Wallet, WalletBalance},
    WalletError, WalletResult,
};

pub struct EVMWallet {
    pub tx_broadcaster: transaction_broadcaster::EVMTransactionBroadcaster,
    provider: Arc<WebsocketWalletProvider>,
    deposit_key_storage: Option<Arc<DepositKeyStorage>>,
}

impl EVMWallet {
    pub fn new(
        provider: Arc<WebsocketWalletProvider>,
        debug_rpc_url: String,
        confirmations: u64,
        deposit_key_storage: Option<Arc<DepositKeyStorage>>,
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
            deposit_key_storage,
        }
    }
    pub async fn ensure_eip7702_delegation(
        &self,
        sender_signer: PrivateKeySigner,
    ) -> WalletResult<()> {
        let derived_address = sender_signer.address();
        if derived_address != self.tx_broadcaster.sender {
            return Err(WalletError::InvalidSender {
                expected: self.tx_broadcaster.sender,
                actual: derived_address,
            });
        }
        let sender = derived_address;
        let delegator_contract_address =
            Address::from_str(EIP7702_DELEGATOR_CROSSCHAIN_ADDRESS).unwrap();

        let code =
            self.provider
                .get_code_at(sender)
                .await
                .map_err(|e| WalletError::RpcCallError {
                    source: e,
                    loc: location!(),
                })?;

        let mut delegation_pattern = hex!("ef0100").to_vec();
        delegation_pattern.extend_from_slice(&delegator_contract_address.0 .0);

        if code.starts_with(&delegation_pattern) {
            info!("EOA already has the proper delegation, skipping");
            return Ok(());
        }

        info!("EOA does not have the proper delegation, sending tx...");

        let nonce = self
            .provider
            .get_transaction_count(sender)
            .await
            .map_err(|e| WalletError::RpcCallError {
                source: e,
                loc: location!(),
            })?;

        let chain_id = U256::from(self.provider.get_chain_id().await.map_err(|e| {
            WalletError::RpcCallError {
                source: e,
                loc: location!(),
            }
        })?);

        let authorization = Authorization {
            chain_id,
            address: delegator_contract_address,
            nonce: nonce + 1, // The +1 is important, otherwise nonce will be incorrect but the tx will still succeed
        };

        let signature = sender_signer
            .sign_hash_sync(&authorization.signature_hash())
            .map_err(|e| WalletError::SignatureFailed { source: e })?;

        let signed_authorization = authorization.into_signed(signature);
        let tx = TransactionRequest::default()
            .with_to(sender)
            .with_authorization_list(vec![signed_authorization]);

        // Send the transaction and wait for the broadcast.
        let pending_tx =
            self.provider
                .send_transaction(tx)
                .await
                .map_err(|e| WalletError::RpcCallError {
                    source: e,
                    loc: location!(),
                })?;

        // Wait for the transaction to be included and get the receipt.
        let receipt =
            pending_tx
                .get_receipt()
                .await
                .map_err(|e| WalletError::PendingTransactionError {
                    source: e,
                    loc: location!(),
                })?;
        info!("Receipt: {:?}", receipt);

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
    ) -> WalletResult<String> {
        if lot.currency.chain != ChainType::Ethereum {
            return Err(WalletError::UnsupportedToken {
                token: lot.currency.token.clone(),
                loc: location!(),
            });
        }
        ensure_valid_token(&lot.currency.token)?;
        let transaction_request = create_evm_transfer_transaction(
            &self.tx_broadcaster.sender,
            &self.provider,
            lot,
            to_address,
            mm_payment_validation,
            self.deposit_key_storage.clone(),
        )
        .await?;

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
        match broadcast_result {
            transaction_broadcaster::TransactionExecutionResult::Success(tx_receipt) => {
                info!("Payment created for swap [evm_wallet] {tx_receipt:?}");
                Ok(tx_receipt.transaction_hash.to_string())
            }
            _ => Err(WalletError::TransactionCreationFailed {
                reason: format!("{broadcast_result:?}"),
            }),
        }
    }

    async fn balance(&self, token: &TokenIdentifier) -> WalletResult<WalletBalance> {
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
        let native_balance =
            get_erc20_balance(&self.provider, &token_address, &self.tx_broadcaster.sender).await?;

        let mut net_deposit_key_balance = U256::from(0);

        if let Some(deposit_key_storage) = &self.deposit_key_storage {
            let deposit_key_bal = deposit_key_storage
                .balance(&Currency {
                    chain: ChainType::Ethereum,
                    token: token.clone(),
                    decimals: 8, //TODO(med): this should not be hardcoded
                })
                .await
                .map_err(|e| WalletError::BalanceCheckFailed {
                    source: Box::new(e),
                })?;
            net_deposit_key_balance += deposit_key_bal;
        }

        let total_balance = native_balance.saturating_add(net_deposit_key_balance);

        Ok(WalletBalance {
            total_balance,
            native_balance,
            deposit_key_balance: net_deposit_key_balance,
        })
    }

    fn chain_type(&self) -> ChainType {
        ChainType::Ethereum
    }

    fn receive_address(&self, _token: &TokenIdentifier) -> String {
        self.tx_broadcaster.sender.to_string()
    }

    // TODO(high): This function should rebroadcast the transaction if the transaction does not have any confirmations after a certain number of retries
    async fn guarantee_confirmations(
        &self,
        tx_hash: &str,
        confirmations: u64,
    ) -> Result<(), WalletError> {
        let tx_hash = TxHash::from_str(tx_hash).map_err(|e| WalletError::ParseAddressFailed {
            context: e.to_string(),
        })?;
        loop {
            let tx_receipt = self
                .provider
                .get_transaction_receipt(tx_hash)
                .await
                .map_err(|e| WalletError::RpcCallError {
                    source: e,
                    loc: location!(),
                })?;
            match tx_receipt {
                Some(tx_receipt) => {
                    let current_block_height =
                        self.provider.get_block_number().await.map_err(|e| {
                            WalletError::RpcCallError {
                                source: e,
                                loc: location!(),
                            }
                        })?;
                    if tx_receipt.block_number.unwrap() + confirmations <= current_block_height {
                        break;
                    }
                }
                None => {
                    tokio::time::sleep(Duration::from_secs(12)).await;
                }
            }
        }

        Ok(())
    }
}

async fn get_erc20_balance(
    provider: &Arc<WebsocketWalletProvider>,
    token_address: &Address,
    address: &Address,
) -> WalletResult<U256> {
    let token_contract = GenericEIP3009ERC20Instance::new(*token_address, provider.clone());
    let balance = token_contract
        .balanceOf(*address)
        .call()
        .await
        .map_err(|e| WalletError::GetErc20BalanceFailed {
            context: e.to_string(),
        })?;
    Ok(balance)
}

async fn create_evm_transfer_transaction(
    sender: &Address,
    provider: &Arc<WebsocketWalletProvider>,
    lot: &Lot,
    to_address: &str,
    mm_payment_validation: Option<MarketMakerPaymentValidation>,
    deposit_key_storage: Option<Arc<DepositKeyStorage>>,
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

            let token_contract = GenericEIP3009ERC20Instance::new(token_address, provider);
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

            let erc20_funding_executions = match deposit_key_storage {
                Some(deposit_key_storage) => {
                    // If this is set, we have the option to try to fill this request with funds
                    // stored in the deposit key storage. Technically, this looks like adding some `Execution`s
                    // to the `delegator_executions` vector, that will add funds from the deposit key storage
                    // the the market maker address
                    let fill_status = deposit_key_storage
                        .take_deposits_that_fill_lot(lot)
                        .await
                        .map_err(|e| WalletError::DepositKeyStorageError {
                            source: e,
                            loc: location!(),
                        })?;

                    let provider = provider.clone().erased();
                    match fill_status {
                        FillStatus::Full(deposits) | FillStatus::Partial(deposits) => {
                            let mut executions = Vec::new();
                            for deposit in deposits.iter() {
                                // Move funds from the deposit key wallet to the MM sender address
                                // so the subsequent payment transfer can be covered.
                                let execution = deposit
                                    .to_authorized_erc20_transfer(&provider, sender)
                                    .await?;
                                executions.push(execution);
                            }
                            executions
                        }
                        FillStatus::Empty => {
                            vec![]
                        }
                    }
                }
                None => {
                    vec![]
                }
            };
            let payment_executions = recipients
                .iter()
                .zip(amounts.iter())
                .map(|(recipient, amount)| {
                    let calldata = token_contract
                        .transfer(*recipient, *amount)
                        .calldata()
                        .clone();
                    let target = token_address;
                    let value = U256::from(0);
                    Execution {
                        target,
                        value,
                        callData: calldata,
                    }
                })
                .collect::<Vec<_>>();

            let executions = [erc20_funding_executions, payment_executions].concat();

            println!("executions: {executions:?}");

            let delegator_contract = EIP7702DelegatorInstance::new(
                Address::from_str(EIP7702_DELEGATOR_CROSSCHAIN_ADDRESS).unwrap(),
                provider,
            );

            let mut transaction_request = delegator_contract
                .execute_1(
                    ModeCode::Batch.as_fixed_bytes32(),
                    executions.abi_encode().into(),
                )
                .into_transaction_request();

            // B/c of EIP7702, we need to set the `to` to the actual broadcast address
            transaction_request.set_to(*sender);

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

trait DepositToAuthorizedERC20Transfer {
    async fn to_authorized_erc20_transfer(
        &self,
        provider: &DynProvider,
        recipient: &Address,
    ) -> Result<Execution, WalletError>;
}

impl DepositToAuthorizedERC20Transfer for Deposit {
    async fn to_authorized_erc20_transfer(
        &self,
        provider: &DynProvider,
        recipient: &Address,
    ) -> Result<Execution, WalletError> {
        let lot_signer = PrivateKeySigner::from_str(&self.private_key).map_err(|e| {
            WalletError::InvalidDescriptor {
                reason: e.to_string(),
                loc: location!(),
            }
        })?;
        create_receive_with_authorization_execution(
            &self.holdings,
            &lot_signer,
            provider,
            recipient,
        )
        .await
        .map_err(|e| WalletError::ReceiveAuthorizationFailed {
            source: e,
            loc: location!(),
        })
    }
}
