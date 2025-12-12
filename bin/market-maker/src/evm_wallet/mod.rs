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
use blockchain_utils::{WebsocketWalletProvider, create_transfer_with_authorization_execution};
use eip3009_erc20_contract::GenericEIP3009ERC20::GenericEIP3009ERC20Instance;
use eip7702_delegator_contract::{
    EIP7702Delegator::{EIP7702DelegatorInstance, Execution},
    ModeCode, EIP7702_DELEGATOR_CROSSCHAIN_ADDRESS,
};
use otc_chains::traits::{MarketMakerPaymentVerification, Payment};
use otc_models::{ChainType, Currency, Lot, TokenIdentifier};
use snafu::location;
use tokio::task::JoinSet;
use tracing::{debug, info};

use crate::{
    db::{Deposit, DepositRepository, DepositStore, FillStatus},
    wallet::{Wallet, WalletBalance},
    WalletError, WalletResult,
};

pub struct EVMWallet {
    pub tx_broadcaster: transaction_broadcaster::EVMTransactionBroadcaster,
    pub provider: Arc<WebsocketWalletProvider>,
    deposit_repository: Option<Arc<DepositRepository>>,
    max_deposits_per_lot: usize,
    chain_type: ChainType,
}

impl EVMWallet {
    pub fn new(
        provider: Arc<WebsocketWalletProvider>,
        debug_rpc_url: String,
        confirmations: u64,
        chain_type: ChainType,
        deposit_repository: Option<Arc<DepositRepository>>,
        max_deposits_per_lot: usize,
        join_set: &mut JoinSet<crate::Result<()>>,
    ) -> Self {
        let tx_broadcaster = transaction_broadcaster::EVMTransactionBroadcaster::new(
            provider.clone(),
            debug_rpc_url,
            confirmations,
            join_set,
        );

        if chain_type != ChainType::Ethereum && chain_type != ChainType::Base {
            panic!("Unsupported chain type for EVM wallet: {:?}", chain_type);
        }

        Self {
            tx_broadcaster,
            provider,
            deposit_repository,
            max_deposits_per_lot,
            chain_type,
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
        info!(
            "Receipt: tx={}, status={:?}, gas_used={:?}",
            receipt.transaction_hash,
            receipt.status(),
            receipt.gas_used
        );

        Ok(())
    }
}

#[async_trait]
impl Wallet for EVMWallet {
    async fn cancel_tx(&self, _tx_hash: &str) -> WalletResult<String> {
        unimplemented!()
    }

    async fn check_tx_confirmations(&self, _tx_hash: &str) -> WalletResult<u64> {
        // Mock implementation - always return that the transaction is confirmed
        // This is acceptable since Ethereum batches aren't relevant for the batch monitor
        Ok(u64::MAX)
    }

    async fn create_batch_payment(
        &self,
        payments: Vec<Payment>,
        mm_payment_validation: Option<MarketMakerPaymentVerification>,
    ) -> WalletResult<String> {
        let first_payment = &payments[0];
        if first_payment.lot.currency.chain != self.chain_type {
            return Err(WalletError::UnsupportedToken {
                token: first_payment.lot.currency.token.clone(),
                loc: location!(),
            });
        }
        ensure_valid_token(self.chain_type, &first_payment.lot.currency.token)?;
        // now make sure all payments lots currencies are the same
        for payment in &payments {
            if payment.lot.currency != first_payment.lot.currency {
                return Err(WalletError::InvalidBatchPaymentRequest { loc: location!() });
            }
        }

        let mut funding_executions = Vec::new();
        let mut reserved_deposit_keys = Vec::new();
        
        if let Some(deposit_repository) = &self.deposit_repository {
            for payment in &payments {
                let (consolidation_executions, fill_status) = get_funding_executions_from_deposits(
                    deposit_repository,
                    &payment.lot,
                    &self.provider,
                    &self.tx_broadcaster.sender,
                    self.max_deposits_per_lot,
                )
                .await?;

                // Track deposit keys for potential unreserving on failure
                match fill_status {
                    FillStatus::Full(deposits) | FillStatus::Partial(deposits) => {
                        reserved_deposit_keys.extend(
                            deposits.iter().map(|d| d.private_key.clone())
                        );
                    }
                    FillStatus::Empty => {}
                }

                funding_executions.extend(consolidation_executions);
                if funding_executions.len() >= self.max_deposits_per_lot {
                    // roughly limit number of funding executions for this batch 
                    break;
                }
            }
        };

        let transaction_request = create_evm_transfer_transaction(
            self.chain_type,
            &self.tx_broadcaster.sender,
            &self.provider,
            payments,
            mm_payment_validation,
            funding_executions,
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
                info!(
                    "Payment created for swap [evm_wallet] tx={}, status={:?}, gas_used={:?}",
                    tx_receipt.transaction_hash,
                    tx_receipt.status(),
                    tx_receipt.gas_used
                );
                Ok(tx_receipt.transaction_hash.to_string())
            }
            _ => {
                // Broadcast failed - unreserve the deposits so they can be used again
                if !reserved_deposit_keys.is_empty() {
                    if let Some(deposit_repository) = &self.deposit_repository {
                        match deposit_repository.unreserve_deposits(&reserved_deposit_keys).await {
                            Ok(count) => {
                                tracing::warn!(
                                    "Unreserved {} deposits after broadcast failure: {:?}",
                                    count,
                                    broadcast_result
                                );
                            }
                            Err(unreserve_err) => {
                                tracing::error!(
                                    "Failed to unreserve {} deposits after broadcast failure. \
                                    Original error: {:?}, Unreserve error: {:?}",
                                    reserved_deposit_keys.len(),
                                    broadcast_result,
                                    unreserve_err
                                );
                            }
                        }
                    }
                }
                Err(WalletError::TransactionCreationFailed {
                    reason: format!("{broadcast_result:?}"),
                })
            }
        }
    }

    async fn balance(&self, token: &TokenIdentifier) -> WalletResult<WalletBalance> {
        match token {
            TokenIdentifier::Native => {
                let native_balance = self
                    .provider
                    .get_balance(self.tx_broadcaster.sender)
                    .await
                    .map_err(|e| WalletError::BalanceCheckFailed {
                        source: Box::new(e),
                    })?;

                Ok(WalletBalance {
                    total_balance: native_balance,
                    native_balance,
                    deposit_key_balance: U256::from(0),
                })
            }
            TokenIdentifier::Address(address) => {
                // TODO: This check should also include a check that we can pay for gas
                ensure_valid_token(self.chain_type, token)?;

                let token_address = match address.parse::<Address>() {
                    Ok(addr) => addr,
                    Err(_) => {
                        return Err(WalletError::UnsupportedToken {
                            token: token.clone(),
                            loc: location!(),
                        });
                    }
                };

                let native_balance =
                    get_erc20_balance(&self.provider, &token_address, &self.tx_broadcaster.sender)
                        .await?;

                let mut net_deposit_key_balance = U256::from(0);

                if let Some(deposit_repository) = &self.deposit_repository {
                    let deposit_key_bal = deposit_repository
                        .balance(&Currency {
                            chain: self.chain_type,
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
        }
    }

    async fn consolidate(
        &self,
        lot: &otc_models::Lot,
        max_deposits_per_iteration: usize,
    ) -> WalletResult<crate::wallet::ConsolidationSummary> {
        let deposit_repository = self.deposit_repository.as_ref().ok_or_else(|| {
            WalletError::TransactionCreationFailed {
                reason: "No deposit repository available for consolidation".to_string(),
            }
        })?;

        let mut total_amount = U256::from(0);
        let mut iterations = 0;
        let mut tx_hashes = Vec::new();

        loop {
            // Acquire funding executions from deposit storage if available

            let (consolidation_executions, fill_status) = get_funding_executions_from_deposits(
                deposit_repository,
                &lot,
                &self.provider,
                &self.tx_broadcaster.sender,
                max_deposits_per_iteration,
            )
            .await?;

            if consolidation_executions.is_empty() {
                break;
            }

            let deposits = match &fill_status {
                FillStatus::Empty => break,
                FillStatus::Full(deposits) | FillStatus::Partial(deposits) => {
                    deposits
                }
            };
            
            // Track deposit keys for potential unreserving on failure
            let reserved_deposit_keys: Vec<String> = deposits
                .iter()
                .map(|d| d.private_key.clone())
                .collect();
 
            // Sum the amount in this batch
            let mut batch_amount = U256::from(0);
            for deposit in deposits {
                batch_amount = batch_amount.saturating_add(deposit.holdings.amount);
            }
            total_amount = total_amount.saturating_add(batch_amount);

            // Create a consolidation payment to our own address
            let receive_address = self.receive_address(&lot.currency.token);
            let payment = Payment {
                lot: Lot {
                    currency: lot.currency.clone(),
                    amount: batch_amount,
                },
                to_address: receive_address,
            };

            info!(
                "Consolidating {} deposits with total amount {} to {}",
                deposits.len(),
                batch_amount,
                payment.to_address
            );

            // Create and broadcast the consolidation transaction
            let transaction_request = create_evm_transfer_transaction(
                self.chain_type,
                &self.tx_broadcaster.sender,
                &self.provider,
                vec![payment],
                None, // No MM validation for consolidation
                consolidation_executions,
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
                    let tx_hash = tx_receipt.transaction_hash.to_string();
                    info!("Consolidation transaction successful: {}", tx_hash);
                    tx_hashes.push(tx_hash);
                    iterations += 1;
                }
                _ => {
                    // Broadcast failed - unreserve the deposits so they can be used again
                    match deposit_repository.unreserve_deposits(&reserved_deposit_keys).await {
                        Ok(count) => {
                            tracing::warn!(
                                "Unreserved {} deposits after consolidation broadcast failure: {:?}",
                                count,
                                broadcast_result
                            );
                        }
                        Err(unreserve_err) => {
                            tracing::error!(
                                "Failed to unreserve {} deposits after consolidation broadcast failure. \
                                Original error: {:?}, Unreserve error: {:?}",
                                reserved_deposit_keys.len(),
                                broadcast_result,
                                unreserve_err
                            );
                        }
                    }
                    return Err(WalletError::TransactionCreationFailed {
                        reason: format!("Consolidation broadcast failed: {broadcast_result:?}"),
                    });
                }
            }
        }

        Ok(crate::wallet::ConsolidationSummary {
            total_amount,
            iterations,
            tx_hashes,
        })
    }

    fn chain_type(&self) -> ChainType {
        self.chain_type
    }

    fn receive_address(&self, _token: &TokenIdentifier) -> String {
        self.tx_broadcaster.sender.to_string()
    }

    // TODO(high): This function should rebroadcast the transaction if the transaction does not have any confirmations after a certain number of retries
    async fn guarantee_confirmations(
        &self,
        tx_hash: &str,
        confirmations: u64,
        poll_interval: Duration,
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
                    tokio::time::sleep(poll_interval).await;
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

/// Attempts to acquire funding executions from deposit key storage to cover the lot.
/// Returns executions that transfer funds from deposit keys to the sender address.
async fn get_funding_executions_from_deposits(
    deposit_repository: &Arc<DepositRepository>,
    lot: &Lot,
    provider: &Arc<WebsocketWalletProvider>,
    sender: &Address,
    max_funding_deposits: usize,
) -> Result<(Vec<Execution>, FillStatus), WalletError> {
    let fill_status = deposit_repository
        .take_deposits_that_fill_lot(lot, Some(max_funding_deposits))
        .await
        .map_err(|e| WalletError::DepositRepositoryError {
            source: e,
            loc: location!(),
        })?;

    let provider = provider.clone().erased();
    let executions = match &fill_status {
        FillStatus::Full(deposits) | FillStatus::Partial(deposits) => {
            let mut executions = Vec::with_capacity(deposits.len());
            for deposit in deposits.iter() {
                // Move funds from the deposit key wallet to the MM sender address
                // so the subsequent payment transfer can be covered.
                let execution = deposit
                    .to_authorized_erc20_transfer(&provider, sender)
                    .await?;
                executions.push(execution);
            }
            Ok(executions)
        }
        FillStatus::Empty => Ok(Vec::new()),
    }?;
    Ok((executions, fill_status))
}

/// Creates payment executions for transferring tokens to recipients.
pub fn create_payment_executions(
    token_contract: &GenericEIP3009ERC20Instance<DynProvider>,
    sender: &Address,
    recipients: &[Address],
    amounts: &[U256],
) -> Vec<Execution> {
    let token_address = *token_contract.address();
    recipients
        .iter()
        .zip(amounts.iter())
        // filter out any self-transfers, may occur during consolidation
        .filter(|(recipient, _)| **recipient != *sender)
        .map(|(recipient, amount)| {
            let calldata = token_contract
                .transfer(*recipient, *amount)
                .calldata()
                .clone();
            Execution {
                target: token_address,
                value: U256::ZERO,
                callData: calldata,
            }
        })
        .collect()
}

/// Builds the final transaction request with delegator contract execution.
/// Applies mm_payment_validation nonce if provided.
pub fn build_transaction_with_validation(
    sender: &Address,
    provider: DynProvider,
    executions: Vec<Execution>,
    mm_payment_validation: Option<&MarketMakerPaymentVerification>,
) -> Result<TransactionRequest, WalletError> {
    debug!("executions: {executions:?}");

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
    if let Some(mm_payment_validation) = mm_payment_validation {
        let nonce = mm_payment_validation.batch_nonce_digest;
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

pub async fn create_evm_transfer_transaction(
    chain_type: ChainType,
    sender: &Address,
    provider: &Arc<WebsocketWalletProvider>,
    payments: Vec<Payment>,
    mm_payment_validation: Option<MarketMakerPaymentVerification>,
    additional_funding_executions: Vec<Execution>,
) -> Result<TransactionRequest, WalletError> {
    // TODO: Temporary requirement that all tokens are the same in a batch
    for (i, payment) in payments.iter().enumerate() {
        if i == 0 {
            continue;
        }
        if payment.lot.currency != payments[i - 1].lot.currency {
            return Err(WalletError::InvalidBatchPaymentRequest { loc: location!() });
        }
    }

    // -> extension of the above TODO, since we know all payments are to the same lot, the following is safe
    match &payments[0].lot.currency.token {
        TokenIdentifier::Native => unimplemented!(),
        TokenIdentifier::Address(address) => {
            let token_address =
                address
                    .parse::<Address>()
                    .map_err(|_| WalletError::ParseAddressFailed {
                        context: "invalid token address".to_string(),
                    })?;

            let mut recipients = payments
                .iter()
                .map(|payment| {
                    payment.to_address.parse::<Address>().map_err(|_| {
                        WalletError::ParseAddressFailed {
                            context: format!(
                                "invalid recipient {} when parsing address for payment",
                                payment.to_address
                            ),
                        }
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            let fee_address =
                Address::from_str(&otc_models::FEE_ADDRESSES_BY_CHAIN[&chain_type])
                    .unwrap();

            let mut amounts: Vec<U256> =
                payments.iter().map(|payment| payment.lot.amount).collect();

            // Determine recipients and amounts based on whether we have payment validation
            if let Some(validation) = &mm_payment_validation {
                recipients.push(fee_address);
                amounts.push(validation.aggregated_fee);
            }



            // Create payment executions for transferring tokens
            let token_contract =
                GenericEIP3009ERC20Instance::new(token_address, provider.clone().erased());
            let payment_executions =
                create_payment_executions(&token_contract, sender, &recipients, &amounts);

            // Combine funding and payment executions
            let executions = [additional_funding_executions, payment_executions].concat();

            // Build final transaction with validation
            build_transaction_with_validation(
                sender,
                provider.clone().erased(),
                executions,
                mm_payment_validation.as_ref(),
            )
        }
    }
}

fn ensure_valid_token(chain_type: ChainType, token: &TokenIdentifier) -> Result<(), WalletError> {
    if !otc_models::SUPPORTED_TOKENS_BY_CHAIN
        .get(&chain_type)
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
        create_transfer_with_authorization_execution(
            &self.holdings,
            &lot_signer,
            provider,
            recipient,
        )
        .await
        .map_err(|e| WalletError::TransferAuthorizationFailed {
            source: e,
            loc: location!(),
        })
    }
}
