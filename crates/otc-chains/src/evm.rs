use crate::traits::{
    FeeSettlementVerification, MarketMakerBatch, UserDepositCandidateStatus, VerifiedUserDeposit,
};
use crate::{key_derivation, ChainOperations, Result};
use alloy::consensus::Transaction;
use alloy::primitives::{Address, Signature, TxHash, B256, U256};
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use alloy::rpc::types::{Log as RpcLog, TransactionReceipt};
use alloy::signers::local::{LocalSigner, PrivateKeySigner};
use alloy::{hex, sol};
use async_trait::async_trait;
use blockchain_utils::create_transfer_with_authorization_execution;
use eip3009_erc20_contract::GenericEIP3009ERC20::GenericEIP3009ERC20Instance;
use otc_models::{
    ChainType, ConfirmedTxStatus, Currency, Lot, PendingTxStatus, TokenIdentifier, TxStatus, Wallet,
};
use snafu::location;
use std::str::FromStr;
use std::time::Duration;
use tracing::{debug, info, warn};

sol! {
    #[derive(Debug)]
    event Transfer(address indexed from, address indexed to, uint256 value);
}

sol! {
    #[derive(Debug)]
    #[sol(rpc)]
    interface IERC1271 {
        function isValidSignature(bytes32 hash, bytes signature) external view returns (bytes4 magicValue);
    }
}

const ERC1271_MAGIC_VALUE: [u8; 4] = [0x16, 0x26, 0xba, 0x7e];

pub struct EvmChain {
    provider: DynProvider,
    allowed_token: Address,
    chain_type: ChainType,
    wallet_seed_tag: Vec<u8>,
    min_confirmations: u32,
    est_block_time: Duration,
}

impl EvmChain {
    pub async fn new(
        rpc_url: &str,
        allowed_token: &str,
        chain_type: ChainType,
        wallet_seed_tag: &[u8],
        min_confirmations: u32,
        est_block_time: Duration,
    ) -> Result<Self> {
        let url = rpc_url.parse().map_err(|_| crate::Error::Serialization {
            message: "Invalid RPC URL".to_string(),
        })?;

        let client = alloy::rpc::client::ClientBuilder::default()
            .layer(crate::rpc_metrics_layer::RpcMetricsLayer::new(
                chain_type.to_db_string().to_string(),
            ))
            .http(url);

        let provider = ProviderBuilder::new().connect_client(client).erased();
        let allowed_token =
            Address::from_str(allowed_token).map_err(|_| crate::Error::Serialization {
                message: "Invalid allowed token address".to_string(),
            })?;

        Ok(Self {
            provider,
            allowed_token,
            chain_type,
            wallet_seed_tag: wallet_seed_tag.to_vec(),
            min_confirmations,
            est_block_time,
        })
    }

    pub async fn verify_participant_signature(
        &self,
        signer_address: &str,
        digest: B256,
        signature_bytes: &[u8],
    ) -> Result<bool> {
        let signer =
            Address::from_str(signer_address).map_err(|_| crate::Error::Serialization {
                message: "Invalid participant signer address".to_string(),
            })?;

        if signature_bytes.len() == 65 {
            let signature =
                Signature::from_raw(signature_bytes).map_err(|_| crate::Error::Serialization {
                    message: "Invalid participant signature bytes".to_string(),
                })?;

            if signature
                .recover_address_from_prehash(&digest)
                .map(|recovered| recovered == signer)
                .unwrap_or(false)
            {
                return Ok(true);
            }
        }

        let code =
            self.provider
                .get_code_at(signer)
                .await
                .map_err(|e| crate::Error::EVMRpcError {
                    source: e,
                    loc: location!(),
                })?;
        if code.is_empty() {
            return Ok(false);
        }

        let contract = IERC1271::IERC1271Instance::new(signer, &self.provider);
        let result = contract
            .isValidSignature(digest, signature_bytes.to_vec().into())
            .call()
            .await;

        match result {
            Ok(response) => Ok(response.0 == ERC1271_MAGIC_VALUE),
            Err(_) => Ok(false),
        }
    }
}

#[async_trait]
impl ChainOperations for EvmChain {
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

        info!(
            "Created new {} wallet: {}",
            self.chain_type.to_db_string(),
            address
        );

        let wallet = Wallet::new(format!("{address:?}"), format!("0x{private_key}"));
        Ok((wallet, salt))
    }

    fn derive_wallet(&self, master_key: &[u8], salt: &[u8; 32]) -> Result<Wallet> {
        // Derive private key using HKDF
        let private_key_bytes =
            key_derivation::derive_private_key(master_key, salt, &self.wallet_seed_tag)?;

        // Create signer from derived key
        let signer = PrivateKeySigner::from_bytes(&private_key_bytes.into()).map_err(|_| {
            crate::Error::Serialization {
                message: "Failed to create signer from derived key".to_string(),
            }
        })?;

        let address = format!("{:?}", signer.address());
        let private_key = format!("0x{}", alloy::hex::encode(private_key_bytes));

        debug!(
            "Derived {} wallet: {}",
            self.chain_type.to_db_string(),
            address
        );

        Ok(Wallet::new(address, private_key))
    }

    async fn get_tx_status(&self, tx_hash: &str) -> Result<TxStatus> {
        let tx_hash_parsed = tx_hash.parse().map_err(|_| crate::Error::Serialization {
            message: "Invalid transaction hash".to_string(),
        })?;
        let receipt = self
            .provider
            .get_transaction_receipt(tx_hash_parsed)
            .await
            .map_err(|e| crate::Error::EVMRpcError {
                source: e,
                loc: location!(),
            })?;
        let current_block_height =
            self.provider
                .get_block_number()
                .await
                .map_err(|e| crate::Error::EVMRpcError {
                    source: e,
                    loc: location!(),
                })?;

        if let Some(receipt) = receipt {
            return Ok(TxStatus::Confirmed(ConfirmedTxStatus {
                confirmations: current_block_height - receipt.block_number.unwrap(),
                current_height: current_block_height,
                inclusion_height: receipt.block_number.unwrap(),
            }));
        }

        let pending_tx = self
            .provider
            .get_transaction_by_hash(tx_hash_parsed)
            .await
            .map_err(|e| crate::Error::EVMRpcError {
                source: e,
                loc: location!(),
            })?;

        if pending_tx.is_some() {
            Ok(TxStatus::Pending(PendingTxStatus {
                current_height: current_block_height,
            }))
        } else {
            Ok(TxStatus::NotFound)
        }
    }
    async fn verify_user_deposit_candidate(
        &self,
        recipient_address: &str,
        currency: &Currency,
        tx_hash: &str,
        transfer_index: u64,
    ) -> Result<UserDepositCandidateStatus> {
        let token_address = match &currency.token {
            TokenIdentifier::Address(address) => address,
            TokenIdentifier::Native => return Ok(UserDepositCandidateStatus::TransferNotFound),
        };
        let token_address =
            Address::from_str(token_address).map_err(|_| crate::Error::Serialization {
                message: "Invalid token address".to_string(),
            })?;

        if token_address != self.allowed_token {
            return Ok(UserDepositCandidateStatus::TransferNotFound);
        }

        let recipient_address =
            Address::from_str(recipient_address).map_err(|_| crate::Error::Serialization {
                message: "Invalid address".to_string(),
            })?;

        let transaction_hash: TxHash =
            tx_hash
                .parse()
                .map_err(|_| crate::Error::TransactionDeserializationFailed {
                    context: format!("Failed to parse EVM transaction hash {tx_hash}"),
                    loc: location!(),
                })?;

        let transaction_receipt = self
            .provider
            .get_transaction_receipt(transaction_hash)
            .await
            .map_err(|e| crate::Error::EVMRpcError {
                source: e,
                loc: location!(),
            })?;

        let Some(transaction_receipt) = transaction_receipt else {
            return Ok(UserDepositCandidateStatus::TxNotFound);
        };

        if !transaction_receipt.status() {
            return Ok(UserDepositCandidateStatus::TransferNotFound);
        }

        let matching_transfer =
            extract_all_transfers_from_transaction_receipt(&transaction_receipt)
                .into_iter()
                .find(|transfer_log| {
                    transfer_log.address() == self.allowed_token
                        && transfer_log.log_index == Some(transfer_index)
                        && transfer_log.inner.data.to == recipient_address
                });

        let Some(transfer_log) = matching_transfer else {
            return Ok(UserDepositCandidateStatus::TransferNotFound);
        };

        let current_block_height =
            self.provider
                .get_block_number()
                .await
                .map_err(|e| crate::Error::EVMRpcError {
                    source: e,
                    loc: location!(),
                })?;
        let confirmations = current_block_height - transaction_receipt.block_number.unwrap();

        Ok(UserDepositCandidateStatus::Verified(VerifiedUserDeposit {
            amount: transfer_log.inner.data.value,
            confirmations,
        }))
    }

    // if this method returns an Err value, then we know the batch had an issue not worth retrying, if it's Ok(None) then either transient issue or batch is not in our rpc's view of the mempool
    async fn verify_market_maker_batch_transaction(
        &self,
        tx_hash: &str,
        market_maker_batch: &MarketMakerBatch,
    ) -> Result<Option<u64>> {
        let embedded_nonce = market_maker_batch.payment_verification.batch_nonce_digest;
        let transaction_hash: TxHash =
            tx_hash
                .parse()
                .map_err(|_| crate::Error::TransactionDeserializationFailed {
                    context: format!("Failed to parse EVM transaction hash {tx_hash}"),
                    loc: location!(),
                })?;
        let transaction = self
            .provider
            .get_transaction_by_hash(transaction_hash)
            .await;

        let transaction = match transaction {
            Ok(transaction) => transaction,
            Err(e) => {
                warn!("RPCError getting transaction by hash: {e}");
                return Ok(None);
            }
        };

        let transaction = match transaction {
            Some(transaction) => transaction,
            None => {
                warn!("Transaction not found for evm batch: {tx_hash}");
                return Ok(None);
            }
        };
        let tx_hex = alloy::hex::encode(transaction.input());
        let nonce_hex = alloy::hex::encode(embedded_nonce);
        if !tx_hex.contains(&nonce_hex) {
            return Err(crate::Error::BadMarketMakerBatch {
                chain: self.chain_type,
                tx_hash: tx_hash.to_string(),
                message: "Transaction does not contain the expected nonce".to_string(),
                loc: location!(),
            });
        }

        // Now get the receipt for the transaction
        let transaction_receipt = self
            .provider
            .get_transaction_receipt(transaction_hash)
            .await
            .map_err(|e| crate::Error::EVMRpcError {
                source: e,
                loc: location!(),
            })?;

        let transaction_receipt = match transaction_receipt {
            Some(transaction_receipt) => transaction_receipt,
            None => {
                warn!("Transaction receipt not found for evm batch: {tx_hash}");
                return Ok(None);
            }
        };

        // create a map of address => expected lot use iter
        let intra_tx_transfers =
            extract_all_transfers_from_transaction_receipt(&transaction_receipt);

        // Invariant: All transfers in the tx in the order of the payments in the batch
        // Find where intra_tx_transfers begins with the first payment
        let start_index = intra_tx_transfers.iter().position(|transfer| {
            transfer.inner.data.to
                == Address::from_str(&market_maker_batch.ordered_payments[0].to_address).unwrap()
                && transfer.inner.data.value >= market_maker_batch.ordered_payments[0].lot.amount
        });
        let start_index = match start_index {
            Some(start_index) => start_index,
            None => {
                return Err(crate::Error::BadMarketMakerBatch {
                    chain: self.chain_type,
                    tx_hash: tx_hash.to_string(),
                    message: "First transfer log not found".to_string(),
                    loc: location!(),
                });
            }
        };
        // Check that we have enough transfer logs to cover all payments
        if intra_tx_transfers.len() - start_index < market_maker_batch.ordered_payments.len() {
            return Err(crate::Error::BadMarketMakerBatch {
                chain: self.chain_type,
                tx_hash: tx_hash.to_string(),
                message: "Not enough transfer logs to cover all payments in batch".to_string(),
                loc: location!(),
            });
        }

        // Zip together the transfers and payments starting from start_index
        let transfers_and_payments = intra_tx_transfers[start_index..]
            .iter()
            .zip(&market_maker_batch.ordered_payments);

        // Track current index for error reporting
        let mut _index = start_index;

        // Validate each transfer matches its corresponding payment
        for (transfer, payment) in transfers_and_payments {
            let expected_recipient = Address::from_str(&payment.to_address).map_err(|_| {
                crate::Error::BadMarketMakerBatch {
                    chain: self.chain_type,
                    tx_hash: tx_hash.to_string(),
                    message: "Invalid recipient address in payment".to_string(),
                    loc: location!(),
                }
            })?;

            if transfer.address() != self.allowed_token {
                return Err(crate::Error::BadMarketMakerBatch {
                    chain: self.chain_type,
                    tx_hash: tx_hash.to_string(),
                    message: "Transfer at index is not from allowed token contract".to_string(),
                    loc: location!(),
                });
            }

            if transfer.inner.data.to != expected_recipient {
                return Err(crate::Error::BadMarketMakerBatch {
                    chain: self.chain_type,
                    tx_hash: tx_hash.to_string(),
                    message: "Transfer recipient at index does not match payment".to_string(),
                    loc: location!(),
                });
            }

            if transfer.inner.data.value < payment.lot.amount {
                return Err(crate::Error::BadMarketMakerBatch {
                    chain: self.chain_type,
                    tx_hash: tx_hash.to_string(),
                    message: "Transfer amount at index is less than payment amount".to_string(),
                    loc: location!(),
                });
            }

            _index += 1;
        }

        let current_block_height = self.provider.get_block_number().await;

        let current_block_height = match current_block_height {
            Ok(current_block_height) => current_block_height,
            Err(e) => {
                warn!("RPCError getting current block height: {e}");
                return Ok(None);
            }
        };

        let confirmations = current_block_height - transaction_receipt.block_number.unwrap();
        Ok(Some(confirmations))
    }

    async fn verify_fee_settlement_transaction(
        &self,
        tx_hash: &str,
        settlement_digest: [u8; 32],
    ) -> Result<Option<FeeSettlementVerification>> {
        let transaction_hash: TxHash =
            tx_hash
                .parse()
                .map_err(|_| crate::Error::TransactionDeserializationFailed {
                    context: format!("Failed to parse EVM transaction hash {tx_hash}"),
                    loc: location!(),
                })?;

        let transaction = self
            .provider
            .get_transaction_by_hash(transaction_hash)
            .await;
        let transaction = match transaction {
            Ok(transaction) => transaction,
            Err(e) => {
                warn!("RPCError getting transaction by hash: {e}");
                return Ok(None);
            }
        };

        let transaction = match transaction {
            Some(transaction) => transaction,
            None => {
                warn!("Transaction not found for evm fee settlement: {tx_hash}");
                return Ok(None);
            }
        };

        // Commitment: digest bytes must appear in calldata (same style as batch nonce).
        let tx_hex = alloy::hex::encode(transaction.input());
        let digest_hex = alloy::hex::encode(settlement_digest);
        if !tx_hex.contains(&digest_hex) {
            return Err(crate::Error::BadMarketMakerBatch {
                chain: self.chain_type,
                tx_hash: tx_hash.to_string(),
                message: "Fee settlement tx does not contain expected digest".to_string(),
                loc: location!(),
            });
        }

        let fee_address = Address::from_str(&otc_models::FEE_ADDRESSES_BY_CHAIN[&self.chain_type])
            .map_err(|_| crate::Error::Serialization {
                message: "Invalid fee address".to_string(),
            })?;

        let transaction_receipt = self
            .provider
            .get_transaction_receipt(transaction_hash)
            .await
            .map_err(|e| crate::Error::EVMRpcError {
                source: e,
                loc: location!(),
            })?;

        let transaction_receipt = match transaction_receipt {
            Some(transaction_receipt) => transaction_receipt,
            None => {
                warn!("Transaction receipt not found for evm fee settlement: {tx_hash}");
                return Ok(None);
            }
        };

        let intra_tx_transfers =
            extract_all_transfers_from_transaction_receipt(&transaction_receipt);

        // Sum all transfers (from allowed token contract) into the protocol fee address.
        let mut amount_paid = U256::ZERO;
        for t in intra_tx_transfers {
            if t.address() != self.allowed_token {
                continue;
            }
            if t.inner.data.to != fee_address {
                continue;
            }
            amount_paid += t.inner.data.value;
        }

        if amount_paid == U256::ZERO {
            return Err(crate::Error::BadMarketMakerBatch {
                chain: self.chain_type,
                tx_hash: tx_hash.to_string(),
                message: "Fee settlement tx transfers 0 to protocol fee address".to_string(),
                loc: location!(),
            });
        }

        let current_block_height = self.provider.get_block_number().await;
        let current_block_height = match current_block_height {
            Ok(current_block_height) => current_block_height,
            Err(e) => {
                warn!("RPCError getting current block height: {e}");
                return Ok(None);
            }
        };

        let confirmations = current_block_height - transaction_receipt.block_number.unwrap();

        Ok(Some(FeeSettlementVerification {
            confirmations,
            amount_sats: amount_paid,
        }))
    }

    /// Dump to address here just gives permission for the recipient to call receiveWithAuthorization
    /// What is returned is an unsigned transaction calldata that the recipient can sign and send
    /// note that it CANNOT just be broadcasted, it must be signed the the recipient
    async fn dump_to_address(
        &self,
        token: &TokenIdentifier,
        private_key: &str,
        recipient_address: &str,
        _fee: U256, //ignore fee b/c we dont sign a full transaction here
    ) -> Result<String> {
        let token_address = match token {
            TokenIdentifier::Address(address) => {
                Address::from_str(address).map_err(|_| crate::Error::DumpToAddress {
                    message: "Invalid token address".to_string(),
                })?
            }
            TokenIdentifier::Native => {
                return Err(crate::Error::DumpToAddress {
                    message: "Native token not supported".to_string(),
                })
            }
        };
        let sender_signer =
            LocalSigner::from_str(private_key).map_err(|_| crate::Error::DumpToAddress {
                message: "Invalid private key".to_string(),
            })?;
        let sender_address = sender_signer.address();
        let recipient_address =
            Address::from_str(recipient_address).map_err(|_| crate::Error::DumpToAddress {
                message: "Invalid recipient address".to_string(),
            })?;
        let token_contract = GenericEIP3009ERC20Instance::new(token_address, &self.provider);
        let token_balance = token_contract
            .balanceOf(sender_address)
            .call()
            .await
            .map_err(|_| crate::Error::DumpToAddress {
                message: "Failed to get token balance".to_string(),
            })?;

        let lot = Lot {
            currency: Currency {
                chain: self.chain_type,
                token: TokenIdentifier::address(token_address.to_string()),
                decimals: 8,
            },
            amount: token_balance,
        };

        let execution = create_transfer_with_authorization_execution(
            &lot,
            &sender_signer,
            &self.provider,
            &recipient_address,
        )
        .await
        .map_err(|e| crate::Error::DumpToAddress {
            message: e.to_string(),
        })?;

        Ok(hex::encode(execution.callData))
    }

    fn validate_address(&self, address: &str) -> bool {
        Address::from_str(address).is_ok()
    }

    fn minimum_block_confirmations(&self) -> u32 {
        self.min_confirmations
    }

    fn estimated_block_time(&self) -> Duration {
        self.est_block_time
    }

    async fn get_block_height(&self) -> Result<u64> {
        self.provider
            .get_block_number()
            .await
            .map_err(|e| crate::Error::EVMRpcError {
                source: e,
                loc: location!(),
            })
    }

    async fn get_best_hash(&self) -> Result<String> {
        Ok(hex::encode(
            self.provider
                .get_block_by_number(alloy::eips::BlockNumberOrTag::Latest)
                .await
                .map_err(|e| crate::Error::EVMRpcError {
                    source: e,
                    loc: location!(),
                })?
                .unwrap()
                .hash(),
        ))
    }
}

fn extract_all_transfers_from_transaction_receipt(
    transaction_receipt: &TransactionReceipt,
) -> Vec<RpcLog<Transfer>> {
    transaction_receipt
        .logs()
        .iter()
        .filter_map(|log| log.log_decode::<Transfer>().ok())
        .collect()
}
