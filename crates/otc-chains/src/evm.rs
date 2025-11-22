use crate::traits::MarketMakerBatch;
use crate::{key_derivation, ChainOperations, Result};
use alloy::consensus::Transaction;
use alloy::primitives::{Address, TxHash, U256};
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use alloy::rpc::types::{Log as RpcLog, TransactionReceipt};
use alloy::signers::local::{LocalSigner, PrivateKeySigner};
use alloy::{hex, sol};
use async_trait::async_trait;
use blockchain_utils::create_receive_with_authorization_execution;
use eip3009_erc20_contract::GenericEIP3009ERC20::GenericEIP3009ERC20Instance;
use evm_token_indexer_client::TokenIndexerClient;
use otc_models::{ChainType, Currency, Lot, TokenIdentifier, TransferInfo, TxStatus, Wallet};
use snafu::location;
use std::str::FromStr;
use std::time::Duration;
use tracing::{debug, info, warn};

sol! {
    #[derive(Debug)]
    event Transfer(address indexed from, address indexed to, uint256 value);
}

pub struct EvmChain {
    provider: DynProvider,
    evm_indexer_client: TokenIndexerClient,
    allowed_token: Address,
    chain_type: ChainType,
    wallet_seed_tag: Vec<u8>,
    min_confirmations: u32,
    est_block_time: Duration,
}

impl EvmChain {
    pub async fn new(
        rpc_url: &str,
        evm_indexer_url: &str,
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

        let provider = ProviderBuilder::new()
            .connect_client(client)
            .erased();

        let evm_indexer_client = TokenIndexerClient::new(evm_indexer_url).map_err(|e| {
            crate::Error::EVMTokenIndexerClientError {
                source: e,
                loc: location!(),
            }
        })?;
        let allowed_token =
            Address::from_str(allowed_token).map_err(|_| crate::Error::Serialization {
                message: "Invalid allowed token address".to_string(),
            })?;

        Ok(Self {
            provider,
            evm_indexer_client,
            allowed_token,
            chain_type,
            wallet_seed_tag: wallet_seed_tag.to_vec(),
            min_confirmations,
            est_block_time,
        })
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

        info!("Created new {} wallet: {}", self.chain_type.to_db_string(), address);

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

        debug!("Derived {} wallet: {}", self.chain_type.to_db_string(), address);

        Ok(Wallet::new(address, private_key))
    }

    async fn get_tx_status(&self, tx_hash: &str) -> Result<TxStatus> {
        let tx_hash_parsed = tx_hash.parse().map_err(|_| crate::Error::Serialization {
            message: "Invalid transaction hash".to_string(),
        })?;
        let tx = self
            .provider
            .get_transaction_receipt(tx_hash_parsed)
            .await
            .map_err(|e| crate::Error::EVMRpcError {
                source: e,
                loc: location!(),
            })?;

        if let Some(tx) = tx {
            let current_block_height =
                self.provider
                    .get_block_number()
                    .await
                    .map_err(|e| crate::Error::EVMRpcError {
                        source: e,
                        loc: location!(),
                    })?;
            Ok(TxStatus::Confirmed(
                current_block_height - tx.block_number.unwrap(),
            ))
        } else {
            Ok(TxStatus::NotFound)
        }
    }
    async fn search_for_transfer(
        &self,
        recipient_address: &str,
        lot: &Lot,
        _from_block_height: Option<u64>,
    ) -> Result<Option<TransferInfo>> {
        let token_address = match &lot.currency.token {
            TokenIdentifier::Address(address) => address,
            TokenIdentifier::Native => return Ok(None),
        };
        let token_address =
            Address::from_str(token_address).map_err(|_| crate::Error::Serialization {
                message: "Invalid token address".to_string(),
            })?;

        if token_address != self.allowed_token {
            debug!("Token address {} is not allowed", token_address);
            return Ok(None);
        }

        let recipient_address =
            Address::from_str(recipient_address).map_err(|_| crate::Error::Serialization {
                message: "Invalid address".to_string(),
            })?;

        let transfer_hint = self.get_transfer(&recipient_address, &lot.amount).await?;
        if transfer_hint.is_none() {
            return Ok(None);
        }

        Ok(Some(transfer_hint.unwrap()))
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

        let fee_address = Address::from_str(
            &otc_models::FEE_ADDRESSES_BY_CHAIN[&self.chain_type],
        )
        .map_err(|_| crate::Error::Serialization {
            message: "Invalid fee address".to_string(),
        })?;
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
        let mut index = start_index;

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

            index += 1;
        }

        // Enforce invariant: the next transfer log must be the fee transfer
        let Some(next_log) = intra_tx_transfers.get(index) else {
            return Err(crate::Error::BadMarketMakerBatch {
                chain: self.chain_type,
                tx_hash: tx_hash.to_string(),
                message: "Missing next transfer log for fee validation".to_string(),
                loc: location!(),
            });
        };
        // Same token and correct recipient
        if next_log.address() != self.allowed_token {
            return Err(crate::Error::BadMarketMakerBatch {
                chain: self.chain_type,
                tx_hash: tx_hash.to_string(),
                message: "Next transfer not from allowed token contract".to_string(),
                loc: location!(),
            });
        }
        if next_log.inner.data.to != fee_address {
            return Err(crate::Error::BadMarketMakerBatch {
                chain: self.chain_type,
                tx_hash: tx_hash.to_string(),
                message: "Fee address is not the expected address in next log".to_string(),
                loc: location!(),
            });
        }
        if next_log.inner.data.value < market_maker_batch.payment_verification.aggregated_fee {
            return Err(crate::Error::BadMarketMakerBatch {
                chain: self.chain_type,
                tx_hash: tx_hash.to_string(),
                message: "Fee amount in next log is less than expected".to_string(),
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
        Ok(Some(confirmations))
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
                token: TokenIdentifier::Address(token_address.to_string()),
                decimals: 8,
            },
            amount: token_balance,
        };

        let execution = create_receive_with_authorization_execution(
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

impl EvmChain {
    // Note this function's response is safe to trust, b/c it will validate the responses from the untrusted evm_indexer_client
    async fn get_transfer(
        &self,
        recipient_address: &Address,
        amount: &U256,
    ) -> Result<Option<TransferInfo>> {
        debug!(
            "Searching for transfer for address: {}, amount: {}",
            recipient_address, amount
        );

        // use the untrusted evm_indexer_client to get the transfer hint - this will only return 50 latest transfers (TODO: how to handle this?)
        let transfers = self
            .evm_indexer_client
            .get_transfers_to(*recipient_address, None, Some(*amount))
            .await
            .map_err(|e| crate::Error::EVMTokenIndexerClientError {
                source: e,
                loc: location!(),
            })?;

        if transfers.transfers.is_empty() {
            debug!("No transfers found");
            return Ok(None);
        }

        debug!("TransfersResponse from evm_indexer_client: {:?}", transfers);

        let mut transfer_hint: Option<TransferInfo> = None;
        for transfer in transfers.transfers {
            let transaction_receipt = self
                .provider
                .get_transaction_receipt(transfer.transaction_hash)
                .await
                .map_err(|e| crate::Error::EVMRpcError {
                    source: e,
                    loc: location!(),
                })?;

            if transaction_receipt.is_none() {
                debug!("Transaction receipt not found for transfer: {:?}", transfer);
                continue;
            }
            let transaction_receipt = transaction_receipt.unwrap();
            if !transaction_receipt.status() {
                debug!(
                    "Transaction receipt not successful for transfer: {:?}",
                    transfer
                );
                continue;
            }

            let intra_tx_transfers =
                extract_all_transfers_from_transaction_receipt(&transaction_receipt);

            for transfer_log in intra_tx_transfers {
                // Ensure this transfer is for the allowed token (same contract)
                if transfer_log.address() != self.allowed_token {
                    continue;
                }
                // validate the recipient
                if transfer_log.inner.data.to != *recipient_address {
                    debug!(
                        "Transfer recipient is not the expected address: {:?}",
                        transfer
                    );
                    continue;
                }
                // validate the amount
                if transfer_log.inner.data.value < *amount {
                    debug!("Transfer amount is less than expected: {:?}", transfer);
                    continue;
                }

                // get the current block height
                let current_block_height = self.provider.get_block_number().await.map_err(|e| {
                    crate::Error::EVMRpcError {
                        source: e,
                        loc: location!(),
                    }
                })?;
                let confirmations =
                    current_block_height - transaction_receipt.block_number.unwrap();

                // only return the transfer if it has more confirmations than the previous transfer hint
                if transfer_hint.is_some()
                    && transfer_hint.as_ref().unwrap().confirmations > confirmations
                {
                    debug!(
                        "Transfer has more confirmations than the previous transfer hint: {:?}",
                        transfer
                    );
                    continue;
                }

                transfer_hint = Some(TransferInfo {
                    tx_hash: alloy::hex::encode(transfer.transaction_hash),
                    detected_at: utc::now(),
                    confirmations,
                    amount: transfer_log.inner.data.value,
                });
            }
        }

        Ok(transfer_hint)
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
