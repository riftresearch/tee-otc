use crate::traits::MarketMakerPaymentValidation;
use crate::{key_derivation, ChainOperations, Result};
use alloy::consensus::Transaction;
use alloy::primitives::{Address, U256};
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
use tracing::{debug, info};

sol! {
    #[derive(Debug)]
    event Transfer(address indexed from, address indexed to, uint256 value);
}

const ALLOWED_TOKEN: &str = "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf";

pub struct EthereumChain {
    provider: DynProvider,
    evm_indexer_client: TokenIndexerClient,
    allowed_token: Address,
}

impl EthereumChain {
    pub async fn new(rpc_url: &str, evm_indexer_url: &str) -> Result<Self> {
        let provider = ProviderBuilder::new()
            .connect_http(rpc_url.parse().map_err(|_| crate::Error::Serialization {
                message: "Invalid RPC URL".to_string(),
            })?)
            .erased();

        let evm_indexer_client = TokenIndexerClient::new(evm_indexer_url).map_err(|e| {
            crate::Error::EVMTokenIndexerClientError {
                source: e,
                loc: location!(),
            }
        })?;
        let allowed_token =
            Address::from_str(ALLOWED_TOKEN).map_err(|_| crate::Error::Serialization {
                message: "Invalid allowed token address".to_string(),
            })?;

        Ok(Self {
            provider,
            evm_indexer_client,
            allowed_token,
        })
    }
}

#[async_trait]
impl ChainOperations for EthereumChain {
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

        info!("Created new Ethereum wallet: {}", address);

        let wallet = Wallet::new(format!("{address:?}"), format!("0x{private_key}"));
        Ok((wallet, salt))
    }

    fn derive_wallet(&self, master_key: &[u8], salt: &[u8; 32]) -> Result<Wallet> {
        // Derive private key using HKDF
        let private_key_bytes =
            key_derivation::derive_private_key(master_key, salt, b"ethereum-wallet")?;

        // Create signer from derived key
        let signer = PrivateKeySigner::from_bytes(&private_key_bytes.into()).map_err(|_| {
            crate::Error::Serialization {
                message: "Failed to create signer from derived key".to_string(),
            }
        })?;

        let address = format!("{:?}", signer.address());
        let private_key = format!("0x{}", alloy::hex::encode(private_key_bytes));

        debug!("Derived Ethereum wallet: {}", address);

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

        if tx.is_some() {
            let current_block_height =
                self.provider
                    .get_block_number()
                    .await
                    .map_err(|e| crate::Error::EVMRpcError {
                        source: e,
                        loc: location!(),
                    })?;
            Ok(TxStatus::Confirmed(
                current_block_height - tx.unwrap().block_number.unwrap(),
            ))
        } else {
            Ok(TxStatus::NotFound)
        }
    }
    async fn search_for_transfer(
        &self,
        recipient_address: &str,
        lot: &Lot,
        mm_payment: Option<MarketMakerPaymentValidation>,
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

        let transfer_hint = self
            .get_transfer(&recipient_address, &lot.amount, mm_payment)
            .await?;
        if transfer_hint.is_none() {
            return Ok(None);
        }

        Ok(Some(transfer_hint.unwrap()))
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
                chain: ChainType::Ethereum,
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
        4
    }

    fn estimated_block_time(&self) -> Duration {
        Duration::from_secs(12) // ~12 seconds
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

impl EthereumChain {
    // Note this function's response is safe to trust, b/c it will validate the responses from the untrusted evm_indexer_client
    async fn get_transfer(
        &self,
        recipient_address: &Address,
        amount: &U256,
        mm_payment: Option<MarketMakerPaymentValidation>,
    ) -> Result<Option<TransferInfo>> {
        info!(
            "Searching for transfer for address: {}, amount: {}, mm_payment: {:?}",
            recipient_address, amount, mm_payment
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
            info!("No transfers found");
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

            for (index, transfer_log) in intra_tx_transfers.iter().enumerate() {
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
                // validate the embedded nonce
                if let Some(mm_payment) = &mm_payment {
                    let embedded_nonce = mm_payment.embedded_nonce;
                    let transaction = self
                        .provider
                        .get_transaction_by_hash(transfer.transaction_hash)
                        .await
                        .map_err(|e| crate::Error::EVMRpcError {
                            source: e,
                            loc: location!(),
                        })?;
                    if transaction.is_none() {
                        debug!("Transaction not found for transfer: {:?}", transfer);
                        continue;
                    }
                    let transaction = transaction.unwrap();
                    let tx_hex = alloy::hex::encode(transaction.input());
                    let nonce_hex = alloy::hex::encode(embedded_nonce);
                    if !tx_hex.contains(&nonce_hex) {
                        debug!(
                            "Transaction does not contain the expected nonce: {:?}",
                            transfer
                        );
                        continue;
                    }

                    let fee_address = Address::from_str(
                        &otc_models::FEE_ADDRESSES_BY_CHAIN[&ChainType::Ethereum],
                    )
                    .map_err(|_| crate::Error::Serialization {
                        message: "Invalid fee address".to_string(),
                    })?;

                    // Enforce invariant: the next transfer log must be the fee transfer
                    let Some(next_log) = intra_tx_transfers.get(index + 1) else {
                        debug!("Missing next transfer log for fee validation");
                        continue;
                    };
                    // Same token and correct recipient
                    if next_log.address() != self.allowed_token {
                        debug!("Next transfer not from allowed token contract");
                        continue;
                    }
                    if next_log.inner.data.to != fee_address {
                        info!("Fee address is not the expected address in next log");
                        continue;
                    }
                    if next_log.inner.data.value < mm_payment.fee_amount {
                        info!("Fee amount in next log is less than expected");
                        continue;
                    }
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
    let mut transfers = Vec::new();
    for log in transaction_receipt.logs() {
        let decoded = log.log_decode::<Transfer>();
        if let Ok(decoded) = decoded {
            // Preserve full decoded log to keep contract address and event fields
            transfers.push(decoded);
        }
    }
    transfers
}
