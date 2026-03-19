use std::{collections::HashMap, str::FromStr, time::Duration};

use alloy::{
    eips::BlockNumberOrTag,
    primitives::{keccak256, Address, B256, U256},
    providers::{DynProvider, Provider, ProviderBuilder},
    rpc::types::{Filter, TransactionReceipt},
    sol,
};
use async_trait::async_trait;
use chrono::Utc;
use evm_token_indexer_client::TokenIndexerClient;
use otc_models::{ChainType, TokenIdentifier};
use snafu::ResultExt;
use tracing::warn;

use crate::{
    config::SauronArgs,
    discovery::{BlockCursor, BlockScan, DetectedDeposit, DiscoveryBackend},
    error::{EvmRpcSnafu, EvmTokenIndexerSnafu, Result},
    watch::WatchEntry,
};

sol! {
    #[derive(Debug)]
    event Transfer(address indexed from, address indexed to, uint256 value);
}

pub struct EvmErc20DiscoveryBackend {
    name: &'static str,
    chain_type: ChainType,
    provider: DynProvider,
    token_indexer: TokenIndexerClient,
    allowed_token: Address,
    transfer_signature: B256,
    poll_interval: Duration,
}

impl EvmErc20DiscoveryBackend {
    pub async fn new_ethereum(args: &SauronArgs) -> Result<Self> {
        Self::new(
            "ethereum_erc20",
            ChainType::Ethereum,
            &args.ethereum_mainnet_rpc_url,
            &args.untrusted_ethereum_mainnet_token_indexer_url,
            &args.ethereum_allowed_token,
        )
        .await
    }

    pub async fn new_base(args: &SauronArgs) -> Result<Self> {
        Self::new(
            "base_erc20",
            ChainType::Base,
            &args.base_rpc_url,
            &args.untrusted_base_token_indexer_url,
            &args.base_allowed_token,
        )
        .await
    }

    async fn new(
        name: &'static str,
        chain_type: ChainType,
        rpc_url: &str,
        token_indexer_url: &str,
        allowed_token: &str,
    ) -> Result<Self> {
        let url = rpc_url
            .parse()
            .map_err(|error| crate::error::Error::DiscoveryBackendInit {
                backend: name.to_string(),
                message: format!("invalid RPC URL {rpc_url}: {error}"),
            })?;
        let client = alloy::rpc::client::ClientBuilder::default().http(url);
        let provider = ProviderBuilder::new().connect_client(client).erased();
        let token_indexer = TokenIndexerClient::new(token_indexer_url).map_err(|error| {
            crate::error::Error::DiscoveryBackendInit {
                backend: name.to_string(),
                message: format!("invalid token indexer URL {token_indexer_url}: {error}"),
            }
        })?;
        let allowed_token = Address::from_str(allowed_token).map_err(|error| {
            crate::error::Error::DiscoveryBackendInit {
                backend: name.to_string(),
                message: format!("invalid allowed token {allowed_token}: {error}"),
            }
        })?;

        Ok(Self {
            name,
            chain_type,
            provider,
            token_indexer,
            allowed_token,
            transfer_signature: keccak256("Transfer(address,address,uint256)"),
            poll_interval: Duration::from_secs(5),
        })
    }

    fn watch_token_matches(&self, watch: &WatchEntry) -> bool {
        match &watch.source_token {
            TokenIdentifier::Address(token) => Address::from_str(token)
                .map(|parsed| parsed == self.allowed_token)
                .unwrap_or(false),
            TokenIdentifier::Native => false,
        }
    }

    fn address_map<'a>(&self, watches: &'a [WatchEntry]) -> HashMap<Address, Vec<&'a WatchEntry>> {
        let mut addresses: HashMap<Address, Vec<&WatchEntry>> = HashMap::new();

        for watch in watches {
            if !self.watch_token_matches(watch) {
                continue;
            }

            match Address::from_str(&watch.address) {
                Ok(address) => {
                    addresses.entry(address).or_default().push(watch);
                }
                Err(error) => {
                    warn!(
                        backend = self.name,
                        swap_id = %watch.swap_id,
                        address = %watch.address,
                        %error,
                        "Skipping invalid EVM watch address"
                    );
                }
            }
        }

        addresses
    }

    async fn current_tip_height(&self) -> Result<u64> {
        self.provider.get_block_number().await.context(EvmRpcSnafu)
    }

    async fn current_tip_hash(&self) -> Result<String> {
        let latest_block = self
            .provider
            .get_block_by_number(BlockNumberOrTag::Latest)
            .await
            .context(EvmRpcSnafu)?
            .ok_or_else(|| crate::error::Error::ChainInit {
                chain: self.chain_type.to_db_string().to_string(),
                message: "latest block was unavailable".to_string(),
            })?;

        Ok(alloy::hex::encode(latest_block.hash()))
    }

    async fn block_hash_at(&self, height: u64) -> Result<Option<String>> {
        let block = self
            .provider
            .get_block_by_number(height.into())
            .await
            .context(EvmRpcSnafu)?;
        Ok(block.map(|block| alloy::hex::encode(block.hash())))
    }

    async fn resolve_indexed_candidate(
        &self,
        transaction_hash: B256,
        recipient: Address,
        amount: U256,
        current_height: u64,
    ) -> Result<Option<(u64, u64)>> {
        let receipt = self
            .provider
            .get_transaction_receipt(transaction_hash)
            .await
            .context(EvmRpcSnafu)?;

        let Some(receipt) = receipt else {
            return Ok(None);
        };

        if !receipt.status() {
            return Ok(None);
        }

        let confirmations =
            current_height.saturating_sub(receipt.block_number.unwrap_or(current_height));

        Ok(
            extract_transfer_index(&receipt, self.allowed_token, recipient, amount)
                .map(|transfer_index| (transfer_index, confirmations)),
        )
    }
}

#[async_trait]
impl DiscoveryBackend for EvmErc20DiscoveryBackend {
    fn name(&self) -> &'static str {
        self.name
    }

    fn chain(&self) -> ChainType {
        self.chain_type
    }

    fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    async fn indexed_lookup(&self, watch: &WatchEntry) -> Result<Option<DetectedDeposit>> {
        if !self.watch_token_matches(watch) {
            return Ok(None);
        }

        let recipient = Address::from_str(&watch.address).map_err(|error| {
            crate::error::Error::InvalidWatchRow {
                message: format!(
                    "{} watch {} had invalid address {}: {}",
                    self.name, watch.swap_id, watch.address, error
                ),
            }
        })?;

        let mut best_match: Option<(DetectedDeposit, u64)> = None;
        let current_height = self.current_tip_height().await?;
        let mut page = 1;

        loop {
            let response = self
                .token_indexer
                .get_transfers_to(recipient, Some(page), Some(watch.min_amount))
                .await
                .context(EvmTokenIndexerSnafu)?;

            for transfer in response.transfers {
                let amount = U256::from_str_radix(&transfer.amount, 10).map_err(|error| {
                    crate::error::Error::InvalidWatchRow {
                        message: format!(
                            "{} transfer amount {} could not be parsed: {}",
                            self.name, transfer.amount, error
                        ),
                    }
                })?;
                if amount < watch.min_amount || amount > watch.max_amount {
                    continue;
                }

                let Some((transfer_index, confirmations)) = self
                    .resolve_indexed_candidate(
                        transfer.transaction_hash,
                        recipient,
                        amount,
                        current_height,
                    )
                    .await?
                else {
                    continue;
                };

                let candidate = DetectedDeposit {
                    swap_id: watch.swap_id,
                    source_chain: self.chain_type,
                    source_token: watch.source_token.clone(),
                    address: watch.address.clone(),
                    tx_hash: transfer.transaction_hash.to_string(),
                    transfer_index,
                    amount,
                    observed_at: Utc::now(),
                };

                if best_match
                    .as_ref()
                    .is_none_or(|(_, best_confirmations)| confirmations > *best_confirmations)
                {
                    best_match = Some((candidate, confirmations));
                }
            }

            if page >= response.pagination.total_pages || response.pagination.total_pages == 0 {
                break;
            }
            page += 1;
        }

        Ok(best_match.map(|(candidate, _)| candidate))
    }

    async fn current_cursor(&self) -> Result<BlockCursor> {
        Ok(BlockCursor {
            height: self.current_tip_height().await?,
            hash: self.current_tip_hash().await?,
        })
    }

    async fn scan_new_blocks(
        &self,
        from_exclusive: &BlockCursor,
        watches: &[WatchEntry],
    ) -> Result<BlockScan> {
        let current_height = self.current_tip_height().await?;
        let current_hash = self.current_tip_hash().await?;

        let addresses = self.address_map(watches);
        if addresses.is_empty() || current_height <= from_exclusive.height {
            return Ok(BlockScan {
                new_cursor: BlockCursor {
                    height: current_height,
                    hash: current_hash,
                },
                detections: Vec::new(),
            });
        }

        if from_exclusive.height > 0 {
            let expected_hash = self.block_hash_at(from_exclusive.height).await?;
            if expected_hash.as_deref() != Some(from_exclusive.hash.as_str()) {
                warn!(
                    backend = self.name,
                    stored_height = from_exclusive.height,
                    stored_hash = %from_exclusive.hash,
                    current_hash = ?expected_hash,
                    "EVM discovery cursor reorg detected; resetting to current tip"
                );
                return Ok(BlockScan {
                    new_cursor: BlockCursor {
                        height: current_height,
                        hash: current_hash,
                    },
                    detections: Vec::new(),
                });
            }
        }

        let filter = Filter::new()
            .address(self.allowed_token)
            .event_signature(self.transfer_signature)
            .from_block(from_exclusive.height + 1)
            .to_block(current_height);
        let logs = self.provider.get_logs(&filter).await.context(EvmRpcSnafu)?;

        let mut detections = Vec::new();
        for log in logs {
            if log.removed {
                continue;
            }

            let Ok(decoded) = log.log_decode::<Transfer>() else {
                continue;
            };
            let Some(candidates) = addresses.get(&decoded.inner.data.to) else {
                continue;
            };
            let Some(transaction_hash) = decoded.transaction_hash else {
                continue;
            };
            let Some(transfer_index) = decoded.log_index else {
                continue;
            };

            for watch in candidates {
                if decoded.inner.data.value < watch.min_amount
                    || decoded.inner.data.value > watch.max_amount
                {
                    continue;
                }

                detections.push(DetectedDeposit {
                    swap_id: watch.swap_id,
                    source_chain: self.chain_type,
                    source_token: watch.source_token.clone(),
                    address: watch.address.clone(),
                    tx_hash: transaction_hash.to_string(),
                    transfer_index,
                    amount: decoded.inner.data.value,
                    observed_at: Utc::now(),
                });
            }
        }

        Ok(BlockScan {
            new_cursor: BlockCursor {
                height: current_height,
                hash: current_hash,
            },
            detections,
        })
    }
}

fn extract_transfer_index(
    receipt: &TransactionReceipt,
    allowed_token: Address,
    recipient: Address,
    amount: U256,
) -> Option<u64> {
    receipt.logs().iter().find_map(|log| {
        let decoded = log.log_decode::<Transfer>().ok()?;
        if decoded.address() != allowed_token {
            return None;
        }
        if decoded.inner.data.to != recipient || decoded.inner.data.value != amount {
            return None;
        }
        decoded.log_index
    })
}
