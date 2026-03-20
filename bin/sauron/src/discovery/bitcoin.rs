use std::{collections::HashMap, str::FromStr, time::Duration};

use alloy::primitives::U256;
use async_trait::async_trait;
use bitcoin::{Address, Block, BlockHash, ScriptBuf};
use chrono::Utc;
use otc_models::{ChainType, TokenIdentifier};
use snafu::ResultExt;
use tracing::warn;

use crate::{
    config::SauronArgs,
    discovery::{BlockCursor, BlockScan, DetectedDeposit, DiscoveryBackend},
    error::{BitcoinEsploraSnafu, Result},
    watch::{SharedWatchEntry, WatchEntry},
};

const BITCOIN_REORG_RESCAN_DEPTH: u64 = 6;

pub struct BitcoinDiscoveryBackend {
    esplora_client: esplora_client::AsyncClient,
    poll_interval: Duration,
    indexed_lookup_concurrency: usize,
}

impl BitcoinDiscoveryBackend {
    pub async fn new(args: &SauronArgs) -> Result<Self> {
        let electrum_http_server_url = args.electrum_http_server_url.trim_end_matches('/');
        let esplora_client = esplora_client::Builder::new(electrum_http_server_url)
            .build_async()
            .map_err(|error| crate::error::Error::DiscoveryBackendInit {
                backend: "bitcoin".to_string(),
                message: error.to_string(),
            })?;

        Ok(Self {
            esplora_client,
            poll_interval: Duration::from_secs(args.sauron_bitcoin_scan_interval_seconds),
            indexed_lookup_concurrency: args.sauron_bitcoin_indexed_lookup_concurrency,
        })
    }

    fn script_map<'a>(
        &self,
        watches: &'a [SharedWatchEntry],
    ) -> HashMap<ScriptBuf, Vec<&'a WatchEntry>> {
        let mut scripts: HashMap<ScriptBuf, Vec<&WatchEntry>> = HashMap::new();

        for watch in watches {
            let parsed = Address::from_str(&watch.address)
                .map(|address| address.assume_checked().script_pubkey());
            match parsed {
                Ok(script) => scripts.entry(script).or_default().push(watch.as_ref()),
                Err(error) => {
                    warn!(
                        swap_id = %watch.swap_id,
                        address = %watch.address,
                        %error,
                        "Skipping invalid Bitcoin watch address"
                    );
                }
            }
        }

        scripts
    }

    async fn current_tip_height(&self) -> Result<u64> {
        self.esplora_client
            .get_height()
            .await
            .map(u64::from)
            .context(BitcoinEsploraSnafu)
    }

    async fn current_tip_hash(&self) -> Result<BlockHash> {
        self.esplora_client.get_tip_hash().await.context(BitcoinEsploraSnafu)
    }

    async fn block_hash_at_height(&self, height: u64) -> Result<BlockHash> {
        let height = u32::try_from(height).map_err(|_| crate::error::Error::ChainInit {
            chain: ChainType::Bitcoin.to_db_string().to_string(),
            message: format!("block height {height} exceeded u32 range"),
        })?;

        self.esplora_client
            .get_block_hash(height)
            .await
            .context(BitcoinEsploraSnafu)
    }

    async fn block_by_hash(&self, block_hash: &BlockHash) -> Result<Block> {
        self.esplora_client
            .get_block_by_hash(block_hash)
            .await
            .context(BitcoinEsploraSnafu)?
            .ok_or_else(|| crate::error::Error::ChainInit {
                chain: ChainType::Bitcoin.to_db_string().to_string(),
                message: format!("block {block_hash} was unavailable from Esplora"),
            })
    }
}

#[async_trait]
impl DiscoveryBackend for BitcoinDiscoveryBackend {
    fn name(&self) -> &'static str {
        "bitcoin"
    }

    fn chain(&self) -> ChainType {
        ChainType::Bitcoin
    }

    fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    fn indexed_lookup_concurrency(&self) -> usize {
        self.indexed_lookup_concurrency
    }

    async fn indexed_lookup(&self, watch: &WatchEntry) -> Result<Option<DetectedDeposit>> {
        if watch.source_token != TokenIdentifier::Native {
            return Ok(None);
        }

        let address = Address::from_str(&watch.address)
            .map_err(|error| crate::error::Error::InvalidWatchRow {
                message: format!(
                    "Bitcoin watch {} had invalid address {}: {}",
                    watch.swap_id, watch.address, error
                ),
            })?
            .assume_checked();

        let utxos = self
            .esplora_client
            .get_address_utxo(&address)
            .await
            .context(BitcoinEsploraSnafu)?;
        let current_height = self.current_tip_height().await? as u32;

        let mut best_match: Option<(DetectedDeposit, u64)> = None;

        for utxo in utxos {
            let amount = U256::from(utxo.value);
            if amount < watch.min_amount || amount > watch.max_amount {
                continue;
            }

            let confirmations = current_height
                .saturating_sub(utxo.status.block_height.unwrap_or(current_height))
                as u64;

            let candidate = DetectedDeposit {
                swap_id: watch.swap_id,
                source_chain: ChainType::Bitcoin,
                source_token: TokenIdentifier::Native,
                address: watch.address.clone(),
                tx_hash: utxo.txid.to_string(),
                transfer_index: utxo.vout as u64,
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

        Ok(best_match.map(|(candidate, _)| candidate))
    }

    async fn current_cursor(&self) -> Result<BlockCursor> {
        let height = self.current_tip_height().await?;
        let hash = self.current_tip_hash().await?;
        Ok(BlockCursor {
            height,
            hash: hash.to_string(),
        })
    }

    async fn scan_new_blocks(
        &self,
        from_exclusive: &BlockCursor,
        watches: &[SharedWatchEntry],
    ) -> Result<BlockScan> {
        let current_height = self.current_tip_height().await?;
        let current_tip_hash = self.current_tip_hash().await?;

        if watches.is_empty() || current_height <= from_exclusive.height {
            return Ok(BlockScan {
                new_cursor: BlockCursor {
                    height: current_height,
                    hash: current_tip_hash.to_string(),
                },
                detections: Vec::new(),
            });
        }

        if from_exclusive.height > 0 {
            let expected_hash = self.block_hash_at_height(from_exclusive.height).await?;
            if expected_hash.to_string() != from_exclusive.hash {
                let rewind_height = from_exclusive
                    .height
                    .saturating_sub(BITCOIN_REORG_RESCAN_DEPTH);
                let rewind_hash = if rewind_height == 0 {
                    String::new()
                } else {
                    self.block_hash_at_height(rewind_height).await?.to_string()
                };
                warn!(
                    stored_height = from_exclusive.height,
                    stored_hash = %from_exclusive.hash,
                    current_hash = %expected_hash,
                    rewind_height,
                    "Bitcoin discovery cursor reorg detected; rewinding cursor"
                );
                return Ok(BlockScan {
                    new_cursor: BlockCursor {
                        height: rewind_height,
                        hash: rewind_hash,
                    },
                    detections: Vec::new(),
                });
            }
        }

        let scripts = self.script_map(watches);
        let mut detections = Vec::new();
        let mut last_hash = from_exclusive.hash.clone();

        for height in (from_exclusive.height + 1)..=current_height {
            let block_hash = self.block_hash_at_height(height).await?;
            let block = self.block_by_hash(&block_hash).await?;
            last_hash = block_hash.to_string();

            for tx in block.txdata {
                let tx_hash = tx.compute_txid().to_string();
                for (vout, output) in tx.output.iter().enumerate() {
                    let Some(candidates) = scripts.get(&output.script_pubkey) else {
                        continue;
                    };

                    let amount = U256::from(output.value.to_sat());
                    for watch in candidates {
                        if amount < watch.min_amount || amount > watch.max_amount {
                            continue;
                        }

                        detections.push(DetectedDeposit {
                            swap_id: watch.swap_id,
                            source_chain: ChainType::Bitcoin,
                            source_token: TokenIdentifier::Native,
                            address: watch.address.clone(),
                            tx_hash: tx_hash.clone(),
                            transfer_index: vout as u64,
                            amount,
                            observed_at: Utc::now(),
                        });
                    }
                }
            }
        }

        Ok(BlockScan {
            new_cursor: BlockCursor {
                height: current_height,
                hash: last_hash,
            },
            detections,
        })
    }
}
