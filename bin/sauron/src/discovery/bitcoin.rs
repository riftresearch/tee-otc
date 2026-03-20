use std::{collections::HashMap, str::FromStr, time::Duration};

use alloy::primitives::U256;
use async_trait::async_trait;
use bitcoin::{Address, BlockHash, ScriptBuf};
use bitcoincore_rpc_async::{Auth, Client, RpcApi};
use chrono::Utc;
use otc_models::{ChainType, TokenIdentifier};
use snafu::ResultExt;
use tracing::warn;

use crate::{
    config::SauronArgs,
    discovery::{BlockCursor, BlockScan, DetectedDeposit, DiscoveryBackend},
    error::{BitcoinEsploraSnafu, BitcoinRpcSnafu, Result},
    watch::{SharedWatchEntry, WatchEntry},
};

const BITCOIN_REORG_RESCAN_DEPTH: u64 = 6;

pub struct BitcoinDiscoveryBackend {
    rpc_client: Client,
    esplora_client: esplora_client::AsyncClient,
    poll_interval: Duration,
    indexed_lookup_concurrency: usize,
}

impl BitcoinDiscoveryBackend {
    pub async fn new(args: &SauronArgs) -> Result<Self> {
        let rpc_client = Client::new(
            args.bitcoin_rpc_url.clone(),
            normalize_auth(&args.bitcoin_rpc_auth),
        )
        .await
        .map_err(|error| crate::error::Error::DiscoveryBackendInit {
            backend: "bitcoin".to_string(),
            message: error.to_string(),
        })?;

        let esplora_client = esplora_client::Builder::new(&args.untrusted_esplora_http_server_url)
            .build_async()
            .map_err(|error| crate::error::Error::DiscoveryBackendInit {
                backend: "bitcoin".to_string(),
                message: error.to_string(),
            })?;

        Ok(Self {
            rpc_client,
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
        self.rpc_client
            .get_block_count()
            .await
            .context(BitcoinRpcSnafu)
    }

    async fn current_tip_hash(&self) -> Result<BlockHash> {
        self.rpc_client
            .get_best_block_hash()
            .await
            .context(BitcoinRpcSnafu)
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
            let expected_hash = self
                .rpc_client
                .get_block_hash(from_exclusive.height)
                .await
                .context(BitcoinRpcSnafu)?;
            if expected_hash.to_string() != from_exclusive.hash {
                let rewind_height = from_exclusive
                    .height
                    .saturating_sub(BITCOIN_REORG_RESCAN_DEPTH);
                let rewind_hash = if rewind_height == 0 {
                    String::new()
                } else {
                    self.rpc_client
                        .get_block_hash(rewind_height)
                        .await
                        .context(BitcoinRpcSnafu)?
                        .to_string()
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
            let block_hash = self
                .rpc_client
                .get_block_hash(height)
                .await
                .context(BitcoinRpcSnafu)?;
            let block = self
                .rpc_client
                .get_block(&block_hash)
                .await
                .context(BitcoinRpcSnafu)?;
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

fn normalize_auth(auth: &Auth) -> Auth {
    match auth {
        Auth::None => Auth::None,
        Auth::UserPass(user, password) => Auth::UserPass(user.clone(), password.clone()),
        Auth::CookieFile(path) => Auth::CookieFile(path.clone()),
    }
}
