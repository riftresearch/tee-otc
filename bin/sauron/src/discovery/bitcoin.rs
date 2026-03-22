use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use bitcoin::{
    base64::{engine::general_purpose::STANDARD, Engine},
    consensus::deserialize,
    hashes::Hash,
    hex::FromHex,
    Address, Block, BlockHash, ScriptBuf, Transaction, Txid,
};
use bitcoincore_rpc_async::{jsonrpc, Client as BitcoinRpcClient, RpcApi};
use chrono::Utc;
use otc_models::{ChainType, TokenIdentifier};
use reqwest::Url;
use snafu::ResultExt;
use tokio::{
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        Mutex, RwLock,
    },
    time::sleep,
};
use tracing::{info, warn};
use zeromq::{Socket, SocketRecv, ZmqMessage};

use crate::{
    config::SauronArgs,
    discovery::{BlockCursor, BlockScan, DetectedDeposit, DiscoveryBackend},
    error::{BitcoinEsploraSnafu, BitcoinRpcSnafu, Result},
    watch::{SharedWatchEntry, WatchEntry},
};

const BITCOIN_REORG_RESCAN_DEPTH: u64 = 6;
const BITCOIN_ZMQ_RECONNECT_DELAY: Duration = Duration::from_secs(5);

pub struct BitcoinDiscoveryBackend {
    esplora_client: esplora_client::AsyncClient,
    rpc_client: Arc<BitcoinRpcClient>,
    mempool_support: Arc<BitcoinMempoolSupport>,
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

        let rpc_transport =
            ReqwestRpcTransport::new(&args.bitcoin_rpc_url, args.bitcoin_rpc_auth.clone())
                .map_err(|error| crate::error::Error::DiscoveryBackendInit {
                    backend: "bitcoin".to_string(),
                    message: error,
                })?;
        let jsonrpc_client = jsonrpc::Client::with_transport(rpc_transport);
        let rpc_client = Arc::new(BitcoinRpcClient::from_jsonrpc(jsonrpc_client));

        info!("Bitcoin Core RPC support enabled for Sauron block scanning");
        info!(
            rawtx_endpoint = %args.bitcoin_zmq_rawtx_endpoint,
            sequence_endpoint = %args.bitcoin_zmq_sequence_endpoint,
            "Bitcoin Core ZMQ mempool support enabled for Sauron"
        );
        let mempool_support = BitcoinMempoolSupport::spawn(
            args.bitcoin_zmq_rawtx_endpoint.clone(),
            args.bitcoin_zmq_sequence_endpoint.clone(),
        );

        Ok(Self {
            esplora_client,
            rpc_client,
            mempool_support,
            poll_interval: Duration::from_secs(args.sauron_bitcoin_scan_interval_seconds),
            indexed_lookup_concurrency: args.sauron_bitcoin_indexed_lookup_concurrency,
        })
    }

    fn script_map(
        &self,
        watches: &[SharedWatchEntry],
    ) -> HashMap<ScriptBuf, Vec<SharedWatchEntry>> {
        let mut scripts: HashMap<ScriptBuf, Vec<SharedWatchEntry>> = HashMap::new();

        for watch in watches {
            let parsed = Address::from_str(&watch.address)
                .map(|address| address.assume_checked().script_pubkey());
            match parsed {
                Ok(script) => scripts.entry(script).or_default().push(watch.clone()),
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
        match self.rpc_client.get_block_count().await {
            Ok(height) => return Ok(height),
            Err(error) => {
                warn!(
                    %error,
                    "Bitcoin RPC tip-height lookup failed; falling back to Esplora"
                );
            }
        }

        self.esplora_client
            .get_height()
            .await
            .map(u64::from)
            .context(BitcoinEsploraSnafu)
    }

    async fn current_tip_hash(&self) -> Result<BlockHash> {
        match self.rpc_client.get_best_block_hash().await {
            Ok(hash) => return Ok(hash),
            Err(error) => {
                warn!(
                    %error,
                    "Bitcoin RPC tip-hash lookup failed; falling back to Esplora"
                );
            }
        }

        self.esplora_client
            .get_tip_hash()
            .await
            .context(BitcoinEsploraSnafu)
    }

    async fn block_hash_at_height(&self, height: u64) -> Result<BlockHash> {
        match self.rpc_client.get_block_hash(height).await {
            Ok(hash) => return Ok(hash),
            Err(error) => {
                warn!(
                    height,
                    %error,
                    "Bitcoin RPC block-hash lookup failed; falling back to Esplora"
                );
            }
        }

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
        match self.rpc_client.get_block(block_hash).await {
            Ok(block) => return Ok(block),
            Err(error) => {
                warn!(
                    %block_hash,
                    %error,
                    "Bitcoin RPC block fetch failed; falling back to Esplora"
                );
            }
        }

        self.esplora_client
            .get_block_by_hash(block_hash)
            .await
            .context(BitcoinEsploraSnafu)?
            .ok_or_else(|| crate::error::Error::ChainInit {
                chain: ChainType::Bitcoin.to_db_string().to_string(),
                message: format!("block {block_hash} was unavailable from Esplora"),
            })
    }

    async fn resync_mempool_watches_from_esplora(
        &self,
        watches: &[SharedWatchEntry],
    ) -> Vec<DetectedDeposit> {
        let mut detections = Vec::new();

        for watch in watches {
            match self.indexed_lookup(watch.as_ref()).await {
                Ok(Some(detected)) => detections.push(detected),
                Ok(None) => {}
                Err(error) => {
                    warn!(
                        swap_id = %watch.swap_id,
                        %error,
                        "Bitcoin mempool resync lookup failed for active watch"
                    );
                }
            }
        }

        detections
    }

    async fn drain_mempool_detections(
        &self,
        watches: &[SharedWatchEntry],
        current_height: u64,
    ) -> Vec<DetectedDeposit> {
        let resync_requested = self.mempool_support.take_resync_request();
        let snapshot_height = self.mempool_support.current_snapshot_height().await;
        let snapshot_outdated = snapshot_height != Some(current_height);
        let mut detections = Vec::new();

        if resync_requested || snapshot_outdated {
            if let Err(error) = self
                .mempool_support
                .sync_from_rpc(&self.rpc_client, current_height)
                .await
            {
                warn!(
                    current_height,
                    %error,
                    "Bitcoin mempool RPC sync failed; falling back to Esplora watch resync"
                );
                detections.extend(self.resync_mempool_watches_from_esplora(watches).await);
            }
        }

        detections.extend(self.mempool_support.drain_detection_queue().await);
        detections
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

    async fn sync_watches(&self, watches: &[SharedWatchEntry]) -> Result<()> {
        self.mempool_support
            .replace_watches(self.script_map(watches))
            .await;
        Ok(())
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
            let amount = alloy::primitives::U256::from(utxo.value);
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
        let mut detections = self.drain_mempool_detections(watches, current_height).await;

        if watches.is_empty() || current_height <= from_exclusive.height {
            return Ok(BlockScan {
                new_cursor: BlockCursor {
                    height: current_height,
                    hash: current_tip_hash.to_string(),
                },
                detections,
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
                    detections,
                });
            }
        }

        let scripts = self.script_map(watches);
        let mut last_hash = from_exclusive.hash.clone();

        for height in (from_exclusive.height + 1)..=current_height {
            let block_hash = self.block_hash_at_height(height).await?;
            let block = self.block_by_hash(&block_hash).await?;
            last_hash = block_hash.to_string();
            detections.extend(match_transaction_against_scripts(
                &txs_from_block(block),
                &scripts,
            ));
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

fn txs_from_block(block: Block) -> Vec<Transaction> {
    block.txdata
}

fn match_transaction_against_scripts(
    transactions: &[Transaction],
    scripts: &HashMap<ScriptBuf, Vec<SharedWatchEntry>>,
) -> Vec<DetectedDeposit> {
    let mut detections = Vec::new();

    for tx in transactions {
        detections.extend(match_single_transaction(tx, scripts, Utc::now()));
    }

    detections
}

fn match_single_transaction(
    tx: &Transaction,
    scripts: &HashMap<ScriptBuf, Vec<SharedWatchEntry>>,
    observed_at: chrono::DateTime<chrono::Utc>,
) -> Vec<DetectedDeposit> {
    let tx_hash = tx.compute_txid().to_string();
    let mut detections = Vec::new();

    for (vout, output) in tx.output.iter().enumerate() {
        let Some(candidates) = scripts.get(&output.script_pubkey) else {
            continue;
        };

        let amount = alloy::primitives::U256::from(output.value.to_sat());
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
                observed_at,
            });
        }
    }

    detections
}

struct BitcoinMempoolSupport {
    watches_by_script: RwLock<HashMap<ScriptBuf, Vec<SharedWatchEntry>>>,
    detection_tx: UnboundedSender<DetectedDeposit>,
    detection_rx: Mutex<UnboundedReceiver<DetectedDeposit>>,
    mempool_transactions: Mutex<HashMap<Txid, Transaction>>,
    matched_txids: Mutex<HashSet<Txid>>,
    rawtx_message_sequence: Mutex<Option<u32>>,
    mempool_sequence: Mutex<Option<u64>>,
    snapshot_height: Mutex<Option<u64>>,
    resync_requested: AtomicBool,
}

impl BitcoinMempoolSupport {
    fn spawn(rawtx_endpoint: String, sequence_endpoint: String) -> Arc<Self> {
        let (detection_tx, detection_rx) = mpsc::unbounded_channel();
        let support = Arc::new(Self {
            watches_by_script: RwLock::new(HashMap::new()),
            detection_tx: detection_tx.clone(),
            detection_rx: Mutex::new(detection_rx),
            mempool_transactions: Mutex::new(HashMap::new()),
            matched_txids: Mutex::new(HashSet::new()),
            rawtx_message_sequence: Mutex::new(None),
            mempool_sequence: Mutex::new(None),
            snapshot_height: Mutex::new(None),
            resync_requested: AtomicBool::new(false),
        });

        tokio::spawn(run_rawtx_listener(rawtx_endpoint, support.clone()));

        tokio::spawn(run_sequence_listener(sequence_endpoint, support.clone()));

        support
    }

    async fn replace_watches(&self, watches_by_script: HashMap<ScriptBuf, Vec<SharedWatchEntry>>) {
        *self.watches_by_script.write().await = watches_by_script;
        self.queue_detections_from_snapshot().await;
    }

    async fn drain_detection_queue(&self) -> Vec<DetectedDeposit> {
        let mut rx = self.detection_rx.lock().await;
        let mut detections = Vec::new();

        loop {
            match rx.try_recv() {
                Ok(detected) => detections.push(detected),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }

        detections
    }

    fn request_resync(&self) {
        self.resync_requested.store(true, Ordering::Release);
    }

    fn take_resync_request(&self) -> bool {
        self.resync_requested.swap(false, Ordering::AcqRel)
    }

    async fn current_snapshot_height(&self) -> Option<u64> {
        *self.snapshot_height.lock().await
    }

    async fn sync_from_rpc(
        &self,
        rpc_client: &BitcoinRpcClient,
        current_height: u64,
    ) -> Result<()> {
        let mempool_txids = rpc_client
            .get_raw_mempool()
            .await
            .context(BitcoinRpcSnafu)?;
        let expected_txids = mempool_txids.into_iter().collect::<HashSet<_>>();

        let removed_txids = {
            let mut transactions = self.mempool_transactions.lock().await;
            let removed = transactions
                .keys()
                .filter(|txid| !expected_txids.contains(*txid))
                .copied()
                .collect::<Vec<_>>();
            transactions.retain(|txid, _| expected_txids.contains(txid));
            removed
        };

        if !removed_txids.is_empty() {
            let mut matched_txids = self.matched_txids.lock().await;
            for txid in removed_txids {
                matched_txids.remove(&txid);
            }
        }

        let missing_txids = {
            let transactions = self.mempool_transactions.lock().await;
            expected_txids
                .iter()
                .filter(|txid| !transactions.contains_key(*txid))
                .copied()
                .collect::<Vec<_>>()
        };

        if !missing_txids.is_empty() {
            let mut fetched_transactions = Vec::with_capacity(missing_txids.len());
            for txid in &missing_txids {
                let tx_bytes = Vec::<u8>::from_hex(
                    &rpc_client
                        .get_raw_transaction_hex(txid, None)
                        .await
                        .context(BitcoinRpcSnafu)?,
                )
                .map_err(|error| crate::error::Error::ChainInit {
                    chain: ChainType::Bitcoin.to_db_string().to_string(),
                    message: format!(
                        "failed to decode Bitcoin mempool snapshot transaction {txid}: {error}"
                    ),
                })?;
                fetched_transactions.push(
                    deserialize::<Transaction>(&tx_bytes).map_err(|error| {
                        crate::error::Error::ChainInit {
                            chain: ChainType::Bitcoin.to_db_string().to_string(),
                            message: format!(
                                "failed to deserialize Bitcoin mempool snapshot transaction {txid}: {error}"
                            ),
                        }
                    })?,
                );
            }

            let mut transactions = self.mempool_transactions.lock().await;
            for tx in fetched_transactions {
                transactions.insert(tx.compute_txid(), tx);
            }
        }

        *self.snapshot_height.lock().await = Some(current_height);
        self.queue_detections_from_snapshot().await;
        Ok(())
    }

    async fn observe_rawtx_sequence(&self, sequence: u32) {
        let mut previous = self.rawtx_message_sequence.lock().await;
        if let Some(last_sequence) = *previous {
            if sequence != last_sequence.wrapping_add(1) {
                warn!(
                    last_sequence,
                    current_sequence = sequence,
                    "Bitcoin rawtx ZMQ sequence gap detected"
                );
                self.request_resync();
            }
        }
        *previous = Some(sequence);
    }

    async fn observe_mempool_sequence(&self, txid: Option<Txid>, sequence: u64, removed: bool) {
        let mut previous = self.mempool_sequence.lock().await;
        if let Some(last_sequence) = *previous {
            if sequence != last_sequence.saturating_add(1) {
                warn!(
                    last_sequence,
                    current_sequence = sequence,
                    "Bitcoin mempool sequence gap detected"
                );
                self.request_resync();
            }
        }
        *previous = Some(sequence);

        if removed {
            if let Some(txid) = txid {
                self.mempool_transactions.lock().await.remove(&txid);
                self.matched_txids.lock().await.remove(&txid);
            }
        }
    }

    async fn queue_detections_for_transaction(&self, tx: &Transaction) {
        self.mempool_transactions
            .lock()
            .await
            .insert(tx.compute_txid(), tx.clone());

        let scripts = self.watches_by_script.read().await;
        if scripts.is_empty() {
            return;
        }

        let detections = match_single_transaction(tx, &scripts, Utc::now());
        drop(scripts);

        if detections.is_empty() {
            return;
        }

        let txid = tx.compute_txid();
        if !self.matched_txids.lock().await.insert(txid) {
            return;
        }

        for detected in detections {
            if self.detection_tx.send(detected).is_err() {
                warn!("Bitcoin mempool detection queue closed unexpectedly");
                break;
            }
        }
    }

    async fn queue_detections_from_snapshot(&self) {
        let transactions = self
            .mempool_transactions
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        if transactions.is_empty() {
            return;
        }

        let scripts = self.watches_by_script.read().await;
        if scripts.is_empty() {
            return;
        }

        let mut detected = Vec::new();
        let mut matched_txids = self.matched_txids.lock().await;

        for tx in transactions {
            let matches = match_single_transaction(&tx, &scripts, Utc::now());
            if matches.is_empty() {
                continue;
            }

            let txid = tx.compute_txid();
            if !matched_txids.insert(txid) {
                continue;
            }

            detected.extend(matches);
        }

        drop(matched_txids);
        drop(scripts);

        for detection in detected {
            if self.detection_tx.send(detection).is_err() {
                warn!("Bitcoin mempool detection queue closed unexpectedly");
                break;
            }
        }
    }
}

async fn run_rawtx_listener(endpoint: String, support: Arc<BitcoinMempoolSupport>) {
    loop {
        let mut socket = zeromq::SubSocket::new();

        if let Err(error) = socket.connect(&endpoint).await {
            warn!(
                endpoint,
                %error,
                "Failed to connect to Bitcoin rawtx ZMQ endpoint; retrying"
            );
            support.request_resync();
            sleep(BITCOIN_ZMQ_RECONNECT_DELAY).await;
            continue;
        }

        if let Err(error) = socket.subscribe("rawtx").await {
            warn!(
                endpoint,
                %error,
                "Failed to subscribe to Bitcoin rawtx ZMQ topic; retrying"
            );
            support.request_resync();
            sleep(BITCOIN_ZMQ_RECONNECT_DELAY).await;
            continue;
        }

        loop {
            match socket.recv().await {
                Ok(message) => {
                    if let Err(error) = handle_rawtx_message(message, support.as_ref()).await {
                        warn!(
                            endpoint,
                            %error,
                            "Failed to process Bitcoin rawtx ZMQ message"
                        );
                        support.request_resync();
                    }
                }
                Err(error) => {
                    warn!(
                        endpoint,
                        %error,
                        "Bitcoin rawtx ZMQ stream disconnected; retrying"
                    );
                    support.request_resync();
                    break;
                }
            }
        }

        sleep(BITCOIN_ZMQ_RECONNECT_DELAY).await;
    }
}

async fn run_sequence_listener(endpoint: String, support: Arc<BitcoinMempoolSupport>) {
    loop {
        let mut socket = zeromq::SubSocket::new();

        if let Err(error) = socket.connect(&endpoint).await {
            warn!(
                endpoint,
                %error,
                "Failed to connect to Bitcoin sequence ZMQ endpoint; retrying"
            );
            support.request_resync();
            sleep(BITCOIN_ZMQ_RECONNECT_DELAY).await;
            continue;
        }

        if let Err(error) = socket.subscribe("sequence").await {
            warn!(
                endpoint,
                %error,
                "Failed to subscribe to Bitcoin sequence ZMQ topic; retrying"
            );
            support.request_resync();
            sleep(BITCOIN_ZMQ_RECONNECT_DELAY).await;
            continue;
        }

        loop {
            match socket.recv().await {
                Ok(message) => {
                    if let Err(error) = handle_sequence_message(message, support.as_ref()).await {
                        warn!(
                            endpoint,
                            %error,
                            "Failed to process Bitcoin sequence ZMQ message"
                        );
                        support.request_resync();
                    }
                }
                Err(error) => {
                    warn!(
                        endpoint,
                        %error,
                        "Bitcoin sequence ZMQ stream disconnected; retrying"
                    );
                    support.request_resync();
                    break;
                }
            }
        }

        sleep(BITCOIN_ZMQ_RECONNECT_DELAY).await;
    }
}

async fn handle_rawtx_message(
    message: ZmqMessage,
    support: &BitcoinMempoolSupport,
) -> std::result::Result<(), String> {
    let frames = message.into_vec();
    if frames.len() != 3 {
        return Err(format!(
            "expected 3 rawtx frames but received {}",
            frames.len()
        ));
    }

    if frames[0].as_ref() != b"rawtx" {
        return Ok(());
    }

    let sequence = parse_u32_le(&frames[2])?;
    support.observe_rawtx_sequence(sequence).await;

    let tx = deserialize::<Transaction>(&frames[1])
        .map_err(|error| format!("failed to deserialize rawtx payload: {error}"))?;
    support.queue_detections_for_transaction(&tx).await;
    Ok(())
}

async fn handle_sequence_message(
    message: ZmqMessage,
    support: &BitcoinMempoolSupport,
) -> std::result::Result<(), String> {
    let frames = message.into_vec();
    if frames.len() != 2 {
        return Err(format!(
            "expected 2 sequence frames but received {}",
            frames.len()
        ));
    }

    if frames[0].as_ref() != b"sequence" {
        return Ok(());
    }

    let body = frames[1].as_ref();
    if body.len() < 33 {
        return Err(format!(
            "sequence message body too short: expected at least 33 bytes but received {}",
            body.len()
        ));
    }

    let event = body[32];
    match event {
        b'A' | b'R' => {
            if body.len() != 41 {
                return Err(format!(
                    "expected 41-byte sequence message for mempool event but received {}",
                    body.len()
                ));
            }

            let txid = parse_hash_le::<Txid>(&body[..32])?;
            let sequence = parse_u64_le(&body[33..41])?;
            support
                .observe_mempool_sequence(Some(txid), sequence, event == b'R')
                .await;
        }
        b'C' | b'D' => {}
        other => {
            return Err(format!("unknown sequence event byte: {other}"));
        }
    }

    Ok(())
}

fn parse_u32_le(bytes: &[u8]) -> std::result::Result<u32, String> {
    let array: [u8; 4] = bytes
        .try_into()
        .map_err(|_| format!("expected 4-byte little-endian integer, got {}", bytes.len()))?;
    Ok(u32::from_le_bytes(array))
}

fn parse_u64_le(bytes: &[u8]) -> std::result::Result<u64, String> {
    let array: [u8; 8] = bytes
        .try_into()
        .map_err(|_| format!("expected 8-byte little-endian integer, got {}", bytes.len()))?;
    Ok(u64::from_le_bytes(array))
}

fn parse_hash_le<T>(bytes: &[u8]) -> std::result::Result<T, String>
where
    T: Hash,
{
    let mut reversed = bytes.to_vec();
    reversed.reverse();
    T::from_slice(&reversed).map_err(|error| error.to_string())
}

struct ReqwestRpcTransport {
    url: Url,
    client: reqwest::Client,
    auth_header: Option<String>,
}

impl ReqwestRpcTransport {
    fn new(url: &str, auth: bitcoincore_rpc_async::Auth) -> std::result::Result<Self, String> {
        let url = Url::parse(url).map_err(|error| error.to_string())?;
        let auth_header = match auth.get_user_pass() {
            Ok(Some((user, password))) => Some(format!(
                "Basic {}",
                STANDARD.encode(format!("{user}:{password}"))
            )),
            Ok(None) => None,
            Err(error) => return Err(error.to_string()),
        };

        Ok(Self {
            url,
            client: reqwest::Client::new(),
            auth_header,
        })
    }
}

#[async_trait]
impl jsonrpc::Transport for ReqwestRpcTransport {
    async fn send_request(
        &self,
        request: jsonrpc::Request<'_>,
    ) -> std::result::Result<jsonrpc::Response, jsonrpc::Error> {
        let mut builder = self.client.post(self.url.clone()).json(&request);
        if let Some(auth_header) = &self.auth_header {
            builder = builder.header("Authorization", auth_header);
        }

        let response = builder
            .send()
            .await
            .map_err(|error| jsonrpc::Error::Transport(error.into()))?;
        response
            .json()
            .await
            .map_err(|error| jsonrpc::Error::Transport(error.into()))
    }

    async fn send_batch(
        &self,
        requests: &[jsonrpc::Request<'_>],
    ) -> std::result::Result<Vec<jsonrpc::Response>, jsonrpc::Error> {
        let mut builder = self.client.post(self.url.clone()).json(requests);
        if let Some(auth_header) = &self.auth_header {
            builder = builder.header("Authorization", auth_header);
        }

        let response = builder
            .send()
            .await
            .map_err(|error| jsonrpc::Error::Transport(error.into()))?;
        response
            .json()
            .await
            .map_err(|error| jsonrpc::Error::Transport(error.into()))
    }

    fn fmt_target(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.url)
    }
}
