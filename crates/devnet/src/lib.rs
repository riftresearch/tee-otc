//! `lib.rs` — central library code.

pub mod bitcoin_devnet;
pub mod evm_devnet;
pub mod token_indexerd;

pub use bitcoin_devnet::BitcoinDevnet;
use common::P2WPKHBitcoinWallet;
pub use evm_devnet::EthDevnet;

use evm_devnet::ForkConfig;
use log::info;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::task::JoinSet;
use tokio::time::Instant;

use bitcoincore_rpc_async::RpcApi;

use alloy::{
    network::EthereumWallet,
    primitives::{keccak256, Address},
    providers::Provider,
    signers::local::LocalSigner,
};

// ================== Deploy Function ================== //

use crate::evm_devnet::Mode;

const _LOG_CHUNK_SIZE: u64 = 10000;

#[derive(serde::Serialize, serde::Deserialize)]
struct ContractMetadata {
    rift_exchange_address: String,
    token_address: String,
    verifier_address: String,
    deployment_block_number: u64,
    periphery: Option<PeripheryMetadata>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PeripheryMetadata {
    rift_auction_adapter_address: String,
}

pub struct RiftDevnetCache {
    pub cache_dir: PathBuf,
    populated: bool,
}

const CACHE_DIR_NAME: &str = "rift-devnet";
const BITCOIN_DATADIR_NAME: &str = "bitcoin-datadir";
const ESPLORA_DATADIR_NAME: &str = "esplora-datadir";
const ANVIL_DATADIR_NAME: &str = "anvil-datadir";
const ERROR_MESSAGE: &str = "Cache must be populated before utilizing it,";

pub fn get_new_temp_dir() -> Result<tempfile::TempDir> {
    Ok(tempfile::tempdir().unwrap())
}

pub fn get_new_temp_file() -> Result<NamedTempFile> {
    Ok(NamedTempFile::new().map_err(|e| eyre::eyre!("Failed to create temp file: {}", e))?)
}

impl Default for RiftDevnetCache {
    fn default() -> Self {
        Self::new()
    }
}

impl RiftDevnetCache {
    #[must_use]
    pub fn new() -> Self {
        let cache_dir = dirs::cache_dir().unwrap().join(CACHE_DIR_NAME);
        let populated = cache_dir.exists();
        Self {
            cache_dir,
            populated,
        }
    }

    async fn copy_cached_file(
        &self,
        file_path: &str,
        operation_name: &str,
    ) -> Result<tempfile::NamedTempFile> {
        if !self.populated {
            return Err(eyre::eyre!("{} {}", ERROR_MESSAGE, operation_name).into());
        }

        let cache_file = self.cache_dir.join(file_path);
        let temp_file = get_new_temp_file()?;
        let temp_file_path = temp_file.path().to_path_buf();

        let output = tokio::process::Command::new("cp")
            .arg(&cache_file)
            .arg(&temp_file_path)
            .output()
            .await
            .map_err(|e| eyre::eyre!("Failed to copy {}: {}", operation_name, e))?;

        if !output.status.success() {
            return Err(eyre::eyre!("Failed to copy {}: {}", operation_name, output.status).into());
        }

        Ok(temp_file)
    }

    /// Generic helper to copy a cached directory to a new temporary directory
    async fn copy_cached_dir(
        &self,
        dir_name: &str,
        operation_name: &str,
    ) -> Result<tempfile::TempDir> {
        if !self.populated {
            return Err(eyre::eyre!("{} {}", ERROR_MESSAGE, operation_name).into());
        }

        let cache_dir = self.cache_dir.join(dir_name);
        let temp_dir = get_new_temp_dir()?;

        // We need to copy the directory contents, not the directory itself
        let output = tokio::process::Command::new("cp")
            .arg("-R")
            .arg(format!("{}/.", cache_dir.to_string_lossy()))
            .arg(temp_dir.path())
            .output()
            .await
            .map_err(|e| eyre::eyre!("Failed to copy {}: {}", operation_name, e))?;

        if !output.status.success() {
            return Err(eyre::eyre!(
                "Failed to copy {}: {}",
                operation_name,
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }

        Ok(temp_dir)
    }

    pub async fn create_bitcoin_datadir(&self) -> Result<tempfile::TempDir> {
        let temp_dir = self
            .copy_cached_dir(BITCOIN_DATADIR_NAME, "bitcoin datadir")
            .await?;

        // Remove the cached .cookie file as bitcoind will generate a new one
        let cookie_path = temp_dir.path().join("regtest").join(".cookie");
        if cookie_path.exists() {
            tokio::fs::remove_file(&cookie_path)
                .await
                .map_err(|e| eyre::eyre!("Failed to remove cookie file: {}", e))?;
            tracing::info!("Removed cached .cookie file to allow bitcoind to generate a new one");
        }

        Ok(temp_dir)
    }

    pub async fn create_electrsd_datadir(&self) -> Result<tempfile::TempDir> {
        self.copy_cached_dir(ESPLORA_DATADIR_NAME, "electrsd datadir")
            .await
    }

    pub async fn create_anvil_datadir(&self) -> Result<tempfile::TempDir> {
        self.copy_cached_dir(ANVIL_DATADIR_NAME, "anvil datadir")
            .await
    }

    pub async fn save_devnet(&self, mut devnet: RiftDevnet) -> Result<()> {
        use fs2::FileExt;
        use std::fs;
        let save_start = Instant::now();

        // Create cache directory if it doesn't exist
        fs::create_dir_all(&self.cache_dir)
            .map_err(|e| eyre::eyre!("Failed to create cache directory: {}", e))?;

        // Get a file lock to prevent concurrent saves
        let lock_file_path = self.cache_dir.join(".lock");
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&lock_file_path)
            .map_err(|e| eyre::eyre!("Failed to open lock file: {}", e))?;

        // Try to get exclusive lock
        lock_file
            .try_lock_exclusive()
            .map_err(|_| eyre::eyre!("Another process is already saving the cache"))?;

        // Check if cache was populated while waiting for lock
        if self.cache_dir.join(BITCOIN_DATADIR_NAME).exists() {
            tracing::info!("Cache already populated by another process");
            return Ok(());
        }

        info!("[Cache] Starting devnet save to cache...");

        // stop all tasks in the join set so the services dont complain about bitcoin + evm shutting down
        devnet.join_set.abort_all();

        // 1. Gracefully shut down Bitcoin Core to ensure all blocks are flushed to disk
        let bitcoin_shutdown_start = Instant::now();
        info!("[Cache] Shutting down Bitcoin Core to flush all data to disk...");
        match devnet.bitcoin.rpc_client.stop().await {
            Ok(_) => {
                info!("[Cache] Bitcoin Core shutdown initiated successfully");
                // Wait a bit for shutdown to complete
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                info!(
                    "[Cache] Bitcoin shutdown took {:?}",
                    bitcoin_shutdown_start.elapsed()
                );
            }
            Err(e) => {
                // If stop fails, it might already be shutting down or have other issues
                tracing::warn!(
                    "Failed to stop Bitcoin Core gracefully: {}. Proceeding anyway.",
                    e
                );
            }
        }

        // 2. Gracefully shut down the anvil instance
        let anvil_shutdown_start = Instant::now();
        info!("[Cache] Shutting down Anvil to flush all data to disk...");
        let anvil_pid = devnet.ethereum.anvil.child().id();
        tokio::process::Command::new("kill")
            .arg("-SIGTERM")
            .arg(anvil_pid.to_string())
            .output()
            .await
            .map_err(|e| eyre::eyre!("Failed to shutdown Anvil: {}", e))?;
        info!(
            "[Cache] Anvil shutdown took {:?}",
            anvil_shutdown_start.elapsed()
        );

        // 2. Save Bitcoin datadir (now with all blocks properly flushed)
        let _copy_start = Instant::now();
        info!("[Cache] Starting to copy directories to cache...");
        let bitcoin_datadir_src = devnet.bitcoin.bitcoin_datadir.path();
        let bitcoin_datadir_dst = self.cache_dir.join(BITCOIN_DATADIR_NAME);
        let bitcoin_copy_start = Instant::now();
        Self::copy_dir_recursive(bitcoin_datadir_src, &bitcoin_datadir_dst)
            .await
            .map_err(|e| eyre::eyre!("Failed to copy Bitcoin datadir: {}", e))?;
        info!(
            "[Cache] Bitcoin datadir copied in {:?}",
            bitcoin_copy_start.elapsed()
        );

        // Remove the .cookie file from cache as it will be regenerated on startup
        let cached_cookie = bitcoin_datadir_dst.join("regtest").join(".cookie");
        if cached_cookie.exists() {
            tokio::fs::remove_file(&cached_cookie)
                .await
                .map_err(|e| eyre::eyre!("Failed to remove .cookie file: {}", e))?;
            info!("[Cache] Removed .cookie file from cache");
        }

        // 4. Save Electrsd datadir
        let electrsd_datadir_src = devnet.bitcoin.electrsd_datadir.path();
        let electrsd_datadir_dst = self.cache_dir.join(ESPLORA_DATADIR_NAME);
        let electrsd_copy_start = Instant::now();
        Self::copy_dir_recursive(electrsd_datadir_src, &electrsd_datadir_dst).await?;
        info!(
            "[Cache] Electrsd datadir copied in {:?}",
            electrsd_copy_start.elapsed()
        );

        // 7. Save Anvil state file
        // Anvil automatically dumps state on exit to the anvil_datafile when --dump-state is used
        // We just need to copy it to our cache directory
        let anvil_dump_path = devnet.ethereum.anvil_dump_path.path();
        info!(
            "[Cache] Saving anvil state from {}",
            anvil_dump_path.to_string_lossy()
        );

        let anvil_dst = self.cache_dir.join(ANVIL_DATADIR_NAME);
        let anvil_copy_start = Instant::now();
        Self::copy_dir_recursive(anvil_dump_path, &anvil_dst).await?;
        info!(
            "[Cache] Anvil state copied in {:?}",
            anvil_copy_start.elapsed()
        );

        // Release lock by dropping it
        drop(lock_file);

        info!(
            "[Cache] Devnet saved to cache successfully! Total time: {:?}",
            save_start.elapsed()
        );
        Ok(())
    }

    async fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) -> Result<()> {
        tokio::fs::create_dir_all(dst)
            .await
            .map_err(|e| eyre::eyre!("Failed to create directory: {}", e))?;

        // Copy contents of src to dst
        let output = tokio::process::Command::new("cp")
            .arg("-R")
            .arg(format!("{}/.", src.to_string_lossy()))
            .arg(dst)
            .output()
            .await
            .map_err(|e| eyre::eyre!("Failed to copy directory: {}", e))?;

        if !output.status.success() {
            return Err(eyre::eyre!(
                "Failed to copy directory: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }

        Ok(())
    }
}

#[derive(Debug, snafu::Snafu)]
pub enum DevnetError {
    #[snafu(display("Failed to build devnet: {}", source))]
    Build { source: eyre::Report },
}

impl From<eyre::Report> for DevnetError {
    fn from(report: eyre::Report) -> Self {
        DevnetError::Build { source: report }
    }
}

pub type Result<T, E = DevnetError> = std::result::Result<T, E>;

// ================== RiftDevnet ================== //

/// The "combined" Devnet which holds:
/// - a `BitcoinDevnet`
/// - an `EthDevnet`
/// - an optional `RiftIndexer` and `RiftIndexerServer`
pub struct RiftDevnet {
    pub bitcoin: BitcoinDevnet,
    pub ethereum: EthDevnet,
    pub join_set: JoinSet<Result<()>>,
}

impl RiftDevnet {
    #[must_use]
    pub fn builder() -> RiftDevnetBuilder {
        RiftDevnetBuilder::new()
    }

    #[must_use]
    pub fn builder_for_cached() -> RiftDevnetBuilder {
        RiftDevnetBuilder::for_cached()
    }
}

/// A builder for configuring a `RiftDevnet` instantiation.
#[derive(Default)]
pub struct RiftDevnetBuilder {
    interactive: bool,
    funded_evm_addresses: Vec<String>,
    funded_bitcoin_addreses: Vec<String>,
    fork_config: Option<ForkConfig>,
    using_esplora: bool,
    token_indexer_database_url: Option<String>,
}

impl RiftDevnetBuilder {
    /// Create a new builder with all default values.
    #[must_use]
    pub fn new() -> Self {
        Default::default()
    }

    /// Create a builder with settings for a cached devnet.
    #[must_use]
    pub fn for_cached() -> Self {
        RiftDevnetBuilder {
            interactive: false,
            funded_evm_addresses: vec![],
            funded_bitcoin_addreses: vec![],
            fork_config: None,
            using_esplora: true,
            token_indexer_database_url: None,
        }
    }

    /// Toggle whether the devnet runs in "interactive" mode:
    /// - If true, binds Anvil on a stable port and starts a local `RiftIndexerServer`.
    /// - If false, does minimal ephemeral setup.
    #[must_use]
    pub fn interactive(mut self, value: bool) -> Self {
        self.interactive = value;
        self
    }

    pub fn using_token_indexer(mut self, database_url: String) -> Self {
        self.token_indexer_database_url = Some(database_url);
        self
    }

    /// Optionally fund a given EVM address with Ether and tokens.
    pub fn funded_evm_address<T: Into<String>>(mut self, address: T) -> Self {
        self.funded_evm_addresses.push(address.into());
        self
    }

    /// Optionally fund a given Bitcoin address.
    pub fn funded_bitcoin_address<T: Into<String>>(mut self, address: T) -> Self {
        self.funded_bitcoin_addreses.push(address.into());
        self
    }

    /// Provide a fork configuration (RPC URL/block) if you want to fork a public chain.
    #[must_use]
    pub fn fork_config(mut self, config: ForkConfig) -> Self {
        self.fork_config = Some(config);
        self
    }

    /// Start a blockstream/electrs esplora REST API server for bitcoin data indexing.
    #[must_use]
    pub fn using_esplora(mut self, value: bool) -> Self {
        self.using_esplora = value;
        self
    }

    pub async fn build(self) -> Result<(crate::RiftDevnet, u64)> {
        // dont bother with the cache if we're in interactive mode for now
        // could help startup time a little bit if we care to enable it later
        if self.interactive {
            Ok(self.build_internal(None).await?)
        } else {
            let cache = Arc::new(RiftDevnetCache::new());

            if cache.populated {
                tracing::info!("Cache directory exists, loading devnet from cache...");
                let (devnet, funded_sats) = self.build_internal(Some(cache.clone())).await?;
                Ok((devnet, funded_sats))
            } else {
                tracing::info!("Cache directory does not exist, building fresh devnet...");
                let (devnet, funded_sats) = self.build_internal(None).await?;
                Ok((devnet, funded_sats))
            }
        }
    }

    /// Actually build the `RiftDevnet`, consuming this builder.
    ///
    /// Returns a tuple of:
    ///   - The devnet instance
    ///   - The number of satoshis funded to `funded_bitcoin_address` (if any)
    async fn build_internal(
        self,
        devnet_cache: Option<Arc<RiftDevnetCache>>,
    ) -> Result<(crate::RiftDevnet, u64)> {
        let build_start = Instant::now();
        info!("[Devnet Builder] Starting devnet build...");
        let mut join_set = JoinSet::new();

        // 1) Bitcoin side
        let bitcoin_start = Instant::now();
        let (bitcoin_devnet, current_mined_height) = crate::bitcoin_devnet::BitcoinDevnet::setup(
            self.funded_bitcoin_addreses.clone(),
            self.using_esplora,
            self.interactive,
            &mut join_set,
            devnet_cache.clone(),
        )
        .await
        .map_err(|e| eyre::eyre!("[devnet builder] Failed to setup Bitcoin devnet: {}", e))?;
        info!(
            "[Devnet Builder] Bitcoin devnet setup took {:?}",
            bitcoin_start.elapsed()
        );

        // Drop build lock here, only really necessary for bitcoin devnet setup
        let funding_sats = bitcoin_devnet.funded_sats;

        // 2) Collect Bitcoin checkpoint leaves
        info!(
            "[Devnet Builder] Processing checkpoint leaves from block range 0..{current_mined_height}"
        );

        let deploy_mode = if let Some(fork_config) = self.fork_config.clone() {
            Mode::Fork(fork_config)
        } else {
            Mode::Local
        };

        // 5) Ethereum side
        let ethereum_start = Instant::now();

        let ethereum_devnet = crate::evm_devnet::EthDevnet::setup(
            deploy_mode,
            devnet_cache.clone(),
            self.token_indexer_database_url.clone(),
        )
        .await
        .map_err(|e| eyre::eyre!("[devnet builder] Failed to setup Ethereum devnet: {}", e))?;

        info!(
            "[Devnet Builder] Ethereum devnet setup took {:?}",
            ethereum_start.elapsed()
        );

        // 9) Fund optional EVM address with Ether + tokens
        let funding_start = if self.funded_evm_addresses.is_empty() {
            None
        } else {
            info!(
                "[Devnet Builder] Funding {} EVM addresses...",
                self.funded_evm_addresses.len()
            );
            Some(Instant::now())
        };
        for addr_str in self.funded_evm_addresses.clone() {
            use alloy::primitives::Address;
            use std::str::FromStr;
            let address = Address::from_str(&addr_str)
                .map_err(|e| eyre::eyre!("Failed to parse EVM address: {}", e))?; // TODO: check if this is correct

            // ~10 ETH
            ethereum_devnet
                .fund_eth_address(
                    address,
                    alloy::primitives::U256::from_str("10000000000000000000")
                        .map_err(|e| eyre::eyre!("Failed to parse U256: {}", e))?,
                )
                .await
                .map_err(|e| eyre::eyre!("[devnet builder] Failed to fund ETH address: {}", e))?;

            // Debugging: check funded balances
            let eth_balance = ethereum_devnet
                .funded_provider
                .get_balance(address)
                .await
                .map_err(|e| eyre::eyre!("[devnet builder] Failed to get ETH balance: {}", e))?;
            info!("[Devnet Builder] Ether Balance of {addr_str} => {eth_balance:?}");
        }
        if let Some(start) = funding_start {
            info!("[Devnet Builder] Funded addresses in {:?}", start.elapsed());
        }

        if self.interactive {
            self.setup_interactive_mode(
                &bitcoin_devnet,
                &ethereum_devnet,
                self.using_esplora,
                &mut join_set,
            )
            .await?;
        }

        // 11) Return the final devnet
        let devnet = crate::RiftDevnet {
            bitcoin: bitcoin_devnet,
            ethereum: ethereum_devnet,
            join_set,
        };
        info!(
            "[Devnet Builder] Devnet setup took {:?}",
            build_start.elapsed()
        );

        Ok((devnet, funding_sats))
    }

    /// Setup interactive mode with hypernode, market maker, auto-mining, and logging
    async fn setup_interactive_mode(
        &self,
        bitcoin_devnet: &BitcoinDevnet,
        ethereum_devnet: &EthDevnet,
        using_esplora: bool,
        join_set: &mut JoinSet<Result<()>>,
    ) -> Result<()> {
        let setup_start = Instant::now();
        let hypernode_account = MultichainAccount::new(151);
        let market_maker_account = MultichainAccount::new(152);

        // Fund accounts with ETH
        let funding_start = Instant::now();
        info!("[Interactive Setup] Funding accounts with ETH...");
        ethereum_devnet
            .fund_eth_address(
                hypernode_account.ethereum_address,
                alloy::primitives::U256::from_str_radix("1000000000000000000000000", 10)
                    .map_err(|e| eyre::eyre!("Conversion error: {}", e))?,
            )
            .await
            .map_err(|e| {
                eyre::eyre!(
                    "[devnet builder-hypernode] Failed to fund ETH address: {}",
                    e
                )
            })?;

        ethereum_devnet
            .fund_eth_address(
                market_maker_account.ethereum_address,
                alloy::primitives::U256::from_str_radix("1000000000000000000000000", 10)
                    .map_err(|e| eyre::eyre!("Conversion error: {}", e))?,
            )
            .await
            .map_err(|e| {
                eyre::eyre!(
                    "[devnet builder-market_maker] Failed to fund ETH address: {}",
                    e
                )
            })?;

        // Fund market maker with Bitcoin
        info!("[Interactive Setup] Funding market maker with Bitcoin...");
        bitcoin_devnet
            .deal_bitcoin(
                market_maker_account.bitcoin_wallet.address.clone(),
                bitcoin::Amount::from_btc(100.0).unwrap(),
            )
            .await
            .map_err(|e| {
                eyre::eyre!(
                    "[devnet builder-market_maker] Failed to deal bitcoin: {}",
                    e
                )
            })?;
        info!(
            "[Interactive Setup] Account funding took {:?}",
            funding_start.elapsed()
        );

        // Start auto-mining task
        info!("[Interactive Setup] Starting Bitcoin auto-mining task...");
        let bitcoin_rpc_url = bitcoin_devnet.rpc_url_with_cookie.clone();
        let miner_address = bitcoin_devnet.miner_address.clone();
        let cookie = bitcoin_devnet.cookie.clone();

        join_set.spawn(async move {
            use bitcoincore_rpc_async::{Auth, Client as AsyncBitcoinRpcClient, RpcApi};

            // Create dedicated RPC client for mining
            // Use Auth::None since credentials are already embedded in the URL
            let mining_client =
                match AsyncBitcoinRpcClient::new(bitcoin_rpc_url, Auth::CookieFile(cookie)).await {
                    Ok(client) => client,
                    Err(e) => {
                        log::error!("Failed to create mining RPC client: {e}");
                        return Ok(());
                    }
                };

            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                match mining_client.generate_to_address(1, &miner_address).await {
                    Ok(_) => {
                        log::debug!("Auto-mined Bitcoin block");
                    }
                    Err(e) => {
                        log::warn!("Failed to auto-mine Bitcoin block: {e}");
                    }
                }
            }
        });
        info!("[Interactive Setup] Bitcoin auto-mining task started");

        // Log interactive info
        info!(
            "[Interactive Setup] Interactive mode setup complete in {:?}",
            setup_start.elapsed()
        );
        println!("---RIFT DEVNET---");
        println!(
            "Anvil HTTP Url:             http://0.0.0.0:{}",
            ethereum_devnet.anvil.port()
        );
        println!(
            "Anvil WS Url:               ws://0.0.0.0:{}",
            ethereum_devnet.anvil.port()
        );
        println!(
            "Chain ID:                   {}",
            ethereum_devnet.anvil.chain_id()
        );
        println!(
            "Bitcoin RPC URL:            {}",
            bitcoin_devnet.rpc_url_with_cookie
        );

        if using_esplora {
            println!(
                "Esplora API URL:            {}",
                bitcoin_devnet.esplora_url.as_ref().unwrap()
            );
        }

        println!("Bitcoin Auto-mining:        Every 5 seconds");
        println!("Anvil Auto-mining:          Every 1 second");
        println!("---RIFT DEVNET---");

        Ok(())
    }
}

/// Holds the components of a multichain account including secret bytes and wallets.
#[derive(Debug)]
pub struct MultichainAccount {
    /// The raw secret bytes used to derive wallets
    pub secret_bytes: [u8; 32],
    /// The BIP-39 mnemonic phrase for the Bitcoin wallet (seeded from the secret bytes)
    pub bitcoin_mnemonic: bip39::Mnemonic,
    /// The Ethereum wallet derived from the secret
    pub ethereum_wallet: EthereumWallet,
    /// The Ethereum address associated with the wallet
    pub ethereum_address: Address,
    /// The Bitcoin wallet derived from the secret
    pub bitcoin_wallet: P2WPKHBitcoinWallet,
}

impl MultichainAccount {
    /// Creates a new multichain account from the given derivation salt
    #[must_use]
    pub fn new(derivation_salt: u32) -> Self {
        let secret_bytes: [u8; 32] = keccak256(derivation_salt.to_le_bytes()).into();

        let ethereum_wallet =
            EthereumWallet::new(LocalSigner::from_bytes(&secret_bytes.into()).unwrap());

        let ethereum_address = ethereum_wallet.default_signer().address();

        let bitcoin_mnemonic = bip39::Mnemonic::from_entropy(&secret_bytes).unwrap();

        let bitcoin_wallet = P2WPKHBitcoinWallet::from_mnemonic(
            &bitcoin_mnemonic.to_string(),
            None,
            ::bitcoin::Network::Regtest,
            None,
        );

        Self {
            secret_bytes,
            ethereum_wallet,
            ethereum_address,
            bitcoin_mnemonic,
            bitcoin_wallet: bitcoin_wallet.unwrap(),
        }
    }

    /// Creates a new multichain account with the Bitcoin network explicitly specified
    #[must_use]
    pub fn with_network(derivation_salt: u32, network: ::bitcoin::Network) -> Self {
        let secret_bytes: [u8; 32] = keccak256(derivation_salt.to_le_bytes()).into();

        let ethereum_wallet =
            EthereumWallet::new(LocalSigner::from_bytes(&secret_bytes.into()).unwrap());

        let ethereum_address = ethereum_wallet.default_signer().address();

        let bitcoin_mnemonic = bip39::Mnemonic::from_entropy(&secret_bytes).unwrap();

        let bitcoin_wallet =
            P2WPKHBitcoinWallet::from_mnemonic(&bitcoin_mnemonic.to_string(), None, network, None);

        Self {
            secret_bytes,
            bitcoin_mnemonic,
            ethereum_wallet,
            ethereum_address,
            bitcoin_wallet: bitcoin_wallet.unwrap(),
        }
    }
}
