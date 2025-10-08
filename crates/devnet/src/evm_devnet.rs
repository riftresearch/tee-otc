use std::sync::Arc;

use blockchain_utils::create_websocket_wallet_provider;
use eip3009_erc20_contract::GenericEIP3009ERC20::GenericEIP3009ERC20Instance;
use eip7702_delegator_contract::{
    EIP7702Delegator::EIP7702DelegatorInstance, EIP7702_DELEGATOR_BYTECODE,
    EIP7702_DELEGATOR_CROSSCHAIN_ADDRESS,
};
use eyre::{eyre, Result};
use log::info;
use tokio::time::Instant;

use alloy::{
    node_bindings::{Anvil, AnvilInstance},
    primitives::{Address, U256},
    providers::{ext::AnvilApi, DynProvider, Provider},
    sol,
};

use crate::{get_new_temp_dir, token_indexerd::TokenIndexerInstance, RiftDevnetCache};

const CBBTC_ADDRESS: &str = "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf";

/// Holds all Ethereum-related devnet state.
pub struct EthDevnet {
    pub anvil: Arc<AnvilInstance>,
    pub funded_provider: DynProvider,
    pub funded_address: Address,
    pub deploy_mode: Mode,
    pub anvil_datadir: Option<tempfile::TempDir>,
    pub anvil_dump_path: tempfile::TempDir,
    pub cbbtc_contract: GenericEIP3009ERC20Instance<DynProvider>,
    pub token_indexer: Option<TokenIndexerInstance>,
    pub eip7702_delegator_contract: EIP7702DelegatorInstance<DynProvider>,
}

#[derive(Clone, Debug)]
pub enum Mode {
    Fork(ForkConfig),
    Local,
}

impl EthDevnet {
    /// Spawns Anvil, deploys the EVM contracts, returns `(Self, deployment_block_number)`.
    pub async fn setup(
        deploy_mode: Mode,
        devnet_cache: Option<Arc<RiftDevnetCache>>,
        token_indexer_database_url: Option<String>,
        interactive: bool,
    ) -> Result<Self> {
        let (anvil, anvil_datadir, anvil_dump_path) =
            spawn_anvil(interactive, deploy_mode.clone(), devnet_cache.clone()).await?;
        info!(
            "Anvil spawned at {}, chain_id={}",
            anvil.endpoint(),
            anvil.chain_id()
        );

        let private_key = anvil.keys()[0].clone().to_bytes().into();
        let funded_address = anvil.addresses()[0].clone();

        let funded_provider = create_websocket_wallet_provider(
            anvil.ws_endpoint_url().to_string().as_str(),
            private_key,
        )
        .await
        .map_err(|e| eyre!(e.to_string()))?
        .erased();

        let (cbbtc_contract, eip7702_delegator_contract) =
            deploy_contracts(funded_provider.clone(), devnet_cache.clone()).await?;

        let token_indexer = if let Some(database_url) = token_indexer_database_url {
            Some(
                TokenIndexerInstance::new(
                    interactive,
                    anvil.endpoint_url().to_string().as_str(),
                    anvil.ws_endpoint_url().to_string().as_str(),
                    false,
                    anvil.chain_id(),
                    database_url,
                )
                .await?,
            )
        } else {
            None
        };

        let devnet = EthDevnet {
            anvil: anvil.into(),
            funded_provider,
            funded_address,
            deploy_mode,
            anvil_datadir,
            anvil_dump_path,
            cbbtc_contract,
            token_indexer,
            eip7702_delegator_contract,
        };

        Ok(devnet)
    }

    /// Gives `amount_wei` of Ether to `address` (via `anvil_set_balance`).
    pub async fn fund_eth_address(&self, address: Address, amount_wei: U256) -> Result<()> {
        self.funded_provider
            .anvil_set_balance(address, amount_wei)
            .await?;
        Ok(())
    }

    pub async fn mint_cbbtc(&self, address: Address, amount: U256) -> Result<()> {
        self.cbbtc_contract
            .mint(address, amount)
            .send()
            .await?
            .get_receipt()
            .await?;
        Ok(())
    }

    /*
    /// Mints the mock token for `address`.
    pub async fn mint_token(&self, address: Address, amount: U256) -> Result<()> {
        let impersonate_provider = ProviderBuilder::new()
            .connect_http(format!("http://localhost:{}", self.anvil.port()).parse()?);
        if matches!(self.deploy_mode, Mode::Fork(_)) {
            // 1. Get the master minter address
            let master_minter = self.token_contract.masterMinter().call().await?;

            // 2. Configure master minter with maximum minting allowance
            let max_allowance = U256::MAX;
            let configure_minter_calldata = self
                .token_contract
                .configureMinter(master_minter, max_allowance)
                .calldata()
                .clone();

            let tx = TransactionRequest::default()
                .with_from(master_minter)
                .with_to(*self.token_contract.address())
                .with_input(configure_minter_calldata.clone());

            impersonate_provider
                .anvil_impersonate_account(master_minter)
                .await?;

            impersonate_provider
                .send_transaction(tx)
                .await?
                .get_receipt()
                .await?;

            let mint_calldata = self.token_contract.mint(address, amount).calldata().clone();

            let tx = TransactionRequest::default()
                .with_from(master_minter)
                .with_to(*self.token_contract.address())
                .with_input(mint_calldata.clone());

            // 3. Mint tokens as master minter
            impersonate_provider
                .send_transaction(tx)
                .await?
                .get_receipt()
                .await?;
        } else {
            // For local devnet, directly mint tokens
            self.token_contract
                .mint(address, amount)
                .send()
                .await?
                .get_receipt()
                .await?;
        }
        Ok(())
    }

    */
}

#[derive(Clone, Debug)]
pub struct ForkConfig {
    pub url: String,
    pub block_number: Option<u64>,
}

async fn deploy_contracts(
    provider: DynProvider,
    devnet_cache: Option<Arc<RiftDevnetCache>>,
) -> Result<(
    GenericEIP3009ERC20Instance<DynProvider>,
    EIP7702DelegatorInstance<DynProvider>,
)> {
    if let Some(cache) = devnet_cache {
        // no need to deploy, just create the instance from the cache
        let cbbtc_contract =
            GenericEIP3009ERC20Instance::new(CBBTC_ADDRESS.parse().unwrap(), provider.clone());
        let delegator_contract = EIP7702DelegatorInstance::new(
            EIP7702_DELEGATOR_CROSSCHAIN_ADDRESS.parse().unwrap(),
            provider,
        );
        return Ok((cbbtc_contract, delegator_contract));
    }

    let cbbtc_deployment = GenericEIP3009ERC20Instance::deploy(provider.clone()).await?;
    let cbbtc_deployed_bytecode = provider
        .clone()
        .get_code_at(*cbbtc_deployment.address())
        .await?;

    provider
        .anvil_set_code(
            CBBTC_ADDRESS.parse().unwrap(),
            cbbtc_deployed_bytecode.clone(),
        )
        .await?;

    let cbbtc_contract =
        GenericEIP3009ERC20Instance::new(CBBTC_ADDRESS.parse().unwrap(), provider.clone());

    provider
        .anvil_set_code(
            EIP7702_DELEGATOR_CROSSCHAIN_ADDRESS.parse().unwrap(),
            EIP7702_DELEGATOR_BYTECODE.parse().unwrap(),
        )
        .await?;

    let delegator_contract = EIP7702DelegatorInstance::new(
        EIP7702_DELEGATOR_CROSSCHAIN_ADDRESS.parse().unwrap(),
        provider,
    );

    Ok((cbbtc_contract, delegator_contract))
}

/// Spawns Anvil in a blocking task.
async fn spawn_anvil(
    interactive: bool,
    mode: Mode,
    devnet_cache: Option<Arc<RiftDevnetCache>>,
) -> Result<(AnvilInstance, Option<tempfile::TempDir>, tempfile::TempDir)> {
    let spawn_start = Instant::now();
    // Create or load anvil datafile
    let anvil_datadir = if devnet_cache.is_some() {
        let cache_start = Instant::now();
        let datadir = Some(
            devnet_cache
                .as_ref()
                .unwrap()
                .create_anvil_datadir()
                .await?,
        );
        info!(
            "[Anvil] Created anvil datadir from cache in {:?}",
            cache_start.elapsed()
        );
        datadir
    } else {
        None
    };

    let anvil_datadir_pathbuf = anvil_datadir.as_ref().map(|dir| dir.path().to_path_buf());

    // get a directory for the --dump-state flag
    let anvil_dump_path = get_new_temp_dir()?;
    let anvil_dump_pathbuf = anvil_dump_path.path().to_path_buf();

    let anvil_instance = tokio::task::spawn_blocking(move || {
        let mut anvil = Anvil::new()
            .prague()
            .arg("--host")
            .arg("0.0.0.0")
            .chain_id(1337)
            .block_time(1)
            // .arg("--steps-tracing")
            .arg("--dump-state")
            .arg(anvil_dump_pathbuf.to_string_lossy().to_string());

        // Load state if file exists and has content - Anvil can handle the file format directly
        if let Some(state_path) = anvil_datadir_pathbuf {
            info!(
                "[Anvil] Loading state from {}",
                state_path.to_string_lossy()
            );
            anvil = anvil
                .arg("--load-state")
                .arg(state_path.to_string_lossy().to_string());
        }

        match mode {
            Mode::Fork(fork_config) => {
                anvil = anvil.port(50101_u16);
                anvil = anvil.fork(fork_config.url);
                if let Some(block_number) = fork_config.block_number {
                    anvil = anvil.fork_block_number(block_number);
                }
            }
            Mode::Local => {
                if interactive {
                    anvil = anvil.port(50101_u16);
                }
            }
        }
        anvil.try_spawn().map_err(|e| {
            eprintln!("Failed to spawn Anvil: {e:?}");
            eyre!(e)
        })
    })
    .await??;

    info!("[Anvil] Anvil spawned in {:?}", spawn_start.elapsed());

    // print the stdout of the anvil instance
    /*
    let anvil_child = anvil_instance.child_mut();
    let anvil_stdout = anvil_child.stdout.take().unwrap();

    tokio::task::spawn_blocking(move || {
        use std::io::{BufRead, BufReader};

        let stdout_reader = BufReader::new(anvil_stdout);
        for line in stdout_reader.lines().map_while(Result::ok) {
            println!("anvil stdout: {}", line);
        }
    });
    */

    Ok((anvil_instance, anvil_datadir, anvil_dump_path))
}
