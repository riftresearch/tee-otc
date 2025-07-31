use std::sync::Arc;

use common::{
    create_websocket_wallet_provider,
    GenericERC20::{self, GenericERC20Instance},
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

//solc 0.8.28; solc SimpleERC20.sol --via-ir --optimize --bin-runtime
const CBBTC_BYTECODE: &str = "60806040526004361015610011575f80fd5b5f3560e01c806306fdde031461074a578063095ea7b3146106d157806318160ddd146106b457806318cb0ec0146101fc57806323b872dd146105d7578063313ce5671461021757806340c10f191461055357806353f927b11461026f57806370a0823114610237578063948442481461021757806395d89b41146101fc578063a9059cbb146101cb578063bba1964f146101075763dd62ed3e146100b3575f80fd5b34610103576040366003190112610103576100cc610803565b6100d4610819565b6001600160a01b039182165f908152600560209081526040808320949093168252928352819020549051908152f35b5f80fd5b34610103575f366003190112610103576040515f5f546101268161082f565b80845290600181169081156101a7575060011461015e575b61015a8361014e81850382610867565b604051918291826107d9565b0390f35b5f8080525f516020610a785f395f51905f52939250905b80821061018d5750909150810160200161014e61013e565b919260018160209254838588010152019101909291610175565b60ff191660208086019190915291151560051b8401909101915061014e905061013e565b34610103576040366003190112610103576101f16101e7610803565b60243590336109b5565b602060405160018152f35b34610103575f3660031901126101035761015a61014e610889565b34610103575f36600319011261010357602060ff60025416604051908152f35b34610103576020366003190112610103576001600160a01b03610258610803565b165f526004602052602060405f2054604051908152f35b346101035760603660031901126101035760043567ffffffffffffffff8111610103576102a090369060040161092d565b60243567ffffffffffffffff8111610103576102c090369060040161092d565b60443560ff811680910361010357825167ffffffffffffffff8111610461576102e95f5461082f565b601f81116104ec575b506020601f821160011461048057819293945f92610475575b50508160011b915f199060031b1c1916175f555b815167ffffffffffffffff81116104615761033b60015461082f565b601f81116103f9575b50602092601f821160011461038d57928192935f92610382575b50508160011b915f199060031b1c1916176001555b60ff1960025416176002555f80f35b01519050838061035e565b601f1982169360015f525f516020610a985f395f51905f52915f5b8681106103e157508360019596106103c9575b505050811b01600155610373565b01515f1960f88460031b161c191690558380806103bb565b919260206001819286850151815501940192016103a8565b60015f52601f820160051c5f516020610a985f395f51905f5201906020831061044c575b601f0160051c5f516020610a985f395f51905f5201905b8181106104415750610344565b5f8155600101610434565b5f516020610a985f395f51905f52915061041d565b634e487b7160e01b5f52604160045260245ffd5b01519050848061030b565b601f198216905f80525f516020610a785f395f51905f52915f5b8181106104d4575095836001959697106104bc575b505050811b015f5561031f565b01515f1960f88460031b161c191690558480806104af565b9192602060018192868b01518155019401920161049a565b5f8052601f820160051c5f516020610a785f395f51905f5201906020831061053e575b601f0160051c5f516020610a785f395f51905f5201905b81811061053357506102f2565b5f8155600101610526565b5f516020610a785f395f51905f52915061050f565b346101035760403660031901126101035761056c610803565b6001600160a01b03165f7fddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef60206024356105a7851515610983565b6105b381600354610a6a565b60035584845260048252604084206105cc828254610a6a565b9055604051908152a3005b34610103576060366003190112610103576105f0610803565b6105f8610819565b6001600160a01b0382165f81815260056020908152604080832033845290915290205490926044359291838110610683576001810161063d575b506101f193506109b5565b83810390811161066f576101f1945f52600560205260405f2060018060a01b0333165f5260205260405f205584610632565b634e487b7160e01b5f52601160045260245ffd5b60405162461bcd60e51b8152602060048201526009602482015268616c6c6f77616e636560b81b6044820152606490fd5b34610103575f366003190112610103576020600354604051908152f35b34610103576040366003190112610103576106ea610803565b335f8181526005602090815260408083206001600160a01b03909516808452948252918290206024359081905591519182527f8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b92591a3602060405160018152f35b34610103575f366003190112610103576040515f5f546107698161082f565b80845290600181169081156101a757506001146107905761015a8361014e81850382610867565b5f8080525f516020610a785f395f51905f52939250905b8082106107bf5750909150810160200161014e61013e565b9192600181602092548385880101520191019092916107a7565b602060409281835280519182918282860152018484015e5f828201840152601f01601f1916010190565b600435906001600160a01b038216820361010357565b602435906001600160a01b038216820361010357565b90600182811c9216801561085d575b602083101461084957565b634e487b7160e01b5f52602260045260245ffd5b91607f169161083e565b90601f8019910116810190811067ffffffffffffffff82111761046157604052565b604051905f826001549161089c8361082f565b808352926001811690811561090e57506001146108c2575b6108c092500383610867565b565b5060015f90815290915f516020610a985f395f51905f525b8183106108f25750509060206108c0928201016108b4565b60209193508060019154838589010152019101909184926108da565b602092506108c094915060ff191682840152151560051b8201016108b4565b81601f820112156101035780359067ffffffffffffffff82116104615760405192610962601f8401601f191660200185610867565b8284526020838301011161010357815f926020809301838601378301015290565b1561098a57565b606460405162461bcd60e51b81526020600482015260046024820152630746f3d360e41b6044820152fd5b6001600160a01b0390911691906109cd831515610983565b6001600160a01b03165f81815260046020526040902054909190818110610a3b57817fddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef92602092855f52600484520360405f2055845f526004825260405f20818154019055604051908152a3565b60405162461bcd60e51b815260206004820152600760248201526662616c616e636560c81b6044820152606490fd5b9190820180921161066f5756fe290decd9548b62a8d60345a988386fc84ba6bc95484008f6362f93160ef3e563b10e2d527612073b26eecdfd717e6a320cf44b4afac2b0732d9fcbe2b7fa0cf6a2646970667358221220046639b5c3b89eb5a8e808d3efb9abeda6d1f1c31c9dd41cdb37b5dd31a9673164736f6c634300081c0033";

/// Holds all Ethereum-related devnet state.
pub struct EthDevnet {
    pub anvil: Arc<AnvilInstance>,
    pub funded_provider: DynProvider,
    pub funded_address: Address,
    pub deploy_mode: Mode,
    pub anvil_datadir: Option<tempfile::TempDir>,
    pub anvil_dump_path: tempfile::TempDir,
    pub cbbtc_contract: GenericERC20Instance<DynProvider>,
    pub token_indexer: Option<TokenIndexerInstance>,
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
    ) -> Result<Self> {
        let (anvil, anvil_datadir, anvil_dump_path) =
            spawn_anvil(deploy_mode.clone(), devnet_cache.clone()).await?;
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

        let cbbtc_contract = deploy_cbbtc(funded_provider.clone(), devnet_cache.clone()).await?;

        let token_indexer = if let Some(database_url) = token_indexer_database_url {
            Some(
                TokenIndexerInstance::new(
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

async fn deploy_cbbtc(
    provider: DynProvider,
    devnet_cache: Option<Arc<RiftDevnetCache>>,
) -> Result<GenericERC20Instance<DynProvider>> {
    if let Some(cache) = devnet_cache {
        // no need to deploy, just create the instance from the cache
        let cbbtc_contract = GenericERC20Instance::new(CBBTC_ADDRESS.parse().unwrap(), provider);
        return Ok(cbbtc_contract);
    }

    provider
        .anvil_set_code(
            CBBTC_ADDRESS.parse().unwrap(),
            CBBTC_BYTECODE.parse().unwrap(),
        )
        .await?;

    let cbbtc_contract = GenericERC20Instance::new(CBBTC_ADDRESS.parse().unwrap(), provider);

    cbbtc_contract
        .setConfig("CBBTC".to_string(), "CBBTC".to_string(), 9)
        .send()
        .await?
        .get_receipt()
        .await?;

    Ok(cbbtc_contract)
}

/// Spawns Anvil in a blocking task.
async fn spawn_anvil(
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
            .arg("--host")
            .arg("0.0.0.0")
            .chain_id(1337)
            .block_time(1)
            // .arg("--steps-tracing")
            .arg("--timestamp")
            .arg((chrono::Utc::now().timestamp() - 9 * 60 * 60).to_string()) // 9 hours ago? TODO: do we need to do this?
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
            Mode::Local => {}
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
