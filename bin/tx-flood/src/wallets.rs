use std::{
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use alloy::{
    network::TransactionBuilder,
    primitives::{Address, Bytes, U256},
    providers::{DynProvider, Provider, WalletProvider},
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
};
use anyhow::{bail, Context, Result};
use bitcoin::{secp256k1::SecretKey, Network, PrivateKey};
use eip3009_erc20_contract::GenericEIP3009ERC20::GenericEIP3009ERC20Instance;
use futures_util::future::join_all;
use market_maker::{
    bitcoin_wallet::{transaction_broadcaster::BitcoinTransactionBroadcaster, BitcoinWallet},
    evm_wallet::{
        build_transaction_with_validation, create_payment_executions,
        transaction_broadcaster::{
            EVMTransactionBroadcaster, PreflightCheck, TransactionExecutionResult,
        },
        EVMWallet,
    },
    wallet::{Payment, Wallet},
    Result as MarketMakerResult, WalletError,
};
use otc_models::{ChainType, Currency, Lot, TokenIdentifier};
use snafu::location;
use tokio::{fs, task::JoinSet, time::sleep};
use tracing::{debug, info, warn};

use crate::args::{BitcoinWalletConfig, Config, EvmWalletConfig, SwapMode};
use blockchain_utils::create_websocket_wallet_provider;
use eip7702_delegator_contract::EIP7702Delegator::Execution;
use rand::{rngs::OsRng, RngCore};

#[derive(Clone)]
pub enum SinglePaymentWallet {
    Bitcoin(Arc<BitcoinTransactionBroadcaster>),
    Ethereum(Arc<EVMTransactionBroadcaster>, DynProvider),
}

impl SinglePaymentWallet {
    pub fn chain_type(&self) -> ChainType {
        match self {
            SinglePaymentWallet::Bitcoin(_) => ChainType::Bitcoin,
            SinglePaymentWallet::Ethereum(_, _) => ChainType::Ethereum,
        }
    }

    pub async fn create_payment(&self, lot: &Lot, recipient: &str) -> Result<String> {
        match self {
            SinglePaymentWallet::Bitcoin(wallet) => {
                create_bitcoin_payment_with_retry(wallet, lot, recipient).await
            }
            SinglePaymentWallet::Ethereum(wallet, provider) => {
                let sender = wallet.sender;
                let token_address = match &lot.currency.token {
                    TokenIdentifier::Address(address) => address.parse::<Address>().unwrap(),
                    TokenIdentifier::Native => {
                        return Err(map_wallet_error(WalletError::UnsupportedToken {
                            token: lot.currency.token.clone(),
                            loc: location!(),
                        }))
                    }
                };
                let token_contract =
                    GenericEIP3009ERC20Instance::new(token_address, provider.clone());
                let payment_executions = create_payment_executions(
                    &token_contract,
                    &[recipient.parse::<Address>().unwrap()],
                    &[lot.amount],
                );
                let transaction_request = build_transaction_with_validation(
                    &sender,
                    provider.clone(),
                    payment_executions,
                    None,
                )?;
                let wallet_result = wallet
                    .broadcast_transaction(transaction_request, PreflightCheck::Simulate)
                    .await
                    .map_err(map_wallet_error)?;
                match wallet_result {
                    TransactionExecutionResult::Success(tx_receipt) => {
                        Ok(tx_receipt.transaction_hash.to_string())
                    }
                    _ => Err(map_wallet_error(WalletError::TransactionCreationFailed {
                        reason: format!("{wallet_result:?}"),
                    })),
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct PaymentWallets {
    mode: Arc<PaymentWalletMode>,
}

enum PaymentWalletMode {
    Shared(SinglePaymentWallet),
    DualShared {
        bitcoin: Arc<BitcoinTransactionBroadcaster>,
        ethereum: (Arc<EVMTransactionBroadcaster>, DynProvider),
    },
    Dedicated {
        _funding_wallet: SinglePaymentWallet,
        dedicated: DedicatedWallets,
    },
    DualDedicated {
        bitcoin: BitcoinDedicatedWallets,
        ethereum: EvmDedicatedWallets,
        // Maps global swap index to chain-specific wallet index
        index_mapping: Vec<(ChainType, usize)>,
    },
}

impl PaymentWallets {
    fn shared(wallet: SinglePaymentWallet) -> Self {
        Self {
            mode: Arc::new(PaymentWalletMode::Shared(wallet)),
        }
    }

    fn dual_shared(
        bitcoin: Arc<BitcoinTransactionBroadcaster>,
        ethereum: (Arc<EVMTransactionBroadcaster>, DynProvider),
    ) -> Self {
        Self {
            mode: Arc::new(PaymentWalletMode::DualShared { bitcoin, ethereum }),
        }
    }

    fn dedicated(funding_wallet: SinglePaymentWallet, dedicated: DedicatedWallets) -> Self {
        Self {
            mode: Arc::new(PaymentWalletMode::Dedicated {
                _funding_wallet: funding_wallet,
                dedicated,
            }),
        }
    }

    fn dual_dedicated(
        bitcoin: BitcoinDedicatedWallets,
        ethereum: EvmDedicatedWallets,
        index_mapping: Vec<(ChainType, usize)>,
    ) -> Self {
        Self {
            mode: Arc::new(PaymentWalletMode::DualDedicated {
                bitcoin,
                ethereum,
                index_mapping,
            }),
        }
    }

    pub fn chain_type(&self) -> ChainType {
        match self.mode.as_ref() {
            PaymentWalletMode::Shared(wallet) => wallet.chain_type(),
            PaymentWalletMode::DualShared { .. } => ChainType::Bitcoin, // arbitrary for dual mode
            PaymentWalletMode::Dedicated { dedicated, .. } => match dedicated {
                DedicatedWallets::Bitcoin(_) => ChainType::Bitcoin,
                DedicatedWallets::Ethereum(_) => ChainType::Ethereum,
            },
            PaymentWalletMode::DualDedicated { .. } => ChainType::Bitcoin, // arbitrary for dual mode
        }
    }

    pub async fn create_payment(
        &self,
        swap_index: usize,
        lot: &Lot,
        recipient: &str,
    ) -> Result<String> {
        match self.mode.as_ref() {
            PaymentWalletMode::Shared(wallet) => wallet.create_payment(lot, recipient).await,
            PaymentWalletMode::DualShared { bitcoin, ethereum } => {
                match lot.currency.chain {
                    ChainType::Bitcoin => {
                        create_bitcoin_payment_with_retry(bitcoin, lot, recipient).await
                    }
                    ChainType::Ethereum => {
                        let sender = ethereum.0.sender;
                        let token_address = match &lot.currency.token {
                            TokenIdentifier::Address(address) => {
                                address.parse::<Address>().unwrap()
                            }
                            TokenIdentifier::Native => {
                                return Err(map_wallet_error(WalletError::UnsupportedToken {
                                    token: lot.currency.token.clone(),
                                    loc: location!(),
                                }))
                            }
                        };
                        let token_contract =
                            GenericEIP3009ERC20Instance::new(token_address, ethereum.1.clone());
                        let payment_executions = create_payment_executions(
                            &token_contract,
                            &[recipient.parse::<Address>().unwrap()],
                            &[lot.amount],
                        );
                        let transaction_request = build_transaction_with_validation(
                            &sender,
                            ethereum.1.clone(),
                            payment_executions,
                            None,
                        )?;
                        let wallet_result = ethereum
                            .0
                            .broadcast_transaction(transaction_request, PreflightCheck::Simulate)
                            .await
                            .map_err(map_wallet_error)?;
                        match wallet_result {
                            TransactionExecutionResult::Success(tx_receipt) => {
                                Ok(tx_receipt.transaction_hash.to_string())
                            }
                            _ => Err(map_wallet_error(
                                WalletError::TransactionCreationFailed {
                                    reason: format!("{wallet_result:?}"),
                                },
                            )),
                        }
                    }
                }
            }
            PaymentWalletMode::Dedicated { dedicated, .. } => {
                dedicated.create_payment(swap_index, lot, recipient).await
            }
            PaymentWalletMode::DualDedicated {
                bitcoin,
                ethereum,
                index_mapping,
            } => {
                let (chain, wallet_index) = index_mapping
                    .get(swap_index)
                    .ok_or_else(|| {
                        anyhow::anyhow!("no wallet mapping for swap index {}", swap_index)
                    })?;
                
                match chain {
                    ChainType::Bitcoin => {
                        bitcoin.create_payment(*wallet_index, lot, recipient).await
                    }
                    ChainType::Ethereum => {
                        ethereum
                            .create_payment(*wallet_index, lot, recipient)
                            .await
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
enum DedicatedWallets {
    Bitcoin(BitcoinDedicatedWallets),
    Ethereum(EvmDedicatedWallets),
}

impl DedicatedWallets {
    async fn create_payment(
        &self,
        swap_index: usize,
        lot: &Lot,
        recipient: &str,
    ) -> Result<String> {
        match self {
            DedicatedWallets::Bitcoin(wallets) => {
                wallets.create_payment(swap_index, lot, recipient).await
            }
            DedicatedWallets::Ethereum(wallets) => {
                wallets.create_payment(swap_index, lot, recipient).await
            }
        }
    }
}

fn map_wallet_error(error: WalletError) -> anyhow::Error {
    anyhow::Error::new(error)
}

const BTC_PAYMENT_RETRY_ATTEMPTS: usize = 10;
const BTC_PAYMENT_RETRY_BACKOFF: Duration = Duration::from_millis(500);

async fn create_bitcoin_payment_with_retry(
    wallet: &Arc<BitcoinTransactionBroadcaster>,
    lot: &Lot,
    recipient: &str,
) -> Result<String> {
    let mut attempt = 0usize;
    loop {
        attempt += 1;
        let payments = vec![Payment {
            to_address: recipient.to_string(),
            lot: lot.clone(),
        }];
        match wallet.broadcast_transaction(payments, vec![], None).await {
            Ok(txid) => {
                if attempt > 1 {
                    debug!(attempt = attempt, "bitcoin payment succeeded after retry");
                }
                return Ok(txid);
            }
            Err(err) => {
                let err_str = err.to_string();
                if should_retry_missing_or_spent(&err_str) && attempt < BTC_PAYMENT_RETRY_ATTEMPTS {
                    warn!(
                        attempt = attempt,
                        error = %err,
                        "bitcoin payment failed due to missing/spent inputs, retrying"
                    );
                    sleep(BTC_PAYMENT_RETRY_BACKOFF).await;
                    continue;
                }
                return Err(map_wallet_error(WalletError::BitcoinWalletClient {
                    source: err,
                    loc: location!(),
                }));
            }
        }
    }
}

fn should_retry_missing_or_spent(error_message: &str) -> bool {
    let lower = error_message.to_ascii_lowercase();
    lower.contains("bad-txns-inputs-missingorspent")
        || lower.contains("bax-txns-inputs-missingorspent")
}

#[derive(Clone)]
struct BitcoinDedicatedWallets {
    wallets: Arc<Vec<Arc<BitcoinWallet>>>,
    _join_sets: Arc<Vec<JoinSet<MarketMakerResult<()>>>>,
}

impl BitcoinDedicatedWallets {
    fn new(wallets: Vec<Arc<BitcoinWallet>>, join_sets: Vec<JoinSet<MarketMakerResult<()>>>) -> Self {
        Self {
            wallets: Arc::new(wallets),
            _join_sets: Arc::new(join_sets),
        }
    }

    fn len(&self) -> usize {
        self.wallets.len()
    }

    fn funding_payments(&self, currency: &Currency, amount: U256) -> Vec<Payment> {
        self.wallets
            .iter()
            .map(|wallet| Payment {
                to_address: wallet.receive_address(&currency.token),
                lot: Lot {
                    currency: currency.clone(),
                    amount,
                },
            })
            .collect()
    }

    async fn create_payment(&self, index: usize, lot: &Lot, recipient: &str) -> Result<String> {
        let wallet = self
            .wallets
            .get(index)
            .with_context(|| format!("missing dedicated bitcoin wallet for swap {}", index))?;

        wallet
            .create_payment(lot, recipient, None)
            .await
            .map_err(map_wallet_error)
    }
}

#[derive(Clone)]
struct EvmDedicatedWallets {
    wallets: Arc<Vec<Arc<DedicatedEvmWallet>>>,
}

impl EvmDedicatedWallets {
    fn new(wallets: Vec<Arc<DedicatedEvmWallet>>) -> Self {
        Self {
            wallets: Arc::new(wallets),
        }
    }

    fn addresses(&self) -> Vec<Address> {
        self.wallets.iter().map(|wallet| wallet.address()).collect()
    }

    fn len(&self) -> usize {
        self.wallets.len()
    }

    async fn create_payment(&self, index: usize, lot: &Lot, recipient: &str) -> Result<String> {
        let wallet = self
            .wallets
            .get(index)
            .with_context(|| format!("missing dedicated EVM wallet for swap {}", index))?;
        wallet.create_payment(lot, recipient).await
    }
}

#[derive(Clone)]
struct DedicatedEvmWallet {
    provider: Arc<blockchain_utils::WebsocketWalletProvider>,
    address: Address,
}

impl DedicatedEvmWallet {
    fn new(provider: Arc<blockchain_utils::WebsocketWalletProvider>) -> Self {
        let address = provider.default_signer_address();
        Self { provider, address }
    }

    fn address(&self) -> Address {
        self.address
    }

    async fn create_payment(&self, lot: &Lot, recipient: &str) -> Result<String> {
        let recipient_address = Address::from_str(recipient)
            .with_context(|| format!("invalid recipient address: {recipient}"))?;

        match &lot.currency.token {
            TokenIdentifier::Native => {
                self.send_native_transfer(lot.amount, recipient_address)
                    .await
            }
            TokenIdentifier::Address(token_address) => {
                self.send_erc20_transfer(token_address, lot.amount, recipient_address)
                    .await
            }
        }
    }

    async fn send_native_transfer(&self, amount: U256, recipient: Address) -> Result<String> {
        let mut tx = TransactionRequest::default();
        tx.set_to(recipient);
        tx.set_value(amount);
        tx.set_from(self.address);

        let pending = self
            .provider
            .send_transaction(tx)
            .await
            .context("failed to send native transfer")?;

        let receipt = pending
            .get_receipt()
            .await
            .context("failed to fetch receipt for native transfer")?;

        Ok(receipt.transaction_hash.to_string())
    }

    async fn send_erc20_transfer(
        &self,
        token_address: &str,
        amount: U256,
        recipient: Address,
    ) -> Result<String> {
        let token_address = Address::from_str(token_address)
            .with_context(|| format!("invalid token address: {token_address}"))?;

        let contract =
            GenericEIP3009ERC20Instance::new(token_address, self.provider.clone().erased());
        let calldata = contract.transfer(recipient, amount).calldata().clone();

        let mut tx = TransactionRequest::default();
        tx.set_to(token_address);
        tx.set_from(self.address);
        tx.set_input(calldata);
        tx.set_input_and_data();
        tx.set_value(U256::ZERO);

        let pending = self
            .provider
            .send_transaction(tx)
            .await
            .context("failed to send ERC20 transfer")?;

        let receipt = pending
            .get_receipt()
            .await
            .context("failed to fetch receipt for ERC20 transfer")?;

        Ok(receipt.transaction_hash.to_string())
    }
}

pub struct WalletResources {
    pub payment_wallets: PaymentWallets,
    pub join_set: JoinSet<MarketMakerResult<()>>,
}

pub async fn setup_wallets(config: &Config) -> Result<WalletResources> {
    let mut join_set = JoinSet::<MarketMakerResult<()>>::new();

    let payment_wallets = match &config.mode {
        SwapMode::EthStart => {
            let evm_cfg = config
                .evm
                .as_ref()
                .context("missing Ethereum wallet configuration")?;
            let wallet = init_evm_wallet(evm_cfg, &mut join_set).await?;
            let provider = wallet.provider.clone().erased();
            let funding_wallet =
                SinglePaymentWallet::Ethereum(Arc::new(wallet.tx_broadcaster), provider);

            if config.dedicated_wallets.enabled {
                let dedicated =
                    create_dedicated_wallets(&funding_wallet, config).await?;
                info!("dedicated single-payment wallets enabled");
                PaymentWallets::dedicated(funding_wallet, dedicated)
            } else {
                PaymentWallets::shared(funding_wallet)
            }
        }
        SwapMode::BtcStart => {
            let btc_cfg = config
                .bitcoin
                .as_ref()
                .context("missing Bitcoin wallet configuration")?;
            let wallet = init_bitcoin_wallet(btc_cfg, &mut join_set).await?;
            let funding_wallet = SinglePaymentWallet::Bitcoin(Arc::new(wallet.tx_broadcaster));

            if config.dedicated_wallets.enabled {
                let dedicated =
                    create_dedicated_wallets(&funding_wallet, config).await?;
                info!("dedicated single-payment wallets enabled");
                PaymentWallets::dedicated(funding_wallet, dedicated)
            } else {
                PaymentWallets::shared(funding_wallet)
            }
        }
        SwapMode::RandStart { directions } => {
            let btc_cfg = config
                .bitcoin
                .as_ref()
                .context("missing Bitcoin wallet configuration for rand-start mode")?;
            let evm_cfg = config
                .evm
                .as_ref()
                .context("missing Ethereum wallet configuration for rand-start mode")?;

            let btc_wallet = init_bitcoin_wallet(btc_cfg, &mut join_set).await?;
            let evm_wallet = init_evm_wallet(evm_cfg, &mut join_set).await?;

            let btc_broadcaster = Arc::new(btc_wallet.tx_broadcaster);
            let evm_broadcaster = Arc::new(evm_wallet.tx_broadcaster);
            let evm_provider = evm_wallet.provider.clone().erased();

            if config.dedicated_wallets.enabled {
                // Build index mapping: map each swap index to (chain_type, wallet_index)
                let mut index_mapping = Vec::with_capacity(directions.len());
                let mut btc_wallet_index = 0;
                let mut eth_wallet_index = 0;

                for direction in directions {
                    if direction.from_currency.chain == ChainType::Bitcoin {
                        index_mapping.push((ChainType::Bitcoin, btc_wallet_index));
                        btc_wallet_index += 1;
                    } else {
                        index_mapping.push((ChainType::Ethereum, eth_wallet_index));
                        eth_wallet_index += 1;
                    }
                }

                let btc_count = btc_wallet_index;
                let eth_count = eth_wallet_index;

                let bitcoin_dedicated = if btc_count > 0 {
                    create_bitcoin_dedicated_wallets_count(btc_cfg, btc_count)
                        .await?
                } else {
                    BitcoinDedicatedWallets::new(Vec::new(), Vec::new())
                };

                let ethereum_dedicated = if eth_count > 0 {
                    create_evm_dedicated_wallets_count(evm_cfg, eth_count).await?
                } else {
                    EvmDedicatedWallets::new(Vec::new())
                };

                // Fund the dedicated wallets
                if btc_count > 0 {
                    fund_bitcoin_wallets_dedicated(&btc_broadcaster, config, &bitcoin_dedicated)
                        .await?;
                }

                if eth_count > 0 {
                    fund_evm_wallets_dedicated(
                        &evm_broadcaster,
                        evm_provider.clone(),
                        config,
                        &ethereum_dedicated,
                    )
                    .await?;
                }

                info!("dedicated dual-chain wallets enabled");
                PaymentWallets::dual_dedicated(bitcoin_dedicated, ethereum_dedicated, index_mapping)
            } else {
                PaymentWallets::dual_shared(btc_broadcaster, (evm_broadcaster, evm_provider))
            }
        }
    };

    Ok(WalletResources {
        payment_wallets,
        join_set,
    })
}

async fn init_bitcoin_wallet(
    cfg: &BitcoinWalletConfig,
    join_set: &mut JoinSet<MarketMakerResult<()>>,
) -> Result<BitcoinWallet> {
    BitcoinWallet::new(
        &cfg.db_path,
        &cfg.descriptor,
        cfg.network,
        &cfg.esplora_url,
        None,
        join_set,
    )
    .await
    .context("failed to initialise Bitcoin wallet")
}

async fn init_evm_wallet(
    cfg: &EvmWalletConfig,
    join_set: &mut JoinSet<MarketMakerResult<()>>,
) -> Result<EVMWallet> {
    let provider = Arc::new(
        create_websocket_wallet_provider(&cfg.rpc_ws_url, cfg.private_key)
            .await
            .context("failed to create websocket wallet provider")?,
    );

    let wallet = EVMWallet::new(
        provider,
        cfg.debug_rpc_url.clone(),
        cfg.confirmations,
        None,
        join_set,
    );

    wallet
        .ensure_eip7702_delegation(
            PrivateKeySigner::from_slice(&cfg.private_key)
                .context("failed to create private key signer")?,
        )
        .await
        .context("failed to ensure EIP-7702 delegation")?;

    Ok(wallet)
}

async fn create_dedicated_wallets(
    funding_wallet: &SinglePaymentWallet,
    config: &Config,
) -> Result<DedicatedWallets> {
    match funding_wallet {
        SinglePaymentWallet::Bitcoin(broadcaster) => {
            let btc_cfg = config
                .bitcoin
                .as_ref()
                .context("missing Bitcoin wallet configuration")?;
            println!("creating bitcoin dedicated wallets");
            let dedicated_wallets =
                create_bitcoin_dedicated_wallets_count(btc_cfg, config.total_swaps)
                    .await?;
            println!("funding dedicated bitcoin wallets");
            fund_bitcoin_wallets_dedicated(broadcaster, config, &dedicated_wallets).await?;
            Ok(DedicatedWallets::Bitcoin(dedicated_wallets))
        }
        SinglePaymentWallet::Ethereum(broadcaster, provider) => {
            let evm_cfg = config
                .evm
                .as_ref()
                .context("missing Ethereum wallet configuration")?;
            let dedicated_wallets =
                create_evm_dedicated_wallets_count(evm_cfg, config.total_swaps).await?;
            fund_evm_wallets_dedicated(broadcaster, provider.clone(), config, &dedicated_wallets)
                .await?;
            Ok(DedicatedWallets::Ethereum(dedicated_wallets))
        }
    }
}

async fn create_bitcoin_dedicated_wallets_count(
    cfg: &BitcoinWalletConfig,
    count: usize,
) -> Result<BitcoinDedicatedWallets> {
    let wallet_dir = dedicated_bitcoin_wallet_dir(&cfg.db_path);
    fs::create_dir_all(&wallet_dir).await.with_context(|| {
        format!(
            "failed to create wallet directory: {}",
            wallet_dir.display()
        )
    })?;

    // Phase 1: Create all wallets in parallel (I/O-bound operations)
    let creation_futures: Vec<_> = (0..count)
        .map(|index| {
            let wallet_dir = wallet_dir.clone();
            let cfg = cfg.clone();
            async move {
                let mut join_set = JoinSet::new();
                let descriptor = generate_bitcoin_descriptor(cfg.network)?;
                let wallet_path = wallet_dir.join(format!("wallet-{index}.sqlite"));
                
                // Remove existing wallet file to avoid descriptor mismatch
                if wallet_path.exists() {
                    fs::remove_file(&wallet_path).await.with_context(|| {
                        format!("failed to remove old wallet file: {}", wallet_path.display())
                    })?;
                }
                
                let wallet_path_str = wallet_path
                    .to_str()
                    .with_context(|| format!("invalid wallet path: {}", wallet_path.display()))?
                    .to_string();

                let wallet = BitcoinWallet::new(
                    &wallet_path_str,
                    &descriptor,
                    cfg.network,
                    &cfg.esplora_url,
                    None,
                    &mut join_set,
                )
                .await
                .with_context(|| format!("failed to initialise dedicated Bitcoin wallet #{index}"))?;

                let address = wallet.receive_address(&TokenIdentifier::Native);
                info!(
                    wallet_index = index,
                    %address,
                    "created dedicated bitcoin wallet"
                );

                Ok::<_, anyhow::Error>((Arc::new(wallet), join_set))
            }
        })
        .collect();

    let results = join_all(creation_futures).await;

    // Phase 2: Collect wallets and tasks, handling any errors
    let mut wallets = Vec::with_capacity(count);
    let mut join_sets = Vec::with_capacity(count);
    
    for (index, result) in results.into_iter().enumerate() {
        let (wallet, join_set) = result.with_context(|| {
            format!("failed to create dedicated Bitcoin wallet #{index}")
        })?;
        wallets.push(wallet);
        join_sets.push(join_set);
    }

    Ok(BitcoinDedicatedWallets::new(wallets, join_sets))
}

fn dedicated_bitcoin_wallet_dir(db_path: &str) -> PathBuf {
    let base = Path::new(db_path);
    let mut dir = base
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    dir.push("tx-flood-dedicated-wallets");
    dir.push("bitcoin");
    dir
}

fn generate_bitcoin_descriptor(network: Network) -> Result<String> {
    let mut rng = OsRng;
    let secret = SecretKey::new(&mut rng);
    let private_key = PrivateKey::new(secret, network);
    Ok(format!("wpkh({})", private_key.to_wif()))
}

async fn fund_bitcoin_wallets_dedicated(
    broadcaster: &Arc<BitcoinTransactionBroadcaster>,
    config: &Config,
    dedicated_wallets: &BitcoinDedicatedWallets,
) -> Result<()> {
    if dedicated_wallets.len() == 0 {
        return Ok(());
    }

    let funding_currency = Currency {
        chain: ChainType::Bitcoin,
        token: TokenIdentifier::Native,
        decimals: 8,
    };

    // Use max_amount to ensure dedicated wallets have enough for the largest possible swap
    let base_amount = config.max_amount;
    let fee_reserve = U256::from(config.dedicated_wallets.bitcoin_fee_reserve_sats);
    let funding_amount = base_amount.saturating_add(fee_reserve);

    if funding_amount > U256::from(u64::MAX) {
        bail!("bitcoin funding amount exceeds u64 capacity");
    }

    let payments = dedicated_wallets.funding_payments(&funding_currency, funding_amount);

    if payments.is_empty() {
        return Ok(());
    }

    let txid = broadcaster
        .broadcast_transaction(payments, vec![], None)
        .await
        .map_err(|err| {
            map_wallet_error(WalletError::BitcoinWalletClient {
                source: err,
                loc: location!(),
            })
        })?;

    info!(
        %txid,
        wallet_count = dedicated_wallets.len(),
        "funded dedicated bitcoin wallets"
    );

    Ok(())
}

async fn create_evm_dedicated_wallets_count(
    cfg: &EvmWalletConfig,
    count: usize,
) -> Result<EvmDedicatedWallets> {
    let mut wallets = Vec::with_capacity(count);

    for index in 0..count {
        let mut private_key = [0u8; 32];
        loop {
            OsRng.fill_bytes(&mut private_key);
            if private_key.iter().any(|byte| *byte != 0) {
                break;
            }
        }

        let provider = Arc::new(
            create_websocket_wallet_provider(&cfg.rpc_ws_url, private_key)
                .await
                .with_context(|| {
                    format!("failed to create websocket provider for dedicated EVM wallet #{index}")
                })?,
        );

        let address = provider.default_signer_address();
        info!(wallet_index = index, %address, "created dedicated EVM wallet");

        wallets.push(Arc::new(DedicatedEvmWallet::new(provider)));
    }

    Ok(EvmDedicatedWallets::new(wallets))
}

async fn fund_evm_wallets_dedicated(
    broadcaster: &Arc<EVMTransactionBroadcaster>,
    provider: DynProvider,
    config: &Config,
    dedicated_wallets: &EvmDedicatedWallets,
) -> Result<()> {
    if dedicated_wallets.len() == 0 {
        return Ok(());
    }

    let cbbtc_currency = Currency {
        chain: ChainType::Ethereum,
        token: TokenIdentifier::Address("0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf".to_string()),
        decimals: 8,
    };

    // Use max_amount to ensure dedicated wallets have enough for the largest possible swap
    let deposit_amount = config.max_amount;
    let fee_reserve = config.dedicated_wallets.evm_fee_reserve_wei;
    let recipients = dedicated_wallets.addresses();

    let mut executions = Vec::new();

    match &cbbtc_currency.token {
        TokenIdentifier::Native => {
            let total_value = deposit_amount.saturating_add(fee_reserve);
            for recipient in &recipients {
                executions.push(Execution {
                    target: *recipient,
                    value: total_value,
                    callData: Bytes::new(),
                });
            }
        }
        TokenIdentifier::Address(token_address) => {
            let token = Address::parse_checksummed(token_address, None)
                .with_context(|| format!("invalid token address: {token_address}"))?;

            let token_contract = GenericEIP3009ERC20Instance::new(token, provider.clone());

            let mut amounts = Vec::with_capacity(recipients.len());
            amounts.resize(recipients.len(), deposit_amount);

            executions.extend(create_payment_executions(
                &token_contract,
                &recipients,
                &amounts,
            ));

            if !fee_reserve.is_zero() {
                for recipient in &recipients {
                    executions.push(Execution {
                        target: *recipient,
                        value: fee_reserve,
                        callData: Bytes::new(),
                    });
                }
            }
        }
    }

    if executions.is_empty() {
        return Ok(());
    }

    let sender = broadcaster.sender;
    let tx_request = build_transaction_with_validation(&sender, provider.clone(), executions, None)
        .map_err(map_wallet_error)?;

    let broadcast_result = broadcaster
        .broadcast_transaction(tx_request, PreflightCheck::Simulate)
        .await
        .map_err(map_wallet_error)?;

    match broadcast_result {
        TransactionExecutionResult::Success(receipt) => {
            info!(
                tx_hash = %receipt.transaction_hash,
                wallet_count = dedicated_wallets.len(),
                "funded dedicated EVM wallets"
            );
            Ok(())
        }
        other => Err(map_wallet_error(WalletError::TransactionCreationFailed {
            reason: format!("{other:?}"),
        })),
    }
}
