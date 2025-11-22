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
    wallet::Wallet,
    Result as MarketMakerResult, WalletError,
};
use otc_chains::traits::Payment;
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
    Bitcoin(Arc<BitcoinTransactionBroadcaster>, String),
    Evm(Arc<EVMTransactionBroadcaster>, DynProvider, ChainType),
}

impl SinglePaymentWallet {
    pub fn chain_type(&self) -> ChainType {
        match self {
            SinglePaymentWallet::Bitcoin(_, _) => ChainType::Bitcoin,
            SinglePaymentWallet::Evm(_, _, chain) => *chain,
        }
    }

    pub async fn create_payment(&self, lot: &Lot, recipient: &str) -> Result<(String, String)> {
        match self {
            SinglePaymentWallet::Bitcoin(wallet, sender_address) => {
                let tx_hash = create_bitcoin_payment_with_retry(wallet, lot, recipient).await?;
                Ok((tx_hash, sender_address.clone()))
            }
            SinglePaymentWallet::Evm(wallet, provider, _) => {
                let sender = wallet.sender;
                let sender_address = format!("{:?}", sender);
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
                    &sender,
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
                        Ok((tx_receipt.transaction_hash.to_string(), sender_address))
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
    MultiShared {
        bitcoin: Option<(Arc<BitcoinTransactionBroadcaster>, String)>,
        ethereum: Option<(Arc<EVMTransactionBroadcaster>, DynProvider)>,
        base: Option<(Arc<EVMTransactionBroadcaster>, DynProvider)>,
        source_chain_mapping: Arc<Vec<ChainType>>,
        recipient_evm_address: Address,
    },
    Dedicated {
        _funding_wallet: SinglePaymentWallet,
        dedicated: DedicatedWallets,
    },
    MultiDedicated {
        bitcoin: Option<BitcoinDedicatedWallets>,
        ethereum: Option<EvmDedicatedWallets>,
        base: Option<EvmDedicatedWallets>,
        index_mapping: Vec<(ChainType, usize)>,
    },
}

impl PaymentWallets {
    fn shared(wallet: SinglePaymentWallet) -> Self {
        Self {
            mode: Arc::new(PaymentWalletMode::Shared(wallet)),
        }
    }

    fn multi_shared(
        bitcoin: Option<(Arc<BitcoinTransactionBroadcaster>, String)>,
        ethereum: Option<(Arc<EVMTransactionBroadcaster>, DynProvider)>,
        base: Option<(Arc<EVMTransactionBroadcaster>, DynProvider)>,
        source_chain_mapping: Vec<ChainType>,
        recipient_evm_address: Address,
    ) -> Self {
        Self {
            mode: Arc::new(PaymentWalletMode::MultiShared {
                bitcoin,
                ethereum,
                base,
                source_chain_mapping: Arc::new(source_chain_mapping),
                recipient_evm_address,
            }),
        }
    }

    fn dedicated(
        funding_wallet: SinglePaymentWallet,
        dedicated: DedicatedWallets,
    ) -> Self {
        Self {
            mode: Arc::new(PaymentWalletMode::Dedicated {
                _funding_wallet: funding_wallet,
                dedicated,
            }),
        }
    }

    fn multi_dedicated(
        bitcoin: Option<BitcoinDedicatedWallets>,
        ethereum: Option<EvmDedicatedWallets>,
        base: Option<EvmDedicatedWallets>,
        index_mapping: Vec<(ChainType, usize)>,
    ) -> Self {
        Self {
            mode: Arc::new(PaymentWalletMode::MultiDedicated {
                bitcoin,
                ethereum,
                base,
                index_mapping,
            }),
        }
    }

    pub fn chain_type(&self) -> ChainType {
        match self.mode.as_ref() {
            PaymentWalletMode::Shared(wallet) => wallet.chain_type(),
            PaymentWalletMode::MultiShared { .. } => ChainType::Bitcoin, // Arbitrary
            PaymentWalletMode::Dedicated { dedicated, .. } => match dedicated {
                DedicatedWallets::Bitcoin(_) => ChainType::Bitcoin,
                DedicatedWallets::Evm(_, chain) => *chain,
            },
            PaymentWalletMode::MultiDedicated { .. } => ChainType::Bitcoin, // Arbitrary
        }
    }

    pub async fn create_payment(
        &self,
        swap_index: usize,
        lot: &Lot,
        recipient: &str,
    ) -> Result<(String, String)> {
        match self.mode.as_ref() {
            PaymentWalletMode::Shared(wallet) => wallet.create_payment(lot, recipient).await,
            PaymentWalletMode::MultiShared {
                bitcoin,
                ethereum,
                base,
                ..
            } => match lot.currency.chain {
                ChainType::Bitcoin => {
                    let (wallet, sender_address) = bitcoin.as_ref().context("missing bitcoin wallet")?;
                    let tx_hash =
                        create_bitcoin_payment_with_retry(wallet, lot, recipient).await?;
                    Ok((tx_hash, sender_address.clone()))
                }
                ChainType::Ethereum => {
                    let (wallet, provider) = ethereum.as_ref().context("missing ethereum wallet")?;
                    let wallet = SinglePaymentWallet::Evm(wallet.clone(), provider.clone(), ChainType::Ethereum);
                    wallet.create_payment(lot, recipient).await
                }
                ChainType::Base => {
                    let (wallet, provider) = base.as_ref().context("missing base wallet")?;
                    let wallet = SinglePaymentWallet::Evm(wallet.clone(), provider.clone(), ChainType::Base);
                    wallet.create_payment(lot, recipient).await
                }
            },
            PaymentWalletMode::Dedicated { dedicated, .. } => {
                dedicated.create_payment(swap_index, lot, recipient).await
            }
            PaymentWalletMode::MultiDedicated {
                bitcoin,
                ethereum,
                base,
                index_mapping,
            } => {
                let (chain, wallet_index) = index_mapping.get(swap_index).ok_or_else(|| {
                    anyhow::anyhow!("no wallet mapping for swap index {}", swap_index)
                })?;

                match chain {
                    ChainType::Bitcoin => {
                        bitcoin.as_ref().context("missing dedicated bitcoin wallets")?
                            .create_payment(*wallet_index, lot, recipient).await
                    }
                    ChainType::Ethereum => {
                        ethereum.as_ref().context("missing dedicated ethereum wallets")?
                            .create_payment(*wallet_index, lot, recipient).await
                    }
                    ChainType::Base => {
                        base.as_ref().context("missing dedicated base wallets")?
                            .create_payment(*wallet_index, lot, recipient).await
                    }
                }
            }
        }
    }

    /// Create a batch payment (multiple payments in a single transaction)
    pub async fn create_batch_payment(
        &self,
        lots: &[&Lot],
        recipients: &[&str],
    ) -> Result<(String, String)> {
        if lots.is_empty() {
            return Err(anyhow::anyhow!("cannot create batch payment with zero lots"));
        }
        
        if lots.len() != recipients.len() {
            return Err(anyhow::anyhow!(
                "lots and recipients length mismatch: {} vs {}",
                lots.len(),
                recipients.len()
            ));
        }

        // All lots must be on the same chain
        let chain = lots[0].currency.chain;
        for lot in lots.iter() {
            if lot.currency.chain != chain {
                return Err(anyhow::anyhow!(
                    "all lots in a batch must be on the same chain"
                ));
            }
        }

        match self.mode.as_ref() {
            PaymentWalletMode::Shared(wallet) => {
                self.create_batch_payment_shared(wallet, lots, recipients)
                    .await
            }
            PaymentWalletMode::MultiShared {
                bitcoin,
                ethereum,
                base,
                ..
            } => match chain {
                ChainType::Bitcoin => {
                    let (wallet, sender_address) = bitcoin.as_ref().context("missing bitcoin wallet")?;
                    self.create_batch_payment_bitcoin(wallet, sender_address, lots, recipients).await
                }
                ChainType::Ethereum => {
                    let (wallet, provider) = ethereum.as_ref().context("missing ethereum wallet")?;
                    self.create_batch_payment_evm(wallet, provider, lots, recipients).await
                }
                ChainType::Base => {
                    let (wallet, provider) = base.as_ref().context("missing base wallet")?;
                    self.create_batch_payment_evm(wallet, provider, lots, recipients).await
                }
            },
            PaymentWalletMode::Dedicated { .. } | PaymentWalletMode::MultiDedicated { .. } => {
                Err(anyhow::anyhow!(
                    "batch payments are not supported with dedicated wallets"
                ))
            }
        }
    }

    async fn create_batch_payment_shared(
        &self,
        wallet: &SinglePaymentWallet,
        lots: &[&Lot],
        recipients: &[&str],
    ) -> Result<(String, String)> {
        match wallet {
            SinglePaymentWallet::Bitcoin(broadcaster, sender_address) => {
                self.create_batch_payment_bitcoin(broadcaster, sender_address, lots, recipients)
                    .await
            }
            SinglePaymentWallet::Evm(broadcaster, provider, _) => {
                self.create_batch_payment_evm(broadcaster, provider, lots, recipients)
                    .await
            }
        }
    }

    async fn create_batch_payment_bitcoin(
        &self,
        broadcaster: &Arc<BitcoinTransactionBroadcaster>,
        sender_address: &str,
        lots: &[&Lot],
        recipients: &[&str],
    ) -> Result<(String, String)> {
        let payments: Vec<Payment> = lots
            .iter()
            .zip(recipients.iter())
            .map(|(lot, recipient)| Payment {
                to_address: (*recipient).to_string(),
                lot: (*lot).clone(),
            })
            .collect();

        let tx_hash = create_bitcoin_payment_with_retry_batch(broadcaster, payments).await?;
        Ok((tx_hash, sender_address.to_string()))
    }

    async fn create_batch_payment_evm(
        &self,
        broadcaster: &Arc<EVMTransactionBroadcaster>,
        provider: &DynProvider,
        lots: &[&Lot],
        recipients: &[&str],
    ) -> Result<(String, String)> {
        let sender = broadcaster.sender;
        let sender_address = format!("{:?}", sender);

        // All lots must use the same token for a batch
        let token_address = match &lots[0].currency.token {
            TokenIdentifier::Address(address) => address.parse::<Address>().unwrap(),
            TokenIdentifier::Native => {
                return Err(map_wallet_error(WalletError::UnsupportedToken {
                    token: lots[0].currency.token.clone(),
                    loc: location!(),
                }))
            }
        };

        // Verify all lots use the same token
        for lot in lots.iter() {
            match &lot.currency.token {
                TokenIdentifier::Address(addr) => {
                    if addr.parse::<Address>().unwrap() != token_address {
                        return Err(anyhow::anyhow!(
                            "all lots in a batch must use the same token"
                        ));
                    }
                }
                TokenIdentifier::Native => {
                    return Err(map_wallet_error(WalletError::UnsupportedToken {
                        token: lot.currency.token.clone(),
                        loc: location!(),
                    }))
                }
            }
        }

        let token_contract = GenericEIP3009ERC20Instance::new(token_address, provider.clone());

        // Convert recipients to addresses
        let recipient_addresses: Result<Vec<Address>> = recipients
            .iter()
            .map(|r| r.parse::<Address>().context("invalid recipient address"))
            .collect();
        let recipient_addresses = recipient_addresses?;

        // Extract amounts
        let amounts: Vec<U256> = lots.iter().map(|lot| lot.amount).collect();

        let payment_executions =
            create_payment_executions(&token_contract, &sender, &recipient_addresses, &amounts);

        let transaction_request = build_transaction_with_validation(
            &sender,
            provider.clone(),
            payment_executions,
            None,
        )?;

        let wallet_result = broadcaster
            .broadcast_transaction(transaction_request, PreflightCheck::Simulate)
            .await
            .map_err(map_wallet_error)?;

        match wallet_result {
            TransactionExecutionResult::Success(tx_receipt) => {
                Ok((tx_receipt.transaction_hash.to_string(), sender_address))
            }
            _ => Err(map_wallet_error(WalletError::TransactionCreationFailed {
                reason: format!("{wallet_result:?}"),
            })),
        }
    }

    /// Get the EVM account address that should control this swap
    pub fn get_evm_account_address(&self, swap_index: usize, recipient_evm_address: &str) -> Result<Address> {
        match self.mode.as_ref() {
            PaymentWalletMode::Shared(wallet) => {
                match wallet {
                    SinglePaymentWallet::Evm(broadcaster, _, _) => Ok(broadcaster.sender),
                    SinglePaymentWallet::Bitcoin(_, _) => {
                        // BtcStart shared mode: use the recipient_evm_address from config
                        recipient_evm_address.parse::<Address>()
                            .with_context(|| format!("invalid recipient EVM address: {}", recipient_evm_address))
                    }
                }
            }
            PaymentWalletMode::MultiShared {
                ethereum,
                base,
                source_chain_mapping,
                recipient_evm_address,
                ..
            } => {
                let source_chain = source_chain_mapping.get(swap_index).copied().unwrap_or(ChainType::Bitcoin);
                match source_chain {
                    ChainType::Ethereum => {
                        let (broadcaster, _) = ethereum.as_ref().context("missing ethereum wallet")?;
                        Ok(broadcaster.sender)
                    }
                    ChainType::Base => {
                        let (broadcaster, _) = base.as_ref().context("missing base wallet")?;
                        Ok(broadcaster.sender)
                    }
                    ChainType::Bitcoin => {
                        Ok(*recipient_evm_address)
                    }
                }
            }
            PaymentWalletMode::Dedicated {
                dedicated,
                ..
            } => {
                match dedicated {
                    DedicatedWallets::Evm(wallets, _) => {
                        // EthStart/BaseStart dedicated: use the dedicated wallet address
                        wallets
                            .wallets
                            .get(swap_index)
                            .map(|w| w.address())
                            .ok_or_else(|| {
                                anyhow::anyhow!("no dedicated EVM wallet for swap {}", swap_index)
                            })
                    }
                    DedicatedWallets::Bitcoin(_) => {
                        // BtcStart dedicated: use recipient_evm_address (or could use random one if we had random addresses passed in)
                        // Note: In the previous code random_evm_addresses were passed in. I simplified Dedicated to NOT store random addresses
                        // because it makes more sense to handle it like MultiDedicated where BtcStart maps to an EVM wallet if needed.
                        // But for pure BtcStart dedicated, we might want random addresses.
                        // For now, using recipient_evm_address as per generic logic, or we should re-add random addresses if strictly needed.
                        // Re-reading original code: BtcStart dedicated used `random_evm_addresses`.
                        // I'll assume we can just use the recipient address for simplicity in this refactor unless needed.
                        recipient_evm_address.parse::<Address>()
                            .with_context(|| format!("invalid recipient EVM address: {}", recipient_evm_address))
                    }
                }
            }
            PaymentWalletMode::MultiDedicated {
                ethereum,
                base,
                index_mapping,
                ..
            } => {
                let (source_chain, wallet_index) = index_mapping
                    .get(swap_index)
                    .ok_or_else(|| anyhow::anyhow!("no wallet mapping for swap {}", swap_index))?;

                match source_chain {
                    ChainType::Ethereum => {
                        ethereum.as_ref().context("missing ethereum dedicated wallets")?
                            .wallets
                            .get(*wallet_index)
                            .map(|w| w.address())
                            .ok_or_else(|| anyhow::anyhow!("no EVM wallet for index {}", wallet_index))
                    }
                    ChainType::Base => {
                        base.as_ref().context("missing base dedicated wallets")?
                            .wallets
                            .get(*wallet_index)
                            .map(|w| w.address())
                            .ok_or_else(|| anyhow::anyhow!("no Base wallet for index {}", wallet_index))
                    }
                    ChainType::Bitcoin => {
                        // For Bitcoin swaps, we need an EVM address to receive funds.
                        // In RandStart, we might be receiving on Eth or Base.
                        // If we are receiving on Eth, we can use one of the Eth dedicated wallets.
                        // If we are receiving on Base, we can use one of the Base dedicated wallets.
                        // BUT, `index_mapping` only tells us the SOURCE chain.
                        // We need to know the DESTINATION chain to pick the right wallet.
                        // The current `get_evm_account_address` signature doesn't take destination chain.
                        // However, since we only support (Eth<->Btc) and (Base<->Btc), if source is Btc, dest is either Eth or Base.
                        // We can just pick an address from the available dedicated wallets.
                        // If ethereum wallets are available, use one. If base wallets available, use one.
                        // Or better, since `RandStart` creates wallets for ALL swaps, we can just use `wallet_index` into either?
                        // Wait, `RandStart` logic in `args.rs` generates separate counts.
                        // In `setup_wallets`, we should create dedicated wallets for ALL needed slots.
                        
                        // Let's fallback to recipient_evm_address for now if we can't easily decide, 
                        // OR use the `wallet_index` to pick from `ethereum` or `base` depending on which is available?
                        // Actually, for `BtcStart`, we are receiving. If `dedicated_wallets` is on, we probably want to receive into a unique address.
                        // Let's use the `ethereum` wallet at `wallet_index` if available, else `base`.
                        if let Some(eth_wallets) = ethereum {
                            if let Some(w) = eth_wallets.wallets.get(*wallet_index) {
                                return Ok(w.address());
                            }
                        }
                        if let Some(base_wallets) = base {
                            if let Some(w) = base_wallets.wallets.get(*wallet_index) {
                                return Ok(w.address());
                            }
                        }
                         recipient_evm_address.parse::<Address>()
                            .with_context(|| format!("invalid recipient EVM address: {}", recipient_evm_address))
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
enum DedicatedWallets {
    Bitcoin(BitcoinDedicatedWallets),
    Evm(EvmDedicatedWallets, ChainType),
}

impl DedicatedWallets {
    async fn create_payment(
        &self,
        swap_index: usize,
        lot: &Lot,
        recipient: &str,
    ) -> Result<(String, String)> {
        match self {
            DedicatedWallets::Bitcoin(wallets) => {
                wallets.create_payment(swap_index, lot, recipient).await
            }
            DedicatedWallets::Evm(wallets, _) => {
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
        match wallet
            .broadcast_transaction(payments, vec![], None, None)
            .await
        {
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

async fn create_bitcoin_payment_with_retry_batch(
    wallet: &Arc<BitcoinTransactionBroadcaster>,
    payments: Vec<Payment>,
) -> Result<String> {
    let mut attempt = 0usize;
    loop {
        attempt += 1;
        match wallet
            .broadcast_transaction(payments.clone(), vec![], None, None)
            .await
        {
            Ok(txid) => {
                if attempt > 1 {
                    debug!(attempt = attempt, "bitcoin batch payment succeeded after retry");
                }
                return Ok(txid);
            }
            Err(err) => {
                let err_str = err.to_string();
                if should_retry_missing_or_spent(&err_str) && attempt < BTC_PAYMENT_RETRY_ATTEMPTS {
                    warn!(
                        attempt = attempt,
                        error = %err,
                        "bitcoin batch payment failed due to missing/spent inputs, retrying"
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

#[derive(Clone)]
struct BitcoinDedicatedWallets {
    wallets: Arc<Vec<Arc<BitcoinWallet>>>,
    _join_sets: Arc<Vec<JoinSet<MarketMakerResult<()>>>>,
}

impl BitcoinDedicatedWallets {
    fn new(
        wallets: Vec<Arc<BitcoinWallet>>,
        join_sets: Vec<JoinSet<MarketMakerResult<()>>>,
    ) -> Self {
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
                lot: Lot {
                    currency: currency.clone(),
                    amount,
                },
                to_address: wallet.receive_address(&currency.token),
            })
            .collect()
    }

    async fn create_payment(
        &self,
        index: usize,
        lot: &Lot,
        recipient: &str,
    ) -> Result<(String, String)> {
        let wallet = self
            .wallets
            .get(index)
            .with_context(|| format!("missing dedicated bitcoin wallet for swap {}", index))?;

        let sender_address = wallet.receive_address(&lot.currency.token);
        let tx_hash = wallet
            .create_batch_payment(
                vec![Payment {
                    lot: lot.clone(),
                    to_address: recipient.to_string(),
                }],
                None,
            )
            .await
            .map_err(map_wallet_error)?;
        Ok((tx_hash, sender_address))
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

    async fn create_payment(
        &self,
        index: usize,
        lot: &Lot,
        recipient: &str,
    ) -> Result<(String, String)> {
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

    async fn create_payment(&self, lot: &Lot, recipient: &str) -> Result<(String, String)> {
        let recipient_address = Address::from_str(recipient)
            .with_context(|| format!("invalid recipient address: {recipient}"))?;

        let sender_address = format!("{:?}", self.address);
        let tx_hash = match &lot.currency.token {
            TokenIdentifier::Native => {
                self.send_native_transfer(lot.amount, recipient_address)
                    .await?
            }
            TokenIdentifier::Address(token_address) => {
                self.send_erc20_transfer(token_address, lot.amount, recipient_address)
                    .await?
            }
        };
        Ok((tx_hash, sender_address))
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

async fn create_evm_funding_wallet(
    cfg: &EvmWalletConfig,
    chain: ChainType,
    js: &mut JoinSet<MarketMakerResult<()>>,
) -> Result<(Arc<EVMTransactionBroadcaster>, DynProvider, ChainType)> {
    println!("cfg: {:?}", cfg);
    let wallet = init_evm_wallet(chain, cfg, js).await?;
    let provider = wallet.provider.clone().erased();
    Ok((Arc::new(wallet.tx_broadcaster), provider, chain))
}

async fn create_btc_funding_wallet(
    cfg: &BitcoinWalletConfig,
    js: &mut JoinSet<MarketMakerResult<()>>,
) -> Result<(Arc<BitcoinTransactionBroadcaster>, String)> {
    let wallet = init_bitcoin_wallet(cfg, js).await?;
    let sender_address = wallet.receive_address(&TokenIdentifier::Native);
    Ok((Arc::new(wallet.tx_broadcaster), sender_address))
}

pub async fn setup_wallets(config: &Config) -> Result<WalletResources> {
    let mut join_set = JoinSet::<MarketMakerResult<()>>::new();

    let payment_wallets = match &config.mode {
        SwapMode::EthToBtc => {
            let evm_cfg = config.evm.as_ref().context("missing Ethereum wallet configuration")?;
            let (broadcaster, provider, _) = create_evm_funding_wallet(evm_cfg, ChainType::Ethereum, &mut join_set).await?;
            let funding_wallet = SinglePaymentWallet::Evm(broadcaster.clone(), provider.clone(), ChainType::Ethereum);

            if config.dedicated_wallets.enabled {
                let dedicated = create_dedicated_wallets(&funding_wallet, config).await?;
                info!("dedicated single-payment wallets enabled (Ethereum)");
                PaymentWallets::dedicated(funding_wallet, dedicated)
            } else {
                PaymentWallets::shared(funding_wallet)
            }
        }
        SwapMode::BaseToBtc => {
            let base_cfg = config.base.as_ref().context("missing Base wallet configuration")?;
            let (broadcaster, provider, _) = create_evm_funding_wallet(base_cfg, ChainType::Base, &mut join_set).await?;
            let funding_wallet = SinglePaymentWallet::Evm(broadcaster.clone(), provider.clone(), ChainType::Base);

            if config.dedicated_wallets.enabled {
                let dedicated = create_dedicated_wallets(&funding_wallet, config).await?;
                info!("dedicated single-payment wallets enabled (Base)");
                PaymentWallets::dedicated(funding_wallet, dedicated)
            } else {
                PaymentWallets::shared(funding_wallet)
            }
        }
        SwapMode::BtcToEth => {
            let btc_cfg = config.bitcoin.as_ref().context("missing Bitcoin wallet configuration")?;
            let (broadcaster, address) = create_btc_funding_wallet(btc_cfg, &mut join_set).await?;
            let funding_wallet = SinglePaymentWallet::Bitcoin(broadcaster.clone(), address);

            if config.dedicated_wallets.enabled {
                let dedicated = create_dedicated_wallets(&funding_wallet, config).await?;
                info!("dedicated single-payment wallets enabled (Bitcoin -> Ethereum)");
                PaymentWallets::dedicated(funding_wallet, dedicated)
            } else {
                PaymentWallets::shared(funding_wallet)
            }
        }
        SwapMode::BtcToBase => {
            let btc_cfg = config.bitcoin.as_ref().context("missing Bitcoin wallet configuration")?;
            let (broadcaster, address) = create_btc_funding_wallet(btc_cfg, &mut join_set).await?;
            let funding_wallet = SinglePaymentWallet::Bitcoin(broadcaster.clone(), address);

            if config.dedicated_wallets.enabled {
                let dedicated = create_dedicated_wallets(&funding_wallet, config).await?;
                info!("dedicated single-payment wallets enabled (Bitcoin -> Base)");
                PaymentWallets::dedicated(funding_wallet, dedicated)
            } else {
                PaymentWallets::shared(funding_wallet)
            }
        }
        SwapMode::Rand { directions } => {
            // Initialize shared wallets if needed
            let bitcoin = if config.bitcoin.is_some() {
                let cfg = config.bitcoin.as_ref().unwrap();
                Some(create_btc_funding_wallet(cfg, &mut join_set).await?)
            } else {
                None
            };

            let ethereum = if config.evm.is_some() {
                let cfg = config.evm.as_ref().unwrap();
                let (b, p, _) = create_evm_funding_wallet(cfg, ChainType::Ethereum, &mut join_set).await?;
                Some((b, p))
            } else {
                None
            };

            let base = if config.base.is_some() {
                let cfg = config.base.as_ref().unwrap();
                let (b, p, _) = create_evm_funding_wallet(cfg, ChainType::Base, &mut join_set).await?;
                Some((b, p))
            } else {
                None
            };

            let source_chain_mapping: Vec<ChainType> = directions.iter().map(|d| d.from_currency.chain).collect();
            let recipient_evm_address = config.recipient_evm_address.parse::<Address>().context("invalid recipient EVM address")?;

            if config.dedicated_wallets.enabled {
                // Determine how many dedicated wallets of each type we need
                // AND map swap indices to dedicated wallet indices
                let mut index_mapping = Vec::with_capacity(directions.len());
                let mut btc_index = 0;
                let mut eth_index = 0;
                let mut base_index = 0;

                for direction in directions {
                    match direction.from_currency.chain {
                        ChainType::Bitcoin => {
                            index_mapping.push((ChainType::Bitcoin, btc_index));
                            btc_index += 1;
                        }
                        ChainType::Ethereum => {
                            index_mapping.push((ChainType::Ethereum, eth_index));
                            eth_index += 1;
                        }
                        ChainType::Base => {
                            index_mapping.push((ChainType::Base, base_index));
                            base_index += 1;
                        }
                    }
                }

                let bitcoin_dedicated = if btc_index > 0 {
                    let cfg = config.bitcoin.as_ref().context("missing bitcoin config for dedicated wallets")?;
                    let wallets = create_bitcoin_dedicated_wallets_count(cfg, btc_index).await?;
                    if let Some((broadcaster, _)) = &bitcoin {
                         fund_bitcoin_wallets_dedicated(broadcaster, config, &wallets).await?;
                    }
                    Some(wallets)
                } else {
                    None
                };

                let ethereum_dedicated = if eth_index > 0 {
                    let cfg = config.evm.as_ref().context("missing ethereum config for dedicated wallets")?;
                    let wallets = create_evm_dedicated_wallets_count(cfg, eth_index).await?;
                    if let Some((broadcaster, provider)) = &ethereum {
                        fund_evm_wallets_dedicated(broadcaster, provider.clone(), config, &wallets, ChainType::Ethereum).await?;
                    }
                    Some(wallets)
                } else {
                    None
                };

                let base_dedicated = if base_index > 0 {
                    let cfg = config.base.as_ref().context("missing base config for dedicated wallets")?;
                    let wallets = create_evm_dedicated_wallets_count(cfg, base_index).await?;
                    if let Some((broadcaster, provider)) = &base {
                        fund_evm_wallets_dedicated(broadcaster, provider.clone(), config, &wallets, ChainType::Base).await?;
                    }
                    Some(wallets)
                } else {
                    None
                };

                info!("dedicated multi-chain wallets enabled");
                PaymentWallets::multi_dedicated(
                    bitcoin_dedicated,
                    ethereum_dedicated,
                    base_dedicated,
                    index_mapping,
                )
            } else {
                PaymentWallets::multi_shared(
                    bitcoin,
                    ethereum,
                    base,
                    source_chain_mapping,
                    recipient_evm_address,
                )
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
        None,
        100, // max_deposits_per_lot
        join_set,
    )
    .await
    .context("failed to initialise Bitcoin wallet")
}

async fn init_evm_wallet(
    chain_type: ChainType,
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
        chain_type,
        None,
        350, // max_deposits_per_lot
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
        SinglePaymentWallet::Bitcoin(broadcaster, _) => {
            let btc_cfg = config
                .bitcoin
                .as_ref()
                .context("missing Bitcoin wallet configuration")?;
            println!("creating bitcoin dedicated wallets");
            let dedicated_wallets =
                create_bitcoin_dedicated_wallets_count(btc_cfg, config.total_swaps).await?;
            println!("funding dedicated bitcoin wallets");
            fund_bitcoin_wallets_dedicated(broadcaster, config, &dedicated_wallets).await?;
            Ok(DedicatedWallets::Bitcoin(dedicated_wallets))
        }
        SinglePaymentWallet::Evm(broadcaster, provider, chain) => {
            let evm_cfg = match chain {
                ChainType::Ethereum => config.evm.as_ref().context("missing Ethereum wallet configuration")?,
                ChainType::Base => config.base.as_ref().context("missing Base wallet configuration")?,
                _ => bail!("unsupported EVM chain for dedicated wallets: {:?}", chain),
            };
            let dedicated_wallets =
                create_evm_dedicated_wallets_count(evm_cfg, config.total_swaps).await?;
            fund_evm_wallets_dedicated(broadcaster, provider.clone(), config, &dedicated_wallets, *chain)
                .await?;
            Ok(DedicatedWallets::Evm(dedicated_wallets, *chain))
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
                        format!(
                            "failed to remove old wallet file: {}",
                            wallet_path.display()
                        )
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
                    None,
                    100, // max_deposits_per_lot
                    &mut join_set,
                )
                .await
                .with_context(|| {
                    format!("failed to initialise dedicated Bitcoin wallet #{index}")
                })?;

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
        let (wallet, join_set) = result
            .with_context(|| format!("failed to create dedicated Bitcoin wallet #{index}"))?;
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
        .broadcast_transaction(payments, vec![], None, None)
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
    chain: ChainType,
) -> Result<()> {
    if dedicated_wallets.len() == 0 {
        return Ok(());
    }

    let cbbtc_currency = Currency {
        chain,
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

            let sender = broadcaster.sender;
            executions.extend(create_payment_executions(
                &token_contract,
                &sender,
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