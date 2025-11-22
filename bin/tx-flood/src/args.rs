use std::{sync::Arc, time::Duration};

use alloy::primitives::{Address, U256};
use anyhow::{anyhow, Result};
use bitcoin::Network;
use clap::{Parser, ValueEnum};
use otc_models::{ChainType, Currency, TokenIdentifier};
use rand::Rng;
use reqwest::Url;
use tempfile::TempDir;

const DEFAULT_RECIPIENT_EVM_ADDRESS: &str = "0x61f11ac1218cb522347f6D430202d8290DA1a28f";
const DEFAULT_RECIPIENT_BITCOIN_ADDRESS: &str = "bcrt1qs758ursh4q9z627kt3pp5yysm78ddny6txaqgw";
const DEFAULT_EVM_PRIVATE_KEY: &str =
    "2230f1b621d6865e08b75856c575da89317a944785f33a16d9f2192adedb9ca8";
const DEFAULT_BITCOIN_DESCRIPTOR: &str =
    "wpkh(cUGWCiMZN5iRpQgYGU5DRFCed9nzLk1qj6MuJotnSXw9gkyw5huj)";
const DEFAULT_EVM_WS_URL: &str = "ws://127.0.0.1:50101";
const DEFAULT_EVM_HTTP_URL: &str = "http://127.0.0.1:50101";
const DEFAULT_BASE_WS_URL: &str = "ws://127.0.0.1:50102";
const DEFAULT_BASE_HTTP_URL: &str = "http://127.0.0.1:50102";
const DEFAULT_ESPLORA_URL: &str = "http://127.0.0.1:50103";
const CBBTC_ADDRESS: &str = "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf";

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum ModeArg {
    /// User sends cbBTC on Ethereum, receives BTC on Bitcoin
    EthToBtc,
    /// User sends cbBTC on Base, receives BTC on Bitcoin
    BaseToBtc,
    /// User sends BTC on Bitcoin, receives cbBTC on Ethereum
    BtcToEth,
    /// User sends BTC on Bitcoin, receives cbBTC on Base
    BtcToBase,
    /// Randomly choose between all supported swap directions
    Rand,
}

#[derive(Debug, Clone)]
pub struct SwapDirection {
    pub from_currency: Currency,
    pub to_currency: Currency,
    pub user_destination_address: String,
}

#[derive(Parser, Debug, Clone)]
#[command(name = "tx-flood")]
#[command(about = "Swap load testing tool for the OTC server")]
pub struct Args {
    /// Path to .env file to load environment variables from
    #[arg(long, env = "ENV_FILE")]
    pub env_file: Option<std::path::PathBuf>,

    /// Global log level (e.g. trace, debug, info)
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    pub log_level: String,

    /// Base URL for the OTC server (e.g. https://otc.example.com)
    #[arg(long, env = "OTC_URL", default_value = "http://127.0.0.1:4422")]
    pub otc_url: Url,

    /// Override URL for the quote endpoint (defaults to <otc_url>/api/v1/quotes/request)
    #[arg(
        long,
        env = "QUOTE_URL",
        default_value = "http://127.0.0.1:3001/api/v1/quotes/request"
    )]
    pub quote_url: Url,

    /// Total number of swaps to execute
    #[arg(long, env = "TOTAL_SWAPS", default_value_t = 1)]
    pub total_swaps: usize,

    /// Number of swaps to start in each interval window
    #[arg(long, env = "SWAPS_PER_INTERVAL", default_value_t = 1)]
    pub swaps_per_interval: usize,

    /// Interval between batches, expressed with humantime syntax (e.g. "5s", "1m")
    #[arg(long, env = "INTERVAL", default_value = "5s", value_parser = parse_duration)]
    pub interval: Duration,

    /// Polling cadence for swap status checks
    #[arg(long, env = "POLL_INTERVAL", default_value = "2s", value_parser = parse_duration)]
    pub poll_interval: Duration,

    /// Maximum time to wait for a swap to settle before marking it failed
    #[arg(long, env = "SWAP_TIMEOUT", default_value = "10m", value_parser = parse_duration)]
    pub swap_timeout: Duration,

    /// Swap mode: eth-to-btc, base-to-btc, btc-to-eth, btc-to-base, or rand (random)
    #[arg(long, env = "MODE", value_enum, default_value_t = ModeArg::EthToBtc)]
    pub mode: ModeArg,

    /// Minimum amount for quote requests (decimal or hex string, e.g. "5000" or "0x1388")
    #[arg(long, env = "MIN_AMOUNT", default_value = "5000", value_parser = parse_u256)]
    pub min_amount: U256,

    /// Maximum amount for quote requests (decimal or hex string, e.g. "100000" or "0x186a0")
    #[arg(long, env = "MAX_AMOUNT", default_value = "100000", value_parser = parse_u256)]
    pub max_amount: U256,

    /// Enable randomized amounts between min_amount and max_amount for each swap
    #[arg(
        long,
        env = "RANDOMIZE_AMOUNTS",
        default_value = "true",
        action = clap::ArgAction::Set
    )]
    pub randomize_amounts: bool,

    /// Bitcoin descriptor representing the funded wallet (wpkh(desc)...)
    #[arg(long, env = "BITCOIN_WALLET_DESCRIPTOR", default_value = DEFAULT_BITCOIN_DESCRIPTOR)]
    pub bitcoin_wallet_descriptor: String,

    /// Bitcoin network for the wallet (bitcoin, testnet, signet, regtest)
    #[arg(long, env = "BITCOIN_WALLET_NETWORK", default_value_t = Network::Regtest)]
    pub bitcoin_network: Network,

    /// Esplora HTTP URL for the Bitcoin chain
    #[arg(long, env = "BITCOIN_WALLET_ESPLORA_URL", default_value = DEFAULT_ESPLORA_URL)]
    pub bitcoin_esplora_url: String,

    /// Private key for the EVM wallet (32-byte hex string)
    #[arg(long, env = "ETHEREUM_WALLET_PRIVATE_KEY", value_parser = parse_private_key, default_value = DEFAULT_EVM_PRIVATE_KEY)]
    pub evm_private_key: [u8; 32],

    /// Ethereum RPC websocket URL used for signing and broadcasting
    #[arg(long, env = "ETHEREUM_RPC_WS_URL", default_value = DEFAULT_EVM_WS_URL)]
    pub evm_rpc_ws_url: String,

    /// Optional HTTP RPC URL for debugging (defaults to the websocket URL)
    #[arg(long, env = "ETHEREUM_RPC_HTTP_URL", default_value = DEFAULT_EVM_HTTP_URL)]
    pub evm_debug_rpc_url: String,

    /// Number of confirmations required before the wallet considers a tx final
    #[arg(long, env = "ETHEREUM_CONFIRMATIONS", default_value_t = 1)]
    pub evm_confirmations: u64,

    /// Private key for the Base wallet (32-byte hex string)
    #[arg(long, env = "BASE_WALLET_PRIVATE_KEY", value_parser = parse_private_key, default_value = DEFAULT_EVM_PRIVATE_KEY)]
    pub base_private_key: [u8; 32],

    /// Base RPC websocket URL used for signing and broadcasting
    #[arg(long, env = "BASE_RPC_WS_URL", default_value = DEFAULT_BASE_WS_URL)]
    pub base_rpc_ws_url: String,

    /// Optional Base HTTP RPC URL for debugging (defaults to the websocket URL)
    #[arg(long, env = "BASE_RPC_HTTP_URL", default_value = DEFAULT_BASE_HTTP_URL)]
    pub base_debug_rpc_url: String,

    /// Number of confirmations required before the wallet considers a tx final on Base
    #[arg(long, env = "BASE_CONFIRMATIONS", default_value_t = 1)]
    pub base_confirmations: u64,

    /// Enable dedicated per-swap wallets funded up-front from the master wallet
    #[arg(long, env = "DEDICATED_WALLETS", default_value_t = false)]
    pub dedicated_wallets: bool,

    /// Additional sats to allocate to each dedicated Bitcoin wallet for miner fees
    #[arg(
        long,
        env = "DEDICATED_WALLET_BITCOIN_FEE_RESERVE_SATS",
        default_value_t = 5_000
    )]
    pub dedicated_wallet_bitcoin_fee_reserve_sats: u64,

    /// Additional wei to allocate to each dedicated EVM wallet for gas fees
    #[arg(long, env = "DEDICATED_WALLET_EVM_FEE_RESERVE_WEI", default_value = "0x2386f26fc10000", value_parser = parse_u256)]
    pub dedicated_wallet_evm_fee_reserve_wei: U256,

    /// Recipient Bitcoin address for receiving swaps
    #[arg(long, env = "RECIPIENT_BITCOIN_ADDRESS", default_value = DEFAULT_RECIPIENT_BITCOIN_ADDRESS)]
    pub recipient_bitcoin_address: String,

    /// Recipient EVM address for receiving swaps
    #[arg(long, env = "RECIPIENT_EVM_ADDRESS", default_value = DEFAULT_RECIPIENT_EVM_ADDRESS)]
    pub recipient_evm_address: String,

    /// Enable payment batching (multiple swaps in single transaction)
    #[arg(long, env = "ENABLE_BATCHING", default_value_t = false)]
    pub enable_batching: bool,

    /// Batch interval for Ethereum payments
    #[arg(long, env = "BATCH_INTERVAL_ETHEREUM", default_value = "5s", value_parser = parse_duration)]
    pub batch_interval_ethereum: Duration,

    /// Batch interval for Base payments
    #[arg(long, env = "BATCH_INTERVAL_BASE", default_value = "5s", value_parser = parse_duration)]
    pub batch_interval_base: Duration,

    /// Batch interval for Bitcoin payments
    #[arg(long, env = "BATCH_INTERVAL_BITCOIN", default_value = "5s", value_parser = parse_duration)]
    pub batch_interval_bitcoin: Duration,
}

#[derive(Debug, Clone)]
pub enum SwapMode {
    EthToBtc,
    BaseToBtc,
    BtcToEth,
    BtcToBase,
    Rand { directions: Vec<SwapDirection> },
}

#[derive(Debug, Clone)]
pub struct Config {
    pub log_level: String,
    pub otc_url: Url,
    pub quote_url: Url,
    pub swaps_per_interval: usize,
    pub total_swaps: usize,
    pub interval: Duration,
    pub poll_interval: Duration,
    pub swap_timeout: Duration,
    pub mode: SwapMode,
    pub min_amount: U256,
    pub max_amount: U256,
    pub randomize_amounts: bool,
    pub bitcoin: Option<BitcoinWalletConfig>,
    pub evm: Option<EvmWalletConfig>,
    pub base: Option<EvmWalletConfig>,
    pub dedicated_wallets: DedicatedWalletsConfig,
    pub recipient_bitcoin_address: String,
    pub recipient_evm_address: String,
    pub enable_batching: bool,
    pub batch_interval_ethereum: Duration,
    pub batch_interval_base: Duration,
    pub batch_interval_bitcoin: Duration,
    pub _bitcoin_wallet_db_dir: Arc<TempDir>,
}

#[derive(Debug, Clone)]
pub struct DedicatedWalletsConfig {
    pub enabled: bool,
    pub bitcoin_fee_reserve_sats: u64,
    pub evm_fee_reserve_wei: U256,
}

#[derive(Debug, Clone)]
pub struct BitcoinWalletConfig {
    pub db_path: String,
    pub descriptor: String,
    pub network: Network,
    pub esplora_url: String,
}

#[derive(Debug, Clone)]
pub struct EvmWalletConfig {
    pub private_key: [u8; 32],
    pub rpc_ws_url: String,
    pub debug_rpc_url: String,
    pub confirmations: u64,
}

impl Args {
    pub fn into_config(self) -> Result<Config> {
        let Args {
            env_file: _,
            log_level,
            otc_url,
            quote_url,
            total_swaps,
            swaps_per_interval,
            interval,
            poll_interval,
            swap_timeout,
            mode,
            min_amount,
            max_amount,
            randomize_amounts,
            bitcoin_wallet_descriptor,
            bitcoin_network,
            bitcoin_esplora_url,
            evm_private_key,
            evm_rpc_ws_url,
            evm_debug_rpc_url,
            evm_confirmations,
            base_private_key,
            base_rpc_ws_url,
            base_debug_rpc_url,
            base_confirmations,
            dedicated_wallets,
            dedicated_wallet_bitcoin_fee_reserve_sats,
            dedicated_wallet_evm_fee_reserve_wei,
            recipient_bitcoin_address,
            recipient_evm_address,
            enable_batching,
            batch_interval_ethereum,
            batch_interval_base,
            batch_interval_bitcoin,
        } = self;

        if total_swaps == 0 {
            return Err(anyhow!("total_swaps must be greater than zero"));
        }
        if swaps_per_interval == 0 {
            return Err(anyhow!("swaps_per_interval must be greater than zero"));
        }
        if min_amount > max_amount {
            return Err(anyhow!(
                "min_amount must be less than or equal to max_amount"
            ));
        }
        
        // Invariant: batching and dedicated wallets are mutually exclusive
        if enable_batching && dedicated_wallets {
            return Err(anyhow!(
                "batching and dedicated wallets cannot both be enabled (batching requires shared wallets)"
            ));
        }

        // Define the currency pairs
        let eth_cbbtc = Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::Address(CBBTC_ADDRESS.to_string()),
            decimals: 8,
        };
        let base_cbbtc = Currency {
            chain: ChainType::Base,
            token: TokenIdentifier::Address(CBBTC_ADDRESS.to_string()),
            decimals: 8,
        };
        let btc_currency = Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            decimals: 8,
        };

        // Build swap mode and determine required wallets
        let (swap_mode, needs_bitcoin, needs_ethereum, needs_base) = match mode {
            ModeArg::EthToBtc => (SwapMode::EthToBtc, false, true, false),
            ModeArg::BaseToBtc => (SwapMode::BaseToBtc, false, false, true),
            ModeArg::BtcToEth => (SwapMode::BtcToEth, true, true, false),
            ModeArg::BtcToBase => (SwapMode::BtcToBase, true, false, true),
            ModeArg::Rand => {
                let mut rng = rand::thread_rng();
                let mut directions = Vec::with_capacity(total_swaps);
                let mut btc_count = 0;
                let mut eth_count = 0;
                let mut base_count = 0;

                for _ in 0..total_swaps {
                    // Randomly choose between:
                    // 0: Eth -> Btc
                    // 1: Btc -> Eth
                    // 2: Base -> Btc
                    // 3: Btc -> Base
                    match rng.gen_range(0..4) {
                        0 => {
                            // eth-start: send cbBTC on Eth, receive BTC
                            directions.push(SwapDirection {
                                from_currency: eth_cbbtc.clone(),
                                to_currency: btc_currency.clone(),
                                user_destination_address: recipient_bitcoin_address.clone(),
                            });
                            eth_count += 1;
                        }
                        1 => {
                            // btc-start: send BTC, receive cbBTC on Eth
                            directions.push(SwapDirection {
                                from_currency: btc_currency.clone(),
                                to_currency: eth_cbbtc.clone(),
                                user_destination_address: recipient_evm_address.clone(),
                            });
                            btc_count += 1;
                        }
                        2 => {
                            // base-start: send cbBTC on Base, receive BTC
                            directions.push(SwapDirection {
                                from_currency: base_cbbtc.clone(),
                                to_currency: btc_currency.clone(),
                                user_destination_address: recipient_bitcoin_address.clone(),
                            });
                            base_count += 1;
                        }
                        3 => {
                            // btc-start: send BTC, receive cbBTC on Base
                            directions.push(SwapDirection {
                                from_currency: btc_currency.clone(),
                                to_currency: base_cbbtc.clone(),
                                user_destination_address: recipient_evm_address.clone(),
                            });
                            btc_count += 1;
                        }
                        _ => unreachable!(),
                    }
                }

                (
                    SwapMode::Rand { directions },
                    btc_count > 0,
                    eth_count > 0,
                    base_count > 0,
                )
            }
        };

        let bitcoin_wallet_db_dir = tempfile::tempdir().unwrap();
        let funding_bitcoin_wallet_db_path =
            bitcoin_wallet_db_dir.path().join("main_fund_wallet.sqlite");
        let bitcoin = if needs_bitcoin {
            Some(BitcoinWalletConfig {
                db_path: funding_bitcoin_wallet_db_path.to_string_lossy().to_string(),
                descriptor: bitcoin_wallet_descriptor,
                network: bitcoin_network,
                esplora_url: bitcoin_esplora_url,
            })
        } else {
            None
        };

        let evm = if needs_ethereum {
            let debug_rpc_url = if evm_debug_rpc_url.is_empty() {
                evm_rpc_ws_url.clone()
            } else {
                evm_debug_rpc_url.clone()
            };

            Some(EvmWalletConfig {
                private_key: evm_private_key,
                rpc_ws_url: evm_rpc_ws_url,
                debug_rpc_url,
                confirmations: evm_confirmations,
            })
        } else {
            None
        };

        let base = if needs_base {
            let debug_rpc_url = if base_debug_rpc_url.is_empty() {
                base_rpc_ws_url.clone()
            } else {
                base_debug_rpc_url.clone()
            };

            Some(EvmWalletConfig {
                private_key: base_private_key,
                rpc_ws_url: base_rpc_ws_url,
                debug_rpc_url,
                confirmations: base_confirmations,
            })
        } else {
            None
        };

        let dedicated_wallets = DedicatedWalletsConfig {
            enabled: dedicated_wallets,
            bitcoin_fee_reserve_sats: dedicated_wallet_bitcoin_fee_reserve_sats,
            evm_fee_reserve_wei: dedicated_wallet_evm_fee_reserve_wei,
        };

        Ok(Config {
            log_level,
            otc_url,
            quote_url,
            swaps_per_interval,
            total_swaps,
            interval,
            poll_interval,
            swap_timeout,
            mode: swap_mode,
            min_amount,
            max_amount,
            randomize_amounts,
            bitcoin,
            evm,
            base,
            dedicated_wallets,
            recipient_bitcoin_address,
            recipient_evm_address,
            enable_batching,
            batch_interval_ethereum,
            batch_interval_base,
            batch_interval_bitcoin,
            _bitcoin_wallet_db_dir: Arc::new(bitcoin_wallet_db_dir),
        })
    }
}
fn parse_duration(value: &str) -> Result<Duration, String> {
    humantime::parse_duration(value).map_err(|err| err.to_string())
}

fn parse_u256(value: &str) -> Result<U256, String> {
    let trimmed = value.trim();
    if let Some(hex) = trimmed.strip_prefix("0x") {
        U256::from_str_radix(hex, 16).map_err(|e| e.to_string())
    } else {
        U256::from_str_radix(trimmed, 10).map_err(|e| e.to_string())
    }
}

#[allow(unused)]
fn parse_address(value: &str) -> Result<Address, String> {
    Address::parse_checksummed(value, None).map_err(|e| e.to_string())
}

fn parse_private_key(value: &str) -> Result<[u8; 32], String> {
    let sanitized = value.trim_start_matches("0x");
    let bytes = alloy::hex::decode(sanitized).map_err(|e| e.to_string())?;
    if bytes.len() != 32 {
        return Err("expected 32-byte hex string".to_string());
    }
    let mut array = [0u8; 32];
    array.copy_from_slice(&bytes);
    Ok(array)
}