use std::time::Duration;

use alloy::primitives::{Address, U256};
use anyhow::{anyhow, Result};
use bitcoin::Network;
use clap::{Parser, ValueEnum};
use otc_models::{ChainType, Currency, QuoteMode, QuoteRequest, TokenIdentifier};
use reqwest::Url;

const DEFAULT_EVM_ADDRESS: &str = "0x61f11ac1218cb522347f6D430202d8290DA1a28f";
const DEFAULT_EVM_PRIVATE_KEY: &str =
    "2230f1b621d6865e08b75856c575da89317a944785f33a16d9f2192adedb9ca8";
const DEFAULT_BITCOIN_DESCRIPTOR: &str =
    "wpkh(cUGWCiMZN5iRpQgYGU5DRFCed9nzLk1qj6MuJotnSXw9gkyw5huj)";
const DEFAULT_EVM_WS_URL: &str = "ws://0.0.0.0:50101";
const DEFAULT_EVM_HTTP_URL: &str = "http://0.0.0.0:50101";
const DEFAULT_ESPLORA_URL: &str = "http://0.0.0.0:50103";
const DEFAULT_CBBTC_ADDRESS: &str = "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf";
const DEFAULT_BITCOIN_ADDRESS: &str = "bcrt1qs758ursh4q9z627kt3pp5yysm78ddny6txaqgw";

#[derive(Parser, Debug, Clone)]
#[command(name = "tx-flood")]
#[command(about = "Swap load testing tool for the OTC server")]
pub struct Args {
    /// Global log level (e.g. trace, debug, info)
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    pub log_level: String,

    /// Base URL for the OTC server (e.g. https://otc.example.com)
    #[arg(long, default_value = "http://0.0.0.0:4422")]
    pub otc_url: Url,

    /// Override URL for the quote endpoint (defaults to <otc_url>/api/v1/quotes/request)
    #[arg(long, default_value = "http://0.0.0.0:3001/api/v1/quotes/request")]
    pub quote_url: Url,

    /// Total number of swaps to execute
    #[arg(long, default_value_t = 1)]
    pub total_swaps: usize,

    /// Number of swaps to start in each interval window
    #[arg(long, default_value_t = 1)]
    pub swaps_per_interval: usize,

    /// Interval between batches, expressed with humantime syntax (e.g. "5s", "1m")
    #[arg(long, default_value = "5s", value_parser = parse_duration)]
    pub interval: Duration,

    /// Polling cadence for swap status checks
    #[arg(long, default_value = "2s", value_parser = parse_duration)]
    pub poll_interval: Duration,

    /// Maximum time to wait for a swap to settle before marking it failed
    #[arg(long, default_value = "10m", value_parser = parse_duration)]
    pub swap_timeout: Duration,

    /// Quote mode to use when requesting quotes
    #[arg(long, value_enum, default_value_t = QuoteModeArg::ExactInput)]
    pub quote_mode: QuoteModeArg,

    /// Chain the user will deposit from (quote.from.chain)
    #[arg(long, value_enum, default_value_t = ChainArg::Ethereum)]
    pub from_chain: ChainArg,

    /// Token identifier for the deposit side ("native" or an address)
    #[arg(long, default_value = DEFAULT_CBBTC_ADDRESS)]
    pub from_token: String,

    /// Decimals for the deposit currency
    #[arg(long, default_value_t = 8)]
    pub from_decimals: u8,

    /// Chain the user will receive funds on (quote.to.chain)
    #[arg(long, value_enum, default_value_t = ChainArg::Bitcoin)]
    pub to_chain: ChainArg,

    /// Token identifier for the receive side ("native" or an address)
    #[arg(long, default_value = "native")]
    pub to_token: String,

    /// Decimals for the receive currency
    #[arg(long, default_value_t = 8)]
    pub to_decimals: u8,

    /// Amount for the quote request (decimal or hex string, e.g. "100000" or "0x186a0")
    #[arg(long, default_value = "10001", value_parser = parse_u256)]
    pub amount: U256,

    /// Destination address the user will receive funds at
    #[arg(long, default_value = DEFAULT_BITCOIN_ADDRESS)]
    pub user_destination_address: String,

    /// EVM account address that controls the swap (used for swap auth)
    #[arg(long, value_parser = parse_address, default_value = DEFAULT_EVM_ADDRESS)]
    pub user_evm_account_address: Address,

    /// Path to the bitcoin wallet database
    #[arg(long, default_value = "./demo-bitcoin-wallet.sqlite")]
    pub bitcoin_wallet_db_path: String,

    /// Bitcoin descriptor representing the funded wallet (wpkh(desc)...)
    #[arg(long, default_value = DEFAULT_BITCOIN_DESCRIPTOR)]
    pub bitcoin_wallet_descriptor: String,

    /// Bitcoin network for the wallet (bitcoin, testnet, signet, regtest)
    #[arg(long, default_value_t = Network::Regtest)]
    pub bitcoin_network: Network,

    /// Esplora HTTP URL for the Bitcoin chain
    #[arg(long, default_value = DEFAULT_ESPLORA_URL)]
    pub bitcoin_esplora_url: String,

    /// Private key for the EVM wallet (32-byte hex string)
    #[arg(long, value_parser = parse_private_key, default_value = DEFAULT_EVM_PRIVATE_KEY)]
    pub evm_private_key: [u8; 32],

    /// Ethereum RPC websocket URL used for signing and broadcasting
    #[arg(long, default_value = DEFAULT_EVM_WS_URL)]
    pub evm_rpc_ws_url: String,

    /// Optional HTTP RPC URL for debugging (defaults to the websocket URL)
    #[arg(long, default_value = DEFAULT_EVM_HTTP_URL)]
    pub evm_debug_rpc_url: String,

    /// Number of confirmations required before the wallet considers a tx final
    #[arg(long, default_value_t = 1)]
    pub evm_confirmations: u64,
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
    pub quote_request: QuoteRequest,
    pub user_destination_address: String,
    pub user_evm_account_address: Address,
    pub bitcoin: Option<BitcoinWalletConfig>,
    pub evm: Option<EvmWalletConfig>,
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

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum QuoteModeArg {
    ExactInput,
    ExactOutput,
}

impl From<QuoteModeArg> for QuoteMode {
    fn from(value: QuoteModeArg) -> Self {
        match value {
            QuoteModeArg::ExactInput => QuoteMode::ExactInput,
            QuoteModeArg::ExactOutput => QuoteMode::ExactOutput,
        }
    }
}

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum ChainArg {
    Bitcoin,
    Ethereum,
}

impl From<ChainArg> for ChainType {
    fn from(value: ChainArg) -> Self {
        match value {
            ChainArg::Bitcoin => ChainType::Bitcoin,
            ChainArg::Ethereum => ChainType::Ethereum,
        }
    }
}

impl Args {
    pub fn into_config(self) -> Result<Config> {
        let Args {
            log_level,
            otc_url,
            quote_url,
            total_swaps,
            swaps_per_interval,
            interval,
            poll_interval,
            swap_timeout,
            quote_mode,
            from_chain,
            from_token,
            from_decimals,
            to_chain,
            to_token,
            to_decimals,
            amount,
            user_destination_address,
            user_evm_account_address,
            bitcoin_wallet_db_path,
            bitcoin_wallet_descriptor,
            bitcoin_network,
            bitcoin_esplora_url,
            evm_private_key,
            evm_rpc_ws_url,
            evm_debug_rpc_url,
            evm_confirmations,
        } = self;

        if total_swaps == 0 {
            return Err(anyhow!("total_swaps must be greater than zero"));
        }
        if swaps_per_interval == 0 {
            return Err(anyhow!("swaps_per_interval must be greater than zero"));
        }

        let quote_mode: QuoteMode = quote_mode.into();
        let from_chain: ChainType = from_chain.into();
        let to_chain: ChainType = to_chain.into();

        let from_currency = Currency {
            chain: from_chain,
            token: parse_token_identifier(&from_token)?,
            decimals: from_decimals,
        };
        let to_currency = Currency {
            chain: to_chain,
            token: parse_token_identifier(&to_token)?,
            decimals: to_decimals,
        };

        let quote_request = QuoteRequest {
            mode: quote_mode,
            from: from_currency,
            to: to_currency,
            amount,
        };

        let bitcoin = if from_chain == ChainType::Bitcoin {
            Some(BitcoinWalletConfig {
                db_path: bitcoin_wallet_db_path,
                descriptor: bitcoin_wallet_descriptor,
                network: bitcoin_network,
                esplora_url: bitcoin_esplora_url,
            })
        } else {
            None
        };

        let evm = if from_chain == ChainType::Ethereum {
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

        Ok(Config {
            log_level,
            otc_url,
            quote_url,
            swaps_per_interval,
            total_swaps,
            interval,
            poll_interval,
            swap_timeout,
            quote_request,
            user_destination_address,
            user_evm_account_address,
            bitcoin,
            evm,
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

fn parse_token_identifier(token: &str) -> Result<TokenIdentifier> {
    if token.eq_ignore_ascii_case("native") {
        Ok(TokenIdentifier::Native)
    } else {
        Ok(TokenIdentifier::Address(token.to_string()))
    }
}

impl Config {
    pub fn deposit_chain(&self) -> ChainType {
        self.quote_request.from.chain
    }
}
