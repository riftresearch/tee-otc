use alloy::primitives::U256;
use otc_models::{ChainType, Currency};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid address format for {address} for network {network:?}: {reason}"))]
    InvalidAddress { address: String, network: ChainType, reason: String },
    
    #[snafu(display("Wallet creation failed: {message}"))]
    WalletCreation { message: String },
    
    #[snafu(display("RPC error: {message}"))]
    Rpc { message: String },

    #[snafu(display("Invalid currency for network {network:?}: {currency:?}"))]
    InvalidCurrency { currency: Currency, network: ChainType },
    
    #[snafu(display("Transaction not found: {tx_hash}"))]
    TransactionNotFound { tx_hash: String },
    
    #[snafu(display("Insufficient balance: required {required}, available {available}"))]
    InsufficientBalance {
        required: U256,
        available: U256,
    },
    
    #[snafu(display("Chain not supported: {chain}"))]
    ChainNotSupported { chain: String },
    
    #[snafu(display("Serialization error: {message}"))]
    Serialization { message: String },
    
    #[snafu(display("Key derivation failed: {message}"))]
    KeyDerivation { message: String },
}

pub type Result<T> = std::result::Result<T, Error>;


impl From<bitcoin::address::ParseError> for Error {
    fn from(error: bitcoin::address::ParseError) -> Self {
        Error::InvalidAddress {
            address: error.to_string(),
            network: ChainType::Bitcoin,
            reason: error.to_string(),
        }
    }
}


impl From<esplora_client::Error> for Error {
    fn from(error: esplora_client::Error) -> Self {
        Error::Rpc { message: format!("Esplora error: {}", error) }
    }
}


impl From<bitcoincore_rpc_async::Error> for Error {
    fn from(error: bitcoincore_rpc_async::Error) -> Self {
        Error::Rpc { message: format!("Bitcoin Core RPC error: {}", error) }
    }
}


impl From<evm_token_indexer_client::Error> for Error {
    fn from(error: evm_token_indexer_client::Error) -> Self {
        Error::Rpc { message: format!("EVM Token Indexer error: {}", error) }
    }
}

impl From<alloy::transports::RpcError<alloy::transports::TransportErrorKind>> for Error {
    fn from(error: alloy::transports::RpcError<alloy::transports::TransportErrorKind>) -> Self {
        Error::Rpc { message: format!("EVM RPC error: {}", error) }
    }
}