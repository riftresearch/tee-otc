use alloy::primitives::U256;
use otc_models::{ChainType, Lot};
use snafu::{prelude::*, Location};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid address format for {address} for network {network:?}: {reason}"))]
    InvalidAddress {
        address: String,
        network: ChainType,
        reason: String,
    },

    #[snafu(display("Wallet creation failed: {message}"))]
    WalletCreation { message: String },

    EVMRpcError {
        source: alloy::transports::RpcError<alloy::transports::TransportErrorKind>,
        #[snafu(implicit)]
        loc: Location,
    },

    BitcoinRpcError {
        source: bitcoincore_rpc_async::Error,
        #[snafu(implicit)]
        loc: Location,
    },
    EsploraClientError {
        source: esplora_client::Error,
        #[snafu(implicit)]
        loc: Location,
    },
    EVMTokenIndexerClientError {
        source: evm_token_indexer_client::Error,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Invalid lot for network {network:?}: {lot:?}"))]
    InvalidCurrency { lot: Lot, network: ChainType },

    #[snafu(display("Transaction not found: {tx_hash}"))]
    TransactionNotFound { tx_hash: String },

    #[snafu(display("Insufficient balance: required {required}, available {available}"))]
    InsufficientBalance { required: U256, available: U256 },

    #[snafu(display("Chain not supported: {chain}"))]
    ChainNotSupported { chain: String },

    #[snafu(display("Serialization error: {message}"))]
    Serialization { message: String },

    #[snafu(display("Key derivation failed: {message}"))]
    KeyDerivation { message: String },

    #[snafu(display("Failed to dump to address: {message}"))]
    DumpToAddress { message: String },
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
