use alloy::primitives::U256;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid address format"))]
    InvalidAddress,
    
    #[snafu(display("Wallet creation failed: {message}"))]
    WalletCreation { message: String },
    
    #[snafu(display("RPC error: {message}"))]
    Rpc { message: String },
    
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
}

pub type Result<T> = std::result::Result<T, Error>;