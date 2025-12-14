use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use crate::{ChainType, TokenIdentifier};

/// Minimum viable output in satoshis. Swaps that would result in an output below this
/// threshold are rejected to prevent dust outputs and uneconomical swaps.
/// Set to 546 sats (Bitcoin's standard dust limit for P2PKH outputs).
pub const MIN_VIABLE_OUTPUT_SATS: u64 = 546;

pub const CB_BTC_CONTRACT_ADDRESS: &str = "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf";

pub const CBBTC_TOKEN: LazyLock<TokenIdentifier> = LazyLock::new(|| TokenIdentifier::Address(CB_BTC_CONTRACT_ADDRESS.to_string()));

pub static SUPPORTED_TOKENS_BY_CHAIN: LazyLock<HashMap<ChainType, HashSet<TokenIdentifier>>> =
    LazyLock::new(|| {
        HashMap::from([
            (ChainType::Bitcoin, HashSet::from([TokenIdentifier::Native])),
            (
                ChainType::Ethereum,
                HashSet::from([CBBTC_TOKEN.clone()]),
            ),
            (
                ChainType::Base,
                HashSet::from([CBBTC_TOKEN.clone()]),
            ),
        ])
    });

pub static FEE_ADDRESSES_BY_CHAIN: LazyLock<HashMap<ChainType, String>> = LazyLock::new(|| {
    HashMap::from([
        (
            ChainType::Bitcoin,
            "bc1q2p8ms86h3namagp4y486udsv4syydhvqztg886".to_string(),
        ),
        (
            ChainType::Ethereum,
            "0xfEe8d79961c529E06233fbF64F96454c2656BFEE".to_string(),
        ),
        (
            ChainType::Base,
            "0xfEe8d79961c529E06233fbF64F96454c2656BFEE".to_string(),
        ),
    ])
});

/// Expected chain IDs for each chain type
pub static EXPECTED_CHAIN_IDS: LazyLock<HashMap<ChainType, u64>> = LazyLock::new(|| {
    HashMap::from([
        // Ethereum mainnet
        (ChainType::Ethereum, 1),
        // Base mainnet
        (ChainType::Base, 8453),
    ])
});
