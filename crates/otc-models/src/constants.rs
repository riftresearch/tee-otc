use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use crate::{ChainType, TokenIdentifier};

pub const CB_BTC_CONTRACT_ADDRESS: &str = "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf";

pub static SUPPORTED_TOKENS_BY_CHAIN: LazyLock<HashMap<ChainType, HashSet<TokenIdentifier>>> =
    LazyLock::new(|| {
        HashMap::from([
            (ChainType::Bitcoin, HashSet::from([TokenIdentifier::Native])),
            (
                ChainType::Ethereum,
                HashSet::from([TokenIdentifier::Address(
                    CB_BTC_CONTRACT_ADDRESS.to_string(),
                )]),
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
    ])
});
