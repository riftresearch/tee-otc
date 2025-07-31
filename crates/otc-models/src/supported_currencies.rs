use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use crate::{ChainType, TokenIdentifier};

pub static SUPPORTED_TOKENS_BY_CHAIN: LazyLock<HashMap<ChainType, HashSet<TokenIdentifier>>> =
    LazyLock::new(|| {
        HashMap::from([
            (ChainType::Bitcoin, HashSet::from([TokenIdentifier::Native])),
            (
                ChainType::Ethereum,
                HashSet::from([TokenIdentifier::Address(
                    "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf".to_string(),
                )]),
            ),
        ])
    });
