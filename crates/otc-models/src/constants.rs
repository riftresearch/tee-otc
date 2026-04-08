use crate::{ChainType, TokenIdentifier};

/// Minimum viable output in satoshis. Swaps that would result in an output below this
/// threshold are rejected to prevent dust outputs and uneconomical swaps.
/// Set to 546 sats (Bitcoin's standard dust limit for P2PKH outputs).
pub const MIN_VIABLE_OUTPUT_SATS: u64 = 546;

pub const CB_BTC_CONTRACT_ADDRESS: &str = "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupportedTokenRef {
    Native,
    Address(&'static str),
}

impl SupportedTokenRef {
    #[must_use]
    pub fn to_token_identifier(self) -> TokenIdentifier {
        match self {
            Self::Native => TokenIdentifier::Native,
            Self::Address(address) => TokenIdentifier::address(address),
        }
    }

    #[must_use]
    pub fn matches(self, token: &TokenIdentifier) -> bool {
        self.to_token_identifier().normalize() == token.normalize()
    }
}

pub const BITCOIN_SUPPORTED_TOKENS: &[SupportedTokenRef] = &[SupportedTokenRef::Native];
pub const ETHEREUM_SUPPORTED_TOKENS: &[SupportedTokenRef] =
    &[SupportedTokenRef::Address(CB_BTC_CONTRACT_ADDRESS)];
pub const BASE_SUPPORTED_TOKENS: &[SupportedTokenRef] =
    &[SupportedTokenRef::Address(CB_BTC_CONTRACT_ADDRESS)];

pub const SUPPORTED_TOKENS_BY_CHAIN: &[(ChainType, &[SupportedTokenRef])] = &[
    (ChainType::Bitcoin, BITCOIN_SUPPORTED_TOKENS),
    (ChainType::Ethereum, ETHEREUM_SUPPORTED_TOKENS),
    (ChainType::Base, BASE_SUPPORTED_TOKENS),
];

pub const FEE_ADDRESSES_BY_CHAIN: &[(ChainType, &str)] = &[
    (
        ChainType::Bitcoin,
        "bc1q2p8ms86h3namagp4y486udsv4syydhvqztg886",
    ),
    (
        ChainType::Ethereum,
        "0xfEe8d79961c529E06233fbF64F96454c2656BFEE",
    ),
    (
        ChainType::Base,
        "0xfEe8d79961c529E06233fbF64F96454c2656BFEE",
    ),
];

/// Expected chain IDs for each chain type
pub const EXPECTED_CHAIN_IDS: &[(ChainType, u64)] = &[
    // Ethereum mainnet
    (ChainType::Ethereum, 1),
    // Base mainnet
    (ChainType::Base, 8453),
];

#[must_use]
pub fn cbbtc_token() -> TokenIdentifier {
    TokenIdentifier::address(CB_BTC_CONTRACT_ADDRESS)
}

#[must_use]
pub const fn supported_tokens_for_chain(chain: ChainType) -> &'static [SupportedTokenRef] {
    match chain {
        ChainType::Bitcoin => BITCOIN_SUPPORTED_TOKENS,
        ChainType::Ethereum => ETHEREUM_SUPPORTED_TOKENS,
        ChainType::Base => BASE_SUPPORTED_TOKENS,
    }
}

#[must_use]
pub const fn fee_address_for_chain(chain: ChainType) -> &'static str {
    match chain {
        ChainType::Bitcoin => "bc1q2p8ms86h3namagp4y486udsv4syydhvqztg886",
        ChainType::Ethereum => "0xfEe8d79961c529E06233fbF64F96454c2656BFEE",
        ChainType::Base => "0xfEe8d79961c529E06233fbF64F96454c2656BFEE",
    }
}

#[must_use]
pub const fn expected_chain_id(chain: ChainType) -> Option<u64> {
    match chain {
        ChainType::Bitcoin => None,
        ChainType::Ethereum => Some(1),
        ChainType::Base => Some(8453),
    }
}
