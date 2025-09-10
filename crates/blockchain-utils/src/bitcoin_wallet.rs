use bitcoin::{
    address::NetworkChecked,
    bip32::{DerivationPath, Xpriv},
    key::{CompressedPublicKey, PrivateKey, PublicKey},
    script::ScriptBuf,
    secp256k1::{Secp256k1, SecretKey},
    Address, Amount, Network, OutPoint, Weight,
};
use bitcoin_coin_selection::WeightedUtxo;
use snafu::prelude::*;
use std::str::FromStr;

// Constants for transaction weight calculations
const _CHANGE_SPEND_W: Weight = Weight::from_wu(108); // Typical P2WPKH input weight

// Lightweight UTXO wrapper implementing WeightedUtxo
#[derive(Debug, Clone)]
pub struct InputUtxo {
    pub outpoint: OutPoint,
    pub value: Amount,
    pub weight: Weight,
}

impl WeightedUtxo for InputUtxo {
    fn satisfaction_weight(&self) -> Weight {
        self.weight
    }
    fn value(&self) -> Amount {
        self.value
    }
}

impl InputUtxo {
    fn _new(outpoint: OutPoint, value: Amount) -> Self {
        Self {
            outpoint,
            value,
            weight: _CHANGE_SPEND_W,
        }
    }
}

#[derive(Debug, Clone)]
pub struct P2WPKHBitcoinWallet {
    pub secret_key: SecretKey,
    pub private_key: PrivateKey,
    pub public_key: String,
    pub address: Address<NetworkChecked>,
}

// Define a proper error type for the wallet
#[derive(Debug, Snafu)]
pub enum BitcoinWalletError {
    #[snafu(display("Invalid mnemonic phrase"))]
    InvalidMnemonic,

    #[snafu(display("Invalid derivation path"))]
    InvalidDerivationPath,

    #[snafu(display("Key derivation failed"))]
    KeyDerivationFailed,

    #[snafu(display("Transaction signing failed: {message}"))]
    SigningFailed { message: String },

    #[snafu(display("Invalid public key"))]
    InvalidPublicKey,
}

impl P2WPKHBitcoinWallet {
    #[must_use]
    pub fn new(
        secret_key: SecretKey,
        private_key: PrivateKey,
        public_key: String,
        address: Address<NetworkChecked>,
    ) -> Self {
        Self {
            secret_key,
            private_key,
            public_key,
            address,
        }
    }

    #[must_use]
    pub fn from_secret_bytes(secret_key: &[u8; 32], network: Network) -> Self {
        let secret_key = SecretKey::from_slice(secret_key).unwrap();
        let secp = Secp256k1::new();
        let pk = PrivateKey::new(secret_key, network);
        let public_key = PublicKey::from_private_key(&secp, &pk);
        let _unlock_script = public_key.p2wpkh_script_code().unwrap().to_bytes();
        let address = Address::p2wpkh(
            &CompressedPublicKey::from_private_key(&secp, &pk).unwrap(),
            network,
        );
        Self::new(secret_key, pk, public_key.to_string(), address)
    }

    /// Creates a wallet from a BIP39 mnemonic phrase.
    pub fn from_mnemonic(
        mnemonic: &str,
        passphrase: Option<&str>,
        network: Network,
        derivation_path: Option<&str>,
    ) -> Result<Self, BitcoinWalletError> {
        use bip39::{Language, Mnemonic};

        // Parse and validate the mnemonic
        let mnemonic = Mnemonic::parse_in(Language::English, mnemonic)
            .map_err(|_| BitcoinWalletError::InvalidMnemonic)?;

        // Determine the appropriate derivation path based on network if not provided
        let path_str = derivation_path.unwrap_or(match network {
            Network::Bitcoin => "m/84'/0'/0'/0/0", // BIP84 for mainnet
            _ => "m/84'/1'/0'/0/0",                // BIP84 for testnet/regtest
        });

        // Parse the derivation path
        let derivation_path = DerivationPath::from_str(path_str)
            .map_err(|_| BitcoinWalletError::InvalidDerivationPath)?;

        // Create seed from mnemonic and optional passphrase
        let seed = mnemonic.to_seed(passphrase.unwrap_or(""));

        // Create master key and derive the child key
        let xpriv = Xpriv::new_master(network, &seed[..])
            .map_err(|_| BitcoinWalletError::KeyDerivationFailed)?;

        let child_xpriv = xpriv
            .derive_priv(&Secp256k1::new(), &derivation_path)
            .map_err(|_| BitcoinWalletError::KeyDerivationFailed)?;

        // Convert to private key and extract secret key
        let private_key = PrivateKey::new(child_xpriv.private_key, network);
        let secret_key = private_key.inner;

        // Generate public key and address
        let secp = Secp256k1::new();
        let public_key = PublicKey::from_private_key(&secp, &private_key);
        let address = Address::p2wpkh(
            &CompressedPublicKey::from_private_key(&secp, &private_key).unwrap(),
            network,
        );

        Ok(Self::new(
            secret_key,
            private_key,
            public_key.to_string(),
            address,
        ))
    }

    #[must_use]
    pub fn get_p2wpkh_script(&self) -> ScriptBuf {
        let public_key = PublicKey::from_str(&self.public_key).expect("Invalid public key");
        ScriptBuf::new_p2wpkh(
            &public_key
                .wpubkey_hash()
                .expect("Invalid public key for P2WPKH"),
        )
    }

    pub fn descriptor(&self) -> String {
        format!("wpkh({})", self.private_key)
    }
}
