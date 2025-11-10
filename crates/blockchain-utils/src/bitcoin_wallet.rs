use async_trait::async_trait;
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


/// Calculates the median fee rate from an Esplora fee histogram.
///
/// The histogram is a list of (fee_rate, vsize_weight) tuples sorted by fee (high to low).
/// This function finds the fee rate at which the cumulative weight crosses the median point.
///
/// # Arguments
/// * `histogram` - Slice of (fee_rate, vsize_weight) tuples from Esplora (fee_rate as f32)
///
/// # Returns
/// The median fee rate, with a minimum floor of 1.01 sat/vB
fn calculate_median_fee_from_histogram(histogram: &[(f32, u64)]) -> f64 {
    if histogram.is_empty() {
        return 1.01;
    }

    // Cap the total weight at 1,000,000 to avoid overly long processing
    let mut total: f64 = histogram.iter().map(|&(_, w)| w).sum::<u64>() as f64;
    if total > 1_000_000.0 {
        total = 1_000_000.0;
    }
    let target = total / 2.0;

    let mut cum = 0.0;
    for &(fee, w) in histogram.iter() {
        let w = w as f64;
        let fee = fee as f64;
        // Stop when we cross the median mass
        if cum + w >= target {
            // Never return below 1.01
            return fee.max(1.01);
        }
        cum += w;
        // If we exceed the 1,000,000 cap, stop considering further bins
        if cum >= 1_000_000.0 {
            break;
        }
    }

    // Fallback if loop didn't early-return (shouldn't happen with non-empty hist)
    1.01
}

#[async_trait]
pub trait MempoolEsploraFeeExt { 
    async fn get_mempool_fee_estimate_next_block(&self) -> Result<f64, esplora_client::Error>;
}

#[async_trait]
impl MempoolEsploraFeeExt for esplora_client::AsyncClient {
    async fn get_mempool_fee_estimate_next_block(&self) -> Result<f64, esplora_client::Error> {
        let mempool_info = self.get_mempool_info().await?;
        Ok(calculate_median_fee_from_histogram(&mempool_info.fee_histogram))
    }
}

#[cfg(test)]
mod tests {
    use super::*;


    #[test]
    fn test_fee_estimate_real_data_histogram() {
        let histogram = vec![
            (4.01727861771058_f32, 51793),
            (3.00629921259843_f32, 51317),
            (2.18453188602442_f32, 53130),
            (2.01372997711671_f32, 50931),
            (2.00710059171598_f32, 50536),
            (2.0_f32, 50367),
            (1.52296450939457_f32, 51907),
            (1.39000515198351_f32, 51629),
            (1.20823798627002_f32, 50090),
            (1.20445761334137_f32, 79575),
            (1.17313291353511_f32, 56447),
            (1.17253747504483_f32, 59094),
            (1.10135674381484_f32, 50201),
            (1.08206302819682_f32, 52771),
            (1.03633916554509_f32, 51428),
            (1.00837890487479_f32, 77137),
            (1.00392692504695_f32, 51407),
            (1.003300330033_f32, 56431),
            (1.00236406619385_f32, 51165),
            (1.00174620366482_f32, 51371),
            (1.00003299567757_f32, 55233),
            (1.0_f32, 62300)
          ];
        let fee = calculate_median_fee_from_histogram(&histogram);
        println!("fee: {fee}");
    }

    #[test]
    fn test_fee_estimate_empty_histogram() {
        let histogram: Vec<(f32, u64)> = vec![];
        let fee = calculate_median_fee_from_histogram(&histogram);
        assert_eq!(fee, 1.01, "Empty histogram should return floor of 1.01");
    }

    #[test]
    fn test_fee_estimate_single_entry() {
        let histogram = vec![(5.0_f32, 100_000)];
        let fee = calculate_median_fee_from_histogram(&histogram);
        assert_eq!(fee, 5.0, "Single entry should return that fee rate");
    }

    #[test]
    fn test_fee_estimate_below_minimum() {
        // Fee rate below 1.01 should be clamped to 1.01
        let histogram = vec![(0.5_f32, 100_000)];
        let fee = calculate_median_fee_from_histogram(&histogram);
        assert_eq!(fee, 1.01, "Fee below 1.01 should be clamped to 1.01");
    }

    #[test]
    fn test_fee_estimate_median_calculation() {
        // Histogram: [(fee_rate, vsize_weight)]
        // Total weight: 100 + 200 + 300 = 600
        // Target (median): 300
        // Cumulative: 0 -> 100 (not yet) -> 300 (crosses target!)
        // Should return the second entry's fee: 15.0
        let histogram = vec![
            (20.0_f32, 100), // High fee, small weight
            (15.0_f32, 200), // Medium fee, medium weight - crosses median here
            (10.0_f32, 300), // Low fee, large weight
        ];
        let fee = calculate_median_fee_from_histogram(&histogram);
        assert_eq!(
            fee, 15.0,
            "Should return fee rate where cumulative weight crosses median"
        );
    }

    #[test]
    fn test_fee_estimate_large_mempool() {
        // Total weight: 1,500,000 (exceeds cap)
        // Capped total: 1,000,000
        // Target: 500,000
        // First entry has 600,000 which immediately crosses target
        let histogram = vec![
            (50.0_f32, 600_000), // Crosses median immediately
            (30.0_f32, 500_000),
            (10.0_f32, 400_000),
        ];
        let fee = calculate_median_fee_from_histogram(&histogram);
        assert_eq!(fee, 50.0, "Should handle large mempool with cap");
    }

    #[test]
    fn test_fee_estimate_exactly_at_target() {
        // Total: 200, Target: 100
        // First entry: exactly 100, should trigger at that point
        let histogram = vec![(25.0_f32, 100), (15.0_f32, 100)];
        let fee = calculate_median_fee_from_histogram(&histogram);
        assert_eq!(fee, 25.0, "Should handle exact target match");
    }

    #[test]
    fn test_fee_estimate_all_below_minimum() {
        let histogram = vec![(0.1_f32, 100), (0.5_f32, 200), (0.9_f32, 300)];
        let fee = calculate_median_fee_from_histogram(&histogram);
        assert_eq!(
            fee, 1.01,
            "All fees below 1.01 should still return 1.01"
        );
    }

    #[test]
    fn test_fee_estimate_realistic_scenario() {
        // Simulating a realistic mempool with various fee levels
        let histogram = vec![
            (100.0_f32, 50_000),   // Very high priority
            (50.0_f32, 100_000),   // High priority
            (25.0_f32, 200_000),   // Medium-high priority
            (10.0_f32, 300_000),   // Medium priority
            (5.0_f32, 350_000),    // Low-medium priority
            (2.0_f32, 500_000),    // Low priority
        ];
        // Total: 1,500,000 -> capped at 1,000,000
        // Target: 500,000
        // Cumulative: 50k -> 150k -> 350k -> 650k (crosses!)
        // Should return 10.0
        let fee = calculate_median_fee_from_histogram(&histogram);
        assert_eq!(fee, 10.0, "Should calculate median fee correctly");
    }

}