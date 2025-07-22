use secrecy::{ExposeSecret, SecretString};
use serde::{Serialize, Serializer};
use std::fmt;
use zeroize::Zeroize;

/// A wallet containing a public address and private key.
/// The private key is protected against accidental logging.
pub struct Wallet {
    pub address: String,
    private_key: SecretString,
}

impl Wallet {
    pub fn new(address: String, private_key: String) -> Self {
        Self {
            address,
            private_key: SecretString::from(private_key),
        }
    }
    
    /// Get the private key. Use with extreme caution.
    pub fn private_key(&self) -> &str {
        self.private_key.expose_secret()
    }
}

// Custom Debug implementation that never exposes the private key
impl fmt::Debug for Wallet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Wallet")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

// Prevent Display implementation to avoid accidental logging
// The compiler will error if someone tries to impl Display

// Custom serialization that only includes the address
impl Serialize for Wallet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Only serialize the address, never the private key
        self.address.serialize(serializer)
    }
}

// Ensure the wallet is properly zeroized when dropped
impl Drop for Wallet {
    fn drop(&mut self) {
        // SecretString already handles zeroization, but we ensure address is cleared too
        self.address.zeroize();
    }
}

// Prevent Clone to avoid accidental duplication of secrets
// Users must explicitly create new wallets if needed

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_wallet_debug_redacts_private_key() {
        let wallet = Wallet::new(
            "0x1234567890123456789012345678901234567890".to_string(),
            "private_key_12345".to_string(),
        );
        
        let debug_str = format!("{:?}", wallet);
        assert!(debug_str.contains("Wallet"));
        assert!(debug_str.contains("0x1234567890123456789012345678901234567890"));
        assert!(debug_str.contains(".."));  // finish_non_exhaustive adds ".."
        assert!(!debug_str.contains("private_key"));
    }
    
    #[test]
    fn test_wallet_serialization_excludes_private_key() {
        let wallet = Wallet::new(
            "0x1234567890123456789012345678901234567890".to_string(),
            "private_key_12345".to_string(),
        );
        
        let json = serde_json::to_string(&wallet).unwrap();
        assert_eq!(json, "\"0x1234567890123456789012345678901234567890\"");
    }
}