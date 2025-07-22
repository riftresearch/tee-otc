use hkdf::Hkdf;
use sha2::Sha256;

/// Derive a private key deterministically from master key and salt
pub fn derive_private_key(master_key: &[u8], salt: &[u8; 32], info: &[u8]) -> [u8; 32] {
    let hk = Hkdf::<Sha256>::new(Some(salt), master_key);
    let mut okm = [0u8; 32];
    hk.expand(info, &mut okm)
        .expect("32 is a valid output length for HKDF-SHA256");
    okm
}