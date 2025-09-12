use argon2::{Argon2, PasswordHash, PasswordVerifier};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicApiKeyRecord {
    pub id: Uuid,
    pub tag: String,
    pub hash: String, // PHC format string from Argon2
}

impl PublicApiKeyRecord {
    /// Verify an API key against the stored hash
    #[must_use] pub fn verify(&self, api_key: &str) -> bool {
        if let Ok(parsed_hash) = PasswordHash::new(&self.hash) {
            Argon2::default()
                .verify_password(api_key.as_bytes(), &parsed_hash)
                .is_ok()
        } else {
            false
        }
    }
}