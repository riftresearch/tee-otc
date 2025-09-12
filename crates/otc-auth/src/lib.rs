pub mod api_keys;
use otc_models::PublicApiKeyRecord;
use snafu::{prelude::*, Whatever};
use std::{collections::HashMap, path::PathBuf};
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum AuthError {
    #[snafu(display("Market maker '{}' not found", market_maker))]
    MarketMakerNotFound { market_maker: String },


    #[snafu(display("API key ID '{}' not found", id))]
    ApiKeyIdNotFound { id: Uuid },

    #[snafu(display("Invalid API key for ID '{}'", id))]
    InvalidApiKeyForId { id: Uuid },
}

type Result<T, E = AuthError> = std::result::Result<T, E>;

/// API key store that loads keys from a JSON file
pub struct ApiKeyStore {
    records_by_id: HashMap<Uuid, PublicApiKeyRecord>,
}

impl ApiKeyStore {
    /// Create a new API key store from a JSON file
    pub async fn new(api_key_records: Vec<PublicApiKeyRecord>) -> Result<Self, Whatever> {

        let mut records_by_id = HashMap::new();

        for record in api_key_records {
            records_by_id.insert(record.id, record);
        }

        Ok(Self { records_by_id })
    }

    /// Validate an API key for a market maker, returns the market maker tag
    pub fn validate(&self, id: &Uuid, api_secret: &str) -> Result<String> {
        let stored_key = self
            .records_by_id
            .get(id)
            .context(ApiKeyIdNotFoundSnafu { id: *id })?;

        if stored_key.verify(api_secret) {
            Ok(stored_key.tag.clone())
        } else {
            Err(AuthError::InvalidApiKeyForId {
                id: *id,
            })
        }
    }

    /// Check if a market maker exists
    #[must_use]
    pub fn exists(&self, id: &Uuid) -> bool {
        self.records_by_id.contains_key(id)
    }

    /// Get API key record by UUID
    #[must_use]
    pub fn get_by_id(&self, id: &Uuid) -> Option<&PublicApiKeyRecord> {
        self.records_by_id.get(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otc_models::PublicApiKeyRecord;
    use std::fs;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_api_key_store() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("whitelist.json");

        let api_keys = vec![PublicApiKeyRecord {
            id: Uuid::new_v4(),
            tag: "test_mm".to_string(),
            hash: "$argon2id$v=19$m=19456,t=2,p=1$test_salt$test_hash".to_string(),
        }];

        fs::write(&file_path, serde_json::to_string(&api_keys).unwrap()).unwrap();

        let store = ApiKeyStore::new(api_keys.clone()).await.unwrap();
        assert!(store.exists(&api_keys[0].id));
        assert!(!store.exists(&Uuid::new_v4()));
    }
}
