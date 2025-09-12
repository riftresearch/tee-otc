use otc_models::PublicApiKeyRecord;
use uuid::Uuid;
use std::sync::LazyLock;

// TODO: implement this as a proper API that can be used to add/remove API keys
pub const API_KEYS: LazyLock<Vec<PublicApiKeyRecord>> = LazyLock::new(|| vec![
    // the integration tests use a hardcoded API key for the test market maker
    // we dont want this to exist in production, so hide it behind the integration-tests feature
    #[cfg(feature = "integration-tests")]
    PublicApiKeyRecord {
        id: "a4c6da0d-a071-40ea-b69c-e23d49327d42".parse().unwrap(),
        tag: "test-mm",
        hash: "$argon2id$v=19$m=19456,t=2,p=1$AxmsqRK3lgwVnzXNwTeQmw$UgBYL3NIShPhC02dVWCsvCbAyWF+N/VpH4Rlkf+Vplo",
    }
    
]);