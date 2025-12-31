use otc_models::PublicApiKeyRecord;
use std::sync::LazyLock;

// TODO: implement this as a proper API that can be used to add/remove API keys
pub static API_KEYS: LazyLock<Vec<PublicApiKeyRecord>> = LazyLock::new(|| {
    vec![
        // the integration tests use hardcoded API keys for the test market makers
        // we don't want these to exist in production, so hide them behind the
        // `test-market-makers` feature
        #[cfg(feature = "test-market-makers")]
        PublicApiKeyRecord {
            id: "96c0bedb-bfda-4680-a8df-1317d1e09c8d".parse().unwrap(),
            tag: "test-mm-eth".to_string(),
            hash: "$argon2id$v=19$m=19456,t=2,p=1$xtwHECDisKE9Vpp71a0XdA$Q+d1QhDH5UGIKaFhLycFPSh7fV7bVbqR1NkKqo+W5jI".to_string(),
        },
        #[cfg(feature = "test-market-makers")]
        PublicApiKeyRecord {
            id: "f901369b-84d7-4c03-8799-f504c22125f9".parse().unwrap(),
            tag: "test-mm-base".to_string(),
            hash: "$argon2id$v=19$m=19456,t=2,p=1$aYjaEm00ZK5mpL0E/qlzEw$UfBaYvQmYSFtHN89A9zlNCjSHKyZ6fnqF3hV2j8jq5A".to_string(),
        },
        PublicApiKeyRecord {
            id: "439505b9-423f-4975-a6c2-b8a2f72e95a9".parse().unwrap(),
            tag: "rift-mm-1".to_string(),
            hash: "$argon2id$v=19$m=19456,t=2,p=1$WRpNdXmZfVvYKMSOswoqHA$QP9uBxjsxVU1cqz/fntfvbpH3mLlYILEOLALWK1nxdQ".to_string(),
        },
        PublicApiKeyRecord {
            id: "38ddb152-1a81-4ecd-9e78-24953ef7b111".parse().unwrap(),
            tag: "rift-mm-2".to_string(),
            hash: "$argon2id$v=19$m=19456,t=2,p=1$5VILdLbU6pupoelhFC9gOw$e1Eq3aD3MEU5Cf69zvnXfpKXhYO9xLDdA0nzYF0hstA".to_string(),
        },
        PublicApiKeyRecord {
            id: "2c5868fa-2dad-4eca-b033-c6e78039890b".parse().unwrap(),
            tag: "rift-mm-3".to_string(),
            hash: "$argon2id$v=19$m=19456,t=2,p=1$g5zLtYCxYXgIZZS+9s5+PA$Qzt/X2AOYofvhJ99W5gh3yz0Y+21yi0wDK5dFqV9KhQ".to_string(),
        },
        PublicApiKeyRecord {
            id: "af6843cf-7fbf-44bb-87e5-d0cc4708bb2b".parse().unwrap(),
            tag: "rift-mm-4".to_string(),
            hash: "$argon2id$v=19$m=19456,t=2,p=1$1o2QgPjqHS3mdeZb0vRGjA$GamDi+DyZoZs8nkfb109McWqi5sAKnfMgXSwWi38Rn4".to_string(),
        }

    ]
});
