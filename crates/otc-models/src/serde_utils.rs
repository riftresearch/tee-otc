//! Serde utilities for consistent serialization across the API.

/// Custom serialization for U256 that outputs decimal strings instead of hex.
/// This is needed for API and database compatibility where numeric strings are expected.
pub mod u256_decimal {
    use alloy::primitives::U256;
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &U256, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<U256, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse::<U256>().map_err(serde::de::Error::custom)
    }
}

/// Custom serialization for Option<U256> that outputs decimal strings instead of hex.
pub mod option_u256_decimal {
    use alloy::primitives::U256;
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &Option<U256>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(v) => serializer.serialize_str(&v.to_string()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<U256>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<String> = Option::deserialize(deserializer)?;
        match opt {
            Some(s) => {
                let v = s.parse::<U256>().map_err(serde::de::Error::custom)?;
                Ok(Some(v))
            }
            None => Ok(None),
        }
    }
}

