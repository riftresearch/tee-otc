use crate::{serde_utils::u256_decimal, ChainType, SwapMode, SwapRates};
use alloy::primitives::{keccak256, U256};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Map, Value};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(tag = "type", content = "data")]
pub enum TokenIdentifier {
    Native,
    Address(String),
}

impl TokenIdentifier {
    /// Normalize the token identifier to lowercase for case-insensitive comparisons.
    ///
    /// Ethereum addresses can be represented in different cases (checksummed, lowercase, uppercase),
    /// but they all represent the same address. This function normalizes to lowercase.
    #[must_use]
    pub fn normalize(&self) -> Self {
        match self {
            Self::Native => Self::Native,
            Self::Address(addr) => Self::Address(addr.to_lowercase()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Currency {
    pub chain: ChainType,
    pub token: TokenIdentifier,
    pub decimals: u8,
}

/// A lot represents a specific amount of a currency.
/// Used for tracking actual deposited/sent amounts in swaps.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Lot {
    pub currency: Currency,
    #[serde(with = "u256_decimal")]
    pub amount: U256,
}

/// Fee amounts for a realized swap.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Fees {
    #[serde(with = "u256_decimal")]
    pub liquidity_fee: U256,
    #[serde(with = "u256_decimal")]
    pub protocol_fee: U256,
    #[serde(with = "u256_decimal")]
    pub network_fee: U256,
}

/// A quote that defines the terms for a swap.
///
/// Contains:
/// - Exact quoted amounts (`from`/`to` lots) for the requested input/output
/// - Valid deposit bounds (`min_input`/`max_input`) - user can deposit any amount within
/// - Rate parameters (`rates`) to compute realized amounts for any deposit
/// - Fee breakdown (`fees`) for the exact quoted amount
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quote {
    pub id: Uuid,

    /// The market maker that created the quote
    pub market_maker_id: Uuid,

    /// What the user sends (currency + exact quoted amount)
    pub from: Lot,

    /// What the user receives (currency + exact quoted amount)
    pub to: Lot,

    /// Rate parameters used for computation
    /// (used to compute realized amounts if user deposits a different amount)
    pub rates: SwapRates,

    /// Fee breakdown for the exact quoted amount
    pub fees: Fees,

    /// Minimum input amount allowed (in from currency smallest unit)
    #[serde(with = "u256_decimal")]
    pub min_input: U256,

    /// Maximum input amount allowed (in from currency smallest unit)
    #[serde(with = "u256_decimal")]
    pub max_input: U256,

    /// Optional affiliate identifier that determined the protocol fee rate
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affiliate: Option<String>,

    /// The expiration time of the quote
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QuoteRequest {
    pub from: Currency,
    pub to: Currency,
    /// Swap mode: ExactInput or ExactOutput with the specified amount
    pub mode: SwapMode,
    /// Optional affiliate identifier for custom protocol fee rates
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affiliate: Option<String>,
}

/// Serialize an f64 as its JSON number, but if it is NaN or ±Inf, serialize as `null`.
pub fn ser_f64_or_null<S>(v: &f64, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if v.is_finite() {
        s.serialize_f64(*v)
    } else {
        s.serialize_none()
    }
}

/// Convert any `Serialize` value to a canonical JSON string:
/// - All object keys sorted lexicographically
/// - No extra whitespace (compact)
/// - UTF-8 characters are emitted directly (serde_json default)
/// - NaN/±Inf must be handled via `ser_f64_or_null` on fields that can contain them
pub fn to_canonical_json<T: Serialize>(data: &T) -> serde_json::Result<String> {
    // 1) Serialize once to a Value. This will fail if there are NaN/Inf **unless**
    //    those fields are annotated with `ser_f64_or_null`.
    let value = serde_json::to_value(data)?;

    // 2) Recursively normalize (sort keys, normalize children).
    let normalized = normalize_value(value);

    // 3) Serialize back to a compact JSON string.
    //    (Default serializer is compact; it does NOT escape non-ASCII unless asked.)
    serde_json::to_string(&normalized)
}

/// Recursively sort all object keys and normalize children.
/// Arrays are left as-is (ordering is considered significant).
fn normalize_value(v: Value) -> Value {
    match v {
        Value::Object(map) => {
            // Gather keys, sort, then reinsert in order (insertion order is preserved by serde_json::Map)
            let mut keys: Vec<_> = map.keys().cloned().collect();
            keys.sort_unstable();
            let mut out = Map::with_capacity(map.len());
            for k in keys {
                let child = map.get(&k).expect("key must exist");
                out.insert(k, normalize_value(child.clone()));
            }
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(arr.into_iter().map(normalize_value).collect()),
        // Numbers are already valid JSON numbers at this point.
        // (NaN/±Inf would have been mapped to null via ser_f64_or_null before this stage.)
        primitive => primitive,
    }
}

impl Quote {
    pub fn hash(&self) -> [u8; 32] {
        // Create a normalized version of the quote with microsecond-precision timestamps
        // to ensure consistent hashing across database round-trips (PostgreSQL uses microseconds)
        let normalized = Quote {
            expires_at: normalize_datetime_to_micros(self.expires_at),
            created_at: normalize_datetime_to_micros(self.created_at),
            ..self.clone()
        };
        keccak256(to_canonical_json(&normalized).unwrap().as_bytes()).into()
    }

    /// Checks if the given input amount is within the allowed bounds.
    pub fn is_valid_input(&self, amount: U256) -> bool {
        amount >= self.min_input && amount <= self.max_input
    }
}

/// Normalize a DateTime to microsecond precision to match PostgreSQL TIMESTAMPTZ precision.
/// This ensures consistent hashing across database round-trips.
fn normalize_datetime_to_micros(dt: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(dt.timestamp_micros())
        .expect("timestamp should be in valid range")
}
