use crate::ChainType;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Currency {
    pub chain: ChainType,
    pub token: TokenIdentifier,
    pub decimals: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lot {
    pub currency: Currency,
    pub amount: U256,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quote {
    pub id: Uuid,

    /// The market maker that created the quote
    pub market_maker_id: Uuid,

    /// The currency the user will send
    pub from: Lot,

    /// The currency the user will receive
    pub to: Lot,

    /// The expiration time of the quote
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum QuoteMode {
    ExactInput,
    ExactOutput,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QuoteRequest {
    pub mode: QuoteMode,
    pub from: Currency,
    pub to: Currency,
    pub amount: U256,
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
        keccak256(to_canonical_json(self).unwrap().as_bytes()).into()
    }
}
