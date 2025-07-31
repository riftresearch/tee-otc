use crate::errors::{ProtocolResult, ProtocolError};
use serde::{Deserialize, Serialize};

/// Current protocol version
pub const PROTOCOL_VERSION: &str = "1.0.0";

/// Minimum supported protocol version
pub const MIN_PROTOCOL_VERSION: &str = "1.0.0";

/// Protocol version information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionInfo {
    /// Current protocol version
    pub version: String,
    /// Minimum supported version
    pub min_version: String,
    /// List of supported features
    pub features: Vec<String>,
}

impl Default for VersionInfo {
    fn default() -> Self {
        Self {
            version: PROTOCOL_VERSION.to_string(),
            min_version: MIN_PROTOCOL_VERSION.to_string(),
            features: vec![
                "validate_quote".to_string(),
                "user_deposit_notification".to_string(),
                "swap_complete_notification".to_string(),
                "health_check".to_string(),
            ],
        }
    }
}

/// Check if a version is compatible
#[must_use] pub fn is_version_compatible(version: &str) -> bool {
    // Simple major version check for now
    // In production, use semver crate for proper version comparison
    version.starts_with("1.")
}

/// Ensure version compatibility, returning error if incompatible
pub fn ensure_version_compatible(version: &str) -> ProtocolResult<()> {
    if is_version_compatible(version) {
        Ok(())
    } else {
        Err(ProtocolError::VersionMismatch {
            expected: PROTOCOL_VERSION.to_string(),
            received: version.to_string(),
        })
    }
}