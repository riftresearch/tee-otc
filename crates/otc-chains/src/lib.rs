pub mod error;
pub mod key_derivation;
pub mod registry;
pub mod traits;

// Chain implementations
pub mod bitcoin;
pub mod ethereum;

pub use error::{Error, Result};
pub use registry::ChainRegistry;
pub use traits::ChainOperations;