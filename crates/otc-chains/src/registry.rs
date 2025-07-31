use otc_models::ChainType;
use std::collections::HashMap;
use std::sync::Arc;
use crate::traits::ChainOperations;

pub struct ChainRegistry {
    chains: HashMap<ChainType, Arc<dyn ChainOperations>>,
}

impl ChainRegistry {
    #[must_use] pub fn new() -> Self {
        Self {
            chains: HashMap::new(),
        }
    }
    
    pub fn register(&mut self, chain_type: ChainType, implementation: Arc<dyn ChainOperations>) {
        self.chains.insert(chain_type, implementation);
    }
    
    #[must_use] pub fn get(&self, chain_type: &ChainType) -> Option<Arc<dyn ChainOperations>> {
        self.chains.get(chain_type).cloned()
    }
    
    #[must_use] pub fn supported_chains(&self) -> Vec<ChainType> {
        self.chains.keys().copied().collect()
    }
}

impl Default for ChainRegistry {
    fn default() -> Self {
        Self::new()
    }
}