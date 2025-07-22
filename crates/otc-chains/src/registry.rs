use otc_models::ChainType;
use std::collections::HashMap;
use std::sync::Arc;
use crate::traits::ChainOperations;

pub struct ChainRegistry {
    chains: HashMap<ChainType, Arc<dyn ChainOperations>>,
}

impl ChainRegistry {
    pub fn new() -> Self {
        Self {
            chains: HashMap::new(),
        }
    }
    
    pub fn register(&mut self, chain_type: ChainType, implementation: Arc<dyn ChainOperations>) {
        self.chains.insert(chain_type, implementation);
    }
    
    pub fn get(&self, chain_type: &ChainType) -> Option<Arc<dyn ChainOperations>> {
        self.chains.get(chain_type).cloned()
    }
    
    pub fn supported_chains(&self) -> Vec<ChainType> {
        self.chains.keys().copied().collect()
    }
}

impl Default for ChainRegistry {
    fn default() -> Self {
        Self::new()
    }
}