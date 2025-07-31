use alloy::{
    primitives::{address, Address, U160, U256},
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::{Filter, Log},
    sol,
    sol_types::SolEvent,
};
use arc_swap::ArcSwap;
use dashmap::DashMap;
use futures::StreamExt;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use snafu::{ResultExt, Snafu};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{interval, timeout},
};
use tracing::{error, info, warn};

use crate::quote::{q64_96_to_float, ConversionRates};
use crate::fixed_point::{FixedPoint, FixedConversionRates};

#[derive(Debug, Snafu)]
pub enum PriceFeedError {
    #[snafu(display("WebSocket connection error: {}", source))]
    WebSocketConnection { source: alloy::transports::TransportError },
    
    #[snafu(display("Failed to subscribe to events: {}", source))]
    SubscriptionError { source: alloy::transports::TransportError },
    
    #[snafu(display("Failed to decode event: {}", source))]
    EventDecode { source: alloy::sol_types::Error },
    
    #[snafu(display("Price cache is empty for pair {}/{}", from, to))]
    CacheEmpty { from: String, to: String },
    
    #[snafu(display("RPC fallback failed: {}", source))]
    RpcFallback { source: anyhow::Error },
    
    #[snafu(display("Invalid pool configuration"))]
    InvalidPoolConfig,
}

type Result<T, E = PriceFeedError> = std::result::Result<T, E>;

// Uniswap V3 event definitions
sol! {
    event Swap(
        address indexed sender,
        address indexed recipient,
        int256 amount0,
        int256 amount1,
        uint160 sqrtPriceX96,
        uint128 liquidity,
        int24 tick
    );
}

#[derive(Debug, Clone)]
pub struct PriceData {
    pub sqrt_price_x96: U160,
    pub block_number: u64,
    pub timestamp: u64,
    pub conversion_rates: ConversionRates,
    pub fixed_rates: FixedConversionRates,
}

#[derive(Debug, Clone)]
struct PoolConfig {
    pool_address: Address,
    weth_address: Address,
    chain_id: u64,
    weth_is_token0: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct WebSocketConfig {
    pub endpoints: HashMap<u64, Vec<String>>,
    pub reconnect_delay: Duration,
    pub max_reconnect_delay: Duration,
    pub health_check_interval: Duration,
    pub connection_pool_size: usize,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        let mut endpoints = HashMap::new();
        
        // Ethereum mainnet endpoints
        endpoints.insert(1, vec![
            "wss://ethereum-rpc.publicnode.com".to_string(),
            "wss://eth.llamarpc.com".to_string(),
            "wss://eth-mainnet.g.alchemy.com/v2/demo".to_string(),
        ]);
        
        // Base mainnet endpoints
        endpoints.insert(8453, vec![
            "wss://base-rpc.publicnode.com".to_string(),
            "wss://base.llamarpc.com".to_string(),
        ]);
        
        Self {
            endpoints,
            reconnect_delay: Duration::from_secs(1),
            max_reconnect_delay: Duration::from_secs(60),
            health_check_interval: Duration::from_secs(60),
            connection_pool_size: 2,
        }
    }
}

static PRICE_CACHE: Lazy<ArcSwap<HashMap<Address, PriceData>>> = 
    Lazy::new(|| ArcSwap::from_pointee(HashMap::new()));

static CONVERSION_CACHE: Lazy<Mutex<lru::LruCache<(U160, bool), (FixedConversionRates, ConversionRates)>>> = 
    Lazy::new(|| Mutex::new(lru::LruCache::new(std::num::NonZeroUsize::new(1000).unwrap())));

pub struct PriceFeedManager {
    pool_configs: Arc<Vec<PoolConfig>>,
    tasks: Arc<parking_lot::RwLock<Vec<JoinHandle<()>>>>,
    ws_config: Arc<WebSocketConfig>,
    health_status: Arc<DashMap<u64, bool>>,
    price_updates: mpsc::UnboundedSender<PriceUpdate>,
}

#[derive(Debug)]
struct PriceUpdate {
    pool_address: Address,
    price_data: PriceData,
}

impl PriceFeedManager {
    pub async fn new() -> Result<Self> {
        Self::with_config(WebSocketConfig::default()).await
    }
    
    pub async fn with_config(ws_config: WebSocketConfig) -> Result<Self> {
        let tasks = Arc::new(parking_lot::RwLock::new(Vec::new()));
        let health_status = Arc::new(DashMap::new());
        
        let pool_configs = vec![
            PoolConfig {
                pool_address: address!("0x7AeA2E8A3843516afa07293a10Ac8E49906dabD1"),
                weth_address: address!("0x4200000000000000000000000000000000000006"),
                chain_id: 8453,
                weth_is_token0: None, // Will be determined dynamically
            },
        ];
        
        let (price_tx, mut price_rx) = mpsc::unbounded_channel::<PriceUpdate>();
        
        let price_processor = tokio::spawn(async move {
            while let Some(update) = price_rx.recv().await {
                PRICE_CACHE.rcu(|cache| {
                    let mut new_cache = HashMap::clone(cache);
                    new_cache.insert(update.pool_address, update.price_data.clone());
                    new_cache
                });
                
                let cache_key = (update.price_data.sqrt_price_x96, true);
                CONVERSION_CACHE.lock().put(
                    cache_key, 
                    (update.price_data.fixed_rates, update.price_data.conversion_rates)
                );
            }
        });
        
        let manager = Self {
            pool_configs: Arc::new(pool_configs),
            tasks,
            ws_config: Arc::new(ws_config),
            health_status,
            price_updates: price_tx,
        };
        
        manager.tasks.write().push(price_processor);
        
        manager.start_price_feeds().await?;
        
        Ok(manager)
    }
    
    async fn start_price_feeds(&self) -> Result<()> {
        let mut tasks = self.tasks.write();
        
        // Start subscription tasks for each pool
        for config in self.pool_configs.iter() {
            let endpoints = self.ws_config.endpoints.get(&config.chain_id).cloned().unwrap_or_default();
            
            // Only create one connection per endpoint
            for endpoint in endpoints.into_iter().take(1) {
                let config = config.clone();
                let price_updates = self.price_updates.clone();
                let ws_config = self.ws_config.clone();
                let health_status = self.health_status.clone();
                let pool_configs = self.pool_configs.clone();
                
                let task = tokio::spawn(async move {
                    let mut retry_delay = ws_config.reconnect_delay;
                    
                    loop {
                        info!("Connecting to {} for pool {}", endpoint, config.pool_address);
                        
                        match subscribe_to_pool_events(
                            &endpoint,
                            config.clone(),
                            price_updates.clone(),
                            pool_configs.clone(),
                            health_status.clone(),
                        ).await {
                            Ok(_) => {
                                info!("Disconnected from {}", endpoint);
                                retry_delay = ws_config.reconnect_delay;
                            }
                            Err(e) => {
                                error!("WebSocket error on {}: {}", endpoint, e);
                            }
                        }
                        
                        tokio::time::sleep(retry_delay).await;
                        retry_delay = (retry_delay * 2).min(ws_config.max_reconnect_delay);
                    }
                });
                
                tasks.push(task);
            }
        }
        
        // Start periodic RPC fallback task
        let pool_configs = self.pool_configs.clone();
        let price_updates = self.price_updates.clone();
        let health_check_interval = self.ws_config.health_check_interval;
        
        let rpc_task = tokio::spawn(async move {
            let mut interval = interval(health_check_interval);
            
            loop {
                interval.tick().await;
                
                // Check each pool for refresh needs
                for config in pool_configs.iter() {
                    if should_refresh_price(&config.pool_address) {
                        info!("Refreshing price via RPC for pool {}", config.pool_address);
                        match fetch_price_via_rpc(&config).await {
                            Ok(price_data) => {
                                let _ = price_updates.send(PriceUpdate {
                                    pool_address: config.pool_address,
                                    price_data,
                                });
                            }
                            Err(e) => {
                                error!("RPC fetch failed: {}", e);
                            }
                        }
                    }
                }
            }
        });
        
        tasks.push(rpc_task);
        
        Ok(())
    }
    
    pub async fn get_fixed_rates(&self, chain_id: u64) -> Result<FixedConversionRates> {
        // Find the pool config for this chain
        let config = self.pool_configs.iter()
            .find(|c| c.chain_id == chain_id)
            .ok_or_else(|| PriceFeedError::CacheEmpty {
                from: "ETH".to_string(),
                to: "cbBTC".to_string(),
            })?;
        
        let cache = PRICE_CACHE.load();
        if let Some(price_data) = cache.get(&config.pool_address) {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            let age_seconds = now - price_data.timestamp;
            
            if age_seconds < 60 {
                info!("Cache hit for chain {} - price age: {}s", chain_id, age_seconds);
                return Ok(price_data.fixed_rates);
            }
            warn!("Cache stale for chain {} - price age: {}s", chain_id, age_seconds);
        }
        
        // Fallback to RPC
        warn!("Falling back to RPC for chain {}", chain_id);
        let price_data = fetch_price_via_rpc(config).await?;
        let _ = self.price_updates.send(PriceUpdate {
            pool_address: config.pool_address,
            price_data: price_data.clone(),
        });
        
        Ok(price_data.fixed_rates)
    }
    
    pub async fn get_conversion_rates(&self, chain_id: u64) -> Result<ConversionRates> {
        let config = self.pool_configs.iter()
            .find(|c| c.chain_id == chain_id)
            .ok_or_else(|| PriceFeedError::CacheEmpty {
                from: "ETH".to_string(),
                to: "cbBTC".to_string(),
            })?;
        
        let cache = PRICE_CACHE.load();
        if let Some(price_data) = cache.get(&config.pool_address) {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            let age_seconds = now - price_data.timestamp;
            
            if age_seconds < 60 {
                return Ok(price_data.conversion_rates);
            }
        }
        
        let price_data = fetch_price_via_rpc(config).await?;
        let _ = self.price_updates.send(PriceUpdate {
            pool_address: config.pool_address,
            price_data: price_data.clone(),
        });
        
        Ok(price_data.conversion_rates)
    }
    
    pub async fn is_healthy(&self, chain_id: u64) -> bool {
        self.health_status.get(&chain_id).map(|v| *v).unwrap_or(false)
    }
    
    pub async fn shutdown(self) {
        let mut tasks = self.tasks.write();
        for task in tasks.drain(..) {
            task.abort();
        }
    }
}

async fn subscribe_to_pool_events(
    ws_url: &str,
    mut config: PoolConfig,
    price_updates: mpsc::UnboundedSender<PriceUpdate>,
    pool_configs: Arc<Vec<PoolConfig>>,
    health_status: Arc<DashMap<u64, bool>>,
) -> Result<()> {
    let ws = WsConnect::new(ws_url);
    let provider = timeout(
        Duration::from_secs(30),
        ProviderBuilder::new().connect_ws(ws),
    )
    .await
    .map_err(|_| PriceFeedError::InvalidPoolConfig)?
    .context(WebSocketConnectionSnafu)?;
    
    let filter = Filter::new()
        .address(config.pool_address)
        .event_signature(Swap::SIGNATURE_HASH);
    
    let subscription = provider.subscribe_logs(&filter).await
        .context(SubscriptionSnafu)?;
    
    let mut stream = subscription.into_stream();
    
    health_status.insert(config.chain_id, true);
    info!("Marked chain {} as healthy", config.chain_id);
    
    // Fetch initial price and determine token ordering
    match fetch_initial_price(&provider, &mut config, &pool_configs).await {
        Ok(price_data) => {
            let _ = price_updates.send(PriceUpdate {
                pool_address: config.pool_address,
                price_data,
            });
        }
        Err(e) => {
            error!("Failed to fetch initial price: {}", e);
        }
    }
    
    let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(30));
    let mut events_received = 0u64;
    
    loop {
        tokio::select! {
            result = stream.next() => {
                match result {
                    Some(log) => {
                        events_received += 1;
                        match process_swap_event(log, &config).await {
                            Ok(price_data) => {
                                let _ = price_updates.send(PriceUpdate {
                                    pool_address: config.pool_address,
                                    price_data,
                                });
                            }
                            Err(e) => {
                                if !matches!(&e, PriceFeedError::EventDecode { .. }) {
                                    error!("Failed to process swap event: {}", e);
                                }
                            }
                        }
                    }
                    None => {
                        warn!("WebSocket stream ended");
                        break;
                    }
                }
            }
            _ = heartbeat_interval.tick() => {
                info!("Heartbeat: {} events received from {}", events_received, ws_url);
                // Try to fetch current price as heartbeat
                match fetch_initial_price(&provider, &mut config, &pool_configs).await {
                    Ok(price_data) => {
                        let _ = price_updates.send(PriceUpdate {
                            pool_address: config.pool_address,
                            price_data,
                        });
                    }
                    Err(e) => {
                        error!("Heartbeat fetch failed: {}", e);
                        break; // Connection might be dead
                    }
                }
            }
        }
    }
    
    health_status.insert(config.chain_id, false);
    Ok(())
}

async fn fetch_initial_price<P: Provider>(
    provider: &P,
    config: &mut PoolConfig,
    _pool_configs: &Arc<Vec<PoolConfig>>,
) -> Result<PriceData> {
    use crate::quote::{IUniswapV3PoolState, IUniswapV3PoolImmutables};
    
    let pool = IUniswapV3PoolState::new(config.pool_address, provider);
    let pool_immut = IUniswapV3PoolImmutables::new(config.pool_address, provider);
    
    // Fetch slot0 and token addresses with timeout
    let (slot0, token0, _token1) = timeout(Duration::from_secs(10), async move {
        let slot0_binding = pool.slot0();
        let slot0_fut = slot0_binding.call();
        
        let token0_binding = pool_immut.token0();
        let token0_fut = token0_binding.call();
        
        let token1_binding = pool_immut.token1();
        let token1_fut = token1_binding.call();
        
        tokio::try_join!(
            slot0_fut,
            token0_fut,
            token1_fut
        )
    }).await
        .map_err(|_| PriceFeedError::RpcFallback { 
            source: anyhow::anyhow!("Timeout fetching pool data") 
        })?
        .map_err(|e| PriceFeedError::RpcFallback { source: e.into() })?;
    
    // Determine and cache token ordering
    let weth_is_token0 = token0 == config.weth_address;
    if config.weth_is_token0.is_none() {
        config.weth_is_token0 = Some(weth_is_token0);
        info!("Determined token ordering for pool {}: WETH is token{}", 
            config.pool_address, 
            if weth_is_token0 { "0" } else { "1" }
        );
    }
    
    // Check conversion cache first
    let cache_key = (slot0.sqrtPriceX96, weth_is_token0);
    let cached_rates = CONVERSION_CACHE.lock().get(&cache_key).cloned();
    
    if let Some((fixed_rates, conversion_rates)) = cached_rates {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let block_number = provider.get_block_number().await
            .map_err(|e| PriceFeedError::RpcFallback { source: e.into() })?;
        
        return Ok(PriceData {
            sqrt_price_x96: slot0.sqrtPriceX96,
            block_number,
            timestamp,
            conversion_rates,
            fixed_rates,
        });
    }
    
    // Calculate rates if not cached
    let fixed_rates = FixedPoint::sqrt_price_to_rates(slot0.sqrtPriceX96, weth_is_token0)
        .map_err(|_| PriceFeedError::InvalidPoolConfig)?;
    
    let conversion_rates = calculate_conversion_rates(slot0.sqrtPriceX96, weth_is_token0);
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let block_number = provider.get_block_number().await
        .map_err(|e| PriceFeedError::RpcFallback { source: e.into() })?;
    
    // Cache the calculation
    CONVERSION_CACHE.lock().put(cache_key, (fixed_rates, conversion_rates));
    
    info!(
        "Fetched price for pool {} - cbBTC/ETH: {:.8}, ETH/cbBTC: {:.8}",
        config.pool_address,
        conversion_rates.cbbtc_per_eth,
        conversion_rates.eth_per_cbbtc
    );
    
    Ok(PriceData {
        sqrt_price_x96: slot0.sqrtPriceX96,
        block_number,
        timestamp,
        conversion_rates,
        fixed_rates,
    })
}

async fn process_swap_event(
    log: Log,
    config: &PoolConfig,
) -> Result<PriceData> {
    // Verify this is a Swap event
    if log.topics().is_empty() || log.topics()[0] != Swap::SIGNATURE_HASH {
        return Err(PriceFeedError::EventDecode { 
            source: alloy::sol_types::Error::Other("Not a Swap event".into()) 
        });
    }
    
    let swap_event = Swap::decode_log(&log.inner)
        .context(EventDecodeSnafu)?;
    
    let sqrt_price = swap_event.sqrtPriceX96;
    let block_number = log.block_number.unwrap_or(0);
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    
    let weth_is_token0 = config.weth_is_token0.unwrap_or(true);
    
    // Check cache first
    let cache_key = (sqrt_price, weth_is_token0);
    if let Some((fixed_rates, conversion_rates)) = CONVERSION_CACHE.lock().get(&cache_key) {
        return Ok(PriceData {
            sqrt_price_x96: sqrt_price,
            block_number,
            timestamp,
            conversion_rates: *conversion_rates,
            fixed_rates: *fixed_rates,
        });
    }
    
    // Calculate if not cached
    let fixed_rates = FixedPoint::sqrt_price_to_rates(sqrt_price, weth_is_token0)
        .map_err(|_| PriceFeedError::InvalidPoolConfig)?;
    
    let conversion_rates = calculate_conversion_rates(sqrt_price, weth_is_token0);
    
    // Validate rates
    if conversion_rates.cbbtc_per_eth > 1.0 || conversion_rates.cbbtc_per_eth < 0.001 {
        error!(
            "Invalid conversion rate: cbBTC/ETH = {:.8}, sqrt_price = {}, weth_is_token0 = {}",
            conversion_rates.cbbtc_per_eth, sqrt_price, weth_is_token0
        );
        return Err(PriceFeedError::InvalidPoolConfig);
    }
    
    // Cache the calculation
    CONVERSION_CACHE.lock().put(cache_key, (fixed_rates, conversion_rates));
    
    Ok(PriceData {
        sqrt_price_x96: sqrt_price,
        block_number,
        timestamp,
        conversion_rates,
        fixed_rates,
    })
}

async fn fetch_price_via_rpc(config: &PoolConfig) -> Result<PriceData> {
    let conversion_rates = crate::quote::fetch_weth_cbbtc_conversion_rates(config.chain_id)
        .await
        .context(RpcFallbackSnafu)?;
    
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    
    let cbbtc_per_eth_fixed = U256::from((conversion_rates.cbbtc_per_eth * 1e18) as u128);
    let eth_per_cbbtc_fixed = U256::from((conversion_rates.eth_per_cbbtc * 1e18) as u128);
    
    let fixed_rates = FixedConversionRates {
        cbbtc_per_eth_fixed,
        eth_per_cbbtc_fixed,
    };
    
    info!(
        "Fetched price via RPC - cbBTC/ETH: {:.8}, ETH/cbBTC: {:.8}",
        conversion_rates.cbbtc_per_eth,
        conversion_rates.eth_per_cbbtc
    );
    
    Ok(PriceData {
        sqrt_price_x96: U160::ZERO,
        block_number: 0,
        timestamp,
        conversion_rates,
        fixed_rates,
    })
}

fn should_refresh_price(pool_address: &Address) -> bool {
    let cache = PRICE_CACHE.load();
    
    match cache.get(pool_address) {
        Some(price_data) => {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            now - price_data.timestamp > 300 // Refresh if older than 5 minutes
        }
        None => true,
    }
}

fn calculate_conversion_rates(sqrt_price_x96: U160, weth_is_token0: bool) -> ConversionRates {
    let p_sqrt = q64_96_to_float(sqrt_price_x96);
    let price_t1_in_t0 = p_sqrt * p_sqrt;
    
    if weth_is_token0 {
        let cbbtc_per_eth = price_t1_in_t0 * 10f64.powi(10); // Adjust for decimals (18 - 8)
        let eth_per_cbbtc = 1.0 / cbbtc_per_eth;
        ConversionRates { cbbtc_per_eth, eth_per_cbbtc }
    } else {
        let eth_per_cbbtc = price_t1_in_t0 / 10f64.powi(10);
        let cbbtc_per_eth = 1.0 / eth_per_cbbtc;
        ConversionRates { cbbtc_per_eth, eth_per_cbbtc }
    }
}