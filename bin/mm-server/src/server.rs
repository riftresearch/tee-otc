use crate::{
    Error, Result,
    price_feed::PriceFeedManager,
    settlement::SettlementManager,

};
use alloy::primitives::{Address, U256};
use clap::Parser;
use tokio::sync::mpsc;
use dashmap::DashMap;
use futures::{stream::StreamExt, SinkExt};
use otc_mm_protocol::{MMRequest, MMResponse, ProtocolMessage};
use otc_models::{ChainType, TokenIdentifier};
use parking_lot::RwLock;
use serde_json;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    time::interval,
    sync::Semaphore,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::Message,
};
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use chrono::{DateTime, Utc};




// Chain configuration
const BASE_CHAIN_ID: u64 = 8453;
const CHANNEL_SIZE: usize = 65536;
const METRICS_INTERVAL: Duration = Duration::from_secs(1);
const QUOTE_EXPIRY_SECONDS: i64 = 30;
const MESSAGE_DEDUP_WINDOW: Duration = Duration::from_secs(60);
const MAX_CONCURRENT_QUOTES: usize = 1000;

#[derive(Parser, Debug, Clone)]
#[command(name = "mm-server")]
pub struct Args {
    /// RFQ server WebSocket URL
    #[arg(long, default_value = "ws://127.0.0.1:3000/ws")]
    pub rfq_url: String,
    
    /// Spread in basis points (1 bp = 0.01%)
    #[arg(long, default_value = "30")]
    pub spread_bps: u32,
    
    /// Market maker name for identification
    #[arg(long, default_value = "default-mm")]
    pub mm_name: String,
    
    /// Enable settlement module
    #[arg(long, default_value = "false")]
    pub enable_settlement: bool,
    
    /// ETH private key for settlement
    #[arg(long, env = "MM_ETH_PRIVATE_KEY")]
    pub eth_private_key: Option<String>,
}

struct Metrics {
    quotes_requested: AtomicU64,
    quotes_generated: AtomicU64,
    quotes_failed: AtomicU64,
    quotes_expired: AtomicU64,
    messages_sent: AtomicU64,
    messages_received: AtomicU64,
    duplicate_messages: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    cache_evictions: AtomicU64,
    price_fetch_timeouts: AtomicU64,
}

impl Metrics {
    fn new() -> Self {
        Self {
            quotes_requested: AtomicU64::new(0),
            quotes_generated: AtomicU64::new(0),
            quotes_failed: AtomicU64::new(0),
            quotes_expired: AtomicU64::new(0),
            messages_sent: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
            duplicate_messages: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            cache_evictions: AtomicU64::new(0),
            price_fetch_timeouts: AtomicU64::new(0),
        }
    }
}

#[allow(dead_code)]
struct MessageDeduplicator {
    seen_messages: Arc<RwLock<HashMap<Uuid, Instant>>>,
    cleanup_interval: Duration,
}

impl MessageDeduplicator {
    fn new() -> Self {
        let dedup = Self {
            seen_messages: Arc::new(RwLock::new(HashMap::new())),
            cleanup_interval: Duration::from_secs(30),
        };
        
        // Start cleanup task
        let seen_messages = dedup.seen_messages.clone();
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                let now = Instant::now();
                seen_messages.write().retain(|_, timestamp| {
                    now.duration_since(*timestamp) < MESSAGE_DEDUP_WINDOW
                });
            }
        });
        
        dedup
    }
    
    fn is_duplicate(&self, request_id: Uuid) -> bool {
        let mut seen = self.seen_messages.write();
        if seen.contains_key(&request_id) {
            true
        } else {
            seen.insert(request_id, Instant::now());
            false
        }
    }
}



// Quote storage with expiry tracking
struct QuoteStore {
    quotes: Arc<DashMap<Uuid, StoredQuote>>,
    metrics: Arc<Metrics>,
}

struct StoredQuote {
    request_id: Uuid,
    from_amount: U256,
    to_amount: U256,
    expires_at: DateTime<Utc>,
    created_at: Instant,
}

impl QuoteStore {
    fn new(metrics: Arc<Metrics>) -> Self {
        let store = Self {
            quotes: Arc::new(DashMap::new()),
            metrics,
        };
        
        // Start cleanup task
        let quotes = store.quotes.clone();
        let metrics = store.metrics.clone();
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                let now = Utc::now();
                let mut expired_count = 0;
                
                quotes.retain(|_, quote| {
                    if quote.expires_at < now {
                        expired_count += 1;
                        false
                    } else {
                        true
                    }
                });
                
                if expired_count > 0 {
                    metrics.quotes_expired.fetch_add(expired_count, Ordering::Relaxed);
                }
            }
        });
        
        store
    }
    
    fn store_quote(&self, quote_id: Uuid, quote: StoredQuote) {
        self.quotes.insert(quote_id, quote);
    }
    
    fn is_valid(&self, quote_id: &Uuid) -> bool {
        if let Some(quote) = self.quotes.get(quote_id) {
            quote.expires_at > Utc::now()
        } else {
            false
        }
    }
}

pub struct QuoteEngine {
    price_feed_manager: Arc<PriceFeedManager>,
    spread_bps: u16,
    metrics: Arc<Metrics>,
    #[allow(dead_code)]
    settlement: Option<Arc<SettlementManager>>,
    quote_store: Arc<QuoteStore>,
    deduplicator: Arc<MessageDeduplicator>,
    quote_semaphore: Arc<Semaphore>,
}

impl QuoteEngine {
    pub fn new(spread_bps: u16, metrics: Arc<Metrics>, settlement: Option<Arc<SettlementManager>>, price_feed_manager: Arc<PriceFeedManager>) -> Self {
        Self {
            price_feed_manager,
            spread_bps,
            metrics: metrics.clone(),
            settlement,
            quote_store: Arc::new(QuoteStore::new(metrics.clone())),
            deduplicator: Arc::new(MessageDeduplicator::new()),
            quote_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_QUOTES)),
        }
    }
    
    pub async fn generate_quote(
        &self,
        request_id: Uuid,
        _from_chain: ChainType,
        from_token: TokenIdentifier,
        from_amount: U256,
        _to_chain: ChainType,
        to_token: TokenIdentifier,
    ) -> Result<MMResponse> {
        // Check for duplicate request
        if self.deduplicator.is_duplicate(request_id) {
            self.metrics.duplicate_messages.fetch_add(1, Ordering::Relaxed);
            return Err(Error::WebSocket { 
                message: "Duplicate request detected".to_string() 
            });
        }
        
        // Acquire semaphore to limit concurrent quotes
        let _permit = self.quote_semaphore.acquire().await
            .map_err(|_| Error::WebSocket { 
                message: "Quote generation capacity exceeded".to_string() 
            })?;
        
        self.metrics.quotes_requested.fetch_add(1, Ordering::Relaxed);
        
        // Get tokens
        let _from_addr = match &from_token {
            TokenIdentifier::Native => Address::ZERO,
            TokenIdentifier::Address(addr) => addr.parse::<Address>().unwrap_or(Address::ZERO),
        };
        let _to_addr = match &to_token {
            TokenIdentifier::Native => Address::ZERO,
            TokenIdentifier::Address(addr) => addr.parse::<Address>().unwrap_or(Address::ZERO),
        };
        
        // Get conversion rates from PriceFeedManager
        let conversion_rates = match self.price_feed_manager.get_fixed_rates(BASE_CHAIN_ID).await {
            Ok(rates) => {
                self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                rates
            }
            Err(e) => {
                self.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
                return Err(Error::WebSocket { 
                    message: format!("Price fetch error: {}", e) 
                });
            }
        };
        
        // Calculate output amount
        let base_to_amount = if from_token == TokenIdentifier::Native {
            conversion_rates.eth_to_cbbtc(from_amount)
                .map_err(|e| Error::WebSocket { message: format!("Conversion error: {}", e) })?
        } else {
            conversion_rates.cbbtc_to_eth(from_amount)
                .map_err(|e| Error::WebSocket { message: format!("Conversion error: {}", e) })?
        };
        
        // Apply spread
        let to_amount = self.apply_spread(base_to_amount, false);
        
        // Generate quote
        let quote_id = Uuid::new_v4();
        let expires_at = Utc::now() + chrono::Duration::seconds(QUOTE_EXPIRY_SECONDS);
        
        // Store quote for validation
        self.quote_store.store_quote(quote_id, StoredQuote {
            request_id,
            from_amount,
            to_amount,
            expires_at,
            created_at: Instant::now(),
        });
        
        self.metrics.quotes_generated.fetch_add(1, Ordering::Relaxed);
        
        info!("Generated quote: quote_id={}, to_amount={}, expires_at={}", quote_id, to_amount, expires_at);
        
        Ok(MMResponse::QuoteResponse {
            request_id,
            quote_id,
            to_amount,
            expires_at,
            timestamp: Utc::now(),
        })
    }
    
    fn apply_spread(&self, base_amount: U256, is_selling: bool) -> U256 {
        let spread_factor = if is_selling {
            10000u128 - self.spread_bps as u128
        } else {
            10000u128 - self.spread_bps as u128
        };
        (base_amount * U256::from(spread_factor)) / U256::from(10000u128)
    }
    
    pub fn validate_quote(&self, quote_id: &Uuid) -> bool {
        self.quote_store.is_valid(quote_id)
    }
}

struct MessageHandler {
    quote_engine: Arc<QuoteEngine>,
    outbound_tx: mpsc::Sender<Message>,
    metrics: Arc<Metrics>,
    sequence_tracker: Arc<RwLock<HashMap<Uuid, u64>>>,
}

impl MessageHandler {
    fn new(
        quote_engine: Arc<QuoteEngine>,
        outbound_tx: mpsc::Sender<Message>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            quote_engine,
            outbound_tx,
            metrics,
            sequence_tracker: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    async fn handle_message(&self, msg: Message) -> Result<()> {
        match msg {
            Message::Text(text) => {
                self.metrics.messages_received.fetch_add(1, Ordering::Relaxed);
                
                // Parse message
                let proto_msg: ProtocolMessage<MMRequest> = serde_json::from_str(&text)
                    .map_err(|e| Error::Serialization { source: e })?;
                
                // Check sequence number for ordering
                if let Some(expected_seq) = self.sequence_tracker.read().get(&proto_msg.payload.get_request_id()) {
                    if proto_msg.sequence <= *expected_seq {
                        warn!("Out of order or duplicate message detected");
                        return Ok(());
                    }
                }
                
                match proto_msg.payload {
                    MMRequest::GetQuote {
                        request_id,
                        from_chain,
                        from_token,
                        from_amount,
                        to_chain,
                        to_token,
                        ..
                    } => {
                        // Update sequence tracker
                        self.sequence_tracker.write().insert(request_id, proto_msg.sequence);
                        
                        // Generate quote asynchronously
                        let quote_result = self.quote_engine.generate_quote(
                            request_id,
                            from_chain,
                            from_token,
                            from_amount,
                            to_chain,
                            to_token,
                        ).await;
                        
                        match quote_result {
                            Ok(response) => {
                                let proto_response = ProtocolMessage {
                                    version: "1.0.0".to_string(),
                                    sequence: proto_msg.sequence + 1,
                                    payload: response,
                                };
                                
                                let json = serde_json::to_string(&proto_response)
                                    .map_err(|e| Error::Serialization { source: e })?;
                                
                                // Send response asynchronously
                                if let Err(e) = self.outbound_tx.send(Message::Text(json)).await {
                                    error!("Failed to send response: {:?}", e);
                                    return Err(Error::WebSocket { 
                                        message: "Failed to send response".to_string() 
                                    });
                                }
                                self.metrics.messages_sent.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(e) => {
                                error!("Failed to generate quote: {}", e);
                                self.quote_engine.metrics.quotes_failed.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    MMRequest::ValidateQuote { quote_id, .. } => {
                        // Check if quote is still valid
                        if !self.quote_engine.validate_quote(&quote_id) {
                            let response = MMResponse::QuoteValidated {
                                request_id: proto_msg.payload.get_request_id(),
                                quote_id,
                                accepted: false,
                                rejection_reason: Some("Quote expired".to_string()),
                                timestamp: Utc::now(),
                            };
                            
                            let proto_response = ProtocolMessage {
                                version: "1.0.0".to_string(),
                                sequence: proto_msg.sequence + 1,
                                payload: response,
                            };
                            
                            let json = serde_json::to_string(&proto_response)
                                .map_err(|e| Error::Serialization { source: e })?;
                                
                            let _ = self.outbound_tx.send(Message::Text(json)).await;
                        }
                    }
                    _ => {
                        info!("Received unknown message");
                    }
                }
            }
            Message::Close(_) => {
                info!("WebSocket closed");
                return Err(Error::WebSocket { message: "Connection closed".to_string() });
            }
            _ => {}
        }
        Ok(())
    }
}

trait MMRequestExt {
    fn get_request_id(&self) -> Uuid;
}

impl MMRequestExt for MMRequest {
    fn get_request_id(&self) -> Uuid {
        match self {
            MMRequest::GetQuote { request_id, .. } => *request_id,
            MMRequest::ValidateQuote { request_id, .. } => *request_id,
            MMRequest::UserDeposited { request_id, .. } => *request_id,
            MMRequest::UserDepositConfirmed { request_id, .. } => *request_id,
            MMRequest::SwapComplete { request_id, .. } => *request_id,
            MMRequest::Ping { request_id, .. } => *request_id,
        }
    }
}

pub struct MarketMaker {
    rfq_url: String,
    spread_bps: u16,
    metrics: Arc<Metrics>,
    running: Arc<AtomicBool>,
    mm_name: String,
    price_feed_manager: Arc<PriceFeedManager>,
}

impl MarketMaker {
    pub async fn new(rfq_url: String, spread_bps: u16, mm_name: String) -> Result<Self> {
        let price_feed_manager = Arc::new(
            PriceFeedManager::new().await
                .map_err(|e| Error::WebSocket { 
                    message: format!("Failed to create PriceFeedManager: {}", e) 
                })?
        );
        
        Ok(Self {
            rfq_url,
            spread_bps,
            metrics: Arc::new(Metrics::new()),
            running: Arc::new(AtomicBool::new(true)),
            mm_name,
            price_feed_manager,
        })
    }
    
    pub async fn run(&self) -> Result<()> {
        info!("Starting Market Maker: {} ({}bps spread)", self.mm_name, self.spread_bps);
        
        // Start metrics reporter
        let metrics = self.metrics.clone();
        let running = self.running.clone();
        tokio::spawn(async move {
            let mut interval = interval(METRICS_INTERVAL);
            while running.load(Ordering::Relaxed) {
                interval.tick().await;
                info!(
                    "Metrics - Quotes: {}/{} (failed: {}, expired: {}), Messages: {}/{} (dup: {}), Cache: {}/{} (evict: {}), Timeouts: {}",
                    metrics.quotes_generated.load(Ordering::Relaxed),
                    metrics.quotes_requested.load(Ordering::Relaxed),
                    metrics.quotes_failed.load(Ordering::Relaxed),
                    metrics.quotes_expired.load(Ordering::Relaxed),
                    metrics.messages_sent.load(Ordering::Relaxed),
                    metrics.messages_received.load(Ordering::Relaxed),
                    metrics.duplicate_messages.load(Ordering::Relaxed),
                    metrics.cache_hits.load(Ordering::Relaxed),
                    metrics.cache_misses.load(Ordering::Relaxed),
                    metrics.cache_evictions.load(Ordering::Relaxed),
                    metrics.price_fetch_timeouts.load(Ordering::Relaxed),
                );
            }
        });
        
        // Connect with retry
        loop {
            match self.connect_and_run().await {
                Ok(_) => {
                    info!("Connection closed normally");
                    if !self.running.load(Ordering::Relaxed) {
                        break;
                    }
                }
                Err(e) => {
                    error!("Connection error: {}", e);
                }
            }
            
            if !self.running.load(Ordering::Relaxed) {
                break;
            }
            
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        
        Ok(())
    }
    
    async fn connect_and_run(&self) -> Result<()> {
        let (ws_stream, _) = connect_async(&self.rfq_url).await
            .map_err(|e| Error::WebSocket { message: e.to_string() })?;
            
        info!("Connected to RFQ server at {}", self.rfq_url);
        
        // Create channels using tokio mpsc for proper async handling
        let (outbound_tx, mut outbound_rx) = mpsc::channel::<Message>(CHANNEL_SIZE);
        let (inbound_tx, mut inbound_rx) = mpsc::channel::<Message>(CHANNEL_SIZE);
        
        // Split WebSocket
        let (mut ws_sender, mut ws_receiver) = ws_stream.split();
        
        // WebSocket sender task
        let sender_handle = tokio::spawn(async move {
            while let Some(msg) = outbound_rx.recv().await {
                if ws_sender.send(msg).await.is_err() {
                    error!("Failed to send WebSocket message");
                    break;
                }
            }
        });
        
        // WebSocket receiver task
        let inbound_tx_clone = inbound_tx.clone();
        let receiver_handle = tokio::spawn(async move {
            let mut msg_count = 0;
            debug!("WebSocket receiver task started");
            while let Some(result) = ws_receiver.next().await {
                match result {
                    Ok(msg) => {
                        msg_count += 1;
                        debug!("WebSocket received message #{}: {:?}", msg_count, msg);

                        match inbound_tx_clone.send(msg).await {
                            Ok(_) => {
                                debug!("Message #{} queued successfully", msg_count);
                            }
                            Err(e) => {
                                error!("Failed to queue message #{} - receiver dropped: {:?}", msg_count, e);
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        error!("WebSocket receive error: {:?}", e);
                        break;
                    }
                }
            }
            info!("WebSocket receiver task ended after {} messages", msg_count);
        });
        
        // Create quote engine
        let quote_engine = Arc::new(QuoteEngine::new(
            self.spread_bps,
            self.metrics.clone(),
            None,
            self.price_feed_manager.clone(),
        ));
        
        // Create message handler
        let handler = Arc::new(MessageHandler::new(
            quote_engine,
            outbound_tx.clone(),
            self.metrics.clone(),
        ));
        
        // Process messages
        let running = self.running.clone();
        let worker_handle = tokio::spawn(async move {
            info!("Starting message processor");
            let mut processed_count = 0;
            let mut timeout_count = 0;
            loop {
                if !running.load(Ordering::Relaxed) {
                    break;
                }
                
                match tokio::time::timeout(
                    Duration::from_millis(100),
                    inbound_rx.recv()
                ).await {
                    Ok(Some(msg)) => {
                        processed_count += 1;
                        debug!("Processing message #{}", processed_count);
                        if let Err(e) = handler.handle_message(msg).await {
                            error!("Error handling message #{}: {}", processed_count, e);
                        }
                    }
                    Ok(None) => {
                        // Channel closed
                        info!("Inbound channel closed after {} messages", processed_count);
                        break;
                    }
                    Err(_) => {
                        timeout_count += 1;
                        if timeout_count % 100 == 0 {
                            debug!("Message processor: {} timeouts, {} messages processed", timeout_count, processed_count);
                        }
                        continue;
                    }
                }
            }
            info!("Message processor stopped");
        });
        
        // Wait for tasks
        let _ = tokio::join!(sender_handle, receiver_handle, worker_handle);
        
        Ok(())
    }
    

}

pub async fn run_mm(rfq_url: String, spread_bps: u16, mm_name: String) -> Result<()> {
    let mm = MarketMaker::new(rfq_url, spread_bps, mm_name).await?;
    mm.run().await
}