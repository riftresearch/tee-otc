use alloy::primitives::U256;
use chrono::{Duration, Utc};
use otc_chains::{bitcoin::BitcoinChain, ethereum::EthereumChain, ChainRegistry};
use otc_models::{ChainType, Currency, Quote, TokenIdentifier};
use otc_server::api::swaps::CreateSwapRequest;
use otc_server::config::Settings;
use sqlx::PgPool;
use std::sync::Arc;
use uuid::Uuid;

#[sqlx::test]
async fn test_create_swap_success(pool: PgPool) -> sqlx::Result<()> {
    // Setup database
    let db = otc_server::db::Database::from_pool(pool.clone()).await.unwrap();
    
    // Create a test quote first
    let quote = Quote {
        id: Uuid::new_v4(),
        from: Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            amount: U256::from(100_000_000u64), // 1 BTC
            decimals: 8,
        },
        to: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::Native,
            amount: U256::from(15u64) * U256::from(10u64).pow(U256::from(18)), // 15 ETH
            decimals: 18,
        },
        market_maker_identifier: "test-mm".to_string(),
        expires_at: Utc::now() + Duration::hours(1),
        created_at: Utc::now(),
    };
    
    // Save the quote
    db.quotes().create(&quote).await.unwrap();
    
    // Create swap request
    let request = CreateSwapRequest {
        quote_id: quote.id,
        market_maker_identifier: "test-mm".to_string(),
        user_destination_address: "0x1234567890123456789012345678901234567890".to_string(),
        user_refund_address: "bc1quser1234567890".to_string(),
    };
    
    // Setup test dependencies
    std::env::set_var("OTC_MASTER_KEY", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
    let settings = Arc::new(Settings::load().unwrap());
    
    let mut chain_registry = ChainRegistry::new();
    
    // Register test chains
    let bitcoin_chain = BitcoinChain::new(
        "http://localhost:8332",
        bitcoincore_rpc::Auth::UserPass("user".to_string(), "pass".to_string()),
        bitcoin::Network::Testnet,
    ).unwrap();
    chain_registry.register(ChainType::Bitcoin, Arc::new(bitcoin_chain));
    
    let ethereum_chain = EthereumChain::new(
        "http://localhost:8545",
        1,
    ).await.unwrap();
    chain_registry.register(ChainType::Ethereum, Arc::new(ethereum_chain));
    
    let chain_registry = Arc::new(chain_registry);
    let swap_manager = otc_server::services::SwapManager::new(db.clone(), settings, chain_registry);
    
    let response = swap_manager.create_swap(request).await.unwrap();
    
    // Verify response
    assert_eq!(response.deposit_chain, "Bitcoin");
    assert_eq!(response.expected_amount, U256::from(100_000_000u64));
    assert_eq!(response.decimals, 8);
    assert_eq!(response.token, "Native");
    assert!(!response.deposit_address.is_empty());
    
    // Verify swap was created in database
    let swap = db.swaps().get(response.swap_id).await.unwrap();
    assert_eq!(swap.quote_id, quote.id);
    assert_eq!(swap.market_maker, "test-mm");
    assert_eq!(swap.user_destination_address, "0x1234567890123456789012345678901234567890");
    assert_eq!(swap.user_refund_address, "bc1quser1234567890");
    assert_eq!(format!("{:?}", swap.status), "WaitingUserDeposit");
    
    Ok(())
}

#[sqlx::test]
async fn test_create_swap_expired_quote(pool: PgPool) -> sqlx::Result<()> {
    let db = otc_server::db::Database::from_pool(pool.clone()).await.unwrap();
    
    // Create an expired quote
    let quote = Quote {
        id: Uuid::new_v4(),
        from: Currency {
            chain: ChainType::Bitcoin,
            token: TokenIdentifier::Native,
            amount: U256::from(100_000_000u64),
            decimals: 8,
        },
        to: Currency {
            chain: ChainType::Ethereum,
            token: TokenIdentifier::Native,
            amount: U256::from(15u64) * U256::from(10u64).pow(U256::from(18)),
            decimals: 18,
        },
        market_maker_identifier: "test-mm".to_string(),
        expires_at: Utc::now() - Duration::hours(1), // Already expired
        created_at: Utc::now() - Duration::hours(2),
    };
    
    db.quotes().create(&quote).await.unwrap();
    
    let request = CreateSwapRequest {
        quote_id: quote.id,
        market_maker_identifier: "test-mm".to_string(),
        user_destination_address: "0x1234567890123456789012345678901234567890".to_string(),
        user_refund_address: "bc1quser1234567890".to_string(),
    };
    
    // Setup test dependencies
    std::env::set_var("OTC_MASTER_KEY", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
    let settings = Arc::new(Settings::load().unwrap());
    
    let mut chain_registry = ChainRegistry::new();
    let bitcoin_chain = BitcoinChain::new(
        "http://localhost:8332",
        bitcoincore_rpc::Auth::UserPass("user".to_string(), "pass".to_string()),
        bitcoin::Network::Testnet,
    ).unwrap();
    chain_registry.register(ChainType::Bitcoin, Arc::new(bitcoin_chain));
    
    let ethereum_chain = EthereumChain::new(
        "http://localhost:8545",
        1,
    ).await.unwrap();
    chain_registry.register(ChainType::Ethereum, Arc::new(ethereum_chain));
    
    let chain_registry = Arc::new(chain_registry);
    let swap_manager = otc_server::services::SwapManager::new(db.clone(), settings, chain_registry);
    
    // Should fail with QuoteExpired error
    let result = swap_manager.create_swap(request).await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        otc_server::services::swap_manager::SwapError::QuoteExpired
    ));
    
    Ok(())
}