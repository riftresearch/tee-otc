# EVM Token Indexer Client

A strongly-typed Rust client for interacting with the EVM Token Indexer API.

## Usage

```rust
use alloy::primitives::address;
use evm_token_indexer_client::TokenIndexerClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create client
    let client = TokenIndexerClient::new("http://localhost:42069")?;
    
    // Get balance
    let address = address!("0xbAB0A8E7e9d002394Ac0AF1492d68cAF87cF910E");
    let balances = client.get_balance(address).await?;
    
    // Get transfers
    let transfers = client.get_transfers_to(address, Some(1), None).await?;
    
    Ok(())
}
```

## API Methods

- `get_table_counts()` - Get counts of accounts and transfer events
- `get_balance(address)` - Get balance for a specific address
- `get_transfers_to(address, page, min_amount)` - Get paginated transfers to an address with optional amount filter

## Running the Example

```bash
cargo run --example basic_usage
```