# TEE-OTC Development Guidelines

## Project Overview

TEE-OTC is a cross-chain OTC settlement system running in Trusted Execution Environments (TEEs). It enables trustless swaps between Bitcoin and EVM chains using TEE-secured wallets.

## Technology Preferences

### EVM Operations

- **Use Alloy for all EVM operations** - Alloy is the preferred Ethereum library for this project
- Do not use ethers-rs or web3 - use Alloy exclusively for consistency

### Error Handling

- Use `snafu` for error handling across all crates
- Avoid `thiserror` and `anyhow`

### Testing

- Run lint and typecheck commands when code changes are made
- Verify builds with `cargo build` after significant changes

## Architecture

- The project uses a workspace structure with shared crates
- `otc-models` crate contains all shared domain types
- Chain-specific implementations should be feature-gated
- Market maker protocol is separated for reusability

## Database

- PostgreSQL with SQLx for async operations
- Schema is created on first run if it doesn't exist
- **Each binary maintains its own migrations** - Database migrations are stored within each binary's directory (e.g., `bin/otc-server/migrations/`)
- Never put migrations at the workspace root - they belong with the binary that uses them
- If a new binary needs database access, create its own `migrations/` directory within that binary's folder
