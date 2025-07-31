# TEE-OTC Development Guidelines

## Project Overview

TEE-OTC is a cross-chain OTC settlement system running in Trusted Execution Environments (TEEs). It enables trustless swaps between Bitcoin and EVM chains using TEE-secured wallets.

## Development Philosophy

- When modifying a component of the system, assume that the old version is completely obsolete and can be fully disregarded. It's never necessary to reason/build in "backwards compatibility" for example

## Recent Implementation Progress

### Swap State Machine (2025-07-22)

- Implemented proper state tracking with PostgreSQL enum type `swap_status`
- Added JSONB columns for rich deposit/settlement data tracking
- Created state transition methods with validation in `swap_transitions.rs`
- Updated all database queries to handle new schema
- Migration: `bin/otc-server/migrations/20250722212703_update_swaps_state_machine.sql`

### Completed Implementation (Sprint 3 - Completed 2025-07-22)

- **Market Maker Quote Validation**: WebSocket protocol for quote approval before swap creation
  - MM Registry service tracks active connections
  - Real-time quote validation with 5-second timeout
  - Market-maker binary with auto-accept/reject for testing
  - Full integration tests demonstrating the flow
  - See MARKET_MAKER_PLAN.md for implementation details

### Next Implementation Priority (Sprint 4)

- **Settlement execution**: Implement actual blockchain transaction sending
- **Refund execution**: Implement refund transaction logic
- **Chain operations**: Real blockchain interaction (currently mocked)

### Current Sprint Status

- Sprint 2 ✅ Complete: Swap state machine with monitoring service
- Sprint 3 ✅ Complete: Market Maker integration for quote validation
- Sprint 4 🔄 Next: Settlement & refund execution
- All tests passing with `make test-clean`

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
- Never put migrations at workspace root - they belong with the binary that uses them
- If a new binary needs database access, create its own `migrations/` directory within that binary's folder
- **Migration Strategy**: Combine migrations into one mega migration scoped by table (e.g., one migration for all swap table changes, one for all quote table changes)
- **Important**: We never have an existing database to migrate - migrations create fresh schemas

## Testing

- When running any test for the first time run:

```
make dev-db
```

- This command spins up a test database
  After this point, you can use normal cargo nextest to run your tests
