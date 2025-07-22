# tee-otc

Cross-chain OTC swaps secured by Trusted Execution Environments (TEEs).

## Components

- `otc-server` - Rust WebSocket server for deposit wallet creation and handling the full swap lifecycle, runs in a TEE
- Bitcoin full node integration
- Helios light client for Ethereum verification
- TEE attestation and secure key management

## Prerequisites

- rust toolchain
- nextest
- orbstack/docker desktop
- docker cli
  // TODO: Links

## Development Workflow

1. **Start Development Database**:

   _In a seperate terminal_

   ```bash
   docker compose -f compose.test-db.yml up
   ```

   This will automatically:

   - Create the `otc_dev` database
   - Apply the schema (embedded in compose file)

   To check logs: `docker compose -f compose.test-db.yml logs`
   To stop: `docker compose -f compose.test-db.yml down`

2. **Build the project**:

   ```bash
   cargo build
   ```

3. **Run tests**:
   ```bash
   make test-clean
   ```
