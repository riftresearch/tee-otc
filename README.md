# tee-otc
Cross-chain OTC swap desk put inside a TEE

## Components

- `otc-server` - Server for deposit wallet creation and handling the full swap lifecycle, runs in a TEE
- `market-maker` - Demo market making bot that responds to RFQs and fills orders
- `rfq-server` - Offchain Server that acts as an entrypoint for connecting market makers to users sending RFQs
- Bitcoin full node for Bitcoin state validation
- Helios light client for EVM chain state validation

## Prerequisites

- rust toolchain
- nextest
- orbstack/docker desktop
- docker cli
  // TODO: Links

## Development Workflow

1. **Build the project**:

   ```bash
   cargo build
   ```

2. **Run tests**:
   ```bash
   make test-clean
   ```
