# tee-otc
Cross-chain OTC swap desk put inside a TEE

## Components

- `otc-server` - Server for deposit wallet creation and handling the full swap lifecycle, runs in a TEE
- `market-maker` - Demo market making bot that responds to RFQs and fills orders
- `rfq-server` - Server that acts as an hub for connecting market makers to users sending RFQs
- Bitcoin full node for Bitcoin state validation
- Helios light client for EVM chain state validation

## Prerequisites

- [Rust](https://www.rust-lang.org/tools/install)
- [cargo-nextest](https://nexte.st/docs/installation/pre-built-binaries/)
- [Docker](https://www.docker.com/get-started/)
- [pnpm](https://pnpm.io/installation)
- [Foundry](https://getfoundry.sh/introduction/installation/)

## Tests

On your first run:
```bash
cd evm-token-indexer && pnpm i && cd ..
make cache-devnet
```

Then on subsequent runs, just run:
```bash
make test
```
