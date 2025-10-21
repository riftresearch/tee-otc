# tee-otc
Cross-chain OTC swap desk put inside a TEE

## Components

- `otc-server` - Server for deposit wallet creation and handling the full swap lifecycle, runs in a TEE
- `market-maker` - Demo market making bot that responds to RFQs and fills orders
- `rfq-server` - Server that acts as an hub for connecting market makers to users sending RFQs
- Bitcoin full node for Bitcoin state validation
- Reth archive node Ethereum chain state validation

## Prerequisites

- [Docker](https://www.docker.com/get-started/)
- [Rust](https://www.rust-lang.org/tools/install)
- [Foundry](https://getfoundry.sh/introduction/installation/)
- [cargo-nextest](https://nexte.st/docs/installation/pre-built-binaries/)
- [pnpm](https://pnpm.io/installation)
- [just](https://just.systems/man/en/packages.html)

## Tests

On your first run:
```bash
cd evm-token-indexer && pnpm i && cd ..
just cache-devnet
```

Then on subsequent runs, just run:
```bash
just test
```
