# Sauron Railway Deployment

This directory contains the operator workflow for running `sauron` on Railway
as a background worker deployed directly from the `tee-otc` GitHub repo.

For the private Bitcoin connectivity plan that brokers `bitcoind` RPC and ZMQ
into Railway through a `rathole` service, see `RATHOLE_TOPOLOGY.md` in this
directory.

## Service Topology

Provision or reuse these Railway services:

1. `tee-otc-primary-tunnel`
2. `tee-otc-replica-db`
3. `sauron-worker`
4. `sauron-bitcoin-rathole-broker` (when brokering private Bitcoin RPC + ZMQ)

Responsibilities:

- `tee-otc-primary-tunnel` exposes the Phala-hosted tee-otc primary over
  Railway private networking.
- `tee-otc-replica-db` is the shared tee-otc logical subscriber already used on
  Railway.
- `sauron-worker` runs the `sauron` binary, watches the replica, ingests chain
  data, and submits candidate deposits back to OTC.
- `sauron-bitcoin-rathole-broker` terminates the public `rathole` client
  connection from the isolated Bitcoin host and re-exposes brokered Bitcoin
  RPC + ZMQ ports over Railway private networking.

If you already run `tee-otc-primary-tunnel` for analytics, reuse it. In the
shared periphery checkout, the existing tunnel assets live at
`/home/alpinevm/Development/rift/periphery/sdk/deploy/railway/analytics/tee-otc-primary-tunnel`.

No Sauron-specific replica is required. Reuse the existing `tee-otc-replica-db`
service. If that shared replica needs to be created or rebuilt, use the
existing bootstrap assets in the sibling SDK checkout at
`/home/alpinevm/Development/rift/periphery/sdk/deploy/railway/analytics/tee-otc-replica-db`.

## Deploy The Worker

Create `sauron-worker` as a repo-backed Railway service connected to the
`tee-otc` GitHub repository. Point it at the same branch/environment you want
Sauron to ship from.

Set this required Railway service variable on `sauron-worker`:

- `RAILWAY_DOCKERFILE_PATH=etc/Dockerfile.sauron`

Required runtime variables:

- `OTC_REPLICA_DATABASE_URL`
- `OTC_INTERNAL_BASE_URL`
- `OTC_DETECTOR_API_ID`
- `OTC_DETECTOR_API_SECRET`
- `ELECTRUM_HTTP_SERVER_URL`
- `BITCOIN_RPC_URL`
- `BITCOIN_RPC_AUTH`
- `BITCOIN_ZMQ_RAWTX_ENDPOINT`
- `BITCOIN_ZMQ_SEQUENCE_ENDPOINT`
- `EVM_RPC_URL`
- `ETHEREUM_TOKEN_INDEXER_URL`
- `ETHEREUM_ALLOWED_TOKEN`
- `BASE_RPC_URL`
- `BASE_TOKEN_INDEXER_URL`
- `BASE_ALLOWED_TOKEN`

Recommended defaults:

- `RUST_LOG=info`
- `OTC_REPLICA_DATABASE_NAME=otc_db`
- `OTC_REPLICA_NOTIFICATION_CHANNEL=sauron_watch_set_changed`
- `SAURON_RECONCILE_INTERVAL_SECONDS=60`
- `SAURON_BITCOIN_SCAN_INTERVAL_SECONDS=15`
- `SAURON_BITCOIN_INDEXED_LOOKUP_CONCURRENCY=32`
- `SAURON_EVM_INDEXED_LOOKUP_CONCURRENCY=8`

Operational notes:

- `sauron` applies the SQL in `bin/sauron/migrations-replica` on startup, so
  `OTC_REPLICA_DATABASE_URL` must use a credential that can create functions,
  triggers, and entries in `_sqlx_migrations` on the shared subscriber
  database. Point it at the shared `tee-otc-replica-db` service, preferably via
  that service's internal `DATABASE_URL`.
- Reusing `tee-otc-replica-db` is safe: the Sauron trigger and notification
  objects use their own names and can coexist with the existing analytics
  trigger on the same replica.
- Railway does not need a public domain for `sauron-worker`; this service can
  run purely as a background worker.
- `OTC_INTERNAL_BASE_URL` should point at the deployed OTC HTTP base URL that
  serves `POST /api/v1/swaps/:swap_id/deposit-observation`. If OTC still runs
  outside Railway, use the operator-reachable Phala/ingress URL.
- Current OTC builds authenticate trusted detector submissions using the single
  detector record compiled into `crates/otc-auth/src/api_keys.rs`. The
  `OTC_DETECTOR_API_ID` and `OTC_DETECTOR_API_SECRET` values on Railway must
  match the OTC image you deploy.
- `sauron` now supports a mixed Bitcoin mode: Esplora remains the indexed
  lookup and fallback path, while `BITCOIN_RPC_URL` lets block/tip reads prefer
  Bitcoin Core and `BITCOIN_ZMQ_RAWTX_ENDPOINT` enables live mempool deposit
  detection. When both RPC and ZMQ are configured, Sauron also keeps a local
  in-process Bitcoin mempool mirror by ingesting `rawtx` updates and
  refreshing from `getrawmempool` on tip changes. If you broker private Bitcoin
  connectivity through `rathole`, wire all four Bitcoin vars to the
  broker endpoints and use fixed Bitcoin RPC credentials rather than a rotating
  `.cookie` value. Prefer server-side `rpcauth` on the Bitcoin host and keep
  the client-side `user:pass` in Railway.
- `BITCOIN_RPC_AUTH` is now mandatory. Use the literal value `none` if the
  brokered RPC endpoint is unauthenticated or already embeds credentials in the
  URL.

## Deploy The Broker

Create `sauron-bitcoin-rathole-broker` as a repo-backed Railway service
connected to the same `tee-otc` GitHub repository.

Set this required Railway service variable on the broker:

- `RAILWAY_DOCKERFILE_PATH=etc/Dockerfile.rathole-broker`

Required runtime variables:

- `RATHOLE_BITCOIN_RPC_TOKEN`
- `RATHOLE_ZMQ_RAWTX_TOKEN`
- `RATHOLE_ZMQ_SEQUENCE_TOKEN`

Recommended runtime variables:

- `RUST_LOG=info`
- `RATHOLE_CONTROL_PORT=2333`
- `RATHOLE_TRANSPORT_TYPE=websocket`
- `RATHOLE_WEBSOCKET_TLS=false`
- `RATHOLE_BITCOIN_RPC_BIND_PORT=40031`
- `RATHOLE_ZMQ_RAWTX_BIND_PORT=40032`
- `RATHOLE_ZMQ_SEQUENCE_BIND_PORT=40033`

Operational notes:

- Railway should expose exactly one public TCP proxy for the service control
  plane. On Railway, the simplest approach is to use the normal public domain
  with `RATHOLE_TRANSPORT_TYPE=websocket`; the broker then binds its websocket
  listener to the Railway-provided `PORT`.
- The broker also listens on the three internal-only bind ports above. Do not
  create public proxies for those service ports.
- Point the isolated Bitcoin host's `rathole client` at the broker's public
  Railway domain on port `443`, for example
  `sauron-bitcoin-rathole-broker-production.up.railway.app:443`, and use
  websocket transport with client-side TLS enabled.
- Point `sauron-worker` at the broker's internal Railway hostname:
  `rathole-broker.railway.internal` or the actual service DNS name Railway
  assigns.

## Smoke Checks

Expected startup log lines:

- `Running Sauron replica migrations...`
- `Sauron replica migrations complete`
- `Sauron startup completed`

Recommended checks:

```bash
railway run -s tee-otc-replica-db -- psql "$DATABASE_URL" -d otc_db \
  -c "select count(*) from public.swaps;"
```

```bash
railway logs -s sauron-worker
```
