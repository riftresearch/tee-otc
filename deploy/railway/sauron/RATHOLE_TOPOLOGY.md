# Sauron Bitcoin Transport Via Rathole

This document captures the exact `rathole` topology discussed for brokering a
private `bitcoind` RPC endpoint and Bitcoin Core ZMQ feeds from an isolated
server into Railway so `sauron` can consume them over Railway private
networking.

## Goal

Keep the Bitcoin host private.

- `bitcoind` stays bound to loopback on the isolated host.
- No public Bitcoin RPC port.
- No public Bitcoin ZMQ ports.
- `sauron-worker` reaches all three feeds over Railway private networking.
- Authentication is handled by `rathole`, not raw SSH keys.

## Topology

Services and hosts:

1. isolated Bitcoin host
2. Railway service `rathole-broker`
3. Railway service `sauron-worker`

Traffic shape:

1. `bitcoind` publishes locally on the isolated host:
   - RPC on `127.0.0.1:8332`
   - ZMQ rawtx on `127.0.0.1:28332`
   - ZMQ sequence on `127.0.0.1:28333`
2. `rathole client` on the isolated host dials outbound to the Railway
   `rathole-broker` control port.
3. `rathole-broker` exposes three server-side bind ports:
   - internal Railway port `40031` -> Bitcoin RPC
   - internal Railway port `40032` -> Bitcoin ZMQ rawtx
   - internal Railway port `40033` -> Bitcoin ZMQ sequence
4. `sauron-worker` connects to:
   - `http://rathole-broker.railway.internal:40031`
   - `tcp://rathole-broker.railway.internal:40032`
   - `tcp://rathole-broker.railway.internal:40033`

Important detail:

- No `rathole client` is needed on the Sauron side. `sauron-worker` talks
  directly to the `rathole` server over Railway private networking.

## Railway Layout

`rathole-broker` should expose exactly one public port:

- `2333` = `rathole` control plane

`rathole-broker` should also listen on internal-only ports:

- `40031` = Bitcoin RPC
- `40032` = Bitcoin ZMQ rawtx
- `40033` = Bitcoin ZMQ sequence

Operational guidance:

- Create a public Railway TCP proxy only for `2333`.
- Do not create public proxies for `40031`, `40032`, or `40033`.
- Bind the forwarded service ports to `0.0.0.0`, not `127.0.0.1`, inside the
  `rathole-broker` container so sibling Railway services can reach them over
  private networking.

## Bitcoin Host Config

`bitcoin.conf`:

```conf
server=1
rpcbind=127.0.0.1
rpcallowip=127.0.0.1
zmqpubrawtx=tcp://127.0.0.1:28332
zmqpubsequence=tcp://127.0.0.1:28333
```

The Bitcoin host should run `rathole client` as a long-lived service, ideally
under `systemd`.

If the host uses the repository's Docker Compose stack at
`etc/compose.electrs.yml`, there is now an optional `rathole-client` service
behind the Compose profile `rathole`. Enable it with:

```bash
docker compose -f etc/compose.electrs.yml --profile rathole up -d
```

That profile requires these environment variables:

- `RATHOLE_REMOTE_ADDR`
- `RATHOLE_BITCOIN_RPC_TOKEN`
- `RATHOLE_ZMQ_RAWTX_TOKEN`
- `RATHOLE_ZMQ_SEQUENCE_TOKEN`

## Rathole Server Config

Suggested `server.toml`:

```toml
[server]
bind_addr = "0.0.0.0:2333"

[server.transport]
type = "noise"

[server.services.bitcoin_rpc]
bind_addr = "0.0.0.0:40031"
token = "replace-with-long-random-rpc-token"

[server.services.zmq_rawtx]
bind_addr = "0.0.0.0:40032"
token = "replace-with-long-random-rawtx-token"

[server.services.zmq_sequence]
bind_addr = "0.0.0.0:40033"
token = "replace-with-long-random-sequence-token"
```

Notes:

- Per-service tokens are preferred over a single `default_token` so RPC and ZMQ
  can be rotated independently.
- `noise` avoids certificate management while still giving encrypted transport.
- If the deployment environment makes raw TCP awkward, revisit `rathole`
  websocket transport later.

## Rathole Client Config

Suggested `client.toml` on the isolated Bitcoin host:

```toml
[client]
remote_addr = "replace-with-rathole-broker-public-host:2333"

[client.transport]
type = "noise"

[client.services.bitcoin_rpc]
local_addr = "127.0.0.1:8332"
token = "replace-with-long-random-rpc-token"

[client.services.zmq_rawtx]
local_addr = "127.0.0.1:28332"
token = "replace-with-long-random-rawtx-token"

[client.services.zmq_sequence]
local_addr = "127.0.0.1:28333"
token = "replace-with-long-random-sequence-token"
```

## Sauron Wiring

`sauron` now supports direct Bitcoin Core RPC and ZMQ alongside the existing
Esplora path. The Railway-side environment should look like:

```env
BITCOIN_RPC_URL=http://rathole-broker.railway.internal:40031
BITCOIN_RPC_AUTH=user:pass-or-none
BITCOIN_ZMQ_RAWTX_ENDPOINT=tcp://rathole-broker.railway.internal:40032
BITCOIN_ZMQ_SEQUENCE_ENDPOINT=tcp://rathole-broker.railway.internal:40033
ELECTRUM_HTTP_SERVER_URL=https://your-esplora-http-endpoint
```

Behavior in this mixed mode:

- Esplora stays enabled for indexed lookups and fallback block reads.
- Bitcoin RPC is preferred for tip and block queries when available.
- ZMQ rawtx feeds live mempool detections.
- ZMQ sequence is used for gap detection and re-sync triggers.

## Railway Service Behavior

`rathole-broker` can be its own small Railway service. `sauron-worker` remains
an ordinary background worker.

Recommended behavior:

- keep `rathole-broker` stateless
- build the config file from Railway env vars at container startup
- use Railway private networking for all consumer traffic
- keep all forwarded service ports internal-only

## Failure Model

`rathole` solves private connectivity and service authentication. It does not
add replay or durability to Bitcoin Core ZMQ.

Design implications:

- if the tunnel drops, ZMQ messages can be missed
- the Sauron-side ZMQ consumer still needs sequence-gap detection
- the Sauron-side Bitcoin integration still needs a backfill/resync path
- brokering RPC through the same `rathole` instance is useful because it gives
  the consumer an immediate recovery path after any ZMQ gap

## Why This Topology

This layout keeps the trust boundaries simple:

- the isolated Bitcoin host makes outbound connections only
- Railway exposes one broker control port publicly
- all forwarded Bitcoin interfaces remain private to the Railway project
- `sauron-worker` stays a normal Railway consumer with no tunnel client sidecar

This is the recommended `rathole` plan for `bitcoind` -> Railway private
networking -> `sauron-worker`.
