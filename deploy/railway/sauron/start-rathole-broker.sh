#!/usr/bin/env bash
set -euo pipefail

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "Missing required environment variable: $name" >&2
    exit 1
  fi
}

require_env "RATHOLE_BITCOIN_RPC_TOKEN"
require_env "RATHOLE_ZMQ_RAWTX_TOKEN"
require_env "RATHOLE_ZMQ_SEQUENCE_TOKEN"

control_port="${RATHOLE_CONTROL_PORT:-${PORT:-2333}}"
transport_type="${RATHOLE_TRANSPORT_TYPE:-websocket}"
rpc_bind_port="${RATHOLE_BITCOIN_RPC_BIND_PORT:-40031}"
rawtx_bind_port="${RATHOLE_ZMQ_RAWTX_BIND_PORT:-40032}"
sequence_bind_port="${RATHOLE_ZMQ_SEQUENCE_BIND_PORT:-40033}"
websocket_tls="${RATHOLE_WEBSOCKET_TLS:-false}"

mkdir -p /etc/rathole

cat > /etc/rathole/server.toml <<EOF
[server]
bind_addr = "0.0.0.0:${control_port}"

[server.transport]
type = "${transport_type}"

[server.transport.noise]

[server.transport.websocket]
tls = ${websocket_tls}

[server.services.bitcoin_rpc]
bind_addr = "0.0.0.0:${rpc_bind_port}"
token = "${RATHOLE_BITCOIN_RPC_TOKEN}"

[server.services.zmq_rawtx]
bind_addr = "0.0.0.0:${rawtx_bind_port}"
token = "${RATHOLE_ZMQ_RAWTX_TOKEN}"

[server.services.zmq_sequence]
bind_addr = "0.0.0.0:${sequence_bind_port}"
token = "${RATHOLE_ZMQ_SEQUENCE_TOKEN}"
EOF

echo "Starting rathole broker"
echo "  control port: ${control_port}"
echo "  transport: ${transport_type}"
echo "  websocket tls: ${websocket_tls}"
echo "  bitcoin_rpc bind port: ${rpc_bind_port}"
echo "  zmq_rawtx bind port: ${rawtx_bind_port}"
echo "  zmq_sequence bind port: ${sequence_bind_port}"

exec /usr/local/bin/rathole --server /etc/rathole/server.toml
