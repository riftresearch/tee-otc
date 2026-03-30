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
private_bind_host="${RATHOLE_PRIVATE_BIND_HOST:-0.0.0.0}"
# Keep the IPv6 bridge pointed at loopback even when rathole binds 0.0.0.0.
bridge_target_host="127.0.0.1"

start_ipv6_bridge() {
  local port="$1"
  socat \
    "TCP6-LISTEN:${port},bind=[::],fork,reuseaddr,ipv6only=1" \
    "TCP4:${bridge_target_host}:${port}" &
}

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
bind_addr = "${private_bind_host}:${rpc_bind_port}"
token = "${RATHOLE_BITCOIN_RPC_TOKEN}"

[server.services.zmq_rawtx]
bind_addr = "${private_bind_host}:${rawtx_bind_port}"
token = "${RATHOLE_ZMQ_RAWTX_TOKEN}"

[server.services.zmq_sequence]
bind_addr = "${private_bind_host}:${sequence_bind_port}"
token = "${RATHOLE_ZMQ_SEQUENCE_TOKEN}"
EOF

echo "Starting rathole broker"
echo "  control port: ${control_port}"
echo "  transport: ${transport_type}"
echo "  websocket tls: ${websocket_tls}"
echo "  private bind host: ${private_bind_host}"
echo "  bridge target host: ${bridge_target_host}"
echo "  bitcoin_rpc bind port: ${rpc_bind_port}"
echo "  zmq_rawtx bind port: ${rawtx_bind_port}"
echo "  zmq_sequence bind port: ${sequence_bind_port}"

start_ipv6_bridge "${rpc_bind_port}"
start_ipv6_bridge "${rawtx_bind_port}"
start_ipv6_bridge "${sequence_bind_port}"

exec /usr/local/bin/rathole --server /etc/rathole/server.toml
