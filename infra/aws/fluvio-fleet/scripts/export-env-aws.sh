#!/usr/bin/env bash
# Source after instances.env: sets PHOTON_FLUVIO_* for multi-EC2 cluster.
set -euo pipefail

# AWS/CI test transport key (not for production).
export PHOTON_TRANSPORT_KEY="${PHOTON_TRANSPORT_KEY:-cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "${INSTANCES_ENV:-$ROOT/instances.env}"

MODE="${1:-cluster}"

export PHOTON_FLUVIO_TOPIC_PREFIX="${PHOTON_FLUVIO_TOPIC_PREFIX:-photon}"
export PHOTON_FLUVIO_RETENTION="${PHOTON_FLUVIO_RETENTION:-15m}"
export PHOTON_FLUVIO_REPLAY_CURSOR="${PHOTON_FLUVIO_REPLAY_CURSOR:-stream_seq}"
export PHOTON_FLUVIO_SYNC_ACK="${PHOTON_FLUVIO_SYNC_ACK:-1}"
export PHOTON_FLUVIO_MAX_INFLIGHT="${PHOTON_FLUVIO_MAX_INFLIGHT:-256}"

ip() {
  local base="$1"
  if [[ "${PHOTON_AWS_USE_PUBLIC_IPS:-0}" == "1" ]]; then
    local key="${base}_PUBLIC_IP"
    echo "${!key}"
  else
    local key="${base}_IP"
    echo "${!key}"
  fi
}

# Clients always connect to SC endpoint.
export PHOTON_FLUVIO_ENDPOINT="$(ip BROKER_SINGLE):9103"
export PHOTON_FLUVIO_REPLICAS="${PHOTON_FLUVIO_REPLICAS:-1}"

case "$MODE" in
  single|n2|cluster4|cluster) ;;
  *) echo "unknown mode: $MODE" >&2; exit 1 ;;
esac

echo "PHOTON_FLUVIO_ENDPOINT=${PHOTON_FLUVIO_ENDPOINT}"
echo "PHOTON_FLUVIO_REPLICAS=${PHOTON_FLUVIO_REPLICAS}"
