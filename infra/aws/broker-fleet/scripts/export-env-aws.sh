#!/usr/bin/env bash
# Source after instances.env: sets PHOTON_NATS_* for multi-EC2 cluster or single broker.
set -euo pipefail

# AWS/CI test transport key (not for production).
export PHOTON_TRANSPORT_KEY="${PHOTON_TRANSPORT_KEY:-cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "${INSTANCES_ENV:-$ROOT/instances.env}"

MODE="${1:-cluster}"

export PHOTON_NATS_STREAM="${PHOTON_NATS_STREAM:-photon}"
export PHOTON_NATS_RETENTION="${PHOTON_NATS_RETENTION:-15m}"

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

if [[ "$MODE" == "single" ]]; then
  export PHOTON_NATS_URL="nats://$(ip BROKER_SINGLE):4222"
  export PHOTON_NATS_REPLICAS="${PHOTON_NATS_REPLICAS:-1}"
elif [[ "$MODE" == "n2" ]]; then
  export PHOTON_NATS_URL="nats://$(ip BROKER_1):4222,nats://$(ip BROKER_2):4222"
  export PHOTON_NATS_REPLICAS="${PHOTON_NATS_REPLICAS:-2}"
elif [[ "$MODE" == "cluster4" ]]; then
  export PHOTON_NATS_URL="nats://$(ip BROKER_1):4222,nats://$(ip BROKER_2):4222,nats://$(ip BROKER_3):4222,nats://$(ip BROKER_SINGLE):4222"
  export PHOTON_NATS_REPLICAS="${PHOTON_NATS_REPLICAS:-4}"
else
  export PHOTON_NATS_URL="nats://$(ip BROKER_1):4222,nats://$(ip BROKER_2):4222,nats://$(ip BROKER_3):4222"
  export PHOTON_NATS_REPLICAS="${PHOTON_NATS_REPLICAS:-3}"
  export PHOTON_NATS_STREAM_SHARDS="${PHOTON_NATS_STREAM_SHARDS:-4}"
fi

echo "PHOTON_NATS_URL=${PHOTON_NATS_URL}"
echo "PHOTON_NATS_REPLICAS=${PHOTON_NATS_REPLICAS}"
