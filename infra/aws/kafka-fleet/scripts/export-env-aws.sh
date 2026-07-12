#!/usr/bin/env bash
# Source after instances.env: sets PHOTON_KAFKA_* for multi-EC2 KRaft cluster.
set -euo pipefail

# AWS/CI test transport key (not for production).
export PHOTON_TRANSPORT_KEY="${PHOTON_TRANSPORT_KEY:-cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "${INSTANCES_ENV:-$ROOT/instances.env}"

MODE="${1:-cluster}"

export PHOTON_KAFKA_TOPIC_PREFIX="${PHOTON_KAFKA_TOPIC_PREFIX:-photon}"
export PHOTON_KAFKA_RETENTION="${PHOTON_KAFKA_RETENTION:-15m}"
export PHOTON_KAFKA_REPLAY_CURSOR="${PHOTON_KAFKA_REPLAY_CURSOR:-stream_seq}"
export PHOTON_KAFKA_SYNC_ACK="${PHOTON_KAFKA_SYNC_ACK:-1}"
export PHOTON_KAFKA_MAX_INFLIGHT="${PHOTON_KAFKA_MAX_INFLIGHT:-256}"

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
  export PHOTON_KAFKA_BROKERS="$(ip BROKER_SINGLE):9092"
  export PHOTON_KAFKA_REPLICAS="${PHOTON_KAFKA_REPLICAS:-1}"
elif [[ "$MODE" == "n2" ]]; then
  export PHOTON_KAFKA_BROKERS="$(ip BROKER_1):9092,$(ip BROKER_2):9092"
  export PHOTON_KAFKA_REPLICAS="${PHOTON_KAFKA_REPLICAS:-1}"
elif [[ "$MODE" == "cluster4" ]]; then
  export PHOTON_KAFKA_BROKERS="$(ip BROKER_1):9092,$(ip BROKER_2):9092,$(ip BROKER_3):9092,$(ip BROKER_SINGLE):9092"
  export PHOTON_KAFKA_REPLICAS="${PHOTON_KAFKA_REPLICAS:-1}"
else
  export PHOTON_KAFKA_BROKERS="$(ip BROKER_1):9092,$(ip BROKER_2):9092,$(ip BROKER_3):9092"
  export PHOTON_KAFKA_REPLICAS="${PHOTON_KAFKA_REPLICAS:-1}"
fi

echo "PHOTON_KAFKA_BROKERS=${PHOTON_KAFKA_BROKERS}"
echo "PHOTON_KAFKA_REPLICAS=${PHOTON_KAFKA_REPLICAS}"
