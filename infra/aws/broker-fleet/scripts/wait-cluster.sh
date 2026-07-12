#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "${INSTANCES_ENV:-$ROOT/instances.env}"

wait_one() {
  local ip="$1"
  for _ in $(seq 1 60); do
    if curl -sf "http://${ip}:8222/varz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "varz check failed for ${ip}" >&2
  return 1
}

wait_cluster_healthy() {
  local ip="$1"
  for _ in $(seq 1 90); do
    if curl -sf "http://${ip}:8222/healthz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 3
  done
  echo "jetstream health check failed for ${ip}" >&2
  return 1
}

resolve() {
  local base="$1"
  if [[ "${PHOTON_AWS_USE_PUBLIC_IPS:-0}" == "1" ]]; then
    local key="${base}_PUBLIC_IP"
    echo "${!key}"
  else
    local key="${base}_IP"
    echo "${!key}"
  fi
}

if [[ "${1:-}" == "single" ]]; then
  wait_cluster_healthy "$(resolve BROKER_SINGLE)"
elif [[ "${1:-}" == "n2" ]]; then
  wait_one "$(resolve BROKER_1)"
  wait_one "$(resolve BROKER_2)"
  sleep 5
  wait_cluster_healthy "$(resolve BROKER_1)"
elif [[ "${1:-}" == "cluster4" ]]; then
  wait_one "$(resolve BROKER_1)"
  wait_one "$(resolve BROKER_2)"
  wait_one "$(resolve BROKER_3)"
  wait_one "$(resolve BROKER_SINGLE)"
  sleep 5
  wait_cluster_healthy "$(resolve BROKER_1)"
else
  wait_one "$(resolve BROKER_1)"
  wait_one "$(resolve BROKER_2)"
  wait_one "$(resolve BROKER_3)"
  sleep 5
  wait_cluster_healthy "$(resolve BROKER_1)"
fi
echo "NATS cluster healthy."
