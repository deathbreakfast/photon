#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "${INSTANCES_ENV:-$ROOT/instances.env}"

MODE="${1:-cluster}"
SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY_PATH")

resolve_host() {
  local base="$1"
  if [[ "${PHOTON_AWS_USE_PUBLIC_IPS:-0}" == "1" ]]; then
    local key="${base}_PUBLIC_IP"
    echo "${!key}"
  else
    local key="${base}_IP"
    echo "${!key}"
  fi
}

wait_sc() {
  local host="$1"
  for _ in $(seq 1 60); do
    if ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" "nc -z 127.0.0.1 9103" 2>/dev/null; then
      echo "Fluvio SC up on ${host}"
      return 0
    fi
    sleep 2
  done
  echo "Fluvio SC not ready on ${host}" >&2
  return 1
}

case "$MODE" in
  single|n2|cluster4|cluster) wait_sc "$(resolve_host BROKER_SINGLE)" ;;
  *) wait_sc "$(resolve_host BROKER_SINGLE)" ;;
esac

echo "Fluvio cluster (${MODE}) healthy."
