#!/usr/bin/env bash
# Bootstrap 2-SPU Fluvio cluster: SC on broker-single, SPUs on broker-1 + broker-2.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "${INSTANCES_ENV:-$ROOT/instances.env}"

SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY_PATH")
REMOTE_DIR="/tmp/photon-fluvio-fleet"
SC_IP="$BROKER_SINGLE_IP"

remote_bootstrap() {
  local host="$1"
  shift
  if [[ "${PHOTON_AWS_USE_PUBLIC_IPS:-0}" == "0" && "$host" == "$BROKER_1_PUBLIC_IP" ]]; then host="$BROKER_1_IP"; fi
  if [[ "${PHOTON_AWS_USE_PUBLIC_IPS:-0}" == "0" && "$host" == "$BROKER_2_PUBLIC_IP" ]]; then host="$BROKER_2_IP"; fi
  if [[ "${PHOTON_AWS_USE_PUBLIC_IPS:-0}" == "0" && "$host" == "$BROKER_SINGLE_PUBLIC_IP" ]]; then host="$BROKER_SINGLE_IP"; fi
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" "mkdir -p ${REMOTE_DIR}"
  scp "${SSH_OPTS[@]}" "$ROOT/bootstrap-broker.sh" "${SSH_USER}@${host}:${REMOTE_DIR}/"
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" \
    "chmod +x ${REMOTE_DIR}/bootstrap-broker.sh && ${REMOTE_DIR}/bootstrap-broker.sh $*"
}

remote_bootstrap "$BROKER_SINGLE_PUBLIC_IP" sc "$SC_IP"
remote_bootstrap "$BROKER_1_PUBLIC_IP" spu 5001 "$BROKER_1_IP" "$SC_IP" 9110 9111
remote_bootstrap "$BROKER_2_PUBLIC_IP" spu 5002 "$BROKER_2_IP" "$SC_IP" 9110 9111

export PHOTON_AWS_USE_PUBLIC_IPS="${PHOTON_AWS_USE_PUBLIC_IPS:-0}"
INSTANCES_ENV="$ROOT/instances.env" "$ROOT/scripts/wait-cluster.sh" n2
echo "Fluvio N2 cluster ready."
