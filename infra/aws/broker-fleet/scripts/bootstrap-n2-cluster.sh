#!/usr/bin/env bash
# Bootstrap 2-node JetStream RAFT cluster on broker-1 + broker-2.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "${INSTANCES_ENV:-$ROOT/instances.env}"

SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY_PATH")
REMOTE_DIR="/tmp/photon-broker-fleet"

remote_bootstrap() {
  local host="$1"
  shift
  if [[ "${PHOTON_AWS_USE_PUBLIC_IPS:-0}" == "0" && "$host" == "$BROKER_1_PUBLIC_IP" ]]; then host="$BROKER_1_IP"; fi
  if [[ "${PHOTON_AWS_USE_PUBLIC_IPS:-0}" == "0" && "$host" == "$BROKER_2_PUBLIC_IP" ]]; then host="$BROKER_2_IP"; fi
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" "mkdir -p ${REMOTE_DIR}/config"
  scp "${SSH_OPTS[@]}" -r "$ROOT/config" "$ROOT/bootstrap-broker.sh" \
    "${SSH_USER}@${host}:${REMOTE_DIR}/"
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" \
    "chmod +x ${REMOTE_DIR}/bootstrap-broker.sh && ${REMOTE_DIR}/bootstrap-broker.sh $*"
}

PEERS=("$BROKER_2_IP")
echo "Bootstrapping broker-1 (n2 cluster)..."
remote_bootstrap "$BROKER_1_PUBLIC_IP" cluster nats-1 "${PEERS[@]}"

PEERS=("$BROKER_1_IP")
echo "Bootstrapping broker-2 (n2 cluster)..."
remote_bootstrap "$BROKER_2_PUBLIC_IP" cluster nats-2 "${PEERS[@]}"

export PHOTON_AWS_USE_PUBLIC_IPS="${PHOTON_AWS_USE_PUBLIC_IPS:-0}"
INSTANCES_ENV="$ROOT/instances.env" "$ROOT/scripts/wait-cluster.sh" n2
echo "N2 cluster ready."
