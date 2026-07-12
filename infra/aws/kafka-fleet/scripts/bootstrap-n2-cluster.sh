#!/usr/bin/env bash
# Bootstrap 2-node Kafka KRaft cluster on broker-1 + broker-2.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "${INSTANCES_ENV:-$ROOT/instances.env}"

SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY_PATH")
REMOTE_DIR="/tmp/photon-kafka-fleet"

remote_bootstrap() {
  local host="$1"
  shift
  if [[ "${PHOTON_AWS_USE_PUBLIC_IPS:-0}" == "0" && "$host" == "$BROKER_1_PUBLIC_IP" ]]; then host="$BROKER_1_IP"; fi
  if [[ "${PHOTON_AWS_USE_PUBLIC_IPS:-0}" == "0" && "$host" == "$BROKER_2_PUBLIC_IP" ]]; then host="$BROKER_2_IP"; fi
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" "mkdir -p ${REMOTE_DIR}"
  scp "${SSH_OPTS[@]}" "$ROOT/bootstrap-broker.sh" "${SSH_USER}@${host}:${REMOTE_DIR}/"
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" \
    "chmod +x ${REMOTE_DIR}/bootstrap-broker.sh && ${REMOTE_DIR}/bootstrap-broker.sh $*"
}

QUORUM="1@${BROKER_1_IP}:9093,2@${BROKER_2_IP}:9093"
remote_bootstrap "$BROKER_1_PUBLIC_IP" cluster 1 "$BROKER_1_IP" "$QUORUM"
remote_bootstrap "$BROKER_2_PUBLIC_IP" cluster 2 "$BROKER_2_IP" "$QUORUM"

export PHOTON_AWS_USE_PUBLIC_IPS="${PHOTON_AWS_USE_PUBLIC_IPS:-0}"
INSTANCES_ENV="$ROOT/instances.env" "$ROOT/scripts/wait-cluster.sh" n2
echo "Kafka N2 cluster ready."
