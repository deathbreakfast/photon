#!/usr/bin/env bash
# SSH bootstrap: install Kafka KRaft on all broker EC2 hosts (3-node cluster baseline).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${INSTANCES_ENV:-$ROOT/instances.env}"

SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY_PATH")
REMOTE_DIR="/tmp/photon-kafka-fleet"

remote_bootstrap() {
  local host="$1"
  shift
  if [[ "${PHOTON_AWS_USE_PUBLIC_IPS:-0}" == "0" ]]; then
    case "$host" in
      "$BROKER_1_PUBLIC_IP") host="$BROKER_1_IP" ;;
      "$BROKER_2_PUBLIC_IP") host="$BROKER_2_IP" ;;
      "$BROKER_3_PUBLIC_IP") host="$BROKER_3_IP" ;;
      "$BROKER_SINGLE_PUBLIC_IP") host="$BROKER_SINGLE_IP" ;;
    esac
  fi
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" "mkdir -p ${REMOTE_DIR}"
  scp "${SSH_OPTS[@]}" "$ROOT/bootstrap-broker.sh" "${SSH_USER}@${host}:${REMOTE_DIR}/"
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" \
    "chmod +x ${REMOTE_DIR}/bootstrap-broker.sh && ${REMOTE_DIR}/bootstrap-broker.sh $*"
}

QUORUM="1@${BROKER_1_IP}:9093,2@${BROKER_2_IP}:9093,3@${BROKER_3_IP}:9093"
remote_bootstrap "$BROKER_1_PUBLIC_IP" cluster 1 "$BROKER_1_IP" "$QUORUM"
remote_bootstrap "$BROKER_2_PUBLIC_IP" cluster 2 "$BROKER_2_IP" "$QUORUM"
remote_bootstrap "$BROKER_3_PUBLIC_IP" cluster 3 "$BROKER_3_IP" "$QUORUM"

echo "Kafka brokers bootstrapped (run wait-cluster.sh from bench host before validation)."
