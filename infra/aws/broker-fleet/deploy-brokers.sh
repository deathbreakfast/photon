#!/usr/bin/env bash
# SSH bootstrap: install NATS on all broker EC2 hosts.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${INSTANCES_ENV:-$ROOT/instances.env}"

SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY_PATH")
REMOTE_DIR="/tmp/photon-broker-fleet"

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
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" "mkdir -p ${REMOTE_DIR}/config"
  scp "${SSH_OPTS[@]}" -r "$ROOT/config" "$ROOT/bootstrap-broker.sh" \
    "${SSH_USER}@${host}:${REMOTE_DIR}/"
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" "chmod +x ${REMOTE_DIR}/bootstrap-broker.sh && ${REMOTE_DIR}/bootstrap-broker.sh $*"
}

PEERS=("$BROKER_2_IP" "$BROKER_3_IP")
echo "Bootstrapping broker-1 on ${BROKER_1_PUBLIC_IP}..."
remote_bootstrap "$BROKER_1_PUBLIC_IP" cluster nats-1 "${PEERS[@]}"

PEERS=("$BROKER_1_IP" "$BROKER_3_IP")
echo "Bootstrapping broker-2 on ${BROKER_2_PUBLIC_IP}..."
remote_bootstrap "$BROKER_2_PUBLIC_IP" cluster nats-2 "${PEERS[@]}"

PEERS=("$BROKER_1_IP" "$BROKER_2_IP")
echo "Bootstrapping broker-3 on ${BROKER_3_PUBLIC_IP}..."
remote_bootstrap "$BROKER_3_PUBLIC_IP" cluster nats-3 "${PEERS[@]}"

echo "Brokers bootstrapped (run wait-cluster.sh from bench host before validation)."
