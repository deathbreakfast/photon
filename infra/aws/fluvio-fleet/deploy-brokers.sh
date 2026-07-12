#!/usr/bin/env bash
# SSH bootstrap: Fluvio SC on broker-single + SPUs on cluster hosts.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${INSTANCES_ENV:-$ROOT/instances.env}"

SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY_PATH")
REMOTE_DIR="/tmp/photon-fluvio-fleet"
SC_IP="$BROKER_SINGLE_IP"

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

remote_bootstrap "$BROKER_SINGLE_PUBLIC_IP" sc "$SC_IP"
remote_bootstrap "$BROKER_1_PUBLIC_IP" spu 5001 "$BROKER_1_IP" "$SC_IP" 9110 9111
remote_bootstrap "$BROKER_2_PUBLIC_IP" spu 5002 "$BROKER_2_IP" "$SC_IP" 9110 9111
remote_bootstrap "$BROKER_3_PUBLIC_IP" spu 5003 "$BROKER_3_IP" "$SC_IP" 9110 9111

echo "Fluvio brokers bootstrapped (SC on ${SC_IP}, 3 SPUs)."
