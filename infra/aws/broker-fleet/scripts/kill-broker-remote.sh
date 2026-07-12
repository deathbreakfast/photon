#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "${INSTANCES_ENV:-$ROOT/instances.env}"

NODE="${1:?usage: kill-broker-remote.sh <1|2|3>}"
SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY_PATH")

case "$NODE" in
  1) HOST="$BROKER_1_PUBLIC_IP" ;;
  2) HOST="$BROKER_2_PUBLIC_IP" ;;
  3) HOST="$BROKER_3_PUBLIC_IP" ;;
  *) echo "node must be 1, 2, or 3" >&2; exit 1 ;;
esac

if [[ "${PHOTON_AWS_USE_PUBLIC_IPS:-0}" == "0" ]]; then
  case "$NODE" in
    1) HOST="$BROKER_1_IP" ;;
    2) HOST="$BROKER_2_IP" ;;
    3) HOST="$BROKER_3_IP" ;;
  esac
fi

ssh "${SSH_OPTS[@]}" "${SSH_USER}@${HOST}" "sudo docker stop photon-nats || true"
echo "Stopped NATS on broker-${NODE} (${HOST})"
