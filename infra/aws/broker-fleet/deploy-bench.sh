#!/usr/bin/env bash
# Copy repo + AWS fleet scripts to bench EC2 and run bootstrap-bench.sh.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$ROOT/../../.." && pwd)"
# shellcheck disable=SC1091
source "${INSTANCES_ENV:-$ROOT/instances.env}"

SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY_PATH")
REMOTE_DIR="${PHOTON_REMOTE_DIR:-/home/${SSH_USER}/photon}"

ssh "${SSH_OPTS[@]}" "${SSH_USER}@${BENCH_PUBLIC_IP}" "mkdir -p ~/.ssh ${REMOTE_DIR}"
if [[ -f "$SSH_KEY_PATH" ]]; then
  scp "${SSH_OPTS[@]}" "$SSH_KEY_PATH" "${SSH_USER}@${BENCH_PUBLIC_IP}:~/.ssh/$(basename "$SSH_KEY_PATH")"
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${BENCH_PUBLIC_IP}" "chmod 600 ~/.ssh/$(basename "$SSH_KEY_PATH")"
fi
rsync -az --exclude target --exclude target-photon-bench \
  -e "ssh ${SSH_OPTS[*]}" \
  "$REPO/" "${SSH_USER}@${BENCH_PUBLIC_IP}:${REMOTE_DIR}/"

ssh "${SSH_OPTS[@]}" "${SSH_USER}@${BENCH_PUBLIC_IP}" \
  "PHOTON_REPO_DIR=${REMOTE_DIR} bash ${REMOTE_DIR}/infra/aws/broker-fleet/bootstrap-bench.sh"

echo "Bench host ${BENCH_PUBLIC_IP} bootstrapped at ${REMOTE_DIR}"
