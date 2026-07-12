#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$ROOT/../../.." && pwd)"
# shellcheck disable=SC1091
source "${INSTANCES_ENV:-$ROOT/instances.env}"

SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY_PATH")
REMOTE_DIR="${PHOTON_REMOTE_DIR:-/home/${SSH_USER}/photon}"

ssh "${SSH_OPTS[@]}" "${SSH_USER}@${BENCH_PUBLIC_IP}" "mkdir -p ${REMOTE_DIR}"
rsync -az --exclude target --exclude 'target-*' \
  -e "ssh ${SSH_OPTS[*]}" \
  "$REPO/" "${SSH_USER}@${BENCH_PUBLIC_IP}:${REMOTE_DIR}/"

ssh "${SSH_OPTS[@]}" "${SSH_USER}@${BENCH_PUBLIC_IP}" \
  "PHOTON_REPO_DIR=${REMOTE_DIR} PHOTON_BENCH_FEATURES=fluvio bash ${REMOTE_DIR}/infra/aws/fluvio-fleet/bootstrap-bench.sh"

echo "Bench host ${BENCH_PUBLIC_IP} ready."
