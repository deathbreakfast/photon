#!/usr/bin/env bash
# Deploy repo + bootstrap photon-bench on all bench EC2 hosts (BENCH_COUNT).
set -euo pipefail
AWS_KEY_NAME="${AWS_KEY_NAME:?Set AWS_KEY_NAME to your EC2 key pair name}"
export SSH_KEY_PATH="${SSH_KEY_PATH:-$HOME/.ssh/${AWS_KEY_NAME}.pem}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../../.." && pwd)"
# shellcheck disable=SC1091
source "${INSTANCES_ENV:-$ROOT/instances.env}"

SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY_PATH")
REMOTE_DIR="${PHOTON_REMOTE_DIR:-/home/${SSH_USER}/photon}"
BENCH_COUNT="${BENCH_COUNT:-1}"

deploy_one() {
  local host="$1"
  echo "=== Deploy bench ${host} ==="
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" "mkdir -p ${REMOTE_DIR}"
  rsync -az --exclude target --exclude 'target-*' \
    -e "ssh ${SSH_OPTS[*]}" \
    "$REPO/" "${SSH_USER}@${host}:${REMOTE_DIR}/"
  scp "${SSH_OPTS[@]}" "${INSTANCES_ENV:-$ROOT/instances.env}" \
    "${SSH_USER}@${host}:${REMOTE_DIR}/infra/aws/broker-fleet/instances.env"
  scp "${SSH_OPTS[@]}" "$SSH_KEY_PATH" \
    "${SSH_USER}@${host}:/tmp/photon-fleet-key.pem"
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" "chmod 600 /tmp/photon-fleet-key.pem"
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" \
    "source \$HOME/.cargo/env 2>/dev/null; export CARGO_TARGET_DIR=/tmp/photon-target CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2 && \
     PHOTON_REPO_DIR=${REMOTE_DIR} bash ${REMOTE_DIR}/infra/aws/broker-fleet/bootstrap-bench.sh"
}

PIDS=()
for i in $(seq 1 "$BENCH_COUNT"); do
  key="BENCH_${i}_PUBLIC_IP"
  host="${!key}"
  deploy_one "$host" &
  PIDS+=($!)
done

FAIL=0
for pid in "${PIDS[@]}"; do
  wait "$pid" || FAIL=1
done
if [[ "$FAIL" -ne 0 ]]; then
  echo "One or more bench deploys failed" >&2
  exit 1
fi

echo "All ${BENCH_COUNT} bench host(s) bootstrapped."
