#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../../.." && pwd)"
export INSTANCES_ENV="${INSTANCES_ENV:-$ROOT/instances.env}"
export PHOTON_AWS_USE_PUBLIC_IPS=0
AWS_KEY_NAME="${AWS_KEY_NAME:?Set AWS_KEY_NAME to your EC2 key pair name}"
export SSH_KEY_PATH="${SSH_KEY_PATH:-$HOME/.ssh/${AWS_KEY_NAME}.pem}"

export PFH_SWEEP_MODE="${PFH_SWEEP_MODE:-baseline}"
export STORAGE=fluvio

SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY_PATH")
# shellcheck disable=SC1091
source "$INSTANCES_ENV"

REMOTE_DIR="${PHOTON_REMOTE_DIR:-/home/${SSH_USER}/photon}"
BENCH_BIN="/tmp/photon-target/release/photon-bench"

echo "=== Sync repo to bench EC2 ${BENCH_PUBLIC_IP} ==="
ssh "${SSH_OPTS[@]}" "${SSH_USER}@${BENCH_PUBLIC_IP}" "mkdir -p ${REMOTE_DIR}/infra/aws/fluvio-fleet"
rsync -az --exclude target --exclude 'target-*' \
  -e "ssh ${SSH_OPTS[*]}" \
  "$REPO/" "${SSH_USER}@${BENCH_PUBLIC_IP}:${REMOTE_DIR}/"
scp "${SSH_OPTS[@]}" "$INSTANCES_ENV" \
  "${SSH_USER}@${BENCH_PUBLIC_IP}:${REMOTE_DIR}/infra/aws/fluvio-fleet/instances.env"
scp "${SSH_OPTS[@]}" "$SSH_KEY_PATH" \
  "${SSH_USER}@${BENCH_PUBLIC_IP}:/tmp/photon-fleet-key.pem"
ssh "${SSH_OPTS[@]}" "${SSH_USER}@${BENCH_PUBLIC_IP}" "chmod 600 /tmp/photon-fleet-key.pem"

ssh "${SSH_OPTS[@]}" "${SSH_USER}@${BENCH_PUBLIC_IP}" \
  "source \$HOME/.cargo/env 2>/dev/null; export CARGO_TARGET_DIR=/tmp/photon-target CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2 && \
   PHOTON_REPO_DIR=${REMOTE_DIR} PHOTON_BENCH_FEATURES=fluvio bash ${REMOTE_DIR}/infra/aws/fluvio-fleet/bootstrap-bench.sh"

ssh "${SSH_OPTS[@]}" "${SSH_USER}@${BENCH_PUBLIC_IP}" \
  "export INSTANCES_ENV=${REMOTE_DIR}/infra/aws/fluvio-fleet/instances.env && \
   export PHOTON_AWS_USE_PUBLIC_IPS=0 && \
   export SSH_KEY_PATH=/tmp/photon-fleet-key.pem && \
   export PHOTON_BENCH_CMD=${BENCH_BIN} && \
   export PFH_SWEEP_MODE=${PFH_SWEEP_MODE} && \
   export PFH_PRIMARY_ONLY=${PFH_PRIMARY_ONLY:-0} && \
   export STORAGE=fluvio && \
   cd ${REMOTE_DIR} && \
   bash ${REMOTE_DIR}/infra/aws/fluvio-fleet/scripts/run-pfh-sweep-aws.sh"

mkdir -p "${REPO}/profiling/photon-bench/reports"
rsync -az -e "ssh ${SSH_OPTS[*]}" \
  "${SSH_USER}@${BENCH_PUBLIC_IP}:${REMOTE_DIR}/profiling/photon-bench/reports/" \
  "${REPO}/profiling/photon-bench/reports/"

echo "Fluvio PFH AWS campaign complete (mode=${PFH_SWEEP_MODE})."
