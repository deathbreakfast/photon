#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../../.." && pwd)"
export INSTANCES_ENV="${INSTANCES_ENV:-$ROOT/instances.env}"
export PHOTON_AWS_USE_PUBLIC_IPS=0
AWS_KEY_NAME="${AWS_KEY_NAME:?Set AWS_KEY_NAME to your EC2 key pair name}"
export SSH_KEY_PATH="${SSH_KEY_PATH:-$HOME/.ssh/${AWS_KEY_NAME}.pem}"
export STORAGE=fluvio
export BENCH_COUNT="${BENCH_COUNT:-4}"

SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY_PATH")
# shellcheck disable=SC1091
source "$INSTANCES_ENV"

REMOTE_DIR="${PHOTON_REMOTE_DIR:-/home/${SSH_USER}/photon}"
BENCH_BIN="/tmp/photon-target-fluvio/release/photon-bench"

echo "=== Sync repo to all bench hosts ==="
for i in $(seq 1 "$BENCH_COUNT"); do
  host_var="BENCH_${i}_PUBLIC_IP"
  host="${!host_var}"
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" "mkdir -p ${REMOTE_DIR}/infra/aws/fluvio-fleet"
  rsync -az --exclude target --exclude 'target-*' \
    -e "ssh ${SSH_OPTS[*]}" \
    "$REPO/" "${SSH_USER}@${host}:${REMOTE_DIR}/"
  scp "${SSH_OPTS[@]}" "$INSTANCES_ENV" \
    "${SSH_USER}@${host}:${REMOTE_DIR}/infra/aws/fluvio-fleet/instances.env"
done

scp "${SSH_OPTS[@]}" "$SSH_KEY_PATH" \
  "${SSH_USER}@${BENCH_1_PUBLIC_IP}:/tmp/photon-fleet-key.pem"
ssh "${SSH_OPTS[@]}" "${SSH_USER}@${BENCH_1_PUBLIC_IP}" "chmod 600 /tmp/photon-fleet-key.pem"

echo "=== Build fluvio photon-bench on bench-1 ==="
ssh "${SSH_OPTS[@]}" "${SSH_USER}@${BENCH_1_PUBLIC_IP}" \
  "source \$HOME/.cargo/env 2>/dev/null; export CARGO_TARGET_DIR=/tmp/photon-target-fluvio CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2 && \
   PHOTON_REPO_DIR=${REMOTE_DIR} PHOTON_BENCH_FEATURES=fluvio bash ${REMOTE_DIR}/infra/aws/fluvio-fleet/bootstrap-bench.sh"

echo "=== Copy binary to bench 2..${BENCH_COUNT} ==="
for i in $(seq 2 "$BENCH_COUNT"); do
  host_var="BENCH_${i}_PUBLIC_IP"
  host="${!host_var}"
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" "mkdir -p /tmp/photon-target-fluvio/release"
  scp "${SSH_OPTS[@]}" \
    "${SSH_USER}@${BENCH_1_PUBLIC_IP}:${BENCH_BIN}" \
    "${SSH_USER}@${host}:${BENCH_BIN}"
done

ssh "${SSH_OPTS[@]}" "${SSH_USER}@${BENCH_1_PUBLIC_IP}" \
  "export INSTANCES_ENV=${REMOTE_DIR}/infra/aws/fluvio-fleet/instances.env && \
   export PHOTON_AWS_USE_PUBLIC_IPS=0 && \
   export SSH_KEY_PATH=/tmp/photon-fleet-key.pem && \
   export PHOTON_BENCH_CMD=${BENCH_BIN} && \
   export STORAGE=fluvio && \
   export BENCH_COUNT=${BENCH_COUNT} && \
   cd ${REMOTE_DIR} && \
   bash ${REMOTE_DIR}/infra/aws/fluvio-fleet/scripts/run-pfh-multibench-sweep-aws.sh"

mkdir -p "${REPO}/profiling/photon-bench/reports"
rsync -az -e "ssh ${SSH_OPTS[*]}" \
  "${SSH_USER}@${BENCH_1_PUBLIC_IP}:${REMOTE_DIR}/profiling/photon-bench/reports/" \
  "${REPO}/profiling/photon-bench/reports/"

echo "Fluvio multibench PFH campaign complete."
