#!/usr/bin/env bash
# Phase 3 PFH campaign: baseline (stream_shards=1) + sharded (stream_shards=N) on AWS in-VPC.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../../.." && pwd)"
export INSTANCES_ENV="${INSTANCES_ENV:-$ROOT/instances.env}"
export PHOTON_AWS_USE_PUBLIC_IPS=0
AWS_KEY_NAME="${AWS_KEY_NAME:?Set AWS_KEY_NAME to your EC2 key pair name}"
export SSH_KEY_PATH="${SSH_KEY_PATH:-$HOME/.ssh/${AWS_KEY_NAME}.pem}"

export HARDWARE="${HARDWARE:-aws-c6i-large}"

SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY_PATH")

# shellcheck disable=SC1091
source "$INSTANCES_ENV"

REMOTE_DIR="${PHOTON_REMOTE_DIR:-/home/${SSH_USER}/photon}"
BENCH_BIN="/tmp/photon-target/release/photon-bench"

echo "=== Sync repo to bench EC2 ${BENCH_PUBLIC_IP} ==="
ssh "${SSH_OPTS[@]}" "${SSH_USER}@${BENCH_PUBLIC_IP}" "mkdir -p ${REMOTE_DIR}/infra/aws/broker-fleet"
rsync -az --exclude target --exclude 'target-*' \
  -e "ssh ${SSH_OPTS[*]}" \
  "$REPO/" "${SSH_USER}@${BENCH_PUBLIC_IP}:${REMOTE_DIR}/"
scp "${SSH_OPTS[@]}" "$INSTANCES_ENV" \
  "${SSH_USER}@${BENCH_PUBLIC_IP}:${REMOTE_DIR}/infra/aws/broker-fleet/instances.env"
scp "${SSH_OPTS[@]}" "$SSH_KEY_PATH" \
  "${SSH_USER}@${BENCH_PUBLIC_IP}:/tmp/photon-fleet-key.pem"
ssh "${SSH_OPTS[@]}" "${SSH_USER}@${BENCH_PUBLIC_IP}" "chmod 600 /tmp/photon-fleet-key.pem"

echo "=== Build release photon-bench on bench host ==="
ssh "${SSH_OPTS[@]}" "${SSH_USER}@${BENCH_PUBLIC_IP}" \
  "source \$HOME/.cargo/env 2>/dev/null; export CARGO_TARGET_DIR=/tmp/photon-target CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2 && \
   PHOTON_REPO_DIR=${REMOTE_DIR} bash ${REMOTE_DIR}/infra/aws/broker-fleet/bootstrap-bench.sh"

run_sweep() {
  local mode="$1"
  echo "=== PFH sweep mode=${mode} (in-VPC, primary row only) ==="
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${BENCH_PUBLIC_IP}" \
    "export INSTANCES_ENV=${REMOTE_DIR}/infra/aws/broker-fleet/instances.env && \
     export PHOTON_AWS_USE_PUBLIC_IPS=0 && \
     export SSH_KEY_PATH=/tmp/photon-fleet-key.pem && \
     export PHOTON_BENCH_CMD=${BENCH_BIN} && \
     export CARGO_TARGET_DIR=/tmp/photon-target && \
     export HARDWARE=${HARDWARE} && \
     export PFH_SWEEP_MODE=${mode} && \
     export PFH_PRIMARY_ONLY=1 && \
     cd ${REMOTE_DIR} && \
     bash ${REMOTE_DIR}/infra/aws/broker-fleet/scripts/run-pfh-sweep-aws.sh"
}

run_sweep baseline
run_sweep sharded

echo "=== Rsync reports back ==="
mkdir -p "${REPO}/profiling/photon-bench/reports" "${REPO}/profiling/nats-bench"
rsync -az -e "ssh ${SSH_OPTS[*]}" \
  "${SSH_USER}@${BENCH_PUBLIC_IP}:${REMOTE_DIR}/profiling/photon-bench/reports/" \
  "${REPO}/profiling/photon-bench/reports/"
rsync -az -e "ssh ${SSH_OPTS[*]}" \
  "${SSH_USER}@${BENCH_PUBLIC_IP}:${REMOTE_DIR}/profiling/nats-bench/" \
  "${REPO}/profiling/nats-bench/" 2>/dev/null || true

echo "=== Scaling curves (apples-to-apples) ==="
cd "$REPO"
cargo run --release -p photon-bench --features nats -- scaling-curve \
  --hardware "${HARDWARE}" --storage nats \
  --reports-dir "${REPO}/profiling/photon-bench/reports" \
  --stream-shards 1 \
  --out "${REPO}/profiling/photon-bench/reports/scaling-curve-${HARDWARE}-nats-firehose-baseline.json"

cargo run --release -p photon-bench --features nats -- scaling-curve \
  --hardware "${HARDWARE}" --storage nats \
  --reports-dir "${REPO}/profiling/photon-bench/reports" \
  --match-broker-nodes \
  --out "${REPO}/profiling/photon-bench/reports/scaling-curve-${HARDWARE}-nats-firehose-sharded.json"

echo "Phase 3 PFH AWS campaign complete."
