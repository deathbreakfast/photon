#!/usr/bin/env bash
# Phase 4 multi-bench PFH campaign: 4 brokers + up to 4 bench clients on AWS in-VPC.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../../.." && pwd)"
export INSTANCES_ENV="${INSTANCES_ENV:-$ROOT/instances.env}"
export PHOTON_AWS_USE_PUBLIC_IPS=0
AWS_KEY_NAME="${AWS_KEY_NAME:?Set AWS_KEY_NAME to your EC2 key pair name}"
export SSH_KEY_PATH="${SSH_KEY_PATH:-$HOME/.ssh/${AWS_KEY_NAME}.pem}"

export HARDWARE="${HARDWARE:-aws-c6i-large}"
export BENCH_COUNT="${BENCH_COUNT:-4}"

SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY_PATH")

# shellcheck disable=SC1091
source "$INSTANCES_ENV"

REMOTE_DIR="${PHOTON_REMOTE_DIR:-/home/${SSH_USER}/photon}"
BENCH_BIN="/tmp/photon-target/release/photon-bench"
COORD_HOST="${BENCH_1_PUBLIC_IP:-$BENCH_PUBLIC_IP}"

echo "=== Deploy all ${BENCH_COUNT} bench hosts ==="
export INSTANCES_ENV
bash "$ROOT/scripts/deploy-all-benches.sh"

echo "=== Run multi-bench sweep from coordinator ${COORD_HOST} ==="
ssh "${SSH_OPTS[@]}" "${SSH_USER}@${COORD_HOST}" \
  "export INSTANCES_ENV=${REMOTE_DIR}/infra/aws/broker-fleet/instances.env && \
   export PHOTON_AWS_USE_PUBLIC_IPS=0 && \
   export SSH_KEY_PATH=/tmp/photon-fleet-key.pem && \
   export PHOTON_BENCH_CMD=${BENCH_BIN} && \
   export CARGO_TARGET_DIR=/tmp/photon-target && \
   export HARDWARE=${HARDWARE} && \
   export BENCH_COUNT=${BENCH_COUNT} && \
   cd ${REMOTE_DIR} && \
   bash ${REMOTE_DIR}/infra/aws/broker-fleet/scripts/run-pfh-multibench-sweep-aws.sh"

echo "=== Rsync reports back ==="
mkdir -p "${REPO}/profiling/photon-bench/reports"
rsync -az -e "ssh ${SSH_OPTS[*]}" \
  "${SSH_USER}@${COORD_HOST}:${REMOTE_DIR}/profiling/photon-bench/reports/" \
  "${REPO}/profiling/photon-bench/reports/"

echo "=== Local scaling curve (multibench) ==="
cd "$REPO"
cargo run --release -p photon-bench --features nats -- scaling-curve \
  --hardware "${HARDWARE}" --storage nats \
  --reports-dir "${REPO}/profiling/photon-bench/reports" \
  --multibench-ladder \
  --out "${REPO}/profiling/photon-bench/reports/scaling-curve-${HARDWARE}-nats-firehose-multibench.json"

echo "Phase 4 multi-bench PFH AWS campaign complete."
