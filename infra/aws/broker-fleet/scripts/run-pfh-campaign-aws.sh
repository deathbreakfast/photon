#!/usr/bin/env bash
# Orchestrate PFH AWS campaign: deploy fleet, run in-VPC sweep on bench EC2, rsync reports back.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../../.." && pwd)"
export INSTANCES_ENV="${INSTANCES_ENV:-$ROOT/instances.env}"
export PHOTON_AWS_USE_PUBLIC_IPS=0
AWS_KEY_NAME="${AWS_KEY_NAME:?Set AWS_KEY_NAME to your EC2 key pair name}"
export SSH_KEY_PATH="${SSH_KEY_PATH:-$HOME/.ssh/${AWS_KEY_NAME}.pem}"


SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY_PATH")

# shellcheck disable=SC1091
source "$INSTANCES_ENV"

REMOTE_DIR="${PHOTON_REMOTE_DIR:-/home/${SSH_USER}/photon}"
BENCH_BIN="/tmp/photon-target/release/photon-bench"

echo "=== Sync repo + instances.env to bench EC2 ${BENCH_PUBLIC_IP} ==="
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

echo "=== Install nats CLI on broker hosts (for baseline) ==="
install_nats='if command -v nats >/dev/null 2>&1; then exit 0; fi
  ver=0.1.5
  arch=$(uname -m); case "$arch" in x86_64) arch=amd64;; aarch64) arch=arm64;; esac
  url="https://github.com/nats-io/natscli/releases/download/v${ver}/nats-${ver}-linux-${arch}.zip"
  curl -sfL -o /tmp/nats.zip "$url" || exit 0
  sudo apt-get install -y -qq unzip >/dev/null 2>&1 || true
  unzip -o /tmp/nats.zip -d /tmp/nats-cli >/dev/null 2>&1 || exit 0
  sudo install -m 755 /tmp/nats-cli/nats /usr/local/bin/nats 2>/dev/null || true'
for host in "$BROKER_SINGLE_IP" "$BROKER_1_IP"; do
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" "$install_nats" \
    || echo "warn: nats CLI install failed on ${host}"
done

echo "=== Run PFH sweep in-VPC from bench host ==="
ssh "${SSH_OPTS[@]}" "${SSH_USER}@${BENCH_PUBLIC_IP}" \
  "export INSTANCES_ENV=${REMOTE_DIR}/infra/aws/broker-fleet/instances.env && \
   export PHOTON_AWS_USE_PUBLIC_IPS=0 && \
   export SSH_KEY_PATH=/tmp/photon-fleet-key.pem && \
   export PHOTON_BENCH_CMD=${BENCH_BIN} && \
   export CARGO_TARGET_DIR=/tmp/photon-target && \
   cd ${REMOTE_DIR} && \
   bash ${REMOTE_DIR}/infra/aws/broker-fleet/scripts/run-pfh-sweep-aws.sh"

echo "=== Rsync reports back to operator repo ==="
mkdir -p "${REPO}/profiling/photon-bench/reports" "${REPO}/profiling/nats-bench"
rsync -az -e "ssh ${SSH_OPTS[*]}" \
  "${SSH_USER}@${BENCH_PUBLIC_IP}:${REMOTE_DIR}/profiling/photon-bench/reports/" \
  "${REPO}/profiling/photon-bench/reports/"
rsync -az -e "ssh ${SSH_OPTS[*]}" \
  "${SSH_USER}@${BENCH_PUBLIC_IP}:${REMOTE_DIR}/profiling/nats-bench/" \
  "${REPO}/profiling/nats-bench/" 2>/dev/null || true

echo "=== Local scaling-curve verify ==="
cd "$REPO"
cargo run --release -p photon-bench --features nats -- scaling-curve \
  --hardware "${HARDWARE:-aws-c6i-large}" --storage nats \
  --reports-dir "${REPO}/profiling/photon-bench/reports" \
  --out "${REPO}/profiling/photon-bench/reports/scaling-curve-${HARDWARE:-aws-c6i-large}-nats-firehose.json"

echo "PFH AWS campaign complete."
