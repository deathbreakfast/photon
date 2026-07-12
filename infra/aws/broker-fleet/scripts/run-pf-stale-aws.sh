#!/usr/bin/env bash
# Re-run PF0/PF1/PF3 on bench EC2 to replace WSL *-aws.json reports with authoritative in-VPC JSON.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../../.." && pwd)"
REPORTS="${REPO}/profiling/photon-bench/reports"
mkdir -p "$REPORTS"

# shellcheck disable=SC1091
source "${INSTANCES_ENV:-$ROOT/instances.env}"
# shellcheck disable=SC1091
source "$ROOT/scripts/export-env-aws.sh" cluster

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/photon-target}"
export CARGO_INCREMENTAL=0
export PHOTON_AWS_USE_PUBLIC_IPS="${PHOTON_AWS_USE_PUBLIC_IPS:-0}"
export PHOTON_NATS_STREAM_SHARDS="${PHOTON_NATS_STREAM_SHARDS:-4}"

HW="--hardware ${HARDWARE:-aws-c6i-large}"
TOPO="--topology broker-cluster --storage nats --telemetry off"
BENCH="${PHOTON_BENCH_CMD:-cargo run --release -p photon-bench --features nats --}"

cd "$REPO"
if ! command -v cargo >/dev/null 2>&1; then
  # shellcheck disable=SC1091
  source "$HOME/.cargo/env"
fi
cargo build --release -p photon-bench --features nats

"$ROOT/scripts/wait-cluster.sh"

run_bench() {
  local exp="$1"
  echo "=== ${exp} ==="
  $BENCH run --experiment "$exp" $TOPO $HW \
    --report "$REPORTS/${exp}-nats-broker-cluster-aws.json"
}

for exp in bm-pf0 bm-pf1 bm-pf3; do
  run_bench "$exp"
done

echo "PF0/PF1/PF3 authoritative reports written to ${REPORTS}"
