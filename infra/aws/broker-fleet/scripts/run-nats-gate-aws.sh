#!/usr/bin/env bash
# Targeted NATS ingress gate — PF2/PF4/P6/PG2/PFS/PFE (skips PB4 ratio + full e2e).
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
cargo build --release -p photon-bench --features nats

"$ROOT/scripts/wait-cluster.sh"

run_bench() {
  local exp="$1"
  echo "=== ${exp} ==="
  $BENCH run --experiment "$exp" $TOPO $HW \
    --report "$REPORTS/${exp}-nats-broker-cluster-aws.json"
}

for exp in bm-p6 bm-pf2 bm-pf4 bm-pg2 bm-pfs bm-pfe; do
  run_bench "$exp"
done

echo "NATS ingress gate experiments complete. Reports in ${REPORTS}"
"$ROOT/scripts/verify-authoritative-reports.sh"
