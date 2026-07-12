#!/usr/bin/env bash
# Master orchestrator — run on bench EC2 after deploy-brokers.sh and bootstrap-bench.sh.
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

for exp in bm-pf0 bm-p6 bm-pf1 bm-pf2 bm-pf3 bm-pf4; do
  run_bench "$exp"
done

for exp in bm-pg0 bm-pg1 bm-pg2; do
  run_bench "$exp"
done

for exp in bm-pfs bm-pfe; do
  run_bench "$exp"
done

"$ROOT/scripts/run-pb4-sweep-aws.sh"  # informational ratio; per-run error gate only
"$ROOT/scripts/run-pb5-aws.sh"

# broker-spike on single broker
# shellcheck disable=SC1091
source "$ROOT/scripts/export-env-aws.sh" single
export PHOTON_NATS_REPLICAS=1
$BENCH matrix --slice broker-spike --storage nats --telemetry off $HW

echo "=== photon-e2e mem (CI slice) ==="
cargo test -p photon-e2e

echo "=== photon-e2e nats (ignored broker matrix; stream_shards=1 for replay scenarios) ==="
export PHOTON_NATS_STREAM_SHARDS=1
# shellcheck disable=SC1091
source "$ROOT/scripts/export-env-aws.sh" single
export PHOTON_NATS_REPLICAS=1
cargo test -p photon-e2e --features nats -- --ignored

cargo test -p photon-testkit --features mem
cargo test -p photon-backend-nats --test nats_contract -- --ignored

"$ROOT/scripts/verify-authoritative-reports.sh"

echo "Full AWS fleet validation complete. Reports in ${REPORTS}"
