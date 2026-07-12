#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../../.." && pwd)"
REPORTS="${REPO}/profiling/photon-bench/reports"
mkdir -p "$REPORTS"

# shellcheck disable=SC1091
source "${INSTANCES_ENV:-$ROOT/instances.env}"
# shellcheck disable=SC1091
source "$ROOT/scripts/export-env-aws.sh" cluster

export PHOTON_BENCH_FAILOVER=1
HW="--hardware ${HARDWARE:-aws-c6i-large}"
TOPO="--topology broker-cluster --storage nats --telemetry off"
BENCH="${PHOTON_BENCH_CMD:-cargo run --release -p photon-bench --features nats --}"

cd "$REPO"
$BENCH run --experiment bm-pb5 $TOPO $HW --nodes 3 --ops 45 \
  --report "$REPORTS/bm-pb5-nats-failover-aws.json" &
BENCH_PID=$!

sleep 22
"$ROOT/scripts/kill-broker-remote.sh" 2

wait "$BENCH_PID"

python3 - <<PY
import json
from pathlib import Path

report = json.loads((Path("${REPORTS}") / "bm-pb5-nats-failover-aws.json").read_text())
print(json.dumps({k: report.get(k) for k in ("pass", "status", "achieved_ops_per_sec", "error_rate")}, indent=2))
if not report.get("pass"):
    raise SystemExit("PB5 failover run FAILED")
print("PB5 failover PASSED")
PY
