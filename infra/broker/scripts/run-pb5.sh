#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../.." && pwd)"
REPORTS="${REPO}/profiling/photon-bench/reports"
mkdir -p "$REPORTS"

"$ROOT/scripts/down.sh" 2>/dev/null || true
"$ROOT/scripts/up.sh"

export PHOTON_NATS_URL="nats://127.0.0.1:4222,nats://127.0.0.1:4225,nats://127.0.0.1:4224"
export PHOTON_NATS_STREAM=photon
export PHOTON_NATS_RETENTION=15m
export PHOTON_NATS_REPLICAS=3
export PHOTON_BENCH_FAILOVER=1

cd "$REPO"
cargo run -p photon-bench --features nats -- run \
  --experiment bm-pb5 --storage nats --topology broker-cluster --telemetry off \
  --nodes 3 --ops 45 \
  --report "$REPORTS/bm-pb5-nats-failover.json" &
BENCH_PID=$!

sleep 22
"$ROOT/scripts/kill-node.sh" 2

wait "$BENCH_PID"

python3 - <<PY
import json
from pathlib import Path

report = json.loads((Path("${REPORTS}") / "bm-pb5-nats-failover.json").read_text())
print(json.dumps({k: report.get(k) for k in ("pass", "status", "achieved_ops_per_sec", "error_rate")}, indent=2))
if not report.get("pass"):
    raise SystemExit("PB5 failover run FAILED")
print("PB5 failover PASSED")
PY
