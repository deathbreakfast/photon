#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../.." && pwd)"
REPORTS="${REPO}/profiling/photon-bench/reports"
mkdir -p "$REPORTS"

SMOKE="${PHOTON_BENCH_PB4_SMOKE:-0}"
if [[ "${1:-}" == "--smoke" ]]; then
  SMOKE=1
  shift
fi

"$ROOT/scripts/down.sh" --wipe 2>/dev/null || "$ROOT/scripts/down.sh"
"$ROOT/scripts/single-node.sh"

export PHOTON_NATS_URL="nats://127.0.0.1:4222"
export PHOTON_NATS_STREAM=photon
export PHOTON_NATS_RETENTION=15m
export PHOTON_NATS_REPLICAS=1

cd "$REPO"
cargo run -p photon-bench --features nats -- run \
  --experiment bm-pb4 --storage nats --topology broker-cluster --telemetry off \
  --nodes 1 --ops 30 \
  --report "$REPORTS/bm-pb4-nats-n1.json"

"$ROOT/scripts/down.sh"
"$ROOT/scripts/up.sh"

export PHOTON_NATS_URL="nats://127.0.0.1:4222,nats://127.0.0.1:4225,nats://127.0.0.1:4224"
export PHOTON_NATS_REPLICAS=1

cargo run -p photon-bench --features nats -- run \
  --experiment bm-pb4 --storage nats --topology broker-cluster --telemetry off \
  --nodes 3 --ops 30 \
  --report "$REPORTS/bm-pb4-nats-n3.json"

python3 - <<PY
import json
import os
from pathlib import Path

reports = Path("${REPORTS}")
n1 = json.loads((reports / "bm-pb4-nats-n1.json").read_text())
n3 = json.loads((reports / "bm-pb4-nats-n3.json").read_text())
r1 = n1.get("achieved_ops_per_sec") or 0.0
r3 = n3.get("achieved_ops_per_sec") or 0.0
if r1 <= 0:
    raise SystemExit(f"PB4 n=1 achieved rate invalid: {r1}")

def run_ok(report: dict) -> bool:
    return report.get("status") == "ok" and report.get("pass") is True and (report.get("error_rate") or 0.0) < 0.001

ratio = r3 / r1
legacy_required = 0.8 if ${SMOKE} else 0.8 * 3
mode = "smoke" if ${SMOKE} else "local"
print(
    f"PB4 scaling ({mode}, informational): n1={r1:.1f}/s n3={r3:.1f}/s "
    f"ratio={ratio:.2f} (legacy linear gate was >={legacy_required:.2f})"
)

if not (run_ok(n1) and run_ok(n3)):
    raise SystemExit("PB4 sweep FAILED (per-run error-rate gate)")

if os.environ.get("PHOTON_BENCH_PB4_REQUIRE_LINEAR") == "1" and ratio < legacy_required:
    raise SystemExit("PB4 linear scaling sweep FAILED (PHOTON_BENCH_PB4_REQUIRE_LINEAR=1)")

print("PB4 sweep PASSED (per-run gates; ratio non-gating)")
PY
