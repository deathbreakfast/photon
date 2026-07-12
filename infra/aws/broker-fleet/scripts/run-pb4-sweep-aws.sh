#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../../.." && pwd)"
REPORTS="${REPO}/profiling/photon-bench/reports"
mkdir -p "$REPORTS"

# shellcheck disable=SC1091
source "${INSTANCES_ENV:-$ROOT/instances.env}"
# shellcheck disable=SC1091
source "$ROOT/scripts/export-env-aws.sh" single

export PHOTON_NATS_REPLICAS=1
HW="--hardware ${HARDWARE:-aws-c6i-large}"
TOPO="--topology broker-cluster --storage nats --telemetry off"
BENCH="${PHOTON_BENCH_CMD:-cargo run --release -p photon-bench --features nats --}"

bootstrap_single() {
  local SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY_PATH")
  local REMOTE_DIR="/tmp/photon-broker-fleet"
  local host="$BROKER_SINGLE_PUBLIC_IP"
  if [[ "${PHOTON_AWS_USE_PUBLIC_IPS:-0}" == "0" ]]; then
    host="$BROKER_SINGLE_IP"
  fi
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" "mkdir -p ${REMOTE_DIR}/config"
  scp "${SSH_OPTS[@]}" -r "$ROOT/config" "$ROOT/bootstrap-broker.sh" \
    "${SSH_USER}@${host}:${REMOTE_DIR}/"
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" \
    "chmod +x ${REMOTE_DIR}/bootstrap-broker.sh && ${REMOTE_DIR}/bootstrap-broker.sh standalone nats-single"
}

cd "$REPO"
bootstrap_single
$BENCH run --experiment bm-pb4 $TOPO $HW --nodes 1 --ops 30 \
  --report "$REPORTS/bm-pb4-nats-n1-aws.json"

# shellcheck disable=SC1091
source "$ROOT/scripts/export-env-aws.sh" cluster
export PHOTON_NATS_REPLICAS=1

"$ROOT/deploy-brokers.sh"
PHOTON_AWS_USE_PUBLIC_IPS="${PHOTON_AWS_USE_PUBLIC_IPS:-1}" \
  INSTANCES_ENV="$ROOT/instances.env" "$ROOT/scripts/wait-cluster.sh"

$BENCH run --experiment bm-pb4 $TOPO $HW --nodes 3 --ops 30 \
  --report "$REPORTS/bm-pb4-nats-n3-aws.json"

python3 - <<PY
import json
import os
from pathlib import Path

reports = Path("${REPORTS}")
n1 = json.loads((reports / "bm-pb4-nats-n1-aws.json").read_text())
n3 = json.loads((reports / "bm-pb4-nats-n3-aws.json").read_text())
r1 = n1.get("achieved_ops_per_sec") or 0.0
r3 = n3.get("achieved_ops_per_sec") or 0.0
if r1 <= 0:
    raise SystemExit(f"PB4 n=1 achieved rate invalid: {r1}")

def run_ok(report: dict) -> bool:
    return report.get("status") == "ok" and report.get("pass") is True and (report.get("error_rate") or 0.0) < 0.001

ratio = r3 / r1
legacy_required = 0.8 * 3
print(
    f"PB4 scaling (aws, informational): n1={r1:.1f}/s n3={r3:.1f}/s "
    f"ratio={ratio:.2f} (legacy linear gate was >={legacy_required:.2f})"
)
print("PB4 ingress gates: PFH + PF2/PF4 supersede this ratio for capacity planning.")

if not (run_ok(n1) and run_ok(n3)):
    raise SystemExit("PB4 sweep FAILED (per-run error-rate gate)")

if os.environ.get("PHOTON_BENCH_PB4_REQUIRE_LINEAR") == "1" and ratio < legacy_required:
    raise SystemExit("PB4 linear scaling sweep FAILED (PHOTON_BENCH_PB4_REQUIRE_LINEAR=1)")

print("PB4 sweep PASSED (per-run gates; ratio non-gating)")
PY
