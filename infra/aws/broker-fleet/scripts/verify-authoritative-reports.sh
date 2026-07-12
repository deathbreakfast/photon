#!/usr/bin/env bash
# Fail if broker-fleet *-aws.json reports were produced on WSL instead of EC2.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../../.." && pwd)"
REPORTS="${REPO}/profiling/photon-bench/reports"

required=(
  bm-pf0-nats-broker-cluster-aws.json
  bm-pf1-nats-broker-cluster-aws.json
  bm-pf2-nats-broker-cluster-aws.json
  bm-pf3-nats-broker-cluster-aws.json
  bm-pf4-nats-broker-cluster-aws.json
  bm-p6-nats-broker-cluster-aws.json
  bm-pg0-nats-broker-cluster-aws.json
  bm-pg1-nats-broker-cluster-aws.json
  bm-pg2-nats-broker-cluster-aws.json
  bm-pfs-nats-broker-cluster-aws.json
  bm-pfe-nats-broker-cluster-aws.json
)

fail=0
for name in "${required[@]}"; do
  path="${REPORTS}/${name}"
  if [[ ! -f "$path" ]]; then
    echo "MISSING: ${name}" >&2
    fail=1
    continue
  fi
  os="$(python3 - <<PY
import json
from pathlib import Path
p = Path("${path}")
d = json.loads(p.read_text())
print(d.get("hardware_detail", {}).get("os", ""))
PY
)"
  if [[ "$os" == *WSL* ]] || [[ "$os" == *microsoft-standard* ]]; then
    echo "STALE (WSL): ${name} — ${os}" >&2
    fail=1
  elif [[ "$os" != *aws* ]]; then
    echo "WARN (non-EC2): ${name} — ${os}" >&2
  else
    echo "OK: ${name}"
  fi
done

if [[ "$fail" -ne 0 ]]; then
  exit 1
fi
echo "All authoritative broker-fleet AWS reports present and EC2-sourced."
