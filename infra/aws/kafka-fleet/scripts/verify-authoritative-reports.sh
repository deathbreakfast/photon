#!/usr/bin/env bash
# Fail if kafka-fleet PFH *-aws.json reports were produced on WSL instead of EC2.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../../.." && pwd)"
REPORTS="${REPO}/profiling/photon-bench/reports"
STORAGE="${STORAGE:-kafka}"

shopt -s nullglob
reports=( "$REPORTS"/bm-pfh-"${STORAGE}"-*-aws.json )
if [[ ${#reports[@]} -eq 0 ]]; then
  echo "MISSING: no bm-pfh-${STORAGE}-*-aws.json reports in ${REPORTS}" >&2
  exit 1
fi

fail=0
for path in "${reports[@]}"; do
  name="$(basename "$path")"
  os="$(python3 - <<PY
import json
from pathlib import Path
d = json.loads(Path("${path}").read_text())
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
echo "All authoritative kafka-fleet PFH AWS reports are EC2-sourced."
