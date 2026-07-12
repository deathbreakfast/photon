#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PFH_SWEEP_MODE=sharded
export STORAGE=fluvio
exec "$ROOT/scripts/run-pfh-campaign-aws.sh"
