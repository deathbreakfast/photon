#!/usr/bin/env bash
# Kafka PFH phase-3 sharded sweep campaign.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PFH_SWEEP_MODE=sharded
export STORAGE=kafka
exec "$ROOT/scripts/run-pfh-campaign-aws.sh"
