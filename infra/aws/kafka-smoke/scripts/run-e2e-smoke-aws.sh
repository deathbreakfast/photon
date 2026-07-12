#!/usr/bin/env bash
# Run Photon Kafka E2E + contract smoke on the current host (local or EC2).
set -euo pipefail

# AWS/CI test transport key (not for production).
export PHOTON_TRANSPORT_KEY="${PHOTON_TRANSPORT_KEY:-cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../../.." && pwd)"

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/photon-target}"
export CARGO_INCREMENTAL=0
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-1}"

# shellcheck disable=SC1091
source "$ROOT/scripts/export-env-aws.sh"
"$ROOT/scripts/wait-kafka.sh"

cd "$REPO"

echo "=== photon-e2e mem (includes topology/telemetry smokes) ==="
cargo test -p photon-e2e

echo "=== photon-e2e kafka (ignored broker matrix) ==="
cargo test -p photon-e2e --features kafka -- --ignored

echo "=== photon-backend-kafka contract (ignored) ==="
cargo test -p photon-backend-kafka --test kafka_contract -- --ignored --test-threads=1

echo "Kafka E2E smoke validation complete."
