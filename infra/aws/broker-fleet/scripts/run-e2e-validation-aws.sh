#!/usr/bin/env bash
# Run Photon E2E correctness tests against a live NATS broker (Photon features only — not bench).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../../.." && pwd)"

export INSTANCES_ENV="${INSTANCES_ENV:-$ROOT/instances.env}"
export PHOTON_AWS_USE_PUBLIC_IPS="${PHOTON_AWS_USE_PUBLIC_IPS:-0}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/photon-target}"
export CARGO_INCREMENTAL=0

# shellcheck disable=SC1091
source "$ROOT/scripts/export-env-aws.sh" cluster

cd "$REPO"
"$ROOT/scripts/wait-cluster.sh"

echo "=== photon-e2e mem (includes topology/telemetry smokes) ==="
cargo test -p photon-e2e

echo "=== photon-e2e nats (ignored broker matrix) ==="
cargo test -p photon-e2e --features nats -- --ignored

echo "Photon E2E validation complete."
