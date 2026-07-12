#!/usr/bin/env bash
# Run Photon SQLite E2E + contract smoke on the current host (EC2 only).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../../.." && pwd)"

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/photon-target}"
export CARGO_INCREMENTAL=0
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-1}"
export PHOTON_SQLITE_PATH="${PHOTON_SQLITE_PATH:-/tmp/photon-e2e-smoke.db}"
# CI/AWS smoke test key (base64 of photon-dev-transport-key-32bytes) — not for production.
export PHOTON_TRANSPORT_KEY="${PHOTON_TRANSPORT_KEY:-cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=}"

cd "$REPO"

echo "=== photon-e2e mem (includes topology/telemetry smokes) ==="
cargo test -p photon-e2e

echo "=== photon-e2e sqlite matrix ==="
cargo test -p photon-e2e --features sqlite

echo "=== photon-backend-sqlite contract ==="
cargo test -p photon-backend-sqlite --test sqlite_contract

echo "SQLite E2E smoke validation complete."
