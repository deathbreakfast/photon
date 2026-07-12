#!/usr/bin/env bash
# Rsync repo to SQLite smoke EC2 and run compile/doc/test checks remotely.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../../.." && pwd)"
ENV_FILE="${INSTANCES_ENV:-$ROOT/instances.env}"
# shellcheck disable=SC1091
source "$ENV_FILE"

HOST="${SMOKE_PUBLIC_IP:-$SMOKE_IP}"
SSH_KEY="${SSH_KEY_PATH:-$HOME/.ssh/${AWS_KEY_NAME:-}.pem}"
SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY")
REMOTE_DIR="/tmp/photon-sqlite-check"

echo "Syncing repo to ${SSH_USER}@${HOST}:${REMOTE_DIR}..."
ssh "${SSH_OPTS[@]}" "${SSH_USER}@${HOST}" "mkdir -p ${REMOTE_DIR}"
rsync -az --delete \
  --exclude target --exclude .git \
  "$REPO/" "${SSH_USER}@${HOST}:${REMOTE_DIR}/"

ssh "${SSH_OPTS[@]}" "${SSH_USER}@${HOST}" bash -s <<REMOTE
set -euo pipefail
source "\$HOME/.cargo/env" 2>/dev/null || true
export CARGO_TARGET_DIR=/tmp/photon-target
export CARGO_INCREMENTAL=0
export CARGO_BUILD_JOBS=1
# CI/AWS smoke test key (base64 of photon-dev-transport-key-32bytes) — not for production.
export PHOTON_TRANSPORT_KEY="\${PHOTON_TRANSPORT_KEY:-cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=}"
cd ${REMOTE_DIR}

echo "=== cargo check (workspace, runtime+mem) ==="
cargo check --workspace --features runtime,mem

echo "=== clippy (workspace, matches CI) ==="
cargo clippy --workspace --all-targets --all-features -- -D warnings

echo "=== cargo doc (-D warnings, workspace) ==="
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features

echo "=== photon-backend tests (runtime) ==="
cargo test -p photon-backend --features runtime --lib
cargo test -p photon-backend --features runtime --tests

echo "Remote check passed."
REMOTE

echo "Remote SQLite check passed."
