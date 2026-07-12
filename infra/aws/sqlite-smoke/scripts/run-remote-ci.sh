#!/usr/bin/env bash
# Mirror GitHub CI jobs that do not require live broker containers.
# Broker e2e/contract jobs (nats/kafka/fluvio --ignored) stay on CI service containers
# or the dedicated broker smoke fleets.
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

ssh "${SSH_OPTS[@]}" "${SSH_USER}@${HOST}" bash -s <<'REMOTE'
set -euo pipefail
source "$HOME/.cargo/env" 2>/dev/null || true
export CARGO_TARGET_DIR=/tmp/photon-target
export CARGO_INCREMENTAL=0
export CARGO_BUILD_JOBS=1
export RUST_BACKTRACE=1
export PHOTON_TRANSPORT_KEY="${PHOTON_TRANSPORT_KEY:-cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=}"
# t3.medium: avoid rust-lld bus errors / OOM when linking large test binaries
export CARGO_PROFILE_TEST_DEBUG="${CARGO_PROFILE_TEST_DEBUG:-0}"
export CARGO_PROFILE_DEV_DEBUG="${CARGO_PROFILE_DEV_DEBUG:-0}"
export RUSTFLAGS="${RUSTFLAGS:-} -C link-arg=-fuse-ld=bfd"
cd /tmp/photon-sqlite-check

# Reclaim space from prior runs before a full CI matrix.
rm -rf /tmp/photon-target
mkdir -p /tmp/photon-target

echo "=== check (photon facade) ==="
cargo check -p uf-photon --features runtime,mem

echo "=== deny ==="
if ! command -v cargo-deny >/dev/null 2>&1; then
  cargo install cargo-deny --locked
fi
cargo deny check

echo "=== clippy (workspace, matches CI) ==="
cargo clippy --workspace --all-targets --all-features -- -D warnings

echo "=== testkit ==="
cargo test -p photon-testkit

echo "=== backend-integration ==="
cargo test -p photon-backend --features runtime --tests

echo "=== e2e mem ==="
cargo test -p photon-e2e

echo "=== e2e sqlite ==="
cargo test -p photon-e2e --features sqlite

echo "=== sqlite contract ==="
cargo test -p photon-backend-sqlite --test sqlite_contract

echo "=== photon-bench release build ==="
cargo build -p photon-bench --release

echo "=== bench-smoke BM-P0 ==="
out=$(cargo run -p photon-bench -- run \
  --experiment bm-p0 --storage mem --telemetry off \
  --ops 100 --hardware ci-small)
echo "$out"
echo "$out" | grep -q '"pass": true'

echo "=== examples ==="
cargo run -p uf-photon --example embedded_mem --features runtime,mem
cargo run -p uf-photon --example consumer_group --features runtime,mem
cargo run -p uf-photon --example manual_subscribe --features runtime,mem
cargo run -p uf-photon --example keyed_topic --features runtime,mem
cargo run -p uf-photon --example telemetry_ops_log --features runtime,mem
cargo run -p uf-photon --example subscribe_v2 --features runtime,mem

echo "=== docs ==="
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
cargo test -p uf-photon --doc --features runtime,mem
cargo test -p photon-runtime --doc --features runtime,mem
cargo test -p photon-macros --doc
cargo test -p photon-backend --doc --features runtime

echo "Remote CI subset passed (broker live jobs excluded)."
REMOTE

echo "Remote CI subset passed."
