#!/usr/bin/env bash
# On bench EC2: Rust toolchain + release photon-bench build.
set -euo pipefail

REPO="${PHOTON_REPO_DIR:-$HOME/photon}"
TARGET="${CARGO_TARGET_DIR:-/tmp/photon-target}"
FEATURES="${PHOTON_BENCH_FEATURES:-fluvio}"

if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
fi
# shellcheck disable=SC1091
source "$HOME/.cargo/env" 2>/dev/null || true

if ! command -v cc >/dev/null 2>&1; then
  sudo apt-get update -qq
  sudo apt-get install -y -qq build-essential pkg-config libssl-dev
fi

export CARGO_TARGET_DIR="$TARGET"
export CARGO_INCREMENTAL=0
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"

cd "$REPO"
cargo build --release -p photon-bench --features "$FEATURES"
echo "Built ${TARGET}/release/photon-bench (features=${FEATURES})"
