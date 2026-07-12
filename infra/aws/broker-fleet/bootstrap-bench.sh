#!/usr/bin/env bash
# Run on bench EC2 (or dev laptop with Rust). Prepares repo for fleet validation.
set -euo pipefail

REPO_URL="${PHOTON_REPO_URL:-}"
REPO_DIR="${PHOTON_REPO_DIR:-$HOME/photon}"

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/photon-target}"
export CARGO_INCREMENTAL=0
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"

if ! command -v cargo >/dev/null 2>&1; then
  sudo apt-get update -qq
  sudo apt-get install -y -qq build-essential pkg-config libssl-dev curl
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
  # shellcheck disable=SC1091
  source "$HOME/.cargo/env"
fi

if [[ -n "$REPO_URL" && ! -d "$REPO_DIR/.git" ]]; then
  git clone "$REPO_URL" "$REPO_DIR"
fi

if [[ -d "$REPO_DIR" ]]; then
  cd "$REPO_DIR"
  if [[ -d .git ]]; then
    git pull --ff-only || true
  fi
  cargo build --release -p photon-bench --features nats
  echo "Bench host ready: $REPO_DIR"
else
  echo "Set PHOTON_REPO_DIR or PHOTON_REPO_URL" >&2
  exit 1
fi
