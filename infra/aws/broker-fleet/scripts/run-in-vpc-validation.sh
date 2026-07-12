#!/usr/bin/env bash
# Run full validation from a broker EC2 host in-VPC (when dedicated bench EC2 unavailable).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export INSTANCES_ENV="${INSTANCES_ENV:-$ROOT/instances.env}"
export PHOTON_AWS_USE_PUBLIC_IPS=0
export PHOTON_BENCH_CMD="${PHOTON_BENCH_CMD:-$ROOT/../../../target/release/photon-bench}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/photon-target}"
AWS_KEY_NAME="${AWS_KEY_NAME:?Set AWS_KEY_NAME to your EC2 key pair name}"
export SSH_KEY_PATH="${SSH_KEY_PATH:-$HOME/.ssh/${AWS_KEY_NAME}.pem}"

if [[ ! -x "$PHOTON_BENCH_CMD" && -x /tmp/photon-target/release/photon-bench ]]; then
  export PHOTON_BENCH_CMD=/tmp/photon-target/release/photon-bench
fi

exec "$ROOT/scripts/run-full-validation.sh"
