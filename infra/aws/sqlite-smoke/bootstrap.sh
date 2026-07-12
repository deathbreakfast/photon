#!/usr/bin/env bash
# Bootstrap SQLite smoke EC2: Rust toolchain only (no broker).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${INSTANCES_ENV:-$ROOT/instances.env}"
# shellcheck disable=SC1091
source "$ENV_FILE"

HOST="${SMOKE_PUBLIC_IP:-$SMOKE_IP}"
SSH_KEY="${SSH_KEY_PATH:-$HOME/.ssh/${AWS_KEY_NAME:-}.pem}"
SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY")

echo "Bootstrapping ${SSH_USER}@${HOST}..."
for _ in $(seq 1 30); do
  if ssh "${SSH_OPTS[@]}" "${SSH_USER}@${HOST}" "echo ok" >/dev/null 2>&1; then
    break
  fi
  sleep 5
done

ssh "${SSH_OPTS[@]}" "${SSH_USER}@${HOST}" bash -s <<'REMOTE'
set -euo pipefail
sudo apt-get update -qq
sudo apt-get install -y -qq curl pkg-config libssl-dev build-essential

if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
fi
source "$HOME/.cargo/env" || true
echo "SQLite smoke bootstrap complete."
REMOTE

echo "Bootstrap complete."
