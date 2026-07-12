#!/usr/bin/env bash
# Rsync repo to SQLite smoke EC2 and run E2E smoke remotely.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../../.." && pwd)"
ENV_FILE="${INSTANCES_ENV:-$ROOT/instances.env}"
# shellcheck disable=SC1091
source "$ENV_FILE"

HOST="${SMOKE_PUBLIC_IP:-$SMOKE_IP}"
SSH_KEY="${SSH_KEY_PATH:-$HOME/.ssh/${AWS_KEY_NAME:-}.pem}"
SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY")
REMOTE_DIR="/tmp/photon-sqlite-smoke"

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
cd ${REMOTE_DIR}
./infra/aws/sqlite-smoke/scripts/run-e2e-smoke-aws.sh
REMOTE

echo "Remote SQLite smoke passed."
