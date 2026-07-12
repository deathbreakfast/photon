#!/usr/bin/env bash
# Orchestrate all Photon E2E validation on AWS (no local cargo).
#
# Usage:
#   export AWS_KEY_NAME=...
#   export SSH_KEY_PATH=...
#   ./infra/aws/scripts/run-all-e2e-aws.sh [--skip-teardown]
#
# Runs sqlite-smoke remotely. Kafka/fluvio/nats require pre-provisioned instances
# or will be skipped when INSTANCES_ENV is unset for those stacks.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SKIP_TEARDOWN="${1:-}"

run_sqlite_smoke() {
  echo "=== SQLite smoke (provision + remote run) ==="
  cd "$REPO/infra/aws/sqlite-smoke"
  chmod +x provision.sh bootstrap.sh scripts/*.sh
  ./provision.sh
  export INSTANCES_ENV="$PWD/instances.env"
  ./bootstrap.sh
  ./scripts/run-remote-smoke.sh
  if [[ "$SKIP_TEARDOWN" != "--skip-teardown" ]]; then
    ./scripts/teardown.sh
  fi
}

run_if_env() {
  local label="$1"
  local env_file="$2"
  local script="$3"
  if [[ -f "$env_file" ]]; then
    echo "=== ${label} (existing ${env_file}) ==="
    export INSTANCES_ENV="$env_file"
    bash "$script"
  else
    echo "=== ${label}: skip (no ${env_file}) ==="
  fi
}

cd "$REPO"
run_sqlite_smoke

run_if_env "Kafka smoke" \
  "$REPO/infra/aws/kafka-smoke/instances.env" \
  "$REPO/infra/aws/kafka-smoke/scripts/run-remote-smoke.sh"

run_if_env "Fluvio smoke" \
  "$REPO/infra/aws/fluvio-smoke/instances.env" \
  "$REPO/infra/aws/fluvio-smoke/scripts/run-remote-smoke.sh"

run_if_env "NATS broker-fleet e2e" \
  "$REPO/infra/aws/broker-fleet/instances.env" \
  "$REPO/infra/aws/broker-fleet/scripts/run-e2e-validation-aws.sh"

echo "All requested AWS E2E gates complete."
