#!/usr/bin/env bash
# Tear down SQLite smoke EC2 + security group from instances.env.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${INSTANCES_ENV:-$ROOT/instances.env}"
# shellcheck disable=SC1091
source "$ENV_FILE"

if [[ -n "${INSTANCE_SMOKE:-}" ]]; then
  aws ec2 terminate-instances --region "${AWS_REGION:-us-west-2}" \
    --instance-ids "$INSTANCE_SMOKE" >/dev/null || true
fi
if [[ -n "${SECURITY_GROUP_ID:-}" ]]; then
  sleep 30
  aws ec2 delete-security-group --region "${AWS_REGION:-us-west-2}" \
    --group-id "$SECURITY_GROUP_ID" >/dev/null || true
fi
echo "SQLite smoke teardown requested."
