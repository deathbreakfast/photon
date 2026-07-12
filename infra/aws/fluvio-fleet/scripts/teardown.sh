#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "${INSTANCES_ENV:-$ROOT/instances.env}"

IDS=()
[[ -n "${INSTANCE_BROKER_SINGLE:-}" ]] && IDS+=("$INSTANCE_BROKER_SINGLE")
[[ -n "${INSTANCE_BROKER_1:-}" ]] && IDS+=("$INSTANCE_BROKER_1")
[[ -n "${INSTANCE_BROKER_2:-}" ]] && IDS+=("$INSTANCE_BROKER_2")
[[ -n "${INSTANCE_BROKER_3:-}" ]] && IDS+=("$INSTANCE_BROKER_3")

TEARDOWN_BENCH="${TEARDOWN_BENCH:-1}"
if [[ "$TEARDOWN_BENCH" != "1" ]]; then
  IDS=("${IDS[@]}")
fi

if [[ ${#IDS[@]} -eq 0 && "$TEARDOWN_BENCH" == "1" ]]; then
  if [[ -n "${BENCH_COUNT:-}" ]]; then
    for i in $(seq 1 "$BENCH_COUNT"); do
      key="INSTANCE_BENCH_${i}"
      [[ -n "${!key:-}" && "${!key}" != "local" ]] && IDS+=("${!key}")
    done
  fi
fi

if [[ ${#IDS[@]} -eq 0 ]]; then
  echo "No instance IDs in instances.env" >&2
  exit 1
fi

aws ec2 terminate-instances --region "${AWS_REGION:-us-west-2}" --instance-ids "${IDS[@]}"
echo "Terminated: ${IDS[*]}"

if [[ -n "${SECURITY_GROUP_ID:-}" ]]; then
  sleep 5
  aws ec2 delete-security-group --region "${AWS_REGION:-us-west-2}" --group-id "$SECURITY_GROUP_ID" 2>/dev/null || true
fi
