#!/usr/bin/env bash
# Temporarily allow operator host to reach NATS client/monitoring ports (for WSL bench runs).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "${INSTANCES_ENV:-$ROOT/instances.env}"

SG="${SECURITY_GROUP_ID:?Set SECURITY_GROUP_ID in instances.env}"
REGION="${AWS_REGION:-us-west-2}"
MY_IP="$(curl -sf https://checkip.amazonaws.com)"

aws ec2 authorize-security-group-ingress --region "$REGION" --group-id "$SG" \
  --ip-permissions \
  "IpProtocol=tcp,FromPort=4222,ToPort=4224,IpRanges=[{CidrIp=${MY_IP}/32,Description=photon-bench-operator}]" \
  "IpProtocol=tcp,FromPort=8222,ToPort=8224,IpRanges=[{CidrIp=${MY_IP}/32,Description=photon-bench-health}]" \
  2>/dev/null || true

echo "Opened NATS ports 4222-4224 and 8222-8224 to ${MY_IP}/32 on ${SG}"
