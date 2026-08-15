#!/usr/bin/env bash
# Operator wrapper: PD AWS campaign lives with the NATS fleet scripts.
set -euo pipefail
: "${UF_PRODUCT_ROOT:=$HOME/unified-field}"
CAMPAIGN="${UF_PRODUCT_ROOT}/uf-live-cloud-lab/photon/infra/aws/broker-fleet/scripts/run-pd-campaign-aws.sh"
if [[ ! -x "$CAMPAIGN" && ! -f "$CAMPAIGN" ]]; then
  echo "PD AWS campaign script not found: $CAMPAIGN" >&2
  echo "Set UF_PRODUCT_ROOT to the unified-field checkout." >&2
  exit 1
fi
exec bash "$CAMPAIGN" "$@"
