#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

NODE="${1:?usage: kill-node.sh <1|2|3>}"
docker stop "photon-nats-${NODE}" || true
echo "Stopped photon-nats-${NODE}"
