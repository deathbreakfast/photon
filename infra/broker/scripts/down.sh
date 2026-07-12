#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WIPE="${1:-}"
docker compose --profile cluster down --remove-orphans
docker compose --profile single down --remove-orphans

if [[ "$WIPE" == "--wipe" ]]; then
  docker volume rm -f broker_nats-1-data broker_nats-2-data \
    broker_nats-3-data broker_nats-single-data 2>/dev/null || true
fi

echo "NATS broker lab stopped."
