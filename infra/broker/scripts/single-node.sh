#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

docker compose --profile cluster down --remove-orphans 2>/dev/null || true
docker compose --profile single up -d

echo "Waiting for single NATS node..."
for _ in $(seq 1 30); do
  if curl -sf "http://127.0.0.1:8222/healthz" >/dev/null 2>&1; then
    echo "NATS single-node lab is up."
    exit 0
  fi
  sleep 1
done
echo "NATS single-node health check failed" >&2
exit 1
