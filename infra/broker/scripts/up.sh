#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

docker stop photon-nats 2>/dev/null || true
docker compose --profile cluster down --remove-orphans 2>/dev/null || true
docker compose --profile single down --remove-orphans 2>/dev/null || true

docker compose --profile cluster up -d

echo "Waiting for NATS cluster health..."
for port in 8222 8225 8224; do
  for _ in $(seq 1 60); do
    if curl -sf "http://127.0.0.1:${port}/healthz" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  curl -sf "http://127.0.0.1:${port}/healthz" >/dev/null
done

# Allow RAFT cluster routes to settle.
sleep 3

echo "NATS 3-node cluster is up."
