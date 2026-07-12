#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

docker compose --profile kafka down --remove-orphans 2>/dev/null || true
docker compose --profile kafka up -d

echo "Waiting for single-node Kafka..."
for _ in $(seq 1 90); do
  if command -v nc >/dev/null 2>&1 && nc -z 127.0.0.1 9092 2>/dev/null; then
    echo "Kafka single-node lab is up on 127.0.0.1:9092"
    exit 0
  fi
  sleep 2
done
echo "Kafka single-node health check failed" >&2
exit 1
