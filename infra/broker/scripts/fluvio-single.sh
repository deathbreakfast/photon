#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

docker compose -f docker-compose.fluvio.yml down --remove-orphans 2>/dev/null || true
docker compose -f docker-compose.fluvio.yml up -d

echo "Waiting for single-node Fluvio SC..."
for _ in $(seq 1 90); do
  if command -v nc >/dev/null 2>&1 && nc -z 127.0.0.1 9103 2>/dev/null; then
    break
  fi
  sleep 2
done

if ! nc -z 127.0.0.1 9103 2>/dev/null; then
  echo "Fluvio SC health check failed" >&2
  exit 1
fi

sleep 3
chmod +x scripts/fluvio-register-spu.sh
./scripts/fluvio-register-spu.sh

echo "Fluvio single-node lab is up on 127.0.0.1:9103"