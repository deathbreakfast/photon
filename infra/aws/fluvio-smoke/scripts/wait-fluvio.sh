#!/usr/bin/env bash
# Wait until Fluvio accepts TCP connections on localhost:9103.
set -euo pipefail

BROKERS="${PHOTON_FLUVIO_ENDPOINT:-127.0.0.1:9103}"
HOST="${BROKERS%%,*}"
HOST="${HOST%%:*}"
PORT="${BROKERS##*:}"

echo "Waiting for Fluvio at ${HOST}:${PORT}..."
for _ in $(seq 1 60); do
  if command -v nc >/dev/null 2>&1 && nc -z "$HOST" "$PORT" 2>/dev/null; then
    echo "Fluvio is up."
    exit 0
  fi
  sleep 2
done
echo "Fluvio wait timeout" >&2
exit 1
