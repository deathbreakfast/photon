#!/usr/bin/env bash
# Wait until Kafka accepts TCP connections on localhost:9092.
set -euo pipefail

BROKERS="${PHOTON_KAFKA_BROKERS:-127.0.0.1:9092}"
HOST="${BROKERS%%,*}"
HOST="${HOST%%:*}"
PORT="${BROKERS##*:}"

echo "Waiting for Kafka at ${HOST}:${PORT}..."
for _ in $(seq 1 60); do
  if command -v nc >/dev/null 2>&1 && nc -z "$HOST" "$PORT" 2>/dev/null; then
    echo "Kafka is up."
    exit 0
  fi
  sleep 2
done
echo "Kafka wait timeout" >&2
exit 1
