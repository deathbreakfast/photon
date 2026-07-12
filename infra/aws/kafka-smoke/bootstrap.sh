#!/usr/bin/env bash
# Bootstrap Kafka smoke EC2: Docker, Kafka container, Rust toolchain.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${INSTANCES_ENV:-$ROOT/instances.env}"
# shellcheck disable=SC1091
source "$ENV_FILE"

HOST="${SMOKE_PUBLIC_IP:-$SMOKE_IP}"
SSH_KEY="${SSH_KEY_PATH:-$HOME/.ssh/${AWS_KEY_NAME:-}.pem}"
SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY")

echo "Bootstrapping ${SSH_USER}@${HOST}..."
for _ in $(seq 1 30); do
  if ssh "${SSH_OPTS[@]}" "${SSH_USER}@${HOST}" "echo ok" >/dev/null 2>&1; then
    break
  fi
  sleep 5
done

ssh "${SSH_OPTS[@]}" "${SSH_USER}@${HOST}" bash -s <<'REMOTE'
set -euo pipefail
sudo apt-get update -qq
sudo apt-get install -y -qq docker.io curl pkg-config libssl-dev build-essential
sudo usermod -aG docker "$USER" || true

if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
fi
source "$HOME/.cargo/env" || true

sudo docker rm -f photon-kafka 2>/dev/null || true
sudo docker run -d --name photon-kafka \
  -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e 'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT' \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e KAFKA_NUM_PARTITIONS=1 \
  apache/kafka:3.8.1

for _ in $(seq 1 90); do
  if command -v nc >/dev/null 2>&1 && nc -z 127.0.0.1 9092 2>/dev/null; then
    echo "Kafka ready on localhost:9092"
    exit 0
  fi
  sleep 2
done
echo "Kafka bootstrap failed" >&2
exit 1
REMOTE

echo "Bootstrap complete."
