#!/usr/bin/env bash
# Run on a Kafka broker EC2 host (via SSH). Installs Docker and starts KRaft broker.
set -euo pipefail

ROLE="${1:?usage: bootstrap-broker.sh <standalone|cluster> <node_id> <advertised_ip> [quorum_voters]}"
NODE_ID="${2:?usage: bootstrap-broker.sh <standalone|cluster> <node_id> <advertised_ip> [quorum_voters]}"
ADVERTISED_IP="${3:?usage: bootstrap-broker.sh <standalone|cluster> <node_id> <advertised_ip> [quorum_voters]}"
QUORUM_VOTERS="${4:-${NODE_ID}@${ADVERTISED_IP}:9093}"

CONTAINER="photon-kafka"
DATA_DIR="/data/kafka"

if ! command -v docker >/dev/null 2>&1; then
  sudo apt-get update -qq
  sudo apt-get install -y -qq docker.io curl netcat-openbsd
  sudo systemctl enable --now docker
  sudo usermod -aG docker "$USER" || true
fi

sudo mkdir -p "$DATA_DIR"
sudo chown -R "$USER:$USER" /data

sudo docker rm -f "$CONTAINER" 2>/dev/null || true
sudo docker run -d --name "$CONTAINER" --restart unless-stopped \
  -p 9092:9092 -p 9093:9093 \
  -v "${DATA_DIR}:/var/lib/kafka/data" \
  -e "KAFKA_NODE_ID=${NODE_ID}" \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093" \
  -e "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://${ADVERTISED_IP}:9092" \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e 'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT' \
  -e "KAFKA_CONTROLLER_QUORUM_VOTERS=${QUORUM_VOTERS}" \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e KAFKA_NUM_PARTITIONS=1 \
  -e KAFKA_LOG_DIRS=/var/lib/kafka/data \
  apache/kafka:3.8.1

echo "Waiting for Kafka (${ROLE}, node ${NODE_ID})..."
for _ in $(seq 1 90); do
  if nc -z 127.0.0.1 9092 2>/dev/null; then
    echo "Kafka broker node ${NODE_ID} is up."
    exit 0
  fi
  sleep 2
done
echo "Kafka health check failed on node ${NODE_ID}" >&2
exit 1
