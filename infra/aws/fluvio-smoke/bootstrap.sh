#!/usr/bin/env bash
# Bootstrap Fluvio smoke EC2: Docker, Fluvio SC+SPU, Rust toolchain.
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
sudo apt-get install -y -qq docker.io docker-compose-v2 curl pkg-config libssl-dev build-essential netcat-openbsd unzip
sudo usermod -aG docker "$USER" || true
sudo systemctl enable --now docker

if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
fi
source "$HOME/.cargo/env" || true

WORKDIR=/tmp/photon-fluvio-lab
mkdir -p "$WORKDIR"
cat >"$WORKDIR/docker-compose.yml" <<'COMPOSE'
services:
  sc:
    image: infinyon/fluvio:stable
    container_name: photon-fluvio-sc
    hostname: sc
    ports:
      - "9103:9003"
    environment:
      - RUST_LOG=info
    command: "./fluvio-run sc --local /fluvio/metadata"
    volumes:
      - fluvio-metadata:/fluvio/metadata
  spu:
    image: infinyon/fluvio:stable
    container_name: photon-fluvio-spu
    hostname: spu
    ports:
      - "9110:9010"
      - "9111:9011"
    environment:
      - RUST_LOG=info
    command: "./fluvio-run spu -i 5001 -p spu:9010 -v spu:9011 --sc-addr sc:9004 --log-base-dir /fluvio/data"
    volumes:
      - fluvio-data:/fluvio/data
    depends_on:
      - sc
volumes:
  fluvio-metadata:
  fluvio-data:
COMPOSE

cd "$WORKDIR"
sudo docker compose down --remove-orphans 2>/dev/null || true
sudo docker compose up -d

for _ in $(seq 1 90); do
  if nc -z 127.0.0.1 9103 2>/dev/null; then
    break
  fi
  sleep 2
done

if ! nc -z 127.0.0.1 9103 2>/dev/null; then
  echo "Fluvio SC health check failed" >&2
  exit 1
fi

sleep 3
if ! command -v fluvio >/dev/null 2>&1; then
  curl -fsS https://raw.githubusercontent.com/fluvio-community/fluvio/master/install.sh | FVM_VERSION=dev bash
fi
source "$HOME/.fvm/env" 2>/dev/null || true
export PATH="$HOME/.fluvio/bin:$HOME/.fvm/bin:$PATH"
fluvio profile add docker 127.0.0.1:9103 docker 2>/dev/null || true
fluvio cluster spu register --id 5001 -p 127.0.0.1:9110 --private-server 127.0.0.1:9111 \
  || fluvio cluster spu list | grep -q 5001

for _ in $(seq 1 30); do
  if nc -z 127.0.0.1 9103 2>/dev/null; then
    echo "Fluvio ready on localhost:9103"
    exit 0
  fi
  sleep 2
done
echo "Fluvio bootstrap failed" >&2
exit 1
REMOTE

echo "Bootstrap complete."
