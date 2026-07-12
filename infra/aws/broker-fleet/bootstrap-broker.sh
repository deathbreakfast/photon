#!/usr/bin/env bash
# Run on a broker EC2 host (via SSH). Installs Docker and starts NATS JetStream.
set -euo pipefail

ROLE="${1:?usage: bootstrap-broker.sh <standalone|cluster> <server_name> [peer_ip...]}"
SERVER_NAME="${2:?usage: bootstrap-broker.sh <standalone|cluster> <server_name> [peer_ip...]}"
shift 2
PEERS=("$@")

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STORE_DIR="/data/jetstream"
CONF_DIR="/etc/photon-nats"
CONTAINER="photon-nats"

if ! command -v docker >/dev/null 2>&1; then
  sudo apt-get update -qq
  sudo apt-get install -y -qq docker.io curl
  sudo systemctl enable --now docker
  sudo usermod -aG docker "$USER" || true
fi

sudo mkdir -p "$STORE_DIR" "$CONF_DIR"
sudo chown -R "$USER:$USER" /data

render_config() {
  local template="$1"
  python3 - "$template" "$SERVER_NAME" "$STORE_DIR" "${PEERS[@]}" <<'PY'
import sys
from pathlib import Path

template = Path(sys.argv[1])
server_name = sys.argv[2]
store_dir = sys.argv[3]
peers = sys.argv[4:]
text = template.read_text()
text = text.replace("@SERVER_NAME@", server_name)
text = text.replace("@STORE_DIR@", store_dir)
if "@CLUSTER_ROUTES@" in text:
    routes = "\n".join(f"    nats://{ip}:6222" for ip in peers)
    text = text.replace("@CLUSTER_ROUTES@", routes)
print(text)
PY
}

if [[ "$ROLE" == "standalone" ]]; then
  render_config "$ROOT/config/nats-standalone.conf.tpl" | sudo tee "$CONF_DIR/nats.conf" >/dev/null
else
  render_config "$ROOT/config/nats-node.conf.tpl" | sudo tee "$CONF_DIR/nats.conf" >/dev/null
fi

sudo docker rm -f "$CONTAINER" 2>/dev/null || true
sudo docker run -d --name "$CONTAINER" --restart unless-stopped \
  -p 4222:4222 -p 6222:6222 -p 8222:8222 \
  -v "$STORE_DIR:$STORE_DIR" \
  -v "$CONF_DIR/nats.conf:/etc/nats/nats.conf:ro" \
  nats:2.10 -c /etc/nats/nats.conf

echo "Waiting for NATS..."
for _ in $(seq 1 30); do
  if curl -sf "http://127.0.0.1:8222/varz" >/dev/null 2>&1; then
    if [[ "$ROLE" == "standalone" ]]; then
      curl -sf "http://127.0.0.1:8222/healthz" >/dev/null 2>&1 || true
    fi
    echo "NATS broker ${SERVER_NAME} is up."
    exit 0
  fi
  sleep 1
done
echo "NATS health check failed on ${SERVER_NAME}" >&2
exit 1
