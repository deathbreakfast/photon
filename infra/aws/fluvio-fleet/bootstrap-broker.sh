#!/usr/bin/env bash
# Run on a Fluvio broker EC2 host. Roles: sc | spu | standalone
set -euo pipefail

ROLE="${1:?usage: bootstrap-broker.sh <sc|spu|standalone> [args...]}"

if ! command -v docker >/dev/null 2>&1; then
  sudo apt-get update -qq
  sudo apt-get install -y -qq docker.io curl netcat-openbsd
  sudo systemctl enable --now docker
  sudo usermod -aG docker "$USER" || true
fi

ensure_fluvio_cli() {
  if command -v fluvio >/dev/null 2>&1; then
    return 0
  fi
  sudo apt-get install -y -qq unzip curl
  curl -fsS https://raw.githubusercontent.com/fluvio-community/fluvio/master/install.sh | FVM_VERSION=dev bash
  # shellcheck disable=SC1091
  source "$HOME/.fvm/env" 2>/dev/null || true
  export PATH="${HOME}/.fluvio/bin:${HOME}/.fvm/bin:${PATH}"
}

start_sc() {
  local sc_ip="${1:?sc_ip required}"
  sudo docker rm -f photon-fluvio-sc photon-fluvio-spu-5001 2>/dev/null || true
  sudo docker run -d --name photon-fluvio-sc --restart unless-stopped \
    -p 9103:9003 -p 9004:9004 \
    -v photon-fluvio-metadata:/fluvio/metadata \
    infinyon/fluvio:stable \
    ./fluvio-run sc --local /fluvio/metadata
  for _ in $(seq 1 90); do
    if nc -z 127.0.0.1 9103 2>/dev/null; then
      echo "Fluvio SC up on ${sc_ip}:9103"
      return 0
    fi
    sleep 2
  done
  echo "Fluvio SC health check failed" >&2
  exit 1
}

start_spu() {
  local spu_id="$1"
  local host_ip="$2"
  local sc_ip="$3"
  local pub_port="$4"
  local priv_port="$5"
  sudo docker rm -f "photon-fluvio-spu-${spu_id}" 2>/dev/null || true
  sudo docker run -d --name "photon-fluvio-spu-${spu_id}" --restart unless-stopped \
    -p "${pub_port}:9010" -p "${priv_port}:9011" \
    -v "photon-fluvio-data-${spu_id}:/fluvio/data" \
    infinyon/fluvio:stable \
    ./fluvio-run spu -i "${spu_id}" -p "0.0.0.0:9010" -v "0.0.0.0:9011" \
      --sc-addr "${sc_ip}:9004" --log-base-dir /fluvio/data
  sleep 5
  ensure_fluvio_cli
  fluvio profile add fleet "${sc_ip}:9103" docker 2>/dev/null || fluvio profile delete fleet 2>/dev/null || true
  fluvio profile add fleet "${sc_ip}:9103" docker
  fluvio cluster spu register --id "${spu_id}" -p "${host_ip}:${pub_port}" --private-server "${host_ip}:${priv_port}" \
    || fluvio cluster spu list | grep -q "${spu_id}"
  echo "Fluvio SPU ${spu_id} registered on ${host_ip}"
}

case "$ROLE" in
  sc)
    start_sc "${2:?sc_ip}"
    ;;
  spu)
    start_spu "${2:?spu_id}" "${3:?host_ip}" "${4:?sc_ip}" "${5:-9110}" "${6:-9111}"
    ;;
  standalone)
    HOST_IP="${2:?host_ip}"
    start_sc "$HOST_IP"
    start_spu 5001 "$HOST_IP" "$HOST_IP" 9110 9111
    ;;
  *)
    echo "unknown role: $ROLE" >&2
    exit 1
    ;;
esac
