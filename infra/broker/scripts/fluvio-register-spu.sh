#!/usr/bin/env bash
# Register SPU 5001 with the local Fluvio SC (requires fluvio CLI on PATH).
set -euo pipefail

ensure_fluvio_cli() {
  if command -v fluvio >/dev/null 2>&1; then
    return 0
  fi
  echo "Installing Fluvio CLI..."
  # GitHub release lookups flake under Actions rate limits; retry a few times.
  local attempt
  for attempt in 1 2 3 4 5; do
    if curl -fsS https://raw.githubusercontent.com/fluvio-community/fluvio/master/install.sh \
      | FVM_VERSION=dev bash; then
      # shellcheck disable=SC1091
      source "$HOME/.fvm/env" 2>/dev/null || true
      export PATH="${HOME}/.fluvio/bin:${HOME}/.fvm/bin:${PATH}"
      if command -v fluvio >/dev/null 2>&1; then
        return 0
      fi
    fi
    echo "Fluvio CLI install attempt ${attempt} failed; retrying..." >&2
    sleep $((attempt * 3))
  done
  echo "Fluvio CLI install failed after retries" >&2
  return 1
}

register_spu() {
  ensure_fluvio_cli
  fluvio profile add docker 127.0.0.1:9103 docker 2>/dev/null || fluvio profile delete docker 2>/dev/null || true
  fluvio profile add docker 127.0.0.1:9103 docker
  fluvio cluster spu register --id 5001 -p 127.0.0.1:9110 --private-server 127.0.0.1:9111 \
    || fluvio cluster spu list | grep -q 5001
}

register_spu
