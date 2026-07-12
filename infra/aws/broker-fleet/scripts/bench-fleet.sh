#!/usr/bin/env bash
# Resolve bench host public/private IP by 1-based index.
set -euo pipefail

bench_public_ip() {
  local idx="$1"
  local key="BENCH_${idx}_PUBLIC_IP"
  echo "${!key}"
}

bench_private_ip() {
  local idx="$1"
  local key="BENCH_${idx}_IP"
  echo "${!key}"
}

resolve_bench_ip() {
  local idx="$1"
  if [[ "${PHOTON_AWS_USE_PUBLIC_IPS:-0}" == "1" ]]; then
    bench_public_ip "$idx"
  else
    bench_private_ip "$idx"
  fi
}
