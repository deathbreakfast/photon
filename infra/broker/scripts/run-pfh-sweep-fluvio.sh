#!/usr/bin/env bash
# PFH firehose sweep (local/WSL smoke) for Fluvio single-node lab.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../.." && pwd)"
REPORTS="${REPO}/profiling/photon-bench/reports"
FLUVIO_BENCH="${REPO}/profiling/fluvio-bench"
mkdir -p "$REPORTS" "$FLUVIO_BENCH"

SMOKE="${PHOTON_BENCH_PFH_SMOKE:-1}"
HARDWARE="${PHOTON_BENCH_HARDWARE:-dev-wsl}"
STORAGE="fluvio"
FEATURES="fluvio"

run_fluvio_bench_baseline() {
  local n="$1"
  echo "Fluvio raw publish baseline not automated for N=${n}; skip"
}

run_pfh_cell() {
  local cursor="$1"
  local ack="$2"
  local pubs="$3"
  local rate="$4"
  local nodes="$5"
  local shards="${6:-1}"
  export PHOTON_FLUVIO_REPLAY_CURSOR="$cursor"
  export PHOTON_FLUVIO_SYNC_ACK="$ack"
  export PHOTON_FLUVIO_MAX_INFLIGHT="${PHOTON_FLUVIO_MAX_INFLIGHT:-256}"
  export PHOTON_FLUVIO_TOPIC_SHARDS="$shards"
  export PHOTON_BENCH_TOPIC_SHARDS="$shards"
  export PHOTON_BENCH_CRYPTO=0
  export PHOTON_BENCH_PUBLISHERS="$pubs"
  export PHOTON_BENCH_NODES="$nodes"
  local tag="bm-pfh-${STORAGE}-n${nodes}-sh${shards}-${cursor}-ack${ack}-p${pubs}-r${rate}"
  cd "$REPO"
  cargo run -p photon-bench --features "$FEATURES" -- run \
    --experiment bm-pfh --storage "$STORAGE" --topology broker-cluster --telemetry off \
    --nodes "$nodes" --publishers "$pubs" --ops "$rate" --hardware "$HARDWARE" \
    --report "$REPORTS/${tag}.json" || true
}

"$ROOT/scripts/fluvio-single.sh"
# shellcheck disable=SC1091
source "$ROOT/scripts/export-fluvio-env.sh"
export PHOTON_FLUVIO_MAX_INFLIGHT="${PHOTON_FLUVIO_MAX_INFLIGHT:-256}"
export PHOTON_FLUVIO_REPLICAS=1

run_fluvio_bench_baseline 1

if [[ "$SMOKE" == "1" ]]; then
  RATES=(1000 10000)
  PUBS=(8 32)
  CURSORS=(tail_only)
  ACKS=(0)
else
  RATES=(1000 10000 50000 100000)
  PUBS=(8 32 128 256)
  CURSORS=(stream_seq tail_only)
  ACKS=(1 0)
fi

for cursor in "${CURSORS[@]}"; do
  for ack in "${ACKS[@]}"; do
    if [[ "$cursor" == "stream_seq" && "$ack" == "0" ]]; then
      continue
    fi
    for pubs in "${PUBS[@]}"; do
      for rate in "${RATES[@]}"; do
        run_pfh_cell "$cursor" "$ack" "$pubs" "$rate" 1 1
      done
    done
  done
done

cd "$REPO"
cargo run -p photon-bench --features "$FEATURES" -- scaling-curve \
  --hardware "$HARDWARE" --storage "$STORAGE" \
  --reports-dir "$REPORTS" \
  --out "$REPORTS/scaling-curve-${HARDWARE}-${STORAGE}-firehose.json" || true

echo "Fluvio PFH sweep complete. Reports in $REPORTS"
