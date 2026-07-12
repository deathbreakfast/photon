#!/usr/bin/env bash
# PFH firehose sweep (local/WSL smoke subset). Full N=1/2/4 on AWS via run-pfh-sweep-aws.sh.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../.." && pwd)"
REPORTS="${REPO}/profiling/photon-bench/reports"
NATS_BENCH="${REPO}/profiling/nats-bench"
mkdir -p "$REPORTS" "$NATS_BENCH"

SMOKE="${PHOTON_BENCH_PFH_SMOKE:-1}"
HARDWARE="${PHOTON_BENCH_HARDWARE:-dev-wsl}"

run_nats_bench_baseline() {
  local n="$1"
  local url="${PHOTON_NATS_URL:-nats://127.0.0.1:4222}"
  if ! command -v nats >/dev/null 2>&1; then
    echo "nats CLI not installed; skipping raw bench baseline for N=${n}"
    return 0
  fi
  echo "nats bench pub baseline N=${n} ..."
  local out="${NATS_BENCH}/n${n}-raw-pub.txt"
  nats bench pub "photon.firehose.raw" --server "$url" --size 512 --msgs 50000 --pub 32 2>&1 | tee "$out" || true
  if grep -qE '[0-9,]+ msgs/sec' "$out"; then
    local peak
    peak="$(grep -oE '[0-9,]+ msgs/sec' "$out" | tail -1 | tr -d ', msgs/sec')"
    echo "{\"broker_nodes\":${n},\"peak_ops_per_sec\":${peak}}" > "${NATS_BENCH}/n${n}-raw-pub.json"
    export PHOTON_BENCH_NATS_BENCH_PEAK="$peak"
  fi
}

run_pfh_cell() {
  local cursor="$1"
  local ack="$2"
  local pubs="$3"
  local rate="$4"
  local nodes="$5"
  export PHOTON_NATS_REPLAY_CURSOR="$cursor"
  export PHOTON_NATS_SYNC_ACK="$ack"
  export PHOTON_NATS_MAX_INFLIGHT="${PHOTON_NATS_MAX_INFLIGHT:-256}"
  export PHOTON_BENCH_CRYPTO=0
  export PHOTON_BENCH_PUBLISHERS="$pubs"
  export PHOTON_BENCH_NODES="$nodes"
  local tag="bm-pfh-nats-n${nodes}-${cursor}-ack${ack}-p${pubs}-r${rate}"
  cd "$REPO"
  cargo run -p photon-bench --features nats -- run \
    --experiment bm-pfh --storage nats --topology broker-cluster --telemetry off \
    --nodes "$nodes" --publishers "$pubs" --ops "$rate" --hardware "$HARDWARE" \
    --report "$REPORTS/${tag}.json" || true
}

"$ROOT/scripts/down.sh" --wipe 2>/dev/null || "$ROOT/scripts/down.sh" || true
"$ROOT/scripts/single-node.sh"

export PHOTON_NATS_URL="${PHOTON_NATS_URL:-nats://127.0.0.1:4222}"
export PHOTON_NATS_STREAM=photon
export PHOTON_NATS_RETENTION=15m
export PHOTON_NATS_REPLICAS=1

run_nats_bench_baseline 1

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
        run_pfh_cell "$cursor" "$ack" "$pubs" "$rate" 1
      done
    done
  done
done

cd "$REPO"
cargo run -p photon-bench --features nats -- scaling-curve \
  --hardware "$HARDWARE" --storage nats \
  --reports-dir "$REPORTS" \
  --out "$REPORTS/scaling-curve-${HARDWARE}-nats-firehose.json" || true

echo "PFH sweep complete. Reports in $REPORTS"
