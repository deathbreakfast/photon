#!/usr/bin/env bash
# PFH firehose sweep (local/WSL smoke) for Kafka single-node lab.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../.." && pwd)"
REPORTS="${REPO}/profiling/photon-bench/reports"
KAFKA_BENCH="${REPO}/profiling/kafka-bench"
mkdir -p "$REPORTS" "$KAFKA_BENCH"

SMOKE="${PHOTON_BENCH_PFH_SMOKE:-1}"
HARDWARE="${PHOTON_BENCH_HARDWARE:-dev-wsl}"
STORAGE="kafka"
FEATURES="kafka"

run_kafka_bench_baseline() {
  local n="$1"
  local brokers="${PHOTON_KAFKA_BROKERS:-127.0.0.1:9092}"
  if ! command -v kafka-producer-perf-test.sh >/dev/null 2>&1; then
    echo "kafka-producer-perf-test.sh not installed; skipping raw bench baseline for N=${n}"
    return 0
  fi
  echo "kafka-producer-perf-test baseline N=${n} ..."
  local out="${KAFKA_BENCH}/n${n}-raw-pub.txt"
  kafka-producer-perf-test.sh \
    --topic photon.firehose.raw \
    --num-records 50000 \
    --record-size 512 \
    --throughput -1 \
    --producer-props bootstrap.servers="${brokers}" \
    2>&1 | tee "$out" || true
  if grep -qE '[0-9,]+ records sent' "$out"; then
    local peak
    peak="$(grep -oE '[0-9,]+\.[0-9]+ records/sec' "$out" | tail -1 | tr -d ', records/sec' | cut -d. -f1)"
    echo "{\"broker_nodes\":${n},\"peak_ops_per_sec\":${peak}}" > "${KAFKA_BENCH}/n${n}-raw-pub.json"
    export PHOTON_BENCH_KAFKA_BENCH_PEAK="$peak"
  fi
}

run_pfh_cell() {
  local cursor="$1"
  local ack="$2"
  local pubs="$3"
  local rate="$4"
  local nodes="$5"
  local shards="${6:-1}"
  export PHOTON_KAFKA_REPLAY_CURSOR="$cursor"
  export PHOTON_KAFKA_SYNC_ACK="$ack"
  export PHOTON_KAFKA_MAX_INFLIGHT="${PHOTON_KAFKA_MAX_INFLIGHT:-256}"
  export PHOTON_KAFKA_TOPIC_SHARDS="$shards"
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

"$ROOT/scripts/kafka-single.sh"
# shellcheck disable=SC1091
source "$ROOT/scripts/export-kafka-env.sh"
export PHOTON_KAFKA_MAX_INFLIGHT="${PHOTON_KAFKA_MAX_INFLIGHT:-256}"
export PHOTON_KAFKA_REPLICAS=1

run_kafka_bench_baseline 1

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

echo "Kafka PFH sweep complete. Reports in $REPORTS"
