#!/usr/bin/env bash
# Authoritative PFH firehose sweep on AWS in-VPC for Kafka (N=1,2,4 broker nodes).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/../../.." && pwd)"
REPORTS="${REPO}/profiling/photon-bench/reports"
KAFKA_BENCH="${REPO}/profiling/kafka-bench"
mkdir -p "$REPORTS" "$KAFKA_BENCH"

STORAGE="${STORAGE:-kafka}"
FEATURES="${PHOTON_BENCH_FEATURES:-kafka}"

# shellcheck disable=SC1091
source "${INSTANCES_ENV:-$ROOT/instances.env}"
export PHOTON_AWS_USE_PUBLIC_IPS="${PHOTON_AWS_USE_PUBLIC_IPS:-0}"
export PHOTON_BENCH_CRYPTO=0
export PHOTON_BENCH_RESOURCE_PROFILE=1
export PHOTON_KAFKA_MAX_INFLIGHT="${PHOTON_KAFKA_MAX_INFLIGHT:-256}"
export PHOTON_KAFKA_RETENTION="${PHOTON_KAFKA_RETENTION:-15m}"
export PFH_SWEEP_MODE="${PFH_SWEEP_MODE:-all}"

HW="--hardware ${HARDWARE:-aws-c6i-large}"
TOPO="--topology broker-cluster --storage ${STORAGE} --telemetry off"
BENCH="${PHOTON_BENCH_CMD:-cargo run --release -p photon-bench --features ${FEATURES} --}"

SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY_PATH")
REMOTE_DIR="/tmp/photon-kafka-fleet"

resolve_host() {
  local base="$1"
  if [[ "${PHOTON_AWS_USE_PUBLIC_IPS:-0}" == "1" ]]; then
    local key="${base}_PUBLIC_IP"
    echo "${!key}"
  else
    local key="${base}_IP"
    echo "${!key}"
  fi
}

bootstrap_single() {
  local host
  host="$(resolve_host BROKER_SINGLE)"
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" "mkdir -p ${REMOTE_DIR}"
  scp "${SSH_OPTS[@]}" "$ROOT/bootstrap-broker.sh" "${SSH_USER}@${host}:${REMOTE_DIR}/"
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" \
    "chmod +x ${REMOTE_DIR}/bootstrap-broker.sh && \
     ${REMOTE_DIR}/bootstrap-broker.sh standalone 1 ${BROKER_SINGLE_IP}"
  # shellcheck disable=SC1091
  source "$ROOT/scripts/export-env-aws.sh" single
  export PHOTON_KAFKA_REPLICAS=1
}

bootstrap_for_nodes() {
  local n="$1"
  unset PHOTON_KAFKA_REPLICAS PHOTON_KAFKA_BROKERS || true
  case "$n" in
    1) bootstrap_single ;;
    2) "$ROOT/scripts/bootstrap-n2-cluster.sh"
       # shellcheck disable=SC1091
       source "$ROOT/scripts/export-env-aws.sh" n2
       export PHOTON_KAFKA_REPLICAS=1 ;;
    4) "$ROOT/scripts/bootstrap-n4-cluster.sh"
       # shellcheck disable=SC1091
       source "$ROOT/scripts/export-env-aws.sh" cluster4
       export PHOTON_KAFKA_REPLICAS=1 ;;
    *) echo "unsupported broker node count: $n" >&2; exit 1 ;;
  esac
}

run_kafka_bench_baseline() {
  local n="$1"
  local host
  if [[ "$n" == "1" ]]; then
    host="$(resolve_host BROKER_SINGLE)"
  else
    host="$(resolve_host BROKER_1)"
  fi
  if ! ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" "command -v kafka-producer-perf-test.sh" >/dev/null 2>&1; then
    echo "kafka-producer-perf-test.sh missing on ${host}; skip raw bench baseline for N=${n}"
    unset PHOTON_BENCH_KAFKA_BENCH_PEAK || true
    return 0
  fi
  local out
  out="$(ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" \
    "kafka-producer-perf-test.sh --topic photon.firehose.raw --num-records 100000 --record-size 512 \
     --throughput -1 --producer-props bootstrap.servers=127.0.0.1:9092" 2>&1 || true)"
  echo "$out" | tee "${KAFKA_BENCH}/n${n}-raw-pub-aws.txt"
  if echo "$out" | grep -qE '[0-9,]+\.[0-9]+ records/sec'; then
    local peak
    peak="$(echo "$out" | grep -oE '[0-9,]+\.[0-9]+ records/sec' | tail -1 | tr -d ', records/sec' | cut -d. -f1)"
    echo "{\"broker_nodes\":${n},\"peak_ops_per_sec\":${peak}}" > "${KAFKA_BENCH}/n${n}-raw-pub-aws.json"
    export PHOTON_BENCH_KAFKA_BENCH_PEAK="$peak"
  else
    unset PHOTON_BENCH_KAFKA_BENCH_PEAK || true
  fi
}

sample_broker_resource() {
  local n="$1"
  local host
  if [[ "$n" == "1" ]]; then
    host="$(resolve_host BROKER_SINGLE)"
  else
    host="$(resolve_host BROKER_1)"
  fi
  local stats
  stats="$(ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" \
    "sudo docker stats photon-kafka --no-stream --format '{{.CPUPerc}},{{.MemUsage}}'" 2>/dev/null || true)"
  export PHOTON_BENCH_BROKER_RESOURCE_JSON="$(
    STATS="$stats" python3 - <<'PY'
import json, os
stats = os.environ.get("STATS", "")
cpu, mem = ("", "")
if stats and "," in stats:
    cpu, mem = stats.split(",", 1)
print(json.dumps({
    "docker_cpu_percent": cpu.strip(),
    "docker_mem_usage": mem.strip(),
}))
PY
  )"
}

run_pfh() {
  local cursor="$1"
  local ack="$2"
  local pubs="$3"
  local rate="$4"
  local nodes="$5"
  local peak_cell="${6:-0}"
  local topic_shards="${7:-1}"
  export PHOTON_KAFKA_REPLAY_CURSOR="$cursor"
  export PHOTON_KAFKA_SYNC_ACK="$ack"
  export PHOTON_KAFKA_TOPIC_SHARDS="$topic_shards"
  export PHOTON_BENCH_TOPIC_SHARDS="$topic_shards"
  export PHOTON_BENCH_PUBLISHERS="$pubs"
  export PHOTON_BENCH_NODES="$nodes"
  if [[ "$peak_cell" == "1" ]]; then
    sample_broker_resource "$nodes" || true
  else
    unset PHOTON_BENCH_BROKER_RESOURCE_JSON || true
  fi
  local tag="bm-pfh-${STORAGE}-n${nodes}-sh${topic_shards}-${cursor}-ack${ack}-p${pubs}-r${rate}-aws"
  cd "$REPO"
  $BENCH run --experiment bm-pfh $TOPO $HW --nodes "$nodes" --publishers "$pubs" \
    --ops "$rate" --report "$REPORTS/${tag}.json"
}

sweep_nodes() {
  local nodes="$1"
  local topic_shards="$2"
  echo "=== PFH sweep ${STORAGE} N=${nodes} topic_shards=${topic_shards} ==="
  bootstrap_for_nodes "$nodes"
  run_kafka_bench_baseline "$nodes"

  if [[ "${PFH_PRIMARY_ONLY:-0}" == "1" || "${PFH_SWEEP_MODE:-all}" == "phase3" ]]; then
    run_pfh stream_seq 1 256 100000 "$nodes" 1 "$topic_shards"
    return
  fi

  for cursor in stream_seq tail_only; do
    for ack in 0 1; do
      if [[ "$cursor" == "stream_seq" && "$ack" == "0" ]]; then
        continue
      fi
      for pubs in 32 128; do
        for rate in 10000 50000; do
          run_pfh "$cursor" "$ack" "$pubs" "$rate" "$nodes" 0 "$topic_shards"
        done
      done
    done
  done

  for cursor in stream_seq tail_only; do
    run_pfh "$cursor" 1 256 100000 "$nodes" 1 "$topic_shards"
  done
}

cd "$REPO"
case "${PFH_SWEEP_MODE:-all}" in
  baseline)
    for n in 1 2 4; do
      sweep_nodes "$n" 1
    done
    CURVE_ARGS=(--stream-shards 1)
    OUT_SUFFIX=""
    ;;
  sharded|phase3)
    for n in 1 2 4; do
      sweep_nodes "$n" "$n"
    done
    CURVE_ARGS=(--match-broker-nodes)
    OUT_SUFFIX="-sharded"
    ;;
  all)
    for n in 1 2 4; do
      sweep_nodes "$n" 1
    done
    for n in 1 2 4; do
      sweep_nodes "$n" "$n"
    done
    CURVE_ARGS=(--stream-shards 1)
    OUT_SUFFIX=""
    ;;
  *)
    echo "unknown PFH_SWEEP_MODE=${PFH_SWEEP_MODE}" >&2
    exit 1
    ;;
esac

$BENCH scaling-curve --hardware "${HARDWARE:-aws-c6i-large}" --storage "${STORAGE}" \
  --reports-dir "$REPORTS" \
  "${CURVE_ARGS[@]}" \
  --out "$REPORTS/scaling-curve-${HARDWARE:-aws-c6i-large}-${STORAGE}-firehose${OUT_SUFFIX}.json"

echo "PFH AWS sweep complete (${STORAGE})."
